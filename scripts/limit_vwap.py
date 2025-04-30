import logging
import math
import os
from decimal import Decimal
# Import Optional
from typing import Dict, Optional

from pydantic import Field

from hummingbot.client.config.config_data_types import BaseClientModel, ClientFieldData
from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.connector.utils import split_hb_trading_pair
from hummingbot.core.data_type.order_candidate import OrderCandidate
from hummingbot.core.event.events import OrderFilledEvent, OrderType, TradeType
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase


class VWAPConfig(BaseClientModel):
    """
    Configuration parameters for the Limit VWAP strategy.
    """
    # Update script_file_name default
    script_file_name: str = Field(default_factory=lambda: os.path.basename(__file__))
    connector_name: str = Field("binance_paper_trade", client_data=ClientFieldData(
        prompt_on_new=True, prompt=lambda mi: "Exchange where the bot will place orders"))
    trading_pair: str = Field("ETH-USDT", client_data=ClientFieldData(
        prompt_on_new=True, prompt=lambda mi: "Trading pair where the bot will place orders"))
    is_buy: bool = Field(True, client_data=ClientFieldData(
        prompt_on_new=True, prompt=lambda mi: "Buying or selling the base asset? (True for buy, False for sell)"))
    total_volume_quote: Decimal = Field(1000, client_data=ClientFieldData(
        prompt_on_new=True, prompt=lambda mi: "Total amount to buy/sell (in quote asset)"))
    price_spread: float = Field(0.001, client_data=ClientFieldData(
        prompt_on_new=True, prompt=lambda mi: "Spread used to calculate the order price"))
    volume_perc: float = Field(0.001, client_data=ClientFieldData(
        prompt_on_new=True, prompt=lambda mi: "Maximum percentage of the order book volume to buy/sell"))
    order_delay_time: int = Field(10, client_data=ClientFieldData(
        prompt_on_new=True, prompt=lambda mi: "Delay time between orders (in seconds)"))
    # Add optional price limits
    buy_price_limit: Optional[Decimal] = Field(default=None, client_data=ClientFieldData(
        prompt=lambda mi: "Optional limit price below which to buy", is_secure=False, is_connect_key=False))
    sell_price_limit: Optional[Decimal] = Field(default=None, client_data=ClientFieldData(
        prompt=lambda mi: "Optional limit price above which to sell", is_secure=False, is_connect_key=False))


# Rename class to LimitVWAP
class LimitVWAP(ScriptStrategyBase):
    """
    VWAP strategy that includes optional price limits for buying and selling.
    Based on the simple VWAP example.
    """

    @classmethod
    def init_markets(cls, config: VWAPConfig):
        cls.markets = {config.connector_name: {config.trading_pair}}

    def __init__(self, connectors: Dict[str, ConnectorBase], config: VWAPConfig):
        super().__init__(connectors)
        self.config = config
        self.initialized = False
        # Add new limits to self.vwap dictionary
        self.vwap: Dict = {"connector_name": self.config.connector_name,
                           "trading_pair": self.config.trading_pair,
                           "is_buy": self.config.is_buy,
                           "total_volume_quote": self.config.total_volume_quote,
                           "price_spread": self.config.price_spread,
                           "volume_perc": self.config.volume_perc,
                           "order_delay_time": self.config.order_delay_time,
                           "buy_price_limit": self.config.buy_price_limit,
                           "sell_price_limit": self.config.sell_price_limit}
        self.last_ordered_ts = 0

    def on_tick(self):
        """
         Every order delay time the strategy will attempt to buy or sell the base asset.
         It checks price limits, computes cumulative order book volume, and buys/sells a percentage.
         """
        # Use self.vwap dictionary for configuration access
        if self.last_ordered_ts < (self.current_timestamp - self.vwap["order_delay_time"]):
            if self.vwap.get("status") is None:
                self.init_vwap_stats()
            elif self.vwap.get("status") == "ACTIVE":
                # Create order candidate (might be None if limits are hit)
                vwap_order: Optional[OrderCandidate] = self.create_order()

                if vwap_order is None:
                    # Price limit was hit or other issue in create_order.
                    # Log message handled within create_order.
                    # Reset timestamp to check again on the next tick after delay.
                    self.last_ordered_ts = self.current_timestamp
                    return  # Skip placing order this tick

                # Proceed only if vwap_order is a valid OrderCandidate
                vwap_order_adjusted = self.vwap["connector"].budget_checker.adjust_candidate(vwap_order, all_or_none=False)

                if math.isclose(vwap_order_adjusted.amount, Decimal("0"), rel_tol=1E-5):
                    self.logger().info(f"Order adjusted amount: {vwap_order_adjusted.amount}, too low to place an order")
                    # Update timestamp even if order is too small, to respect delay
                    self.last_ordered_ts = self.current_timestamp
                else:
                    self.place_order(
                        connector_name=self.vwap["connector_name"],
                        trading_pair=self.vwap["trading_pair"],
                        is_buy=self.vwap["is_buy"],  # Use the original is_buy flag
                        amount=vwap_order_adjusted.amount,
                        order_type=vwap_order_adjusted.order_type,
                        price=vwap_order_adjusted.price  # Use price from adjusted candidate
                    )
                    # Reset timestamp only after placing an order
                    self.last_ordered_ts = self.current_timestamp


    def init_vwap_stats(self):
        # General parameters
        # No changes needed here for limits, as they are already in self.vwap
        vwap = self.vwap.copy()
        vwap["connector"] = self.connectors[vwap["connector_name"]]
        vwap["delta"] = 0
        vwap["trades"] = []
        vwap["status"] = "ACTIVE"
        vwap["trade_type"] = TradeType.BUY if self.vwap["is_buy"] else TradeType.SELL
        vwap["start_price"] = vwap["connector"].get_price(vwap["trading_pair"], vwap["is_buy"])
        # Ensure start_price is not zero before division
        if vwap["start_price"] == Decimal("0"):
             self.logger().error("Start price is zero, cannot calculate target base volume.")
             # Handle error appropriately - maybe stop the strategy or wait
             vwap["status"] = "ERROR" # Mark status as error
             self.vwap = vwap
             return

        vwap["target_base_volume"] = vwap["total_volume_quote"] / vwap["start_price"]

        # Compute market order scenario
        orderbook_query = vwap["connector"].get_quote_volume_for_base_amount(vwap["trading_pair"], vwap["is_buy"],
                                                                             vwap["target_base_volume"])
        vwap["market_order_base_volume"] = orderbook_query.query_volume
        vwap["market_order_quote_volume"] = orderbook_query.result_volume
        vwap["volume_remaining"] = vwap["target_base_volume"]
        vwap["real_quote_volume"] = Decimal(0)
        self.vwap = vwap

    # Update return type hint and logic
    def create_order(self) -> Optional[OrderCandidate]:
        """
         Retrieves the cumulative volume of the order book until the price spread is reached,
         checks against price limits, then takes a percentage of that volume to use as order amount.
         Returns None if price limits are not met or other checks fail.
         """
        connector = self.vwap["connector"]
        trading_pair = self.vwap["trading_pair"]
        is_buy = self.vwap["is_buy"]

        # Compute the potential execution price based on spread
        try:
            mid_price = connector.get_mid_price(trading_pair)
            if not isinstance(mid_price, Decimal) or mid_price <= 0:
                 self.logger().warning(f"Invalid mid price ({mid_price}) for {trading_pair}. Skipping order.")
                 return None
        except Exception as e:
            self.logger().error(f"Error getting mid price for {trading_pair}: {e}. Skipping order.")
            return None

        price_multiplier = Decimal(1 + self.vwap["price_spread"] if is_buy else 1 - self.vwap["price_spread"])
        potential_exec_price = mid_price * price_multiplier

        # --- Add Limit Check ---
        if is_buy:
            buy_limit = self.vwap.get("buy_price_limit")
            if buy_limit is not None and potential_exec_price > buy_limit:
                self.logger().info(f"Potential buy price {potential_exec_price:.6f} is above limit {buy_limit:.6f}. Skipping order.")
                return None  # Don't create order
        else:  # is_sell
            sell_limit = self.vwap.get("sell_price_limit")
            if sell_limit is not None and potential_exec_price < sell_limit:
                self.logger().info(f"Potential sell price {potential_exec_price:.6f} is below limit {sell_limit:.6f}. Skipping order.")
                return None  # Don't create order
        # --- End Limit Check ---

        # Query the cumulative volume up to the potential execution price
        try:
            orderbook_query = connector.get_volume_for_price(
                trading_pair=trading_pair,
                is_buy=is_buy,
                price=float(potential_exec_price)) # Connector methods might still expect float
            volume_at_price = orderbook_query.result_volume
            if not isinstance(volume_at_price, Decimal) or volume_at_price < 0:
                self.logger().warning(f"Invalid volume ({volume_at_price}) returned for price query. Skipping order.")
                return None
        except Exception as e:
             self.logger().error(f"Error getting volume for price for {trading_pair}: {e}. Skipping order.")
             return None


        # Calculate desired amount based on percentage of available volume and remaining target volume
        desired_amount = min(volume_at_price * Decimal(self.vwap["volume_perc"]), Decimal(self.vwap["volume_remaining"]))

        # Quantize the order amount and price
        try:
            quantized_amount = connector.quantize_order_amount(trading_pair, desired_amount)
            # Use the potential_exec_price for quantization as it respects spread/limits
            quantized_price = connector.quantize_order_price(trading_pair, potential_exec_price)

            if quantized_amount <= 0:
                # This can happen if desired_amount is very small
                self.logger().info(f"Desired amount {desired_amount} resulted in zero quantized amount. Skipping order.")
                return None

        except Exception as e:
            self.logger().error(f"Error quantizing order for {trading_pair}: {e}. Skipping order.")
            return None


        # Create the Order Candidate
        vwap_order = OrderCandidate(
            trading_pair=trading_pair,
            is_maker=False,
            order_type=OrderType.MARKET, # Market order to execute near the calculated price
            order_side=self.vwap["trade_type"],
            amount=quantized_amount,
            price=quantized_price) # Use the quantized price derived from spread/limit logic
        return vwap_order

    def place_order(self,
                    connector_name: str,
                    trading_pair: str,
                    is_buy: bool,
                    amount: Decimal,
                    order_type: OrderType,
                    price=Decimal("NaN"), # Price is generally ignored for market orders by exchange but good practice to pass calculated one
                    ):
        if is_buy:
            self.buy(connector_name, trading_pair, amount, order_type, price)
        else:
            self.sell(connector_name, trading_pair, amount, order_type, price)

    def did_fill_order(self, event: OrderFilledEvent):
        """
         Listens to fill order event to log it and notify the Hummingbot application.
         Updates remaining volume and checks for completion.
         """
        # Check if the filled order belongs to this strategy instance
        if event.trading_pair == self.vwap["trading_pair"] and event.trade_type == self.vwap["trade_type"]:
            # Ensure event amount and price are valid Decimals
            event_amount = Decimal(event.amount)
            event_price = Decimal(event.price)

            self.vwap["volume_remaining"] -= event_amount
            self.vwap["real_quote_volume"] += event_price * event_amount
            self.vwap["trades"].append(event) # Store the original event object

            # Avoid division by zero if target volume wasn't calculated correctly
            if self.vwap["target_base_volume"] > 0:
                 self.vwap["delta"] = (self.vwap["target_base_volume"] - self.vwap["volume_remaining"]) / self.vwap["target_base_volume"]
                 # Check completion status
                 if math.isclose(self.vwap["delta"], 1, rel_tol=1e-5) or self.vwap["volume_remaining"] <= 0:
                    self.vwap["status"] = "COMPLETE"
                    self.logger().info(f"VWAP task for {self.vwap['trading_pair']} complete.")
            else:
                 self.vwap["delta"] = Decimal(0) # Or some indicator of error state


            msg = (f"({event.trading_pair}) {event.trade_type.name} order filled: "
                   f"{round(event_amount, 6)} {split_hb_trading_pair(event.trading_pair)[0]} @ "
                   f"{round(event_price, 6)} {split_hb_trading_pair(event.trading_pair)[1]}. "
                   f"REMAINING: {round(self.vwap['volume_remaining'], 6)}. "
                   f"STATUS: {self.vwap['status']}"
                  )

            self.log_with_clock(logging.INFO, msg)
            self.notify_hb_app_with_timestamp(msg)

    def format_status(self) -> str:
        """
         Returns status of the current strategy including VWAP progress and limits.
         """
        if not self.ready_to_trade:
            return "Market connectors are not ready."
        lines = []
        warning_lines = []
        warning_lines.extend(self.network_warning(self.get_market_trading_pair_tuples()))

        balance_df = self.get_balance_df()
        lines.extend(["", "  Balances:"] + ["    " + line for line in balance_df.to_string(index=False).split("\n")])

        try:
            df = self.active_orders_df()
            if not df.empty:
                 lines.extend(["", "  Orders:"] + ["    " + line for line in df.to_string(index=False).split("\n")])
            else:
                 lines.extend(["", "  No active orders."])
        except ValueError:
            lines.extend(["", "  No active orders."])

        # Display VWAP Info, handling potential missing keys gracefully
        lines.append("  VWAP Info:")
        for key, value in self.vwap.items():
             if isinstance(value, str):
                 lines.append(f"    {key}: {value}")
             elif key in ["buy_price_limit", "sell_price_limit"] and value is not None:
                 lines.append(f"    {key}: {value:.6f}") # Format Decimal limits

        # Display VWAP Stats
        lines.append("  VWAP Stats:")
        for key, value in self.vwap.items():
             # Check for numeric types that can be rounded
             if isinstance(value, (int, float, Decimal)) and key not in ["buy_price_limit", "sell_price_limit"]:
                 try:
                     # Handle potential non-numeric values if dictionary structure changes
                     lines.append(f"    {key}: {round(value, 4)}")
                 except TypeError:
                     lines.append(f"    {key}: {value}") # Display as is if rounding fails

        # Add warnings if any
        warning_lines.extend(self.balance_warning(self.get_market_trading_pair_tuples()))
        if self.vwap.get("status") == "ERROR":
             warning_lines.append("VWAP status is ERROR. Check logs.")
        if len(warning_lines) > 0:
            lines.extend(["", "*** WARNINGS ***"] + warning_lines)
        return "\n".join(lines) 