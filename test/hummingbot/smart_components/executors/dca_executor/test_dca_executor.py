from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from test.logger_mixin_for_test import LoggerMixinForTest
from unittest.mock import MagicMock, PropertyMock, patch

from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, TradeUpdate
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee, TokenAmount
from hummingbot.smart_components.executors.dca_executor.data_types import DCAExecutorConfig
from hummingbot.smart_components.executors.dca_executor.dca_executor import DCAExecutor
from hummingbot.smart_components.executors.position_executor.data_types import TrailingStop
from hummingbot.smart_components.models.base import SmartComponentStatus
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase


class TestDCAExecutor(IsolatedAsyncioWrapperTestCase, LoggerMixinForTest):
    def setUp(self) -> None:
        super().setUp()
        self.strategy = self.create_mock_strategy()
        self.update_interval = 0.5

    @staticmethod
    def create_mock_strategy():
        market = MagicMock()
        market_info = MagicMock()
        market_info.market = market

        strategy = MagicMock(spec=ScriptStrategyBase)
        type(strategy).market_info = PropertyMock(return_value=market_info)
        type(strategy).trading_pair = PropertyMock(return_value="ETH-USDT")
        strategy.buy.side_effect = ["OID-BUY-1", "OID-BUY-2", "OID-BUY-3"]
        strategy.sell.side_effect = ["OID-SELL-1", "OID-SELL-2", "OID-SELL-3"]
        strategy.cancel.return_value = None
        connector = MagicMock(spec=ExchangePyBase)
        type(connector).trading_rules = PropertyMock(return_value={"ETH-USDT": TradingRule(trading_pair="ETH-USDT")})
        strategy.connectors = {
            "binance": connector,
        }
        return strategy

    def get_dca_executor_from_config(self, config: DCAExecutorConfig):
        executor = DCAExecutor(self.strategy, config, self.update_interval)
        self.set_loggers(loggers=[executor.logger()])
        return executor

    @patch.object(DCAExecutor, "get_price", MagicMock(return_value=Decimal("120")))
    async def test_control_task_open_orders(self):
        config = DCAExecutorConfig(id="test", timestamp=123, side=TradeType.BUY, connector_name="binance",
                                   trading_pair="ETH-USDT",
                                   amounts_quote=[Decimal(10), Decimal(20), Decimal(30)],
                                   prices=[Decimal(100), Decimal(80), Decimal(60)])
        executor = self.get_dca_executor_from_config(config)
        executor._status = SmartComponentStatus.RUNNING
        await executor.control_task()
        self.assertEqual(executor.active_open_orders[0].order_id, "OID-BUY-1")
        await executor.control_task()
        self.assertEqual(executor.active_open_orders[1].order_id, "OID-BUY-2")
        await executor.control_task()
        self.assertEqual(executor.active_open_orders[2].order_id, "OID-BUY-3")
        self.assertEqual(executor.net_pnl_pct, 0)
        self.assertEqual(executor.net_pnl_quote, 0)
        self.assertEqual(executor.cum_fees_quote, 0)
        self.assertEqual(executor.min_price, Decimal("60"))
        self.assertEqual(executor.max_price, Decimal("100"))
        self.assertEqual(executor.max_amount_quote, Decimal("60"))
        self.assertEqual(executor.close_filled_amount_quote, Decimal("0"))

    @patch.object(DCAExecutor, "get_price")
    async def test_activation_bounds_prevents_order_creation(self, get_price_mock):
        get_price_mock.return_value = Decimal("120")
        config = DCAExecutorConfig(id="test", timestamp=123, side=TradeType.BUY, connector_name="binance",
                                   trading_pair="ETH-USDT",
                                   amounts_quote=[Decimal(10), Decimal(20), Decimal(30)],
                                   prices=[Decimal(100), Decimal(80), Decimal(60)],
                                   activation_bounds=[Decimal("0.01")], )
        executor = self.get_dca_executor_from_config(config)
        executor._status = SmartComponentStatus.RUNNING
        await executor.control_task()
        self.assertEqual(executor.active_open_orders, [])
        self.assertEqual(executor.max_amount_quote, Decimal("60"))

    @patch.object(DCAExecutor, "get_price")
    async def test_activation_bounds_allows_order_creation(self, get_price_mock):
        get_price_mock.return_value = Decimal("101")
        config = DCAExecutorConfig(id="test", timestamp=123, side=TradeType.BUY, connector_name="binance",
                                   trading_pair="ETH-USDT",
                                   amounts_quote=[Decimal(10), Decimal(20), Decimal(30)],
                                   prices=[Decimal(100), Decimal(80), Decimal(60)],
                                   activation_bounds=[Decimal("0.1")], )
        executor = self.get_dca_executor_from_config(config)
        executor._status = SmartComponentStatus.RUNNING
        await executor.control_task()
        self.assertEqual(executor.active_open_orders[0].order_id, "OID-BUY-1")

    @patch.object(DCAExecutor, "get_price")
    async def test_activation_bounds_allows_order_creation_with_sell(self, get_price_mock):
        get_price_mock.return_value = Decimal("99")
        config = DCAExecutorConfig(id="test", timestamp=123, side=TradeType.SELL, connector_name="binance",
                                   trading_pair="ETH-USDT",
                                   amounts_quote=[Decimal(10), Decimal(20), Decimal(30)],
                                   prices=[Decimal(100), Decimal(120), Decimal(140)],
                                   activation_bounds=[Decimal("0.1")], )
        executor = self.get_dca_executor_from_config(config)
        executor._status = SmartComponentStatus.RUNNING
        await executor.control_task()
        self.assertEqual(executor.active_open_orders[0].order_id, "OID-SELL-1")

    @patch.object(DCAExecutor, "get_price")
    async def test_activation_bounds_prevents_order_creation_with_sell(self, get_price_mock):
        get_price_mock.return_value = Decimal("99")
        config = DCAExecutorConfig(id="test", timestamp=123, side=TradeType.SELL, connector_name="binance",
                                   trading_pair="ETH-USDT",
                                   amounts_quote=[Decimal(10), Decimal(20), Decimal(30)],
                                   prices=[Decimal(100), Decimal(120), Decimal(140)],
                                   activation_bounds=[Decimal("0.01")], )
        executor = self.get_dca_executor_from_config(config)
        executor._status = SmartComponentStatus.RUNNING
        await executor.control_task()
        self.assertEqual(executor.active_open_orders, [])

    @patch.object(DCAExecutor, "get_price")
    async def test_dca_activated_and_stop_loss_triggered(self, get_price_mock):
        get_price_mock.side_effect = [Decimal("120"), Decimal("90"), Decimal("50")]
        config = DCAExecutorConfig(id="test", timestamp=123, side=TradeType.BUY, connector_name="binance",
                                   trading_pair="ETH-USDT",
                                   amounts_quote=[Decimal(10), Decimal(20)],
                                   prices=[Decimal(100), Decimal(80)],
                                   stop_loss=Decimal("0.1"))
        executor = self.get_dca_executor_from_config(config)
        executor._status = SmartComponentStatus.RUNNING
        await executor.control_task()
        self.assertEqual(executor.active_open_orders[0].order_id, "OID-BUY-1")

        executor.active_open_orders[0].order = InFlightOrder(
            client_order_id="OID-BUY-1",
            exchange_order_id="EOID4",
            trading_pair="ETH-USDT",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            amount=Decimal(0.1),
            price=Decimal(100),
            creation_timestamp=1640001112.223,
            initial_state=OrderState.COMPLETED
        )
        executor.active_open_orders[0].order.update_with_trade_update(
            TradeUpdate(
                trade_id="1",
                client_order_id="OID-BUY-1",
                exchange_order_id="EOID4",
                trading_pair="ETH-USDT",
                fill_price=Decimal("100"),
                fill_base_amount=Decimal("0.1"),
                fill_quote_amount=Decimal("10"),
                fee=AddedToCostTradeFee(flat_fees=[TokenAmount(token="USDT", amount=Decimal("0.2"))]),
                fill_timestamp=10,
            )
        )

        await executor.control_task()
        self.assertEqual(executor.active_open_orders[1].order_id, "OID-BUY-2")
        executor.active_open_orders[1].order = InFlightOrder(
            client_order_id="OID-BUY-2",
            exchange_order_id="EOID5",
            trading_pair="ETH-USDT",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            amount=Decimal(0.25),
            price=Decimal(80),
            creation_timestamp=1640001112.223,
            initial_state=OrderState.COMPLETED
        )
        executor.active_open_orders[1].order.update_with_trade_update(
            TradeUpdate(
                trade_id="2",
                client_order_id="OID-BUY-2",
                exchange_order_id="EOID5",
                trading_pair="ETH-USDT",
                fill_price=Decimal("80"),
                fill_base_amount=Decimal("0.25"),
                fill_quote_amount=Decimal("20"),
                fee=AddedToCostTradeFee(flat_fees=[TokenAmount(token="USDT", amount=Decimal("0.2"))]),
                fill_timestamp=10,
            )
        )
        await executor.control_task()
        self.assertEqual(executor.active_close_orders[0].order_id, "OID-SELL-1")

    @patch.object(DCAExecutor, "get_price")
    async def test_dca_activated_and_stop_loss_triggered_with_sell(self, get_price_mock):
        get_price_mock.side_effect = [Decimal("100"), Decimal("120"), Decimal("140")]
        config = DCAExecutorConfig(id="test", timestamp=123, side=TradeType.SELL, connector_name="binance",
                                   trading_pair="ETH-USDT",
                                   amounts_quote=[Decimal(10), Decimal(20)],
                                   prices=[Decimal(100), Decimal(120)],
                                   stop_loss=Decimal("0.1"))
        executor = self.get_dca_executor_from_config(config)
        executor._status = SmartComponentStatus.RUNNING
        await executor.control_task()
        self.assertEqual(executor.active_open_orders[0].order_id, "OID-SELL-1")

        executor.active_open_orders[0].order = InFlightOrder(
            client_order_id="OID-SELL-1",
            exchange_order_id="EOID4",
            trading_pair="ETH-USDT",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.SELL,
            amount=Decimal(0.1),
            price=Decimal(100),
            creation_timestamp=1640001112.223,
            initial_state=OrderState.COMPLETED
        )
        executor.active_open_orders[0].order.update_with_trade_update(
            TradeUpdate(
                trade_id="1",
                client_order_id="OID-SELL-1",
                exchange_order_id="EOID4",
                trading_pair="ETH-USDT",
                fill_price=Decimal("100"),
                fill_base_amount=Decimal("0.1"),
                fill_quote_amount=Decimal("10"),
                fee=AddedToCostTradeFee(flat_fees=[TokenAmount(token="USDT", amount=Decimal("0.2"))]),
                fill_timestamp=10,
            )
        )

        await executor.control_task()
        self.assertEqual(executor.active_open_orders[1].order_id, "OID-SELL-2")
        executor.active_open_orders[1].order = InFlightOrder(
            client_order_id="OID-SELL-2",
            exchange_order_id="EOID5",
            trading_pair="ETH-USDT",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.SELL,
            amount=Decimal(0.25),
            price=Decimal(120),
            creation_timestamp=1640001112.223,
            initial_state=OrderState.COMPLETED
        )
        executor.active_open_orders[1].order.update_with_trade_update(
            TradeUpdate(
                trade_id="2",
                client_order_id="OID-SELL-2",
                exchange_order_id="EOID5",
                trading_pair="ETH-USDT",
                fill_price=Decimal("120"),
                fill_base_amount=Decimal("0.25"),
                fill_quote_amount=Decimal("20"),
                fee=AddedToCostTradeFee(flat_fees=[TokenAmount(token="USDT", amount=Decimal("0.2"))]),
                fill_timestamp=10,
            )
        )
        await executor.control_task()
        self.assertEqual(executor.active_close_orders[0].order_id, "OID-BUY-1")

    @patch.object(DCAExecutor, "get_price")
    async def test_dca_activated_and_take_profit_triggered_with_first_order(self, get_price_mock):
        get_price_mock.side_effect = [Decimal("110"), Decimal("100"), Decimal("105"), Decimal("115")]
        config = DCAExecutorConfig(id="test", timestamp=123, side=TradeType.BUY, connector_name="binance",
                                   trading_pair="ETH-USDT",
                                   amounts_quote=[Decimal(10), Decimal(20)],
                                   prices=[Decimal(100), Decimal(80)],
                                   take_profit=Decimal("0.1"))
        executor = self.get_dca_executor_from_config(config)
        executor._status = SmartComponentStatus.RUNNING
        await executor.control_task()
        self.assertEqual(executor.active_open_orders[0].order_id, "OID-BUY-1")
        executor.active_open_orders[0].order = InFlightOrder(
            client_order_id="OID-BUY-1",
            exchange_order_id="EOID4",
            trading_pair="ETH-USDT",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            amount=Decimal(0.1),
            price=Decimal(100),
            creation_timestamp=1640001112.223,
            initial_state=OrderState.COMPLETED
        )
        executor.active_open_orders[0].order.update_with_trade_update(
            TradeUpdate(
                trade_id="1",
                client_order_id="OID-BUY-1",
                exchange_order_id="EOID4",
                trading_pair="ETH-USDT",
                fill_price=Decimal("100"),
                fill_base_amount=Decimal("0.1"),
                fill_quote_amount=Decimal("10"),
                fee=AddedToCostTradeFee(flat_fees=[TokenAmount(token="USDT", amount=Decimal("0.2"))]),
                fill_timestamp=10,
            )
        )
        await executor.control_task()
        self.assertEqual(executor.active_open_orders[1].order_id, "OID-BUY-2")
        executor.active_open_orders[1].order = InFlightOrder(
            client_order_id="OID-BUY-2",
            exchange_order_id="EOID5",
            trading_pair="ETH-USDT",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            amount=Decimal(0.25),
            price=Decimal(80),
            creation_timestamp=1640001112.223,
            initial_state=OrderState.OPEN
        )
        await executor.control_task()
        self.assertEqual(executor.active_close_orders[0].order_id, "OID-SELL-1")

    @patch.object(DCAExecutor, "get_price")
    async def test_dca_activated_and_take_profit_triggered_with_average_price(self, get_price_mock):
        get_price_mock.side_effect = [Decimal("105"), Decimal("95"), Decimal("89"),
                                      Decimal("105")]
        config = DCAExecutorConfig(id="test", timestamp=123, side=TradeType.BUY, connector_name="binance",
                                   trading_pair="ETH-USDT",
                                   amounts_quote=[Decimal(10), Decimal(20)],
                                   prices=[Decimal(100), Decimal(90)],
                                   take_profit=Decimal("0.05"))
        executor = self.get_dca_executor_from_config(config)
        executor._status = SmartComponentStatus.RUNNING
        await executor.control_task()
        self.assertEqual(executor.active_open_orders[0].order_id, "OID-BUY-1")
        executor.active_open_orders[0].order = InFlightOrder(
            client_order_id="OID-BUY-1",
            exchange_order_id="EOID4",
            trading_pair="ETH-USDT",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            amount=Decimal(0.1),
            price=Decimal(100),
            creation_timestamp=1640001112.223,
            initial_state=OrderState.COMPLETED
        )
        executor.active_open_orders[0].order.update_with_trade_update(
            TradeUpdate(
                trade_id="1",
                client_order_id="OID-BUY-1",
                exchange_order_id="EOID4",
                trading_pair="ETH-USDT",
                fill_price=Decimal("100"),
                fill_base_amount=Decimal("0.1"),
                fill_quote_amount=Decimal("10"),
                fee=AddedToCostTradeFee(flat_fees=[TokenAmount(token="USDT", amount=Decimal("0.2"))]),
                fill_timestamp=10,
            )
        )
        await executor.control_task()
        self.assertEqual(executor.active_open_orders[1].order_id, "OID-BUY-2")
        executor.active_open_orders[1].order = InFlightOrder(
            client_order_id="OID-BUY-2",
            exchange_order_id="EOID5",
            trading_pair="ETH-USDT",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            amount=Decimal(0.25),
            price=Decimal(80),
            creation_timestamp=1640001112.223,
            initial_state=OrderState.COMPLETED
        )
        executor.active_open_orders[1].order.update_with_trade_update(
            TradeUpdate(
                trade_id="2",
                client_order_id="OID-BUY-2",
                exchange_order_id="EOID5",
                trading_pair="ETH-USDT",
                fill_price=Decimal("90"),
                fill_base_amount=Decimal("0.25"),
                fill_quote_amount=Decimal("20"),
                fee=AddedToCostTradeFee(flat_fees=[TokenAmount(token="USDT", amount=Decimal("0.2"))]),
                fill_timestamp=10,
            )
        )
        await executor.control_task()
        self.assertEqual(executor.active_close_orders[0].order_id, "OID-SELL-1")

    @patch.object(DCAExecutor, "get_price")
    async def test_dca_activated_and_trailing_stop_triggered(self, get_price_mock):
        get_price_mock.side_effect = [Decimal("105"), Decimal("95"), Decimal("89"), Decimal("105"), Decimal("100")]
        config = DCAExecutorConfig(id="test", timestamp=123, side=TradeType.BUY, connector_name="binance",
                                   trading_pair="ETH-USDT",
                                   amounts_quote=[Decimal(10), Decimal(20)],
                                   prices=[Decimal(100), Decimal(90)],
                                   trailing_stop=TrailingStop(activation_price=Decimal("0.05"),
                                                              trailing_delta=Decimal("0.01")))
        executor = self.get_dca_executor_from_config(config)
        executor._status = SmartComponentStatus.RUNNING
        await executor.control_task()
        self.assertEqual(executor.active_open_orders[0].order_id, "OID-BUY-1")
        executor.active_open_orders[0].order = InFlightOrder(
            client_order_id="OID-BUY-1",
            exchange_order_id="EOID4",
            trading_pair="ETH-USDT",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            amount=Decimal(0.1),
            price=Decimal(100),
            creation_timestamp=1640001112.223,
            initial_state=OrderState.COMPLETED
        )
        executor.active_open_orders[0].order.update_with_trade_update(
            TradeUpdate(
                trade_id="1",
                client_order_id="OID-BUY-1",
                exchange_order_id="EOID4",
                trading_pair="ETH-USDT",
                fill_price=Decimal("100"),
                fill_base_amount=Decimal("0.1"),
                fill_quote_amount=Decimal("10"),
                fee=AddedToCostTradeFee(flat_fees=[TokenAmount(token="USDT", amount=Decimal("0.2"))]),
                fill_timestamp=10,
            )
        )
        await executor.control_task()
        self.assertEqual(executor.active_open_orders[1].order_id, "OID-BUY-2")
        executor.active_open_orders[1].order = InFlightOrder(
            client_order_id="OID-BUY-2",
            exchange_order_id="EOID5",
            trading_pair="ETH-USDT",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            amount=Decimal(0.25),
            price=Decimal(90),
            creation_timestamp=1640001112.223,
            initial_state=OrderState.OPEN
        )
        executor.active_open_orders[1].order.update_with_trade_update(
            TradeUpdate(
                trade_id="2",
                client_order_id="OID-BUY-2",
                exchange_order_id="EOID5",
                trading_pair="ETH-USDT",
                fill_price=Decimal("90"),
                fill_base_amount=Decimal("0.25"),
                fill_quote_amount=Decimal("20"),
                fee=AddedToCostTradeFee(flat_fees=[TokenAmount(token="USDT", amount=Decimal("0.2"))]),
                fill_timestamp=10,
            )
        )
        await executor.control_task()
        await executor.control_task()
        self.assertEqual(executor.active_close_orders[0].order_id, "OID-SELL-1")
