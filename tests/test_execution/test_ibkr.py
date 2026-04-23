"""
tests/test_execution/test_ibkr.py
Unit tests for the IBKR broker connector.

These tests DO NOT require a live IB Gateway connection.
Integration tests (smoke tests against real paper Gateway) are in
scripts/smoke_test_ibkr.py and must be run manually.

Strategy:
  - ib_insync is stubbed out so the module imports cleanly without the dep
  - Tests focus on the deterministic pieces:
      * Dataclass / enum shape
      * Status mapping (_IBKR_STATUS_MAP, _trade_to_status)
      * Live-port safety guard
      * submit_order input validation
      * _TERMINAL_IBKR_STATUSES completeness
"""

from __future__ import annotations

import sys
import types
from dataclasses import dataclass, field
from datetime import datetime
from unittest.mock import MagicMock

import pytest

# ─────────────────────────────────────────────────────────────────────────────
# STUB ib_insync SO THE MODULE IMPORTS
# ─────────────────────────────────────────────────────────────────────────────
# Must happen BEFORE importing worfin.execution.broker.ibkr.
# ib_insync is an optional dependency; in CI without it, the stub lets us
# exercise the logic that doesn't actually hit the network.

if "ib_insync" not in sys.modules:
    stub = types.ModuleType("ib_insync")
    # Minimal classes used by the broker module (used for isinstance/attr access only)
    stub.IB = MagicMock
    stub.Contract = MagicMock
    stub.Future = MagicMock
    stub.LimitOrder = MagicMock
    stub.MarketOrder = MagicMock
    stub.Trade = MagicMock
    sys.modules["ib_insync"] = stub


from worfin.execution.broker.ibkr import (  # noqa: E402
    _IBKR_STATUS_MAP,
    _TERMINAL_IBKR_STATUSES,
    BrokerConnectionError,
    BrokerOrderError,
    BrokerOrderRequest,
    BrokerOrderStatus,
    BrokerPermissionError,
    IBKRBroker,
    OrderStatusValue,
    OrderType,
    Side,
    get_broker,
)

# ─────────────────────────────────────────────────────────────────────────────
# SMALL LOCAL TRADE/ORDER STUBS FOR _trade_to_status TESTS
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class _FakeOrder:
    orderId: int


@dataclass
class _FakeOrderStatus:
    status: str
    filled: float = 0
    remaining: float = 0
    avgFillPrice: float = 0


@dataclass
class _FakeCommissionReport:
    commission: float = 0


@dataclass
class _FakeFill:
    commissionReport: _FakeCommissionReport | None = None


@dataclass
class _FakeLogEntry:
    message: str = ""


@dataclass
class _FakeTrade:
    order: _FakeOrder
    orderStatus: _FakeOrderStatus
    fills: list[_FakeFill] = field(default_factory=list)
    log: list[_FakeLogEntry] = field(default_factory=list)


# ─────────────────────────────────────────────────────────────────────────────
# DATACLASS / ENUM SHAPE
# ─────────────────────────────────────────────────────────────────────────────


class TestDataclassShape:
    def test_broker_order_request_is_frozen(self):
        req = BrokerOrderRequest(
            ticker="CA",
            side=Side.BUY,
            lots=1,
            order_type=OrderType.LIMIT_PASSIVE,
            limit_price=9500.0,
            client_tag="test-1",
        )
        with pytest.raises(Exception):  # FrozenInstanceError
            req.lots = 2  # type: ignore[misc]

    def test_order_status_enum_values(self):
        # Must match the status string set the engine/DB will write
        assert OrderStatusValue.PENDING.value == "pending"
        assert OrderStatusValue.SUBMITTED.value == "submitted"
        assert OrderStatusValue.PARTIAL.value == "partial"
        assert OrderStatusValue.FILLED.value == "filled"
        assert OrderStatusValue.CANCELLED.value == "cancelled"
        assert OrderStatusValue.REJECTED.value == "rejected"

    def test_side_enum(self):
        assert Side.BUY.value == "buy"
        assert Side.SELL.value == "sell"

    def test_order_type_enum(self):
        assert OrderType.LIMIT_PASSIVE.value == "limit_passive"
        assert OrderType.LIMIT_AGGRESSIVE.value == "limit_aggressive"
        assert OrderType.MARKET.value == "market"

    def test_broker_order_status_defaults(self):
        status = BrokerOrderStatus(
            ibkr_order_id="123",
            status=OrderStatusValue.SUBMITTED,
        )
        assert status.filled_lots == 0
        assert status.remaining_lots == 0
        assert status.avg_fill_price is None
        assert status.commission_usd is None
        assert isinstance(status.last_updated, datetime)
        assert status.error_message is None


# ─────────────────────────────────────────────────────────────────────────────
# STATUS MAP COMPLETENESS
# ─────────────────────────────────────────────────────────────────────────────


class TestStatusMapping:
    """Every IBKR status we care about must map to one of our OrderStatusValue."""

    def test_ibkr_status_map_covers_terminal_states(self):
        # If IBKR says it's terminal, we must map it sensibly
        for s in _TERMINAL_IBKR_STATUSES:
            assert (
                s in _IBKR_STATUS_MAP
            ), f"Terminal IBKR status {s!r} has no mapping in _IBKR_STATUS_MAP"

    def test_terminal_set_has_expected_statuses(self):
        # Guard against a future refactor accidentally dropping one
        required = {"Filled", "Cancelled", "ApiCancelled", "Inactive"}
        assert required.issubset(_TERMINAL_IBKR_STATUSES)

    @pytest.mark.parametrize(
        "ibkr_status,expected",
        [
            ("Submitted", OrderStatusValue.SUBMITTED),
            ("Filled", OrderStatusValue.FILLED),
            ("Cancelled", OrderStatusValue.CANCELLED),
            ("ApiCancelled", OrderStatusValue.CANCELLED),
            ("Inactive", OrderStatusValue.REJECTED),
            ("PendingSubmit", OrderStatusValue.PENDING),
            ("PreSubmitted", OrderStatusValue.SUBMITTED),
        ],
    )
    def test_status_translation(self, ibkr_status, expected):
        broker = IBKRBroker()
        trade = _FakeTrade(
            order=_FakeOrder(orderId=42),
            orderStatus=_FakeOrderStatus(status=ibkr_status),
        )
        status = broker._trade_to_status(trade)
        assert status.status == expected
        assert status.ibkr_order_id == "42"

    def test_partial_fill_promotes_submitted_to_partial(self):
        """Submitted + some fills should become PARTIAL."""
        broker = IBKRBroker()
        trade = _FakeTrade(
            order=_FakeOrder(orderId=7),
            orderStatus=_FakeOrderStatus(
                status="Submitted", filled=2, remaining=8, avgFillPrice=9450.5
            ),
        )
        status = broker._trade_to_status(trade)
        assert status.status == OrderStatusValue.PARTIAL
        assert status.filled_lots == 2
        assert status.remaining_lots == 8
        assert status.avg_fill_price == 9450.5

    def test_rejected_pulls_error_from_log(self):
        broker = IBKRBroker()
        trade = _FakeTrade(
            order=_FakeOrder(orderId=99),
            orderStatus=_FakeOrderStatus(status="Inactive"),
            log=[_FakeLogEntry(message="No clearing permission")],
        )
        status = broker._trade_to_status(trade)
        assert status.status == OrderStatusValue.REJECTED
        assert status.error_message == "No clearing permission"

    def test_commission_summed_across_fills(self):
        broker = IBKRBroker()
        trade = _FakeTrade(
            order=_FakeOrder(orderId=11),
            orderStatus=_FakeOrderStatus(status="Filled", filled=3, remaining=0, avgFillPrice=9500),
            fills=[
                _FakeFill(_FakeCommissionReport(commission=1.0)),
                _FakeFill(_FakeCommissionReport(commission=2.0)),
            ],
        )
        status = broker._trade_to_status(trade)
        assert status.status == OrderStatusValue.FILLED
        assert status.commission_usd == 3.0

    def test_no_commission_when_fills_have_no_reports(self):
        broker = IBKRBroker()
        trade = _FakeTrade(
            order=_FakeOrder(orderId=12),
            orderStatus=_FakeOrderStatus(status="Filled", filled=1, remaining=0, avgFillPrice=100),
            fills=[_FakeFill(commissionReport=None)],
        )
        status = broker._trade_to_status(trade)
        assert status.commission_usd is None


# ─────────────────────────────────────────────────────────────────────────────
# CONNECTION GUARDS
# ─────────────────────────────────────────────────────────────────────────────


class TestConnectionGuards:
    def test_require_connection_raises_when_disconnected(self):
        broker = IBKRBroker()
        with pytest.raises(BrokerConnectionError, match="Not connected"):
            broker._require_connection()

    def test_is_connected_false_on_fresh_instance(self):
        broker = IBKRBroker()
        assert broker.is_connected() is False

    @pytest.mark.asyncio
    async def test_submit_order_without_connection_raises(self):
        broker = IBKRBroker()
        req = BrokerOrderRequest(
            ticker="CA",
            side=Side.BUY,
            lots=1,
            order_type=OrderType.LIMIT_PASSIVE,
            limit_price=9500.0,
            client_tag="t1",
        )
        with pytest.raises(BrokerConnectionError):
            await broker.submit_order(req)

    @pytest.mark.asyncio
    async def test_get_positions_without_connection_raises(self):
        broker = IBKRBroker()
        with pytest.raises(BrokerConnectionError):
            await broker.get_positions()


# ─────────────────────────────────────────────────────────────────────────────
# ORDER INPUT VALIDATION
# ─────────────────────────────────────────────────────────────────────────────


class TestOrderValidation:
    @pytest.mark.asyncio
    async def test_submit_rejects_zero_lots(self, monkeypatch):
        """Order with lots=0 must be rejected before touching IBKR."""
        broker = IBKRBroker()
        # Fake a connection so _require_connection passes
        broker._connected = True
        fake_ib = MagicMock()
        fake_ib.isConnected = MagicMock(return_value=True)
        broker._ib = fake_ib

        req = BrokerOrderRequest(
            ticker="CA",
            side=Side.BUY,
            lots=0,
            order_type=OrderType.MARKET,
            limit_price=None,
            client_tag="bad",
        )
        with pytest.raises(BrokerOrderError, match="lots must be positive"):
            await broker.submit_order(req)

    @pytest.mark.asyncio
    async def test_submit_rejects_negative_lots(self):
        broker = IBKRBroker()
        broker._connected = True
        fake_ib = MagicMock()
        fake_ib.isConnected = MagicMock(return_value=True)
        broker._ib = fake_ib

        req = BrokerOrderRequest(
            ticker="CA",
            side=Side.SELL,
            lots=-5,
            order_type=OrderType.MARKET,
            limit_price=None,
            client_tag="bad",
        )
        with pytest.raises(BrokerOrderError, match="lots must be positive"):
            await broker.submit_order(req)


# ─────────────────────────────────────────────────────────────────────────────
# SINGLETON
# ─────────────────────────────────────────────────────────────────────────────


class TestSingleton:
    def test_get_broker_returns_same_instance(self):
        a = get_broker()
        b = get_broker()
        assert a is b

    def test_exceptions_are_distinct_types(self):
        """Each error class must be catchable separately."""
        assert issubclass(BrokerConnectionError, RuntimeError)
        assert issubclass(BrokerOrderError, RuntimeError)
        assert issubclass(BrokerPermissionError, RuntimeError)
        # And distinct from each other
        assert not issubclass(BrokerConnectionError, BrokerOrderError)
        assert not issubclass(BrokerOrderError, BrokerPermissionError)
