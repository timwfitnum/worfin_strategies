"""
tests/test_execution/test_orders.py
Unit tests for the Order lifecycle and state machine.

All tests run with engine=None (in-memory mode). DB integration tests are
deferred to a dedicated integration harness against a live paper DB.
"""

from __future__ import annotations

import sys
import types
from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest

# Stub ib_insync before importing the broker module (test_ibkr.py does this too,
# but test collection order isn't guaranteed)
if "ib_insync" not in sys.modules:
    stub = types.ModuleType("ib_insync")
    stub.IB = MagicMock
    stub.Contract = MagicMock
    stub.Future = MagicMock
    stub.LimitOrder = MagicMock
    stub.MarketOrder = MagicMock
    stub.Trade = MagicMock
    sys.modules["ib_insync"] = stub


from worfin.execution.broker import (  # noqa: E402
    BrokerOrderRequest,
    OrderStatusValue,
    OrderType,
    Side,
)
from worfin.execution.orders import (  # noqa: E402
    _LEGAL_TRANSITIONS,
    _TERMINAL_STATES,
    AuditSeverity,
    Fill,
    InvalidStateTransition,
    Order,
    OrderManager,
    OrderValidationError,
)


def _now() -> datetime:
    return datetime.now(UTC)


# ─────────────────────────────────────────────────────────────────────────────
# STATE MACHINE STRUCTURE
# ─────────────────────────────────────────────────────────────────────────────


class TestStateMachineStructure:
    """Sanity checks on the transition table itself."""

    def test_all_statuses_have_entry_in_table(self):
        for s in OrderStatusValue:
            assert s in _LEGAL_TRANSITIONS, f"{s} missing from _LEGAL_TRANSITIONS"

    def test_terminal_states_have_no_outgoing_transitions(self):
        for s in _TERMINAL_STATES:
            assert (
                _LEGAL_TRANSITIONS[s] == frozenset()
            ), f"Terminal state {s} must have no outgoing transitions"

    def test_expected_terminal_states(self):
        assert (
            frozenset(
                {
                    OrderStatusValue.FILLED,
                    OrderStatusValue.CANCELLED,
                    OrderStatusValue.REJECTED,
                }
            )
            == _TERMINAL_STATES
        )

    def test_pending_can_reach_submitted_rejected_cancelled(self):
        legal = _LEGAL_TRANSITIONS[OrderStatusValue.PENDING]
        assert OrderStatusValue.SUBMITTED in legal
        assert OrderStatusValue.REJECTED in legal
        assert OrderStatusValue.CANCELLED in legal

    def test_submitted_can_reach_partial_filled_cancelled_rejected(self):
        legal = _LEGAL_TRANSITIONS[OrderStatusValue.SUBMITTED]
        assert OrderStatusValue.PARTIAL in legal
        assert OrderStatusValue.FILLED in legal
        assert OrderStatusValue.CANCELLED in legal
        assert OrderStatusValue.REJECTED in legal

    def test_partial_cannot_go_back_to_submitted(self):
        assert OrderStatusValue.SUBMITTED not in _LEGAL_TRANSITIONS[OrderStatusValue.PARTIAL]


# ─────────────────────────────────────────────────────────────────────────────
# ORDER DATACLASS
# ─────────────────────────────────────────────────────────────────────────────


class TestOrderDataclass:
    def _make_order(self, **overrides) -> Order:
        defaults = dict(
            internal_id="test-id",
            strategy_id="S4",
            ticker="CA",
            side=Side.BUY,
            lots_requested=5,
            order_type=OrderType.LIMIT_PASSIVE,
            limit_price=9500.0,
            created_at=_now(),
        )
        defaults.update(overrides)
        return Order(**defaults)

    def test_new_order_starts_pending(self):
        order = self._make_order()
        assert order.status == OrderStatusValue.PENDING
        assert not order.is_terminal

    def test_lots_filled_sums_fills(self):
        order = self._make_order(lots_requested=10)
        order.fills.append(Fill(fill_timestamp=_now(), lots_filled=3, fill_price=100.0))
        order.fills.append(Fill(fill_timestamp=_now(), lots_filled=2, fill_price=101.0))
        assert order.lots_filled == 5
        assert order.lots_remaining == 5

    def test_avg_fill_price_is_weighted(self):
        order = self._make_order(lots_requested=10)
        order.fills.append(Fill(fill_timestamp=_now(), lots_filled=3, fill_price=100.0))
        order.fills.append(Fill(fill_timestamp=_now(), lots_filled=2, fill_price=110.0))
        # (3×100 + 2×110) / 5 = 520 / 5 = 104
        assert order.avg_fill_price == pytest.approx(104.0)

    def test_avg_fill_price_none_when_no_fills(self):
        order = self._make_order()
        assert order.avg_fill_price is None

    def test_total_commission(self):
        order = self._make_order()
        order.fills.append(
            Fill(
                fill_timestamp=_now(),
                lots_filled=1,
                fill_price=100,
                commission_usd=3.0,
            )
        )
        order.fills.append(
            Fill(
                fill_timestamp=_now(),
                lots_filled=1,
                fill_price=100,
                commission_usd=2.5,
            )
        )
        order.fills.append(
            Fill(fill_timestamp=_now(), lots_filled=1, fill_price=100)
        )  # no commission
        assert order.total_commission_usd == pytest.approx(5.5)

    def test_is_terminal_true_for_filled(self):
        order = self._make_order(status=OrderStatusValue.FILLED)
        assert order.is_terminal

    def test_is_terminal_true_for_cancelled(self):
        order = self._make_order(status=OrderStatusValue.CANCELLED)
        assert order.is_terminal

    def test_is_terminal_true_for_rejected(self):
        order = self._make_order(status=OrderStatusValue.REJECTED)
        assert order.is_terminal

    def test_is_terminal_false_for_pending(self):
        order = self._make_order(status=OrderStatusValue.PENDING)
        assert not order.is_terminal

    def test_is_terminal_false_for_partial(self):
        order = self._make_order(status=OrderStatusValue.PARTIAL)
        assert not order.is_terminal

    def test_to_broker_request(self):
        order = self._make_order()
        req = order.to_broker_request()
        assert isinstance(req, BrokerOrderRequest)
        assert req.ticker == "CA"
        assert req.side == Side.BUY
        assert req.lots == 5
        assert req.order_type == OrderType.LIMIT_PASSIVE
        assert req.limit_price == 9500.0
        assert req.client_tag == "test-id"


# ─────────────────────────────────────────────────────────────────────────────
# FILL DATACLASS
# ─────────────────────────────────────────────────────────────────────────────


class TestFillDataclass:
    def test_fill_is_frozen(self):
        fill = Fill(fill_timestamp=_now(), lots_filled=1, fill_price=100.0)
        with pytest.raises(Exception):  # FrozenInstanceError
            fill.lots_filled = 2  # type: ignore[misc]

    def test_fill_defaults(self):
        fill = Fill(fill_timestamp=_now(), lots_filled=1, fill_price=100.0)
        assert fill.commission_usd is None
        assert fill.ibkr_exec_id is None
        assert fill.slippage_bps is None


# ─────────────────────────────────────────────────────────────────────────────
# ORDER MANAGER — CREATION
# ─────────────────────────────────────────────────────────────────────────────


class TestOrderManagerCreate:
    def test_create_returns_pending_order(self):
        mgr = OrderManager(engine=None)
        order = mgr.create(
            strategy_id="S4",
            ticker="CA",
            side=Side.BUY,
            lots=2,
            order_type=OrderType.LIMIT_PASSIVE,
            limit_price=9500.0,
        )
        assert order.status == OrderStatusValue.PENDING
        assert order.strategy_id == "S4"
        assert order.ticker == "CA"
        assert order.side == Side.BUY
        assert order.lots_requested == 2
        assert order.limit_price == 9500.0
        assert order.internal_id  # non-empty UUID

    def test_create_assigns_unique_ids(self):
        mgr = OrderManager(engine=None)
        a = mgr.create("S4", "CA", Side.BUY, 1, OrderType.MARKET, None)
        b = mgr.create("S4", "CA", Side.BUY, 1, OrderType.MARKET, None)
        assert a.internal_id != b.internal_id

    def test_create_rejects_zero_lots(self):
        mgr = OrderManager(engine=None)
        with pytest.raises(OrderValidationError, match="lots must be positive"):
            mgr.create("S4", "CA", Side.BUY, 0, OrderType.MARKET, None)

    def test_create_rejects_negative_lots(self):
        mgr = OrderManager(engine=None)
        with pytest.raises(OrderValidationError, match="lots must be positive"):
            mgr.create("S4", "CA", Side.BUY, -1, OrderType.MARKET, None)

    def test_limit_order_requires_limit_price(self):
        mgr = OrderManager(engine=None)
        with pytest.raises(OrderValidationError, match="requires limit_price"):
            mgr.create("S4", "CA", Side.BUY, 1, OrderType.LIMIT_PASSIVE, None)

    def test_limit_order_rejects_zero_limit_price(self):
        mgr = OrderManager(engine=None)
        with pytest.raises(OrderValidationError, match="limit_price must be positive"):
            mgr.create("S4", "CA", Side.BUY, 1, OrderType.LIMIT_AGGRESSIVE, 0.0)

    def test_market_order_doesnt_need_limit_price(self):
        mgr = OrderManager(engine=None)
        order = mgr.create("S4", "CA", Side.BUY, 1, OrderType.MARKET, None)
        assert order.limit_price is None
        assert order.order_type == OrderType.MARKET

    def test_strategy_id_too_long(self):
        mgr = OrderManager(engine=None)
        with pytest.raises(OrderValidationError, match="strategy_id"):
            mgr.create("TOOLONG", "CA", Side.BUY, 1, OrderType.MARKET, None)

    def test_ticker_too_long(self):
        mgr = OrderManager(engine=None)
        with pytest.raises(OrderValidationError, match="ticker"):
            mgr.create("S4", "TOOLONG", Side.BUY, 1, OrderType.MARKET, None)


# ─────────────────────────────────────────────────────────────────────────────
# ORDER MANAGER — STATE TRANSITIONS
# ─────────────────────────────────────────────────────────────────────────────


class TestOrderManagerTransitions:
    def _fresh(self) -> tuple[OrderManager, Order]:
        mgr = OrderManager(engine=None)
        order = mgr.create("S4", "CA", Side.BUY, 5, OrderType.LIMIT_PASSIVE, 9500.0)
        return mgr, order

    def test_mark_submitted_from_pending(self):
        mgr, order = self._fresh()
        mgr.mark_submitted(order, "IBKR-123")
        assert order.status == OrderStatusValue.SUBMITTED
        assert order.ibkr_order_id == "IBKR-123"

    def test_mark_submitted_requires_ibkr_id(self):
        mgr, order = self._fresh()
        with pytest.raises(OrderValidationError, match="ibkr_order_id required"):
            mgr.mark_submitted(order, "")

    def test_mark_submitted_from_submitted_is_illegal(self):
        mgr, order = self._fresh()
        mgr.mark_submitted(order, "IBKR-123")
        with pytest.raises(InvalidStateTransition):
            mgr.mark_submitted(order, "IBKR-456")

    def test_mark_rejected_from_pending(self):
        mgr, order = self._fresh()
        mgr.mark_rejected(order, "Insufficient margin")
        assert order.status == OrderStatusValue.REJECTED
        assert order.rejection_reason == "Insufficient margin"

    def test_mark_rejected_from_submitted(self):
        mgr, order = self._fresh()
        mgr.mark_submitted(order, "IBKR-1")
        mgr.mark_rejected(order, "Post-ack reject")
        assert order.status == OrderStatusValue.REJECTED
        assert order.rejection_reason == "Post-ack reject"

    def test_cannot_transition_out_of_filled(self):
        mgr, order = self._fresh()
        mgr.mark_submitted(order, "IBKR-1")
        mgr.record_fill(
            order,
            Fill(fill_timestamp=_now(), lots_filled=5, fill_price=9500.0),
        )
        assert order.status == OrderStatusValue.FILLED
        with pytest.raises(InvalidStateTransition):
            mgr.mark_cancelled(order)

    def test_cannot_transition_out_of_cancelled(self):
        mgr, order = self._fresh()
        mgr.mark_submitted(order, "IBKR-1")
        mgr.mark_cancelled(order)
        assert order.status == OrderStatusValue.CANCELLED
        assert order.cancelled_at is not None
        with pytest.raises(InvalidStateTransition):
            mgr.mark_rejected(order, "after cancel")

    def test_cannot_transition_out_of_rejected(self):
        mgr, order = self._fresh()
        mgr.mark_rejected(order, "gone")
        with pytest.raises(InvalidStateTransition):
            mgr.mark_submitted(order, "IBKR-zombie")


# ─────────────────────────────────────────────────────────────────────────────
# FILL RECORDING
# ─────────────────────────────────────────────────────────────────────────────


class TestFillRecording:
    def _submitted(self, **kwargs) -> tuple[OrderManager, Order]:
        mgr = OrderManager(engine=None)
        order = mgr.create("S4", "CA", Side.BUY, 5, OrderType.LIMIT_PASSIVE, 9500.0, **kwargs)
        mgr.mark_submitted(order, "IBKR-1")
        return mgr, order

    def test_full_fill_in_one_transition(self):
        mgr, order = self._submitted()
        mgr.record_fill(
            order,
            Fill(fill_timestamp=_now(), lots_filled=5, fill_price=9500.0),
        )
        assert order.status == OrderStatusValue.FILLED
        assert order.lots_filled == 5

    def test_partial_then_complete(self):
        mgr, order = self._submitted()
        mgr.record_fill(
            order,
            Fill(fill_timestamp=_now(), lots_filled=2, fill_price=9500.0),
        )
        assert order.status == OrderStatusValue.PARTIAL
        assert order.lots_filled == 2

        mgr.record_fill(
            order,
            Fill(fill_timestamp=_now(), lots_filled=3, fill_price=9505.0),
        )
        assert order.status == OrderStatusValue.FILLED
        assert order.lots_filled == 5
        assert order.avg_fill_price == pytest.approx((2 * 9500 + 3 * 9505) / 5)

    def test_partial_stays_partial_across_multiple_fills(self):
        mgr, order = self._submitted()
        mgr.record_fill(
            order,
            Fill(fill_timestamp=_now(), lots_filled=1, fill_price=9500.0),
        )
        mgr.record_fill(
            order,
            Fill(fill_timestamp=_now(), lots_filled=1, fill_price=9501.0),
        )
        mgr.record_fill(
            order,
            Fill(fill_timestamp=_now(), lots_filled=1, fill_price=9502.0),
        )
        assert order.status == OrderStatusValue.PARTIAL
        assert order.lots_filled == 3
        assert order.lots_remaining == 2

    def test_fill_on_pending_order_is_illegal(self):
        mgr = OrderManager(engine=None)
        order = mgr.create("S4", "CA", Side.BUY, 5, OrderType.LIMIT_PASSIVE, 9500.0)
        # Order is still PENDING, never submitted
        with pytest.raises(InvalidStateTransition, match="before submission"):
            mgr.record_fill(
                order,
                Fill(fill_timestamp=_now(), lots_filled=1, fill_price=9500.0),
            )

    def test_fill_on_terminal_order_is_illegal(self):
        mgr, order = self._submitted()
        mgr.mark_cancelled(order)
        with pytest.raises(InvalidStateTransition, match="terminal"):
            mgr.record_fill(
                order,
                Fill(fill_timestamp=_now(), lots_filled=1, fill_price=9500.0),
            )

    def test_fill_overfill_rejected(self):
        mgr, order = self._submitted()
        with pytest.raises(OrderValidationError, match="overfill"):
            mgr.record_fill(
                order,
                Fill(fill_timestamp=_now(), lots_filled=10, fill_price=9500.0),
            )

    def test_zero_lots_fill_rejected(self):
        mgr, order = self._submitted()
        with pytest.raises(OrderValidationError, match="lots_filled must be positive"):
            mgr.record_fill(
                order,
                Fill(fill_timestamp=_now(), lots_filled=0, fill_price=9500.0),
            )


# ─────────────────────────────────────────────────────────────────────────────
# SLIPPAGE COMPUTATION
# ─────────────────────────────────────────────────────────────────────────────


class TestSlippage:
    def _setup(self, side: Side, arrival_mid: float | None) -> tuple[OrderManager, Order]:
        mgr = OrderManager(engine=None)
        order = mgr.create(
            strategy_id="S4",
            ticker="CA",
            side=side,
            lots=1,
            order_type=OrderType.LIMIT_PASSIVE,
            limit_price=9500.0,
            arrival_mid_price=arrival_mid,
        )
        mgr.mark_submitted(order, "IBKR-1")
        return mgr, order

    def test_buy_above_mid_is_adverse(self):
        """Buy filled at 9510 when mid was 9500 → +10.51 bps adverse."""
        mgr, order = self._setup(Side.BUY, arrival_mid=9500.0)
        mgr.record_fill(
            order,
            Fill(fill_timestamp=_now(), lots_filled=1, fill_price=9510.0),
        )
        fill = order.fills[0]
        # (9510 - 9500) / 9500 * 10000 * (+1) = +10.526...
        assert fill.slippage_bps == pytest.approx(10.526, rel=1e-3)

    def test_sell_below_mid_is_adverse(self):
        """Sell filled at 9490 when mid was 9500 → +10.53 bps adverse."""
        mgr, order = self._setup(Side.SELL, arrival_mid=9500.0)
        mgr.record_fill(
            order,
            Fill(fill_timestamp=_now(), lots_filled=1, fill_price=9490.0),
        )
        fill = order.fills[0]
        # (9490 - 9500) / 9500 * 10000 * (-1) = +10.526...
        assert fill.slippage_bps == pytest.approx(10.526, rel=1e-3)

    def test_buy_below_mid_is_favourable(self):
        """Buy filled at 9490 when mid was 9500 → negative slippage."""
        mgr, order = self._setup(Side.BUY, arrival_mid=9500.0)
        mgr.record_fill(
            order,
            Fill(fill_timestamp=_now(), lots_filled=1, fill_price=9490.0),
        )
        fill = order.fills[0]
        assert fill.slippage_bps is not None
        assert fill.slippage_bps < 0

    def test_slippage_none_when_no_arrival_mid(self):
        mgr, order = self._setup(Side.BUY, arrival_mid=None)
        mgr.record_fill(
            order,
            Fill(fill_timestamp=_now(), lots_filled=1, fill_price=9510.0),
        )
        assert order.fills[0].slippage_bps is None

    def test_slippage_preserved_if_already_set(self):
        """If caller pre-computed slippage, manager shouldn't overwrite."""
        mgr, order = self._setup(Side.BUY, arrival_mid=9500.0)
        mgr.record_fill(
            order,
            Fill(
                fill_timestamp=_now(),
                lots_filled=1,
                fill_price=9510.0,
                slippage_bps=42.0,  # implausible pre-set value
            ),
        )
        assert order.fills[0].slippage_bps == 42.0


# ─────────────────────────────────────────────────────────────────────────────
# DB PERSISTENCE (with mock engine)
# ─────────────────────────────────────────────────────────────────────────────


class TestDBPersistence:
    """
    Smoke-test that the right DB operations are attempted with correct params.
    Full integration against a real Postgres is deferred to the paper-DB setup.
    """

    def _mock_engine(self, returned_id: int = 42) -> MagicMock:
        """Build a Mock engine that supports: engine.begin() context → execute()."""
        engine = MagicMock()
        conn = MagicMock()
        result = MagicMock()
        result.scalar_one.return_value = returned_id
        conn.execute.return_value = result
        # begin() is a context manager
        cm = MagicMock()
        cm.__enter__ = MagicMock(return_value=conn)
        cm.__exit__ = MagicMock(return_value=False)
        engine.begin.return_value = cm
        return engine

    def test_create_writes_to_order_log(self):
        engine = self._mock_engine(returned_id=100)
        mgr = OrderManager(engine=engine)
        order = mgr.create("S4", "CA", Side.BUY, 2, OrderType.LIMIT_PASSIVE, 9500.0)
        # create() does: INSERT order_log (with RETURNING) + INSERT audit event
        # engine.begin() should have been called at least twice
        assert engine.begin.call_count >= 2
        assert order.db_id == 100

    def test_submit_triggers_update_and_audit(self):
        engine = self._mock_engine()
        mgr = OrderManager(engine=engine)
        order = mgr.create("S4", "CA", Side.BUY, 2, OrderType.MARKET, None)
        engine.begin.reset_mock()
        mgr.mark_submitted(order, "IBKR-99")
        # Expect: UPDATE order_log + INSERT audit event
        assert engine.begin.call_count >= 2

    def test_fill_triggers_fill_log_and_update(self):
        engine = self._mock_engine()
        mgr = OrderManager(engine=engine)
        order = mgr.create("S4", "CA", Side.BUY, 1, OrderType.MARKET, None)
        mgr.mark_submitted(order, "IBKR-1")
        engine.begin.reset_mock()
        mgr.record_fill(
            order,
            Fill(
                fill_timestamp=_now(),
                lots_filled=1,
                fill_price=9500.0,
                commission_usd=3.0,
            ),
        )
        # Expect: INSERT fill_log + audit + UPDATE order_log
        assert engine.begin.call_count >= 3

    def test_db_failure_does_not_raise(self):
        """DB errors are logged but never propagate — in-memory state is truth."""
        engine = MagicMock()
        engine.begin.side_effect = RuntimeError("DB down")
        mgr = OrderManager(engine=engine)
        # Should not raise
        order = mgr.create("S4", "CA", Side.BUY, 1, OrderType.MARKET, None)
        assert order.status == OrderStatusValue.PENDING
        assert order.db_id is None  # Never assigned because INSERT failed

    def test_engine_none_skips_all_writes(self):
        mgr = OrderManager(engine=None)
        order = mgr.create("S4", "CA", Side.BUY, 1, OrderType.MARKET, None)
        mgr.mark_submitted(order, "IBKR-1")
        mgr.record_fill(
            order,
            Fill(fill_timestamp=_now(), lots_filled=1, fill_price=100.0),
        )
        # All state transitions succeeded without a DB
        assert order.status == OrderStatusValue.FILLED


# ─────────────────────────────────────────────────────────────────────────────
# AUDIT SEVERITY ENUM
# ─────────────────────────────────────────────────────────────────────────────


class TestAuditSeverity:
    def test_values_match_schema_comment(self):
        # audit.system_events.severity comment: "info", "warning", "critical", "kill"
        assert AuditSeverity.INFO.value == "info"
        assert AuditSeverity.WARNING.value == "warning"
        assert AuditSeverity.CRITICAL.value == "critical"
        assert AuditSeverity.KILL.value == "kill"
