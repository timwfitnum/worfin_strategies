"""
tests/test_execution/test_engine.py
Unit tests for ExecutionEngine.

Strategy:
  • ib_insync is stubbed at import time (no broker required).
  • The broker is mocked throughout — no real IBKR connection.
  • The DB engine is either None (data-load tests skipped) or a MagicMock
    where we verify the right SQL is called with the right parameters.
  • Tests focus on logic that's deterministic given inputs:
      - Constants / enum round-trips
      - Price-for-step mapping (passive / aggressive / market)
      - Quote dataclass semantics (mid, is_live)
      - ReconciliationResult.is_clean
      - Rebalance cadence (should_rebalance)
      - Escalation control flow with a mocked broker + order manager
"""

from __future__ import annotations

import sys
import types
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# Stub ib_insync before any engine import
if "ib_insync" not in sys.modules:
    stub = types.ModuleType("ib_insync")
    stub.IB = MagicMock
    stub.Contract = MagicMock
    stub.Future = MagicMock
    stub.LimitOrder = MagicMock
    stub.MarketOrder = MagicMock
    stub.Trade = MagicMock
    sys.modules["ib_insync"] = stub


from worfin.execution.broker import OrderStatusValue, OrderType, Quote, Side  # noqa: E402
from worfin.execution.engine import (  # noqa: E402
    ESCALATION_WAIT_SECONDS,
    PARTIAL_FILL_THRESHOLD,
    RECONCILIATION_TOLERANCE_LOTS,
    CycleResult,
    ExecutionEngine,
    ReconciliationResult,
    StrategyRebalanceResult,
)

# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTS & INVARIANTS
# ─────────────────────────────────────────────────────────────────────────────


class TestConstants:
    def test_partial_fill_threshold_is_30pct(self):
        # execution/CLAUDE.md: if unfilled_pct < 0.30 → leave; >= 0.30 → escalate
        assert PARTIAL_FILL_THRESHOLD == 0.30

    def test_escalation_wait_is_60s(self):
        # execution/CLAUDE.md: "Step 1: Passive limit (60 seconds)"
        assert ESCALATION_WAIT_SECONDS == 60

    def test_reconciliation_tolerance_is_1_lot(self):
        # execution/CLAUDE.md: "±1 lot tolerance for rounding"
        assert RECONCILIATION_TOLERANCE_LOTS == 1


# ─────────────────────────────────────────────────────────────────────────────
# QUOTE DATACLASS
# ─────────────────────────────────────────────────────────────────────────────


class TestQuote:
    def _now(self):
        return datetime.now(UTC)

    def test_mid_is_average_of_bid_and_offer(self):
        q = Quote(ticker="CA", timestamp=self._now(), bid=9500.0, offer=9510.0)
        assert q.mid == 9505.0

    def test_mid_is_none_when_bid_missing(self):
        q = Quote(ticker="CA", timestamp=self._now(), bid=None, offer=9510.0)
        assert q.mid is None

    def test_mid_is_none_when_offer_missing(self):
        q = Quote(ticker="CA", timestamp=self._now(), bid=9500.0, offer=None)
        assert q.mid is None

    def test_mid_is_none_on_zero_prices(self):
        q = Quote(ticker="CA", timestamp=self._now(), bid=0.0, offer=9510.0)
        assert q.mid is None

    def test_is_live_requires_both_sides(self):
        live = Quote(ticker="CA", timestamp=self._now(), bid=9500, offer=9510)
        dead = Quote(ticker="CA", timestamp=self._now(), bid=None, offer=9510)
        assert live.is_live
        assert not dead.is_live

    def test_is_live_rejects_zero_prices(self):
        q = Quote(ticker="CA", timestamp=self._now(), bid=0.0, offer=9510)
        assert not q.is_live


# ─────────────────────────────────────────────────────────────────────────────
# PRICE-FOR-STEP MAPPING
# ─────────────────────────────────────────────────────────────────────────────


class TestPriceForStep:
    def _quote(self, bid=9500.0, offer=9510.0):
        return Quote(ticker="CA", timestamp=datetime.now(UTC), bid=bid, offer=offer)

    def test_passive_uses_mid(self):
        q = self._quote()
        p = ExecutionEngine._price_for_step(OrderType.LIMIT_PASSIVE, Side.BUY, q)
        assert p == 9505.0

    def test_aggressive_buy_crosses_at_offer(self):
        q = self._quote()
        p = ExecutionEngine._price_for_step(OrderType.LIMIT_AGGRESSIVE, Side.BUY, q)
        assert p == 9510.0

    def test_aggressive_sell_crosses_at_bid(self):
        q = self._quote()
        p = ExecutionEngine._price_for_step(OrderType.LIMIT_AGGRESSIVE, Side.SELL, q)
        assert p == 9500.0

    def test_market_returns_none(self):
        q = self._quote()
        p = ExecutionEngine._price_for_step(OrderType.MARKET, Side.BUY, q)
        assert p is None


# ─────────────────────────────────────────────────────────────────────────────
# RECONCILIATION MATH
# ─────────────────────────────────────────────────────────────────────────────


class TestReconciliation:
    def test_clean_when_targets_match(self):
        recon = ReconciliationResult(
            targets={"CA": 2, "AH": 1},
            actual={"CA": 2, "AH": 1},
            mismatches={},
        )
        assert recon.is_clean

    def test_not_clean_when_mismatch(self):
        recon = ReconciliationResult(
            targets={"CA": 2},
            actual={"CA": 5},
            mismatches={"CA": (2, 5, 3)},
        )
        assert not recon.is_clean


# ─────────────────────────────────────────────────────────────────────────────
# CYCLE RESULT AGGREGATION
# ─────────────────────────────────────────────────────────────────────────────


class TestStrategyRebalanceResult:
    def test_skipped_result(self):
        srr = StrategyRebalanceResult.skipped_result("S4", "no_signals")
        assert srr.strategy_id == "S4"
        assert srr.skipped
        assert srr.skip_reason == "no_signals"
        assert srr.n_orders == 0
        assert srr.n_filled == 0
        assert srr.n_rejected == 0

    def test_counts_reflect_order_statuses(self):
        order_a = MagicMock()
        order_a.status = OrderStatusValue.FILLED
        order_b = MagicMock()
        order_b.status = OrderStatusValue.FILLED
        order_c = MagicMock()
        order_c.status = OrderStatusValue.REJECTED
        order_d = MagicMock()
        order_d.status = OrderStatusValue.CANCELLED

        srr = StrategyRebalanceResult(
            strategy_id="S4",
            skipped=False,
            orders=[order_a, order_b, order_c, order_d],
        )
        assert srr.n_orders == 4
        assert srr.n_filled == 2
        assert srr.n_rejected == 1


class TestCycleResult:
    def test_duration_computed_when_end_set(self):
        start = datetime.now(UTC)
        result = CycleResult(
            correlation_id="abc",
            cycle_start=start,
            cycle_end=start + timedelta(seconds=42),
        )
        assert result.duration_seconds == 42.0

    def test_duration_none_while_in_flight(self):
        result = CycleResult(correlation_id="abc", cycle_start=datetime.now(UTC))
        assert result.duration_seconds is None


# ─────────────────────────────────────────────────────────────────────────────
# ENGINE INSTANTIATION
# ─────────────────────────────────────────────────────────────────────────────


class TestEngineInit:
    def test_requires_at_least_one_strategy(self):
        fake_db = MagicMock()
        with pytest.raises(ValueError, match="at least one strategy"):
            ExecutionEngine(strategies=[], db_engine=fake_db)

    def test_accepts_explicit_trading_capital(self):
        fake_db = MagicMock()
        fake_strategy = MagicMock()
        fake_strategy.strategy_id = "S4"
        engine = ExecutionEngine(
            strategies=[fake_strategy],
            db_engine=fake_db,
            broker=MagicMock(),
            trading_capital_gbp=50_000.0,
        )
        assert engine._nav_gbp == 50_000.0


# ─────────────────────────────────────────────────────────────────────────────
# REBALANCE CADENCE
# ─────────────────────────────────────────────────────────────────────────────


class TestRebalanceCadence:
    def _engine_with_last_rebalance(self, days_ago: int | None):
        fake_db = MagicMock()
        fake_strategy = MagicMock()
        fake_strategy.strategy_id = "S4"
        fake_strategy.config.rebalance_every_n_days = 10
        engine = ExecutionEngine(
            strategies=[fake_strategy],
            db_engine=fake_db,
            broker=MagicMock(),
        )
        last = datetime.now(UTC) - timedelta(days=days_ago) if days_ago is not None else None
        engine._last_rebalance = MagicMock(return_value=last)
        return engine, fake_strategy

    def test_rebalances_when_no_prior_rebalance(self):
        engine, strategy = self._engine_with_last_rebalance(None)
        assert engine._should_rebalance(strategy) is True

    def test_skips_when_not_yet_due(self):
        # rebalance_every_n_days=10, last=3 days ago → skip
        engine, strategy = self._engine_with_last_rebalance(3)
        assert engine._should_rebalance(strategy) is False

    def test_rebalances_when_exactly_due(self):
        engine, strategy = self._engine_with_last_rebalance(10)
        assert engine._should_rebalance(strategy) is True

    def test_rebalances_when_overdue(self):
        engine, strategy = self._engine_with_last_rebalance(25)
        assert engine._should_rebalance(strategy) is True

    def test_daily_strategy_always_rebalances(self):
        """rebalance_every_n_days <= 1 means every cycle."""
        fake_db = MagicMock()
        fake_strategy = MagicMock()
        fake_strategy.strategy_id = "S_DAILY"
        fake_strategy.config.rebalance_every_n_days = 1
        engine = ExecutionEngine(
            strategies=[fake_strategy],
            db_engine=fake_db,
            broker=MagicMock(),
        )
        # Even if last_rebalance is 0 days ago, daily strategy rebalances
        engine._last_rebalance = MagicMock(return_value=datetime.now(UTC))
        assert engine._should_rebalance(fake_strategy) is True


# ─────────────────────────────────────────────────────────────────────────────
# ESCALATION CONTROL FLOW
# ─────────────────────────────────────────────────────────────────────────────


class TestEscalationFlow:
    """
    Tests the escalation-step decision logic in _submit_with_escalation.
    _try_order_step is mocked out so we verify ONLY the control flow:
    which steps run, when we escalate, when we leave unfilled.
    """

    def _make_engine(self) -> ExecutionEngine:
        fake_db = MagicMock()
        fake_strategy = MagicMock()
        fake_strategy.strategy_id = "S4"
        engine = ExecutionEngine(
            strategies=[fake_strategy],
            db_engine=fake_db,
            broker=MagicMock(),
        )
        # Always return a live quote
        engine._broker.get_quote = AsyncMock(
            return_value=Quote(
                ticker="CA",
                timestamp=datetime.now(UTC),
                bid=9500.0,
                offer=9510.0,
            )
        )
        return engine

    @pytest.mark.asyncio
    async def test_full_fill_on_step_1_stops(self):
        """If passive limit fills completely, no escalation."""
        engine = self._make_engine()

        async def fake_try(order, *args, **kwargs):
            # Simulate full fill on the first call
            order.status = OrderStatusValue.FILLED
            order.fills.append(MagicMock(lots_filled=5, fill_price=9505.0))

        # Replace the order manager's create to return mutable mock orders
        engine._order_mgr.create = MagicMock(
            side_effect=lambda **kw: MagicMock(
                ticker=kw["ticker"],
                side=kw["side"],
                lots_requested=kw["lots"],
                order_type=kw["order_type"],
                limit_price=kw["limit_price"],
                status=OrderStatusValue.PENDING,
                internal_id=f"id-{kw['order_type'].value}",
                lots_filled=0,
                fills=[],
                is_terminal=False,
            )
        )

        with patch.object(engine, "_try_order_step", side_effect=fake_try) as mock_try:
            # Make lots_filled reflect fills (after fake_try mutates)
            def make_order_with_fill(**kw):
                o = MagicMock()
                o.ticker = kw["ticker"]
                o.side = kw["side"]
                o.lots_requested = kw["lots"]
                o.order_type = kw["order_type"]
                o.limit_price = kw["limit_price"]
                o.status = OrderStatusValue.PENDING
                o.internal_id = f"id-{kw['order_type'].value}"
                o.lots_filled = 0
                o.fills = []
                return o

            engine._order_mgr.create = MagicMock(side_effect=make_order_with_fill)

            # Update fake_try to set lots_filled properly
            async def fake_try_v2(order, *a, **kw):
                order.status = OrderStatusValue.FILLED
                order.lots_filled = 5

            mock_try.side_effect = fake_try_v2

            orders = await engine._submit_with_escalation(
                strategy_id="S4",
                ticker="CA",
                side=Side.BUY,
                lots=5,
                signal_direction=1,
                signal_timestamp=datetime.now(UTC),
                portfolio=MagicMock(),
                usd_gbp=1.27,
            )
        # Exactly one order — no escalation
        assert len(orders) == 1
        assert mock_try.call_count == 1

    @pytest.mark.asyncio
    async def test_escalates_past_step_1_when_mostly_unfilled(self):
        """If <30% filled after step 1, escalate to aggressive limit."""
        engine = self._make_engine()

        # Each call fills 1 lot of 10 → 90% unfilled on step 1, then
        # aggressive also 1 lot → 80% cumulatively unfilled → escalate to market
        fills_seq = [1, 1, 1]  # step 1, 2, 3
        fills_iter = iter(fills_seq)

        def make_order(**kw):
            o = MagicMock()
            o.ticker = kw["ticker"]
            o.side = kw["side"]
            o.lots_requested = kw["lots"]
            o.order_type = kw["order_type"]
            o.limit_price = kw["limit_price"]
            o.status = OrderStatusValue.PENDING
            o.internal_id = f"id-{kw['order_type'].value}"
            o.lots_filled = 0
            return o

        engine._order_mgr.create = MagicMock(side_effect=make_order)

        async def fake_try(order, *a, **kw):
            order.status = OrderStatusValue.PARTIAL
            order.lots_filled = next(fills_iter)

        with patch.object(engine, "_try_order_step", side_effect=fake_try) as mock_try:
            orders = await engine._submit_with_escalation(
                strategy_id="S4",
                ticker="CA",
                side=Side.BUY,
                lots=10,
                signal_direction=1,
                signal_timestamp=datetime.now(UTC),
                portfolio=MagicMock(),
                usd_gbp=1.27,
            )
        # Should try all 3 steps because each leaves >30% unfilled
        assert len(orders) == 3
        assert mock_try.call_count == 3
        # Order types should be passive, aggressive, market
        assert orders[0].order_type == OrderType.LIMIT_PASSIVE
        assert orders[1].order_type == OrderType.LIMIT_AGGRESSIVE
        assert orders[2].order_type == OrderType.MARKET

    @pytest.mark.asyncio
    async def test_leaves_unfilled_when_below_threshold(self):
        """If <30% unfilled after step 1 — leave the remainder, skip escalation."""
        engine = self._make_engine()

        def make_order(**kw):
            o = MagicMock()
            o.ticker = kw["ticker"]
            o.side = kw["side"]
            o.lots_requested = kw["lots"]
            o.order_type = kw["order_type"]
            o.limit_price = kw["limit_price"]
            o.status = OrderStatusValue.PENDING
            o.internal_id = f"id-{kw['order_type'].value}"
            o.lots_filled = 0
            return o

        engine._order_mgr.create = MagicMock(side_effect=make_order)

        # Step 1 fills 8 of 10 → 20% unfilled → below 30% threshold → leave
        async def fake_try(order, *a, **kw):
            order.status = OrderStatusValue.PARTIAL
            order.lots_filled = 8

        with patch.object(engine, "_try_order_step", side_effect=fake_try) as mock_try:
            orders = await engine._submit_with_escalation(
                strategy_id="S4",
                ticker="CA",
                side=Side.BUY,
                lots=10,
                signal_direction=1,
                signal_timestamp=datetime.now(UTC),
                portfolio=MagicMock(),
                usd_gbp=1.27,
            )
        assert len(orders) == 1  # Only step 1 ran
        assert mock_try.call_count == 1

    @pytest.mark.asyncio
    async def test_rejection_stops_escalation(self):
        """If an order is rejected, no further steps are attempted."""
        engine = self._make_engine()

        def make_order(**kw):
            o = MagicMock()
            o.ticker = kw["ticker"]
            o.side = kw["side"]
            o.lots_requested = kw["lots"]
            o.order_type = kw["order_type"]
            o.limit_price = kw["limit_price"]
            o.status = OrderStatusValue.PENDING
            o.internal_id = f"id-{kw['order_type'].value}"
            o.lots_filled = 0
            return o

        engine._order_mgr.create = MagicMock(side_effect=make_order)

        async def fake_try(order, *a, **kw):
            order.status = OrderStatusValue.REJECTED
            order.lots_filled = 0

        with patch.object(engine, "_try_order_step", side_effect=fake_try) as mock_try:
            orders = await engine._submit_with_escalation(
                strategy_id="S4",
                ticker="CA",
                side=Side.BUY,
                lots=10,
                signal_direction=1,
                signal_timestamp=datetime.now(UTC),
                portfolio=MagicMock(),
                usd_gbp=1.27,
            )
        assert len(orders) == 1
        assert orders[0].status == OrderStatusValue.REJECTED
        assert mock_try.call_count == 1


# ─────────────────────────────────────────────────────────────────────────────
# MULTI-STRATEGY GUARD
# ─────────────────────────────────────────────────────────────────────────────


class TestMultiStrategyGuard:
    @pytest.mark.asyncio
    async def test_multi_strategy_raises_not_implemented(self):
        s1 = MagicMock()
        s1.strategy_id = "S1"
        s4 = MagicMock()
        s4.strategy_id = "S4"
        engine = ExecutionEngine(
            strategies=[s1, s4],
            db_engine=MagicMock(),
            broker=MagicMock(),
        )
        with pytest.raises(NotImplementedError, match="Multi-strategy"):
            await engine.run_cycle()
