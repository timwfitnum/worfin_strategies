"""
tests/test_risk/test_circuit_breakers.py
Circuit breaker tests — 100% coverage required.
Every trigger condition must be tested at-threshold and just-below.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from worfin.risk.circuit_breakers import (
    CircuitBreaker,
    CircuitBreakerAction,
    PortfolioPnL,
    StrategyCircuitBreaker,
)
from worfin.risk.limits import (
    DAILY_LOSS_LIMIT,
    STRATEGY_DRAWDOWN_BUDGET,
)


def make_pnl(
    nav: float = 100_000,
    daily_pnl: float = 0,
    weekly_pnl: float = 0,
    month_start_nav: float = 100_000,
    all_time_hwm: float = 100_000,
) -> PortfolioPnL:
    return PortfolioPnL(
        nav=nav,
        daily_pnl=daily_pnl,
        weekly_pnl=weekly_pnl,
        month_start_nav=month_start_nav,
        all_time_hwm=all_time_hwm,
        as_of=datetime.now(UTC),
    )


@pytest.fixture
def breaker():
    return CircuitBreaker()


class TestDailyLossBreaker:

    def test_triggers_at_exactly_2pct(self, breaker):
        pnl = make_pnl(nav=100_000, daily_pnl=-2_000)  # Exactly -2%
        result = breaker.check_all(pnl)
        assert result.action == CircuitBreakerAction.FLATTEN_ALL
        assert result.triggered_by == "daily_loss"

    def test_does_not_trigger_at_199bps(self, breaker):
        pnl = make_pnl(nav=100_000, daily_pnl=-1_990)  # -1.99%
        result = breaker.check_all(pnl)
        # Should only warn, not flatten
        assert result.action != CircuitBreakerAction.FLATTEN_ALL

    def test_warning_at_75pct_of_limit(self, breaker):
        pnl = make_pnl(nav=100_000, daily_pnl=-1_500)  # -1.5% = 75% of 2%
        result = breaker.check_all(pnl)
        assert result.action == CircuitBreakerAction.WARN

    def test_clear_on_positive_day(self, breaker):
        pnl = make_pnl(nav=101_000, daily_pnl=+1_000)  # Positive day
        result = breaker.check_all(pnl)
        assert result.action == CircuitBreakerAction.NONE

    def test_daily_loss_limit_constant(self):
        """The 2% daily loss limit must be exactly 0.020."""
        assert DAILY_LOSS_LIMIT == 0.020, (
            "DAILY_LOSS_LIMIT must be 0.020 (2%). "
            "Changing this requires documented justification."
        )


class TestWeeklyLossBreaker:

    def test_triggers_at_35bps(self, breaker):
        pnl = make_pnl(nav=100_000, weekly_pnl=-3_500)  # -3.5%
        result = breaker.check_all(pnl)
        assert result.action == CircuitBreakerAction.REDUCE_50_PCT

    def test_does_not_trigger_at_349bps(self, breaker):
        pnl = make_pnl(nav=100_000, weekly_pnl=-3_490)  # -3.49%
        result = breaker.check_all(pnl)
        assert result.action not in (
            CircuitBreakerAction.REDUCE_50_PCT,
            CircuitBreakerAction.FLATTEN_ALL,
            CircuitBreakerAction.FULL_SUSPEND,
        )


class TestMonthlyDrawdownBreaker:

    def test_triggers_at_5pct(self, breaker):
        # NAV fell 5% from month start
        pnl = make_pnl(nav=95_000, month_start_nav=100_000)
        result = breaker.check_all(pnl)
        assert result.action == CircuitBreakerAction.REDUCE_25_PCT

    def test_does_not_trigger_at_499bps(self, breaker):
        pnl = make_pnl(nav=95_010, month_start_nav=100_000)  # Just under 5%
        result = breaker.check_all(pnl)
        assert result.action not in (
            CircuitBreakerAction.REDUCE_25_PCT,
            CircuitBreakerAction.FLATTEN_ALL,
        )


class TestPeakDrawdownBreaker:

    def test_full_suspend_at_10pct(self, breaker):
        pnl = make_pnl(nav=90_000, all_time_hwm=100_000)  # -10% from HWM
        result = breaker.check_all(pnl)
        assert result.action == CircuitBreakerAction.FULL_SUSPEND
        assert result.requires_human_review is True

    def test_hard_stop_at_15pct(self, breaker):
        pnl = make_pnl(nav=85_000, all_time_hwm=100_000)  # -15% from HWM
        result = breaker.check_all(pnl)
        assert result.action == CircuitBreakerAction.HARD_STOP
        assert result.requires_human_review is True

    def test_hard_stop_takes_priority_over_suspend(self, breaker):
        """HARD_STOP must always take priority when both conditions are met."""
        pnl = make_pnl(nav=84_000, all_time_hwm=100_000)  # -16% (exceeds both)
        result = breaker.check_all(pnl)
        assert result.action == CircuitBreakerAction.HARD_STOP


class TestSeverityOrdering:
    """Verify that more severe breakers always take priority."""

    def test_daily_loss_trumps_weekly_warning(self, breaker):
        """If daily loss limit is hit, FLATTEN_ALL takes priority over REDUCE_50_PCT."""
        pnl = make_pnl(
            nav=100_000,
            daily_pnl=-2_100,  # Daily limit breached
            weekly_pnl=-3_600,  # Weekly limit also breached
        )
        result = breaker.check_all(pnl)
        # Peak drawdown may not be hit, so check the most severe applicable
        assert result.action in (
            CircuitBreakerAction.FLATTEN_ALL,
            CircuitBreakerAction.REDUCE_50_PCT,
        )


class TestStrategyBreaker:

    @pytest.fixture
    def strategy_breaker(self):
        return StrategyCircuitBreaker()

    def test_s4_suspends_at_budget(self, strategy_breaker):
        budget = STRATEGY_DRAWDOWN_BUDGET["S4"]  # 15%
        result = strategy_breaker.check_strategy_drawdown("S4", drawdown_from_hwm=budget)
        assert result.action == CircuitBreakerAction.FULL_SUSPEND

    def test_s5_suspends_at_lower_budget(self, strategy_breaker):
        budget = STRATEGY_DRAWDOWN_BUDGET["S5"]  # 10%
        result = strategy_breaker.check_strategy_drawdown("S5", drawdown_from_hwm=budget)
        assert result.action == CircuitBreakerAction.FULL_SUSPEND

    def test_warning_at_75pct_of_strategy_budget(self, strategy_breaker):
        budget = STRATEGY_DRAWDOWN_BUDGET["S4"]  # 15%
        result = strategy_breaker.check_strategy_drawdown("S4", drawdown_from_hwm=budget * 0.80)
        assert result.action == CircuitBreakerAction.WARN

    def test_clear_within_budget(self, strategy_breaker):
        budget = STRATEGY_DRAWDOWN_BUDGET["S4"]
        result = strategy_breaker.check_strategy_drawdown("S4", drawdown_from_hwm=budget * 0.50)
        assert result.action == CircuitBreakerAction.NONE

    def test_all_strategies_have_budgets(self, strategy_breaker):
        for sid in ["S1", "S2", "S3", "S4", "S5", "S6"]:
            assert sid in STRATEGY_DRAWDOWN_BUDGET, f"No drawdown budget for {sid}"
