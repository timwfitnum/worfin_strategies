"""
risk/circuit_breakers.py
Automated portfolio circuit breakers.

ARCHITECTURE: This module runs as a COMPLETELY SEPARATE PROCESS from the
signal engine. It cannot be blocked, overridden, or silenced by strategy code.

Circuit breakers check P&L and positions every 60 seconds during trading hours.
All actions are logged to audit.risk_breaches with full context.

The kill switch must work even if the main execution process is hung.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum

from worfin.risk.limits import (
    DAILY_FLATTEN_REINSTATEMENT_PCT,
    DAILY_LOSS_LIMIT,
    HARD_STOP_DRAWDOWN,
    MONTHLY_DRAWDOWN_LIMIT,
    PEAK_DRAWDOWN_COOL_OFF_DAYS,
    PEAK_DRAWDOWN_RESTART_PCT,
    PEAK_DRAWDOWN_SUSPEND,
    STRATEGY_DRAWDOWN_BUDGET,
    STRATEGY_RESTART_PCT,
    STRATEGY_SCALE_UP_DAYS,
    WEEKLY_LOSS_LIMIT,
    WEEKLY_SCALE_UP_PER_DAY,
)

logger = logging.getLogger(__name__)


class CircuitBreakerAction(str, Enum):
    """Actions the circuit breaker can trigger."""
    NONE = "NONE"                    # All clear — no action
    WARN = "WARN"                    # Warning — approaching limit
    REDUCE_50_PCT = "REDUCE_50_PCT"  # Reduce all positions to 50%
    REDUCE_25_PCT = "REDUCE_25_PCT"  # Reduce all positions to 25%
    FLATTEN_ALL = "FLATTEN_ALL"      # Flatten all positions immediately
    FULL_SUSPEND = "FULL_SUSPEND"    # Full suspension — human review required
    HARD_STOP = "HARD_STOP"         # Full liquidation + system shutdown


@dataclass
class CircuitBreakerResult:
    """Result of a circuit breaker check."""
    action: CircuitBreakerAction
    triggered_by: str          # Which limit was breached
    threshold: float           # The limit value
    current_value: float       # The current P&L / drawdown value
    severity: str              # "warning" | "breach"
    message: str
    timestamp: datetime
    requires_human_review: bool = False

    @property
    def is_action_required(self) -> bool:
        return self.action != CircuitBreakerAction.NONE


@dataclass
class PortfolioPnL:
    """Current P&L state for circuit breaker evaluation."""
    nav: float                    # Current NAV in GBP
    daily_pnl: float              # Today's P&L in GBP
    weekly_pnl: float             # This week's P&L in GBP
    month_start_nav: float        # NAV at start of current calendar month
    all_time_hwm: float           # All-time high watermark in GBP
    as_of: datetime

    @property
    def daily_pnl_pct(self) -> float:
        return self.daily_pnl / self.nav if self.nav > 0 else 0.0

    @property
    def weekly_pnl_pct(self) -> float:
        return self.weekly_pnl / self.nav if self.nav > 0 else 0.0

    @property
    def monthly_drawdown_pct(self) -> float:
        if self.month_start_nav <= 0:
            return 0.0
        return (self.month_start_nav - self.nav) / self.month_start_nav

    @property
    def peak_drawdown_pct(self) -> float:
        if self.all_time_hwm <= 0:
            return 0.0
        return (self.all_time_hwm - self.nav) / self.all_time_hwm


class CircuitBreaker:
    """
    Portfolio-level circuit breakers.

    Call check_all() every 60 seconds during trading hours.
    The returned action MUST be executed immediately and unconditionally.
    """

    def check_all(self, pnl: PortfolioPnL) -> CircuitBreakerResult:
        """
        Run all circuit breaker checks in severity order.
        Returns the most severe action required.

        Priority: HARD_STOP > FULL_SUSPEND > FLATTEN_ALL > REDUCE_25_PCT > REDUCE_50_PCT > WARN
        """
        checks = [
            self._check_hard_stop,
            self._check_peak_drawdown,
            self._check_monthly_drawdown,
            self._check_weekly_loss,
            self._check_daily_loss,
            self._check_daily_loss_warning,
        ]

        for check in checks:
            result = check(pnl)
            if result.is_action_required:
                self._log_result(result)
                return result

        return CircuitBreakerResult(
            action=CircuitBreakerAction.NONE,
            triggered_by="none",
            threshold=0.0,
            current_value=0.0,
            severity="ok",
            message="All circuit breakers clear.",
            timestamp=pnl.as_of,
        )

    def _check_hard_stop(self, pnl: PortfolioPnL) -> CircuitBreakerResult:
        """
        15% drawdown from all-time HWM.
        FULL LIQUIDATION. System shutdown. 3-month paper trading before redeployment.
        """
        dd = pnl.peak_drawdown_pct
        if dd >= HARD_STOP_DRAWDOWN:
            return CircuitBreakerResult(
                action=CircuitBreakerAction.HARD_STOP,
                triggered_by="peak_drawdown_hard_stop",
                threshold=HARD_STOP_DRAWDOWN,
                current_value=dd,
                severity="breach",
                message=(
                    f"HARD STOP: Peak drawdown {dd:.1%} >= {HARD_STOP_DRAWDOWN:.1%}. "
                    f"FULL LIQUIDATION. All systems shutting down. "
                    f"3-month paper trading required before redeployment."
                ),
                timestamp=pnl.as_of,
                requires_human_review=True,
            )
        return self._no_action(pnl.as_of)

    def _check_peak_drawdown(self, pnl: PortfolioPnL) -> CircuitBreakerResult:
        """
        10% drawdown from all-time HWM.
        Full suspension. Flatten to cash. Formal review required.
        Min 10 trading day cooling period. Restart at 25%, scale over 40 days.
        """
        dd = pnl.peak_drawdown_pct
        if dd >= PEAK_DRAWDOWN_SUSPEND:
            return CircuitBreakerResult(
                action=CircuitBreakerAction.FULL_SUSPEND,
                triggered_by="peak_drawdown_suspension",
                threshold=PEAK_DRAWDOWN_SUSPEND,
                current_value=dd,
                severity="breach",
                message=(
                    f"FULL SUSPENSION: Peak drawdown {dd:.1%} >= {PEAK_DRAWDOWN_SUSPEND:.1%}. "
                    f"Flatten all positions to cash. Formal review required. "
                    f"Min {PEAK_DRAWDOWN_COOL_OFF_DAYS} trading day cooling period. "
                    f"Restart at {PEAK_DRAWDOWN_RESTART_PCT:.0%}."
                ),
                timestamp=pnl.as_of,
                requires_human_review=True,
            )
        return self._no_action(pnl.as_of)

    def _check_monthly_drawdown(self, pnl: PortfolioPnL) -> CircuitBreakerResult:
        """
        5% drawdown from month-start NAV.
        Reduce all positions to 25%. No new entries. Hold until next calendar month.
        """
        dd = pnl.monthly_drawdown_pct
        if dd >= MONTHLY_DRAWDOWN_LIMIT:
            return CircuitBreakerResult(
                action=CircuitBreakerAction.REDUCE_25_PCT,
                triggered_by="monthly_drawdown",
                threshold=MONTHLY_DRAWDOWN_LIMIT,
                current_value=dd,
                severity="breach",
                message=(
                    f"MONTHLY DRAWDOWN: {dd:.1%} >= {MONTHLY_DRAWDOWN_LIMIT:.1%}. "
                    f"Reducing all positions to 25% of target. "
                    f"No new entries until next calendar month."
                ),
                timestamp=pnl.as_of,
            )
        return self._no_action(pnl.as_of)

    def _check_weekly_loss(self, pnl: PortfolioPnL) -> CircuitBreakerResult:
        """
        3.5% weekly loss.
        Reduce to 50%. No new entries. Scale back +10% per day.
        """
        loss = pnl.weekly_pnl_pct
        if loss <= -WEEKLY_LOSS_LIMIT:
            return CircuitBreakerResult(
                action=CircuitBreakerAction.REDUCE_50_PCT,
                triggered_by="weekly_loss",
                threshold=WEEKLY_LOSS_LIMIT,
                current_value=abs(loss),
                severity="breach",
                message=(
                    f"WEEKLY LOSS: {abs(loss):.1%} >= {WEEKLY_LOSS_LIMIT:.1%}. "
                    f"Reducing all positions to 50%. No new entries. "
                    f"Scale back +{WEEKLY_SCALE_UP_PER_DAY:.0%}/day."
                ),
                timestamp=pnl.as_of,
            )
        return self._no_action(pnl.as_of)

    def _check_daily_loss(self, pnl: PortfolioPnL) -> CircuitBreakerResult:
        """
        2% daily loss.
        FLATTEN ALL POSITIONS IMMEDIATELY.
        No new trades for remainder of day.
        Reinstate at 75% next day.
        """
        loss = pnl.daily_pnl_pct
        if loss <= -DAILY_LOSS_LIMIT:
            return CircuitBreakerResult(
                action=CircuitBreakerAction.FLATTEN_ALL,
                triggered_by="daily_loss",
                threshold=DAILY_LOSS_LIMIT,
                current_value=abs(loss),
                severity="breach",
                message=(
                    f"DAILY LOSS LIMIT: {abs(loss):.1%} >= {DAILY_LOSS_LIMIT:.1%}. "
                    f"FLATTENING ALL POSITIONS. No new trades today. "
                    f"Reinstate at {DAILY_FLATTEN_REINSTATEMENT_PCT:.0%} tomorrow."
                ),
                timestamp=pnl.as_of,
            )
        return self._no_action(pnl.as_of)

    def _check_daily_loss_warning(self, pnl: PortfolioPnL) -> CircuitBreakerResult:
        """Warning at 75% of daily loss limit (1.5% loss)."""
        loss = pnl.daily_pnl_pct
        warning_threshold = DAILY_LOSS_LIMIT * 0.75
        if loss <= -warning_threshold:
            return CircuitBreakerResult(
                action=CircuitBreakerAction.WARN,
                triggered_by="daily_loss_warning",
                threshold=warning_threshold,
                current_value=abs(loss),
                severity="warning",
                message=(
                    f"WARNING: Daily loss {abs(loss):.1%} approaching limit "
                    f"{DAILY_LOSS_LIMIT:.1%}. Monitor closely."
                ),
                timestamp=pnl.as_of,
            )
        return self._no_action(pnl.as_of)

    @staticmethod
    def _no_action(timestamp: datetime) -> CircuitBreakerResult:
        return CircuitBreakerResult(
            action=CircuitBreakerAction.NONE,
            triggered_by="none",
            threshold=0.0,
            current_value=0.0,
            severity="ok",
            message="",
            timestamp=timestamp,
        )

    def _log_result(self, result: CircuitBreakerResult) -> None:
        if result.action in (CircuitBreakerAction.HARD_STOP, CircuitBreakerAction.FULL_SUSPEND,
                             CircuitBreakerAction.FLATTEN_ALL):
            logger.critical("CIRCUIT BREAKER: %s", result.message)
        elif result.severity == "breach":
            logger.error("CIRCUIT BREAKER: %s", result.message)
        else:
            logger.warning("CIRCUIT BREAKER: %s", result.message)


class StrategyCircuitBreaker:
    """
    Strategy-level drawdown monitoring.
    Checked independently for each active strategy.
    """

    def check_strategy_drawdown(
        self,
        strategy_id: str,
        drawdown_from_hwm: float,
    ) -> CircuitBreakerResult:
        """
        Check if a strategy has exceeded its drawdown budget.
        If breached: suspend strategy, close positions over 3 days.
        """
        budget = STRATEGY_DRAWDOWN_BUDGET.get(strategy_id)
        if budget is None:
            logger.error("No drawdown budget defined for strategy %s", strategy_id)
            return CircuitBreakerResult(
                action=CircuitBreakerAction.NONE,
                triggered_by="unknown_strategy",
                threshold=0.0,
                current_value=drawdown_from_hwm,
                severity="warning",
                message=f"No drawdown budget defined for {strategy_id}",
                timestamp=datetime.utcnow(),
            )

        if drawdown_from_hwm >= budget:
            return CircuitBreakerResult(
                action=CircuitBreakerAction.FULL_SUSPEND,
                triggered_by=f"strategy_drawdown_{strategy_id}",
                threshold=budget,
                current_value=drawdown_from_hwm,
                severity="breach",
                message=(
                    f"STRATEGY SUSPENSION: {strategy_id} drawdown {drawdown_from_hwm:.1%} "
                    f">= budget {budget:.1%}. "
                    f"Closing positions over max 3 trading days. "
                    f"Restart at {STRATEGY_RESTART_PCT:.0%} after root cause analysis. "
                    f"Scale back to 100% over {STRATEGY_SCALE_UP_DAYS} trading days."
                ),
                timestamp=datetime.utcnow(),
                requires_human_review=True,
            )

        # Warning at 75% of budget
        if drawdown_from_hwm >= budget * 0.75:
            return CircuitBreakerResult(
                action=CircuitBreakerAction.WARN,
                triggered_by=f"strategy_drawdown_warning_{strategy_id}",
                threshold=budget * 0.75,
                current_value=drawdown_from_hwm,
                severity="warning",
                message=(
                    f"WARNING: {strategy_id} drawdown {drawdown_from_hwm:.1%} "
                    f"approaching budget {budget:.1%}."
                ),
                timestamp=datetime.utcnow(),
            )

        return CircuitBreakerResult(
            action=CircuitBreakerAction.NONE,
            triggered_by="none",
            threshold=budget,
            current_value=drawdown_from_hwm,
            severity="ok",
            message="",
            timestamp=datetime.utcnow(),
        )