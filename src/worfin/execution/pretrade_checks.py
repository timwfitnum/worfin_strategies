"""
execution/pretrade_checks.py
Pre-trade risk validation — ALL checks must pass before ANY order is submitted.

If ANY check fails:
  1. Block the order — do not submit
  2. Log to audit.risk_breaches
  3. Send Telegram alert
  4. Do NOT retry automatically — human review required

This module is the last line of defence before real orders hit the market.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum

from worfin.config.metals import ALL_METALS
from worfin.risk.limits import (
    FAT_FINGER_PRICE_DEVIATION_PCT,
    MAX_ADV_PCT,
    MAX_DAILY_ORDERS,
    MAX_PORTFOLIO_GROSS,
    MAX_PORTFOLIO_NET,
    MAX_SIGNAL_AGE_HOURS,
    MAX_SINGLE_METAL_PCT,
)

logger = logging.getLogger(__name__)


class CheckStatus(str, Enum):
    PASS = "PASS"
    FAIL = "FAIL"
    SKIP = "SKIP"  # Check not applicable for this order type


@dataclass
class CheckResult:
    check_name: str
    status: CheckStatus
    message: str
    actual_value: float | None = None
    limit_value: float | None = None


@dataclass
class PreTradeResult:
    """Result of running all pre-trade checks on a proposed order."""

    ticker: str
    strategy_id: str
    proposed_lots: int
    proposed_notional_usd: float
    timestamp: datetime
    checks: list[CheckResult] = field(default_factory=list)

    @property
    def all_passed(self) -> bool:
        return all(
            c.status == CheckStatus.PASS for c in self.checks if c.status != CheckStatus.SKIP
        )

    @property
    def failed_checks(self) -> list[CheckResult]:
        return [c for c in self.checks if c.status == CheckStatus.FAIL]

    def summary(self) -> str:
        if self.all_passed:
            return f"✅ {self.ticker} {self.proposed_lots:+.6f} lots — all pre-trade checks PASSED"
        fails = ", ".join(c.check_name for c in self.failed_checks)
        return f"❌ {self.ticker} {self.proposed_lots:+.6f} lots — BLOCKED. Failed: {fails}"


@dataclass
class PortfolioState:
    """Current portfolio state needed for pre-trade checks."""

    nav_gbp: float
    current_positions: dict[str, float]  # {ticker: notional_gbp signed}
    current_orders_today: int
    gross_exposure_gbp: float
    net_exposure_gbp: float
    average_daily_volume: dict[str, float]  # {ticker: ADV in lots}


class PreTradeChecker:
    """
    Runs all pre-trade checks for a proposed order.
    Instantiate once, call check_order() for each proposed order.
    """

    def check_order(
        self,
        ticker: str,
        strategy_id: str,
        proposed_lots: int,
        proposed_notional_usd: float,
        current_mid_price: float,
        order_price: float,
        signal_timestamp: datetime,
        signal_direction: int,  # +1 long, -1 short
        portfolio: PortfolioState,
        usd_gbp_rate: float,
        reference_time: datetime | None = None,
    ) -> PreTradeResult:
        """
        Run all 8 pre-trade checks.

        Returns PreTradeResult — check .all_passed before submitting.
        If .all_passed is False, DO NOT SUBMIT THE ORDER.
        """
        proposed_notional_gbp = abs(proposed_notional_usd) / usd_gbp_rate
        metal = ALL_METALS.get(ticker)
        now = reference_time if reference_time is not None else datetime.now(UTC)

        result = PreTradeResult(
            ticker=ticker,
            strategy_id=strategy_id,
            proposed_lots=proposed_lots,
            proposed_notional_usd=proposed_notional_usd,
            timestamp=now,
        )

        # Run all 8 checks
        result.checks = [
            self._check_position_limit(ticker, proposed_notional_gbp, portfolio),
            self._check_gross_exposure(proposed_notional_gbp, portfolio),
            self._check_net_exposure(proposed_lots, proposed_notional_gbp, portfolio),
            self._check_liquidity_tier(ticker, proposed_lots, portfolio, metal),
            self._check_fat_finger(order_price, current_mid_price),
            self._check_daily_order_count(portfolio),
            self._check_signal_direction(proposed_lots, signal_direction),
            self._check_signal_staleness(signal_timestamp, now),
        ]

        if result.all_passed:
            logger.info("%s", result.summary())
        else:
            logger.error("%s", result.summary())
            for fail in result.failed_checks:
                logger.error("  FAILED CHECK: %s — %s", fail.check_name, fail.message)

        return result

    # ─────────────────────────────────────────────────────────────────────────
    # THE 8 CHECKS
    # ─────────────────────────────────────────────────────────────────────────

    def _check_position_limit(
        self,
        ticker: str,
        proposed_notional_gbp: float,
        portfolio: PortfolioState,
    ) -> CheckResult:
        """Check 1: Single-metal notional within 20% of NAV."""
        existing = abs(portfolio.current_positions.get(ticker, 0.0))
        total_after = existing + proposed_notional_gbp
        limit = portfolio.nav_gbp * MAX_SINGLE_METAL_PCT

        if total_after > limit:
            return CheckResult(
                check_name="position_limit",
                status=CheckStatus.FAIL,
                message=(
                    f"{ticker}: new total {total_after:,.0f} GBP would exceed "
                    f"single-metal limit {limit:,.0f} GBP ({MAX_SINGLE_METAL_PCT:.0%} of NAV)"
                ),
                actual_value=total_after,
                limit_value=limit,
            )
        return CheckResult(
            check_name="position_limit",
            status=CheckStatus.PASS,
            message=f"{ticker}: {total_after:,.0f} / {limit:,.0f} GBP limit",
            actual_value=total_after,
            limit_value=limit,
        )

    def _check_gross_exposure(
        self,
        proposed_notional_gbp: float,
        portfolio: PortfolioState,
    ) -> CheckResult:
        """Check 2: Portfolio gross exposure within 250% of NAV."""
        gross_after = portfolio.gross_exposure_gbp + proposed_notional_gbp
        limit = portfolio.nav_gbp * MAX_PORTFOLIO_GROSS

        if gross_after > limit:
            return CheckResult(
                check_name="gross_exposure",
                status=CheckStatus.FAIL,
                message=(
                    f"Gross exposure {gross_after:,.0f} GBP would exceed "
                    f"limit {limit:,.0f} GBP ({MAX_PORTFOLIO_GROSS:.0%} of NAV)"
                ),
                actual_value=gross_after,
                limit_value=limit,
            )
        return CheckResult(
            check_name="gross_exposure",
            status=CheckStatus.PASS,
            message=f"Gross: {gross_after:,.0f} / {limit:,.0f} GBP",
            actual_value=gross_after,
            limit_value=limit,
        )

    def _check_net_exposure(
        self,
        proposed_lots: int,
        proposed_notional_gbp: float,
        portfolio: PortfolioState,
    ) -> CheckResult:
        """Check 3: Portfolio net exposure within 80% of NAV."""
        direction = 1 if proposed_lots > 0 else -1
        net_after = portfolio.net_exposure_gbp + direction * proposed_notional_gbp
        limit = portfolio.nav_gbp * MAX_PORTFOLIO_NET

        if abs(net_after) > limit:
            return CheckResult(
                check_name="net_exposure",
                status=CheckStatus.FAIL,
                message=(
                    f"Net exposure |{net_after:,.0f}| GBP would exceed "
                    f"limit {limit:,.0f} GBP ({MAX_PORTFOLIO_NET:.0%} of NAV)"
                ),
                actual_value=abs(net_after),
                limit_value=limit,
            )
        return CheckResult(
            check_name="net_exposure",
            status=CheckStatus.PASS,
            message=f"Net: {net_after:,.0f} / ±{limit:,.0f} GBP",
            actual_value=abs(net_after),
            limit_value=limit,
        )

    def _check_liquidity_tier(
        self,
        ticker: str,
        proposed_lots: int,
        portfolio: PortfolioState,
        metal: object,
    ) -> CheckResult:
        """Check 4: Order within liquidity tier maximum (% of ADV)."""
        if metal is None:
            return CheckResult(
                check_name="liquidity_tier",
                status=CheckStatus.FAIL,
                message=f"Unknown metal ticker: {ticker}",
            )

        adv = portfolio.average_daily_volume.get(ticker)
        if adv is None or adv <= 0:
            return CheckResult(
                check_name="liquidity_tier",
                status=CheckStatus.SKIP,
                message=f"No ADV data for {ticker} — skipping liquidity check",
            )

        tier = metal.liquidity_tier.value
        max_pct = MAX_ADV_PCT[tier]
        max_lots = adv * max_pct

        if abs(proposed_lots) > max_lots:
            return CheckResult(
                check_name="liquidity_tier",
                status=CheckStatus.FAIL,
                message=(
                    f"{ticker} Tier {tier}: {abs(proposed_lots)} lots exceeds "
                    f"max {max_lots:.1f} lots ({max_pct:.0%} of ADV {adv:.0f})"
                ),
                actual_value=abs(proposed_lots),
                limit_value=max_lots,
            )
        return CheckResult(
            check_name="liquidity_tier",
            status=CheckStatus.PASS,
            message=f"{ticker}: {abs(proposed_lots)} lots / max {max_lots:.1f} lots (Tier {tier})",
            actual_value=abs(proposed_lots),
            limit_value=max_lots,
        )

    def _check_fat_finger(
        self,
        order_price: float,
        current_mid: float,
    ) -> CheckResult:
        """Check 5: Order price within 2% of current mid (fat-finger protection)."""
        if current_mid <= 0:
            return CheckResult(
                check_name="fat_finger",
                status=CheckStatus.SKIP,
                message="No mid price available — skipping fat-finger check",
            )

        deviation = abs(order_price - current_mid) / current_mid
        if deviation > FAT_FINGER_PRICE_DEVIATION_PCT:
            return CheckResult(
                check_name="fat_finger",
                status=CheckStatus.FAIL,
                message=(
                    f"Order price {order_price:.2f} deviates {deviation:.1%} from mid {current_mid:.2f}. "
                    f"Max allowed: {FAT_FINGER_PRICE_DEVIATION_PCT:.0%}"
                ),
                actual_value=deviation,
                limit_value=FAT_FINGER_PRICE_DEVIATION_PCT,
            )
        return CheckResult(
            check_name="fat_finger",
            status=CheckStatus.PASS,
            message=f"Price deviation: {deviation:.2%} within {FAT_FINGER_PRICE_DEVIATION_PCT:.0%} limit",
            actual_value=deviation,
            limit_value=FAT_FINGER_PRICE_DEVIATION_PCT,
        )

    def _check_daily_order_count(self, portfolio: PortfolioState) -> CheckResult:
        """Check 6: Daily order count within maximum."""
        if portfolio.current_orders_today >= MAX_DAILY_ORDERS:
            return CheckResult(
                check_name="daily_order_count",
                status=CheckStatus.FAIL,
                message=(
                    f"Daily order limit reached: {portfolio.current_orders_today} / {MAX_DAILY_ORDERS}. "
                    f"No further orders today."
                ),
                actual_value=float(portfolio.current_orders_today),
                limit_value=float(MAX_DAILY_ORDERS),
            )
        return CheckResult(
            check_name="daily_order_count",
            status=CheckStatus.PASS,
            message=f"Orders today: {portfolio.current_orders_today} / {MAX_DAILY_ORDERS}",
            actual_value=float(portfolio.current_orders_today),
            limit_value=float(MAX_DAILY_ORDERS),
        )

    def _check_signal_direction(
        self,
        proposed_lots: int,
        signal_direction: int,
    ) -> CheckResult:
        """Check 7: Order direction matches the signal (prevents sign errors)."""
        order_direction = 1 if proposed_lots > 0 else -1
        if order_direction != signal_direction:
            return CheckResult(
                check_name="signal_direction",
                status=CheckStatus.FAIL,
                message=(
                    f"Order direction ({'long' if order_direction > 0 else 'short'}) "
                    f"does not match signal direction "
                    f"({'long' if signal_direction > 0 else 'short'}). "
                    f"Possible sign error in order generation."
                ),
            )
        return CheckResult(
            check_name="signal_direction",
            status=CheckStatus.PASS,
            message=f"Direction matches signal ({'long' if signal_direction > 0 else 'short'})",
        )

    def _check_signal_staleness(
        self, signal_timestamp: datetime, now: datetime = None
    ) -> CheckResult:
        """Check 8: Signal is less than 24 hours old."""
        now = now if now else datetime.now(UTC)
        if signal_timestamp.tzinfo is None:
            signal_timestamp = signal_timestamp.replace(tzinfo=UTC)
        age_hours = (now - signal_timestamp).total_seconds() / 3600

        if age_hours > MAX_SIGNAL_AGE_HOURS:
            return CheckResult(
                check_name="signal_staleness",
                status=CheckStatus.FAIL,
                message=(
                    f"Signal is {age_hours:.1f} hours old — exceeds {MAX_SIGNAL_AGE_HOURS}h limit. "
                    f"Do not execute on stale signals."
                ),
                actual_value=age_hours,
                limit_value=float(MAX_SIGNAL_AGE_HOURS),
            )
        return CheckResult(
            check_name="signal_staleness",
            status=CheckStatus.PASS,
            message=f"Signal age: {age_hours:.1f}h (limit: {MAX_SIGNAL_AGE_HOURS}h)",
            actual_value=age_hours,
            limit_value=float(MAX_SIGNAL_AGE_HOURS),
        )
