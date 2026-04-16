"""
backtest/pretrade_integration.py
Wiring module that connects the backtest engine to PreTradeChecker.

The engine calls into this module once per rebalance:
  1. build_portfolio_state()      → constructs PortfolioState from simulated book
  2. compute_trailing_adv()       → rolling 20-day ADV per ticker, with fallback
  3. run_pretrade_checks()        → runs each proposed trade through the checker
  4. log_rejection()              → writes rejected trades to audit.risk_breaches

Rejected trades do NOT execute. The ticker keeps yesterday's position; no
phantom fill is recorded in positions.trades.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime

import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from worfin.config.metals import ALL_METALS, get_metal
from worfin.execution.pretrade_checks import (
    PortfolioState,
    PreTradeChecker,
    PreTradeResult,
)

logger = logging.getLogger(__name__)

# ADV reliability threshold — if more than 50% of the lookback window is
# zero/NaN volume, we fall back to the conservative typical_adv_lots from
# the metal spec rather than return a nonsense rolling mean.
ADV_RELIABILITY_THRESHOLD = 0.50
ADV_LOOKBACK_DAYS = 20


# ─────────────────────────────────────────────────────────────────────────────
# ADV COMPUTATION
# ─────────────────────────────────────────────────────────────────────────────


def compute_trailing_adv(
    price_data: Mapping[str, pd.DataFrame],
    as_of_ts: pd.Timestamp,
    lookback_days: int = ADV_LOOKBACK_DAYS,
) -> dict[str, float]:
    """
    Compute trailing ADV (average daily volume, in lots) per ticker.

    NASDAQ DATA LINK CAVEAT:
      Volume data for LME metals is sometimes missing or zero. If more than
      50% of the lookback window is zero/NaN, we fall back to the hardcoded
      `typical_adv_lots` field on MetalSpec (conservative, per Execution
      Playbook). This is better than SKIP-ing the check (false confidence)
      or crashing the engine.

    Same slicing convention as _compute_vols — `df[df.index <= as_of_ts]` —
    ensures no look-ahead bias.

    Args:
        price_data:     {ticker: DataFrame with 'volume' column indexed by timestamp}
        as_of_ts:       Current bar timestamp
        lookback_days:  Number of trading days in the rolling window

    Returns:
        {ticker: ADV in lots}
    """
    adv: dict[str, float] = {}

    for ticker, df in price_data.items():
        if "volume" not in df.columns:
            # No volume column at all — fallback
            adv[ticker] = _typical_adv_fallback(ticker, reason="no volume column")
            continue

        # Slice to rows up to and including as_of, keeping look-ahead clean
        window = df[df.index <= as_of_ts].tail(lookback_days)
        if window.empty:
            adv[ticker] = _typical_adv_fallback(ticker, reason="empty window")
            continue

        vol = window["volume"]
        unreliable_count = (vol.fillna(0) <= 0).sum()
        if unreliable_count / len(window) > ADV_RELIABILITY_THRESHOLD:
            adv[ticker] = _typical_adv_fallback(
                ticker,
                reason=f"{unreliable_count}/{len(window)} bars zero/NaN",
            )
            continue

        mean_vol = float(vol.replace(0, np.nan).dropna().mean())
        if np.isnan(mean_vol) or mean_vol <= 0:
            adv[ticker] = _typical_adv_fallback(ticker, reason="mean=NaN/0 after cleaning")
        else:
            adv[ticker] = mean_vol

    return adv


def _typical_adv_fallback(ticker: str, reason: str) -> float:
    """
    Fallback to typical_adv_lots on the metal spec.

    If the field doesn't exist on MetalSpec yet (see patches), we return 0.0
    which will cause the liquidity check to return SKIP status — at which
    point the user should add the field and re-run.
    """
    metal = ALL_METALS.get(ticker)
    fallback = getattr(metal, "typical_adv_lots", None) if metal else None
    if fallback is None:
        logger.warning(
            "%s: no volume data (%s) AND no typical_adv_lots on MetalSpec — "
            "liquidity check will SKIP. Add typical_adv_lots to metals.py.",
            ticker,
            reason,
        )
        return 0.0
    logger.debug(
        "%s: using typical_adv_lots=%.0f (reason: %s)",
        ticker,
        fallback,
        reason,
    )
    return float(fallback)


# ─────────────────────────────────────────────────────────────────────────────
# PORTFOLIO STATE BUILDER
# ─────────────────────────────────────────────────────────────────────────────


def build_portfolio_state(
    nav_gbp: float,
    current_lots: Mapping[str, int],
    current_prices_usd: Mapping[str, float],
    usd_gbp_rate: float,
    orders_today: int,
    adv_by_ticker: Mapping[str, float],
) -> PortfolioState:
    """
    Construct the PortfolioState the PreTradeChecker expects.

    Translates from the backtest's natural representation (lots per ticker)
    to the checker's representation (signed GBP notional per ticker, plus
    aggregate gross/net exposure in GBP).
    """
    current_positions_gbp: dict[str, float] = {}
    gross_gbp = 0.0
    net_gbp = 0.0

    for ticker, lots in current_lots.items():
        if lots == 0 or ticker not in current_prices_usd:
            current_positions_gbp[ticker] = 0.0
            continue
        metal = get_metal(ticker)
        notional_usd = lots * metal.lot_size * current_prices_usd[ticker]
        notional_gbp = notional_usd / usd_gbp_rate
        current_positions_gbp[ticker] = notional_gbp
        gross_gbp += abs(notional_gbp)
        net_gbp += notional_gbp

    return PortfolioState(
        nav_gbp=nav_gbp,
        current_positions=current_positions_gbp,
        current_orders_today=orders_today,
        gross_exposure_gbp=gross_gbp,
        net_exposure_gbp=net_gbp,
        average_daily_volume=dict(adv_by_ticker),
    )


# ─────────────────────────────────────────────────────────────────────────────
# CHECK RUNNER
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class TradeDecision:
    """Result of running pre-trade checks on a single proposed trade."""

    ticker: str
    strategy_id: str
    proposed_lots: int
    approved: bool
    pretrade_result: PreTradeResult
    # If not approved, hold_lots = yesterday's lots (no trade)
    hold_lots: int


def run_pretrade_checks(
    checker: PreTradeChecker,
    proposed_deltas: Mapping[str, int],  # {ticker: lots_delta from current}
    current_lots: Mapping[str, int],  # yesterday's lots per ticker
    prices_usd: Mapping[str, float],
    signals: Mapping[str, float],
    signal_timestamp: datetime,
    strategy_id: str,
    portfolio: PortfolioState,
    usd_gbp_rate: float,
) -> list[TradeDecision]:
    """
    Run pre-trade checks on a batch of proposed deltas.

    A `delta` is the NEW trade — not the absolute target. If current=10 and
    target=15, the delta is +5 (buy 5 more lots).

    Returns one TradeDecision per delta. Rejected trades carry hold_lots =
    the caller's current position for that ticker (execute no trade).
    """
    decisions: list[TradeDecision] = []

    for ticker, lots_delta in proposed_deltas.items():
        current = int(current_lots.get(ticker, 0))
        if lots_delta == 0:
            decisions.append(
                TradeDecision(
                    ticker=ticker,
                    strategy_id=strategy_id,
                    proposed_lots=0,
                    approved=True,
                    pretrade_result=_empty_pass_result(ticker, strategy_id),
                    hold_lots=current,
                )
            )
            continue
        if ticker not in prices_usd:
            logger.warning("No price for %s at check time — skipping trade.", ticker)
            continue

        metal = get_metal(ticker)
        price = prices_usd[ticker]
        proposed_notional_usd = abs(lots_delta) * metal.lot_size * price
        signal = signals.get(ticker, 0.0)
        signal_direction = 1 if signal >= 0 else -1

        result = checker.check_order(
            ticker=ticker,
            strategy_id=strategy_id,
            proposed_lots=lots_delta,
            proposed_notional_usd=proposed_notional_usd * (1 if lots_delta > 0 else -1),
            current_mid_price=price,
            order_price=price,  # backtest uses mark as fill price
            signal_timestamp=signal_timestamp,
            signal_direction=signal_direction,
            portfolio=portfolio,
            usd_gbp_rate=usd_gbp_rate,
        )

        approved = result.all_passed
        decisions.append(
            TradeDecision(
                ticker=ticker,
                strategy_id=strategy_id,
                proposed_lots=lots_delta,
                approved=approved,
                pretrade_result=result,
                hold_lots=current,
            )
        )

    return decisions


def _empty_pass_result(ticker: str, strategy_id: str) -> PreTradeResult:
    """A zero-delta trade produces a trivially-passing result."""
    return PreTradeResult(
        ticker=ticker,
        strategy_id=strategy_id,
        proposed_lots=0,
        proposed_notional_usd=0.0,
        timestamp=datetime.now(UTC),
        checks=[],
    )


# ─────────────────────────────────────────────────────────────────────────────
# REJECTION LOGGING
# ─────────────────────────────────────────────────────────────────────────────


def log_rejections_to_audit(
    engine: Engine | None,
    decisions: list[TradeDecision],
    as_of_ts: datetime,
    backtest_run_id: str | None,
) -> None:
    """
    Write rejected trades to audit.risk_breaches with source='backtest' and
    the backtest_run_id for this simulation run.

    If engine is None (pure in-memory run), we only log via the logger and
    skip the DB write.
    """
    rejected = [d for d in decisions if not d.approved and d.proposed_lots != 0]
    if not rejected:
        return

    # Always log to logger first — audit-write failures must not mask rejections
    for d in rejected:
        fails = ", ".join(f"{c.check_name}={c.message}" for c in d.pretrade_result.failed_checks)
        logger.warning(
            "BACKTEST REJECTION: %s %s %+d lots — %s (run=%s)",
            d.strategy_id,
            d.ticker,
            d.proposed_lots,
            fails,
            backtest_run_id,
        )

    if engine is None:
        return

    try:
        rows = []
        for d in rejected:
            for failed in d.pretrade_result.failed_checks:
                rows.append(
                    {
                        "breach_timestamp": as_of_ts,
                        "breach_type": f"pretrade_{failed.check_name}",
                        "action_taken": "trade_rejected_backtest",
                        "threshold": (
                            float(failed.limit_value) if failed.limit_value is not None else None
                        ),
                        "actual_value": (
                            float(failed.actual_value) if failed.actual_value is not None else None
                        ),
                        "strategy_id": d.strategy_id,
                        "ticker": d.ticker,
                        "message": (
                            f"Pre-trade check '{failed.check_name}' failed: {failed.message}. "
                            f"Proposed {d.proposed_lots:+d} lots rejected."
                        ),
                        "source": "backtest",
                        "backtest_run_id": backtest_run_id,
                        "context_json": json.dumps(
                            {
                                "proposed_lots": d.proposed_lots,
                                "check_name": failed.check_name,
                            }
                        ),
                    }
                )
        if not rows:
            return
        stmt = text(
            """
            INSERT INTO audit.risk_breaches
              (breach_timestamp, breach_type, action_taken, threshold, actual_value,
               strategy_id, ticker, message, source, backtest_run_id, context_json,
               created_at)
            VALUES
              (:breach_timestamp, :breach_type, :action_taken, :threshold, :actual_value,
               :strategy_id, :ticker, :message, :source, :backtest_run_id, :context_json,
               NOW())
        """
        )
        with engine.begin() as conn:
            conn.execute(stmt, rows)
    except Exception as exc:
        logger.error("Failed to write backtest rejections to audit.risk_breaches: %s", exc)
