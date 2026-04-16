"""
data/pipeline/continuous.py
Roll-adjusted continuous price series construction.

THE PROBLEM:
  Futures contracts expire. The "front month" is a moving target — today's
  front becomes last month's expired contract tomorrow. A naive stitch of
  front-month prices produces artificial jumps at every roll, which
  contaminate returns and break backtests.

THE TWO ADJUSTMENT METHODS:
  Back-adjusted (panama):
    Preserves returns. Adjusts historical prices by the accumulated roll
    gap so the series is seamless for return calculations. LEVEL IS
    DISTORTED — absolute prices are not meaningful, but relative moves are.
    Use for S1/S2/S3/S4 (return-based signals).

  Ratio-adjusted:
    Preserves price ratios. Multiplies historical prices by the accumulated
    roll ratio. Useful for cointegration and spread analysis where the
    ratio between two series is the signal. Use for S6 (pairs).

EXCHANGE CONVENTIONS:
  LME (via Nasdaq CHRIS LME_{SYMBOL}{1,2}):
    Nasdaq CHRIS keeps _1 as "current front" and _2 as "current second".
    Underlying contract shifts happen inside the series — we detect the
    roll from a sudden discontinuity in _1 combined with _1[D] matching
    the prior day's _2[D-1]. LME doesn't use calendar monthly expiry.

  COMEX (via Nasdaq CHRIS CME_{SYMBOL}{1,2}):
    Calendar roll: N business days before first-notice-day (FND).
    Per-metal N values (GC=8, SI=8, PL=12, PA=13) — tin/palladium illiquid
    back months mean earlier rolls. Data detection is used as a CHECK
    against the calendar rule.

EVERY ROLL EVENT IS LOGGED to audit.roll_log for full reproducibility.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Literal

import numpy as np
import pandas as pd
from sqlalchemy.engine import Engine

from worfin.config.metals import ALL_METALS, Exchange

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────

# Data-detection threshold: a day is treated as a roll candidate if the
# front-month log return exceeds this magnitude AND the new front price
# is close to yesterday's second-month price.
ROLL_DETECTION_JUMP_THRESHOLD = 0.015  # 1.5% single-day move flags candidate
ROLL_DETECTION_MATCH_TOLERANCE = 0.005  # _1[D] must be within 0.5% of _2[D-1]

# COMEX roll-ahead-of-FND per metal (business days before first notice day)
COMEX_ROLL_DAYS_BEFORE_FND: dict[str, int] = {
    "GC": 8,  # Gold — standard 5–10 BD roll, we pick 8 (Execution Playbook)
    "SI": 8,  # Silver — same as gold
    "PL": 12,  # Platinum — thin back months, roll earlier (7–15 BD)
    "PA": 13,  # Palladium — least liquid, roll earliest (10–15 BD)
}

RollMethod = Literal["back_adjusted", "ratio_adjusted"]


# ─────────────────────────────────────────────────────────────────────────────
# RESULT TYPES
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class RollEvent:
    """A single roll event with full audit detail."""

    roll_timestamp: datetime
    ticker: str
    exchange: str
    old_front_price: float
    new_front_price: float
    gap_absolute: float
    gap_pct: float
    roll_method: str
    days_before_fnd: int | None = None
    detection_method: str = "data"  # "data" | "calendar" | "explicit"


@dataclass
class ContinuousSeriesResult:
    """Output of continuous-series construction."""

    ticker: str
    method: str
    adjusted_series: pd.Series  # main output — use for signals
    unadjusted_front: pd.Series  # raw front series (for reference/S6)
    roll_events: list[RollEvent] = field(default_factory=list)
    series_id: str = field(default_factory=lambda: str(uuid.uuid4()))

    def summary(self) -> str:
        if self.adjusted_series.empty:
            return f"{self.ticker} [{self.method}]: empty series"
        return (
            f"{self.ticker} [{self.method}]: "
            f"{self.adjusted_series.index.min().date()} → "
            f"{self.adjusted_series.index.max().date()} "
            f"({len(self.adjusted_series)} obs, {len(self.roll_events)} rolls)"
        )


# ─────────────────────────────────────────────────────────────────────────────
# ROLL DETECTION
# ─────────────────────────────────────────────────────────────────────────────


def detect_lme_rolls(front: pd.Series, second: pd.Series) -> list[pd.Timestamp]:
    """
    Detect LME roll dates from price data.

    An LME roll is flagged when:
      1. front[D] vs front[D-1] shows a discontinuity (>1.5% log move), AND
      2. front[D] is close to second[D-1] (within 0.5%) — confirms the jump
         is contract-shift, not a real market move.

    Using BOTH conditions avoids flagging genuine large moves as rolls
    (e.g., the March 2022 nickel squeeze must NOT be treated as a roll).
    """
    if len(front) < 2:
        return []

    # Align front and second on the union index, forward-fill gaps for detection
    aligned = pd.concat([front.rename("f1"), second.rename("f2")], axis=1).ffill()
    aligned = aligned.dropna()

    f1 = aligned["f1"]
    f2 = aligned["f2"]

    # Log returns of front series
    log_ret = np.log(f1 / f1.shift(1))

    # Ratio of today's front to yesterday's second — should be ≈ 1 on roll day
    # if the roll is clean (new front today ≈ second contract yesterday, because
    # second-yesterday is the same underlying contract as front-today)
    f2_prev = f2.shift(1)
    match_ratio = (f1 / f2_prev - 1).abs()

    jump_mask = log_ret.abs() > ROLL_DETECTION_JUMP_THRESHOLD
    match_mask = match_ratio < ROLL_DETECTION_MATCH_TOLERANCE

    roll_mask = jump_mask & match_mask
    return list(roll_mask[roll_mask].index)


def detect_comex_rolls(
    front: pd.Series,
    ticker: str,
    fnd_dates: list[date] | None = None,
) -> list[pd.Timestamp]:
    """
    Detect COMEX roll dates from calendar rule (first notice day - N).

    If fnd_dates is supplied (list of FND for each expiring contract), we
    compute exact roll dates. Otherwise we fall back to an approximation:
    the front-month series shows a discontinuity typically on the last
    business day of the month preceding delivery, offset by the metal's
    roll-ahead window.

    Returns dates where the series should be rolled going backwards.
    """
    if len(front) < 2:
        return []

    n_before = COMEX_ROLL_DAYS_BEFORE_FND.get(ticker, 5)

    if fnd_dates is not None:
        # Roll exactly n_before business days before each FND
        rolls: list[pd.Timestamp] = []
        idx = pd.DatetimeIndex(front.index)
        for fnd in fnd_dates:
            fnd_ts = pd.Timestamp(fnd).tz_localize(idx.tz) if idx.tz else pd.Timestamp(fnd)
            # Closest trading day index on-or-before (fnd - n_before business days)
            target = fnd_ts - pd.tseries.offsets.BusinessDay(n_before)
            # Snap to nearest index value on or before target
            candidates = idx[idx <= target]
            if len(candidates) > 0:
                rolls.append(candidates[-1])
        return rolls

    # Fallback: data-driven detection for COMEX too (jump in front series)
    # This is less precise than FND-based but works when FND list unavailable.
    log_ret = np.log(front / front.shift(1))
    # COMEX precious metals are smoother than LME — tighter threshold
    threshold = ROLL_DETECTION_JUMP_THRESHOLD * 0.7
    jumps = log_ret.abs() > threshold
    logger.info(
        "%s: no FND dates supplied — using data-driven fallback " "(detected %d candidate rolls)",
        ticker,
        int(jumps.sum()),
    )
    return list(jumps[jumps].index)


# ─────────────────────────────────────────────────────────────────────────────
# CORE ADJUSTMENT
# ─────────────────────────────────────────────────────────────────────────────


def _back_adjust(
    front: pd.Series,
    second: pd.Series,
    roll_dates: list[pd.Timestamp],
) -> tuple[pd.Series, list[tuple[pd.Timestamp, float, float, float]]]:
    """
    Back-adjusted (panama) method.

    At each roll date D:
      gap = front[D-1] - second[D-1]
         = (price of old front on last day it was front)
         - (price of new front on that same day)

    Then subtract the CUMULATIVE gap from all prices BEFORE each roll.
    Walking backwards from today, this produces a seamless series.

    Returns adjusted series plus (roll_ts, old_price, new_price, gap) tuples
    so callers can build RollEvent records with full detail.
    """
    adjusted = front.copy().astype(float)
    roll_details: list[tuple[pd.Timestamp, float, float, float]] = []

    # Sort rolls chronologically; walk backwards
    sorted_rolls = sorted(roll_dates)

    # Align second onto front's index for lookups
    second_aligned = second.reindex(front.index).ffill()

    for roll_ts in reversed(sorted_rolls):
        if roll_ts not in adjusted.index:
            continue
        idx_pos = adjusted.index.get_loc(roll_ts)
        if idx_pos == 0:
            continue  # Can't adjust anything before the very first bar
        prev_ts = adjusted.index[idx_pos - 1]

        # Gap using the D-1 quotes: old front (prev_ts) vs new front (second on prev_ts)
        old_price = float(front.loc[prev_ts])
        new_price_on_prev = second_aligned.loc[prev_ts]
        if pd.isna(new_price_on_prev) or new_price_on_prev == 0:
            # Fall back to front[D] as "new" price — less clean but avoids NaN
            new_price_on_prev = float(front.loc[roll_ts])
        new_price = float(new_price_on_prev)
        gap = old_price - new_price

        # Apply gap to all observations strictly BEFORE the roll
        adjusted.iloc[:idx_pos] = adjusted.iloc[:idx_pos] - gap

        roll_details.append((roll_ts, old_price, new_price, gap))

    return adjusted, roll_details


def _ratio_adjust(
    front: pd.Series,
    second: pd.Series,
    roll_dates: list[pd.Timestamp],
) -> tuple[pd.Series, list[tuple[pd.Timestamp, float, float, float]]]:
    """
    Ratio-adjusted method. Multiplies historical prices by the cumulative
    ratio of old-to-new so the series is continuous in proportional terms.

    Preferred for S6 pairs because the log-price relationship (used in
    cointegration tests) is preserved.
    """
    adjusted = front.copy().astype(float)
    roll_details: list[tuple[pd.Timestamp, float, float, float]] = []

    second_aligned = second.reindex(front.index).ffill()

    for roll_ts in reversed(sorted(roll_dates)):
        if roll_ts not in adjusted.index:
            continue
        idx_pos = adjusted.index.get_loc(roll_ts)
        if idx_pos == 0:
            continue
        prev_ts = adjusted.index[idx_pos - 1]

        old_price = float(front.loc[prev_ts])
        new_price_on_prev = second_aligned.loc[prev_ts]
        if pd.isna(new_price_on_prev) or new_price_on_prev == 0:
            new_price_on_prev = float(front.loc[roll_ts])
        new_price = float(new_price_on_prev)

        if old_price <= 0 or new_price <= 0:
            logger.warning(
                "Cannot ratio-adjust at %s: non-positive prices (old=%s, new=%s)",
                roll_ts,
                old_price,
                new_price,
            )
            continue

        ratio = new_price / old_price
        adjusted.iloc[:idx_pos] = adjusted.iloc[:idx_pos] * ratio

        gap = old_price - new_price  # still recorded for audit consistency
        roll_details.append((roll_ts, old_price, new_price, gap))

    return adjusted, roll_details


# ─────────────────────────────────────────────────────────────────────────────
# PUBLIC ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────


def build_continuous_series(
    ticker: str,
    front: pd.Series,
    second: pd.Series,
    method: RollMethod = "back_adjusted",
    roll_dates: list[pd.Timestamp] | None = None,
    fnd_dates: list[date] | None = None,
    environment: str = "backtest",
    backtest_run_id: str | None = None,
    engine: Engine | None = None,
) -> ContinuousSeriesResult:
    """
    Construct a roll-adjusted continuous series.

    Args:
        ticker:            Metal ticker (e.g. "CA", "GC")
        front:             Raw front-month price series (tz-aware DatetimeIndex)
        second:            Raw second-month price series (tz-aware DatetimeIndex)
        method:            "back_adjusted" (default) or "ratio_adjusted"
        roll_dates:        Explicit roll dates. If None, detected automatically.
        fnd_dates:         COMEX first-notice-days (optional; improves accuracy)
        environment:       "backtest" | "paper" | "live" — tagged on roll log
        backtest_run_id:   UUID string — groups rolls for a single backtest run
        engine:            SQLAlchemy engine. If supplied, roll events are
                           persisted to audit.roll_log.

    Returns:
        ContinuousSeriesResult with adjusted series, unadjusted front,
        and list of RollEvent records.
    """
    if ticker not in ALL_METALS:
        raise KeyError(f"Unknown ticker: {ticker!r}")
    spec = ALL_METALS[ticker]
    exchange = spec.exchange.value if hasattr(spec.exchange, "value") else str(spec.exchange)

    # Clean inputs
    front = front.sort_index().astype(float)
    second = second.sort_index().astype(float)

    # Use explicit rolls if given, else detect per exchange
    if roll_dates is not None:
        rolls = [pd.Timestamp(r) for r in roll_dates]
        detection_method = "explicit"
    else:
        if spec.exchange == Exchange.LME:
            rolls = detect_lme_rolls(front, second)
            detection_method = "data"
        elif spec.exchange == Exchange.COMEX:
            rolls = detect_comex_rolls(front, ticker, fnd_dates=fnd_dates)
            detection_method = "calendar" if fnd_dates else "data"
        else:
            logger.warning(
                "%s: unknown exchange %s — using LME detection as fallback", ticker, exchange
            )
            rolls = detect_lme_rolls(front, second)
            detection_method = "data"

    # Apply the adjustment
    if method == "back_adjusted":
        adjusted, details = _back_adjust(front, second, rolls)
    elif method == "ratio_adjusted":
        adjusted, details = _ratio_adjust(front, second, rolls)
    else:
        raise ValueError(f"Unknown method: {method!r}")

    # Build roll-event records
    series_id = str(uuid.uuid4())
    roll_events: list[RollEvent] = []
    for roll_ts, old_price, new_price, gap in details:
        gap_pct = gap / old_price if old_price != 0 else 0.0
        py_ts = roll_ts.to_pydatetime()
        if py_ts.tzinfo is None:
            py_ts = py_ts.replace(tzinfo=UTC)
        roll_events.append(
            RollEvent(
                roll_timestamp=py_ts,
                ticker=ticker,
                exchange=exchange,
                old_front_price=old_price,
                new_front_price=new_price,
                gap_absolute=gap,
                gap_pct=gap_pct,
                roll_method=method,
                detection_method=detection_method,
            )
        )

    logger.info(
        "%s: built %s series %s → %s (%d rolls, %s detection)",
        ticker,
        method,
        front.index.min().date() if len(front) else "?",
        front.index.max().date() if len(front) else "?",
        len(roll_events),
        detection_method,
    )

    # Persist to audit.roll_log if engine provided
    if engine is not None and roll_events:
        _persist_roll_events(
            engine=engine,
            events=roll_events,
            series_id=series_id,
            environment=environment,
            backtest_run_id=backtest_run_id,
        )

    return ContinuousSeriesResult(
        ticker=ticker,
        method=method,
        adjusted_series=adjusted,
        unadjusted_front=front,
        roll_events=roll_events,
        series_id=series_id,
    )


# ─────────────────────────────────────────────────────────────────────────────
# AUDIT PERSISTENCE
# ─────────────────────────────────────────────────────────────────────────────


def _persist_roll_events(
    engine: Engine,
    events: list[RollEvent],
    series_id: str,
    environment: str,
    backtest_run_id: str | None,
) -> None:
    """Write roll events to audit.roll_log. Silent-fails on DB errors to
    avoid killing a backtest over audit-write issues — but always logs."""
    try:
        from sqlalchemy import text

        rows = [
            {
                "roll_timestamp": e.roll_timestamp,
                "ticker": e.ticker,
                "exchange": e.exchange,
                "from_contract": None,
                "to_contract": None,
                "old_front_price_usd": Decimal(str(e.old_front_price)),
                "new_front_price_usd": Decimal(str(e.new_front_price)),
                "gap_absolute": Decimal(str(e.gap_absolute)),
                "gap_pct": Decimal(str(e.gap_pct)),
                "roll_method": e.roll_method,
                "theoretical_fair_spread": None,
                "roll_cost_vs_fair_bps": None,
                "days_before_fnd": e.days_before_fnd,
                "environment": environment,
                "backtest_run_id": backtest_run_id,
                "series_id": series_id,
                "detection_method": e.detection_method,
            }
            for e in events
        ]
        stmt = text(
            """
            INSERT INTO audit.roll_log
              (roll_timestamp, ticker, exchange, from_contract, to_contract,
               old_front_price_usd, new_front_price_usd, gap_absolute, gap_pct,
               roll_method, theoretical_fair_spread, roll_cost_vs_fair_bps,
               days_before_fnd, environment, backtest_run_id, series_id,
               detection_method)
            VALUES
              (:roll_timestamp, :ticker, :exchange, :from_contract, :to_contract,
               :old_front_price_usd, :new_front_price_usd, :gap_absolute, :gap_pct,
               :roll_method, :theoretical_fair_spread, :roll_cost_vs_fair_bps,
               :days_before_fnd, :environment, :backtest_run_id, :series_id,
               :detection_method)
        """
        )
        with engine.begin() as conn:
            conn.execute(stmt, rows)
        logger.info("Persisted %d roll events to audit.roll_log (series %s)", len(rows), series_id)
    except Exception as exc:
        logger.error("Failed to persist roll events to audit.roll_log: %s", exc)
