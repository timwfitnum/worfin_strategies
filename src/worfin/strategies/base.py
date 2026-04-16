"""
strategies/base.py
Abstract base class for all strategies — daily AND future intraday.

INTRADAY-READY DESIGN DECISIONS (implemented now, used later):
  - StrategyConfig carries `frequency` and `bar_size` fields
  - compute_signals() takes a timestamp, not a date — works at any granularity
  - SignalResult carries `valid_from` / `valid_until` — execution engine is
    frequency-agnostic, just checks whether current time is in validity window
  - Signals normalised to [-1, +1] at every frequency

Adding a new intraday strategy later = new StrategyConfig instance + new subclass.
No changes to base class, execution engine, or scheduler required.
"""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# FREQUENCY CONSTANTS
# Used in StrategyConfig and scheduler routing — never hardcode elsewhere
# ─────────────────────────────────────────────────────────────────────────────

class Frequency:
    """Strategy execution frequencies. Add new ones here as needed."""
    DAILY    = "daily"     # Current: all six strategies
    SWING    = "swing"     # Future: multi-day mean reversion
    INTRADAY = "intraday"  # Future: sub-daily signals
    HOURLY   = "hourly"    # Future: hourly bars
    MINUTE_5 = "5min"      # Future: 5-minute bars


class BarSize:
    """Data granularity consumed by the strategy."""
    DAILY    = "daily"
    HOURLY   = "1hour"
    MIN_15   = "15min"
    MIN_5    = "5min"
    MIN_1    = "1min"
    TICK     = "tick"


# Signal validity windows per frequency
# How long a signal remains valid before it must be recomputed
SIGNAL_VALIDITY: dict[str, timedelta] = {
    Frequency.DAILY:    timedelta(hours=24),
    Frequency.SWING:    timedelta(hours=8),
    Frequency.INTRADAY: timedelta(hours=1),
    Frequency.HOURLY:   timedelta(hours=1),
    Frequency.MINUTE_5: timedelta(minutes=5),
}


@dataclass
class StrategyConfig:
    """
    Immutable configuration for a strategy instance.

    frequency and bar_size are the two fields that drive all routing decisions:
    - Which data granularity to fetch
    - How long signals remain valid
    - When the scheduler runs this strategy
    - Which execution window applies

    Adding an intraday strategy = new StrategyConfig with frequency="intraday".
    No other code changes required.
    """
    strategy_id: str                # e.g. "S4"
    name: str                       # e.g. "Basis-Momentum"
    universe: list[str]             # Metal tickers in scope
    frequency: str                  # Frequency.DAILY | SWING | INTRADAY | HOURLY
    bar_size: str                   # BarSize.DAILY | HOURLY | MIN_5 etc.
    rebalance_freq: str             # "weekly", "biweekly", "monthly", "continuous"
    target_vol: float               # Annualised vol target (e.g. 0.11)
    max_drawdown_budget: float      # Max drawdown before suspension
    min_history_bars: int           # Minimum bars needed (not days — bar-agnostic)
    parameters: dict = field(default_factory=dict)

    @property
    def signal_validity_window(self) -> timedelta:
        """How long a signal from this strategy remains valid for execution."""
        return SIGNAL_VALIDITY.get(self.frequency, timedelta(hours=24))

    @property
    def is_intraday(self) -> bool:
        return self.frequency not in (Frequency.DAILY, Frequency.SWING)


@dataclass
class SignalResult:
    """
    Output of compute_signals().

    valid_from / valid_until: execution engine checks these to determine
    whether a signal is still actionable. Frequency-agnostic — works
    identically for daily signals valid for 24 hours and 5-min signals
    valid for 5 minutes.
    """
    computed_at: datetime           # UTC timestamp when signal was computed
    valid_from: datetime            # Signal becomes actionable from this time
    valid_until: datetime           # Signal expires at this time — do not execute after
    strategy_id: str
    bar_size: str                   # Which granularity produced this signal
    signals: dict[str, float]       # {ticker: signal in [-1, +1]}
    signal_metadata: dict[str, dict]
    is_valid: bool
    invalid_tickers: list[str]

    def __post_init__(self) -> None:
        for ticker, signal in self.signals.items():
            if abs(signal) > 1.0 + 1e-9:
                raise ValueError(
                    f"Strategy {self.strategy_id}: signal for {ticker} = {signal:.4f} "
                    f"is outside [-1, +1]. Normalisation failed."
                )

    @property
    def is_expired(self) -> bool:
        """True if this signal is past its valid_until timestamp."""
        return datetime.now(timezone.utc) > self.valid_until

    @property
    def is_actionable(self) -> bool:
        """True if signal is valid, not expired, and not empty."""
        return self.is_valid and not self.is_expired and bool(self.signals)


class BaseStrategy(ABC):
    """
    Abstract base for all strategies at any frequency.

    Daily strategies subclass this today.
    Future intraday strategies subclass this identically —
    only their StrategyConfig.frequency and bar_size differ.
    """

    def __init__(self, config: StrategyConfig) -> None:
        self.config = config
        self.logger = logging.getLogger(f"strategy.{config.strategy_id}")

    @property
    def strategy_id(self) -> str:
        return self.config.strategy_id

    @property
    def universe(self) -> list[str]:
        return self.config.universe

    @property
    def frequency(self) -> str:
        return self.config.frequency

    @property
    def bar_size(self) -> str:
        return self.config.bar_size

    @abstractmethod
    def compute_signals(
        self,
        data: dict[str, pd.DataFrame],
        as_of: datetime,
    ) -> SignalResult:
        """
        Generate normalised trading signals.

        Args:
            data:   {ticker: DataFrame} indexed by timestamp (TIMESTAMPTZ-aware).
                    Works for daily bars (one row per day) or intraday bars
                    (one row per bar). Strategy uses self.config.bar_size
                    to know what granularity it expects.
            as_of:  UTC datetime for which signals are being computed.
                    Data must only include information available at this moment.
                    No look-ahead bias regardless of frequency.

        Returns:
            SignalResult with signals in [-1, +1] and validity window set.
        """
        ...

    @abstractmethod
    def validate_inputs(
        self,
        data: dict[str, pd.DataFrame],
        as_of: datetime,
    ) -> tuple[bool, list[str]]:
        """Validate data quality. Returns (all_valid, invalid_tickers)."""
        ...

    def run(
        self,
        data: dict[str, pd.DataFrame],
        as_of: Optional[datetime] = None,
    ) -> SignalResult:
        """
        Main entry point — validates inputs then computes signals.
        Never call compute_signals() directly.
        """
        import time
        if as_of is None:
            as_of = datetime.now(timezone.utc)

        start = time.monotonic()

        try:
            all_valid, invalid_tickers = self.validate_inputs(data, as_of)
        except Exception as e:
            self.logger.error("Input validation error on %s: %s", as_of, e, exc_info=True)
            return self._flat_result(as_of, is_valid=False)

        valid_count = len(self.universe) - len(invalid_tickers)
        if valid_count < 4:
            self.logger.error(
                "%s: Only %d valid tickers on %s — need ≥4 for cross-sectional ranking.",
                self.strategy_id, valid_count, as_of,
            )
            return self._flat_result(as_of, is_valid=False)

        if invalid_tickers:
            self.logger.warning("%s: Excluding %s due to data issues.", self.strategy_id, invalid_tickers)

        try:
            result = self.compute_signals(data, as_of)
            elapsed_ms = (time.monotonic() - start) * 1000
            self.logger.info(
                "%s signals computed in %.1fms | %s | valid tickers: %d/%d",
                self.strategy_id, elapsed_ms, as_of.strftime("%Y-%m-%d %H:%M UTC"),
                len([v for v in result.signals.values() if v != 0]),
                len(self.universe),
            )
            return result
        except Exception as e:
            self.logger.error("%s: compute_signals error: %s", self.strategy_id, e, exc_info=True)
            return self._flat_result(as_of, is_valid=False)

    def _flat_result(self, as_of: datetime, is_valid: bool = True) -> SignalResult:
        """Return zero-signal result for all universe members."""
        validity = self.config.signal_validity_window
        return SignalResult(
            computed_at=as_of,
            valid_from=as_of,
            valid_until=as_of + validity,
            strategy_id=self.strategy_id,
            bar_size=self.bar_size,
            signals={ticker: 0.0 for ticker in self.universe},
            signal_metadata={},
            is_valid=is_valid,
            invalid_tickers=self.universe if not is_valid else [],
        )

    def _check_min_history(
        self,
        data: dict[str, pd.DataFrame],
        as_of: datetime,
    ) -> list[str]:
        """Check each ticker has >= min_history_bars bars up to as_of."""
        insufficient = []
        for ticker in self.universe:
            if ticker not in data or data[ticker].empty:
                insufficient.append(ticker)
                continue
            df = data[ticker]
            available = len(df[df.index <= pd.Timestamp(as_of)])
            if available < self.config.min_history_bars:
                insufficient.append(ticker)
                self.logger.warning(
                    "%s: %s has %d bars, needs %d.",
                    self.strategy_id, ticker, available, self.config.min_history_bars,
                )
        return insufficient

    @staticmethod
    def cross_sectional_zscore(series: pd.Series, clip: float = 2.0) -> pd.Series:
        """
        Normalise cross-sectional series to z-scores clipped to ±clip,
        then map to [-1, +1].

        Works identically at daily and intraday frequency.
        """
        if series.std() < 1e-10:
            return pd.Series(0.0, index=series.index)
        z = (series - series.mean()) / series.std()
        return z.clip(-clip, clip) / clip