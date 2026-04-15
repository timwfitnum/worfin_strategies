"""
strategies/base.py
Abstract base class for all six strategies.

Every strategy MUST:
  - Inherit from BaseStrategy
  - Implement compute_signals() — returns normalised signals in [-1, +1]
  - Implement validate_inputs() — returns False if data is unfit for use
  - Be stateless: data in → signals out. No side effects. No DB writes.

Position sizing is NOT done here — it happens in risk/sizing.py.
"""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import date

import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class StrategyConfig:
    """Immutable configuration for a strategy instance."""
    strategy_id: str                # e.g., "S4"
    name: str                       # e.g., "Basis-Momentum"
    universe: list[str]             # Metal tickers in scope
    rebalance_freq: str             # "daily", "weekly", "biweekly", "monthly"
    target_vol: float               # Annualised vol target
    max_drawdown_budget: float      # Max drawdown before suspension
    min_history_days: int           # Minimum history needed for signals
    parameters: dict = field(default_factory=dict)  # Strategy-specific params


@dataclass
class SignalResult:
    """Output of compute_signals() — normalised signals with metadata."""
    as_of_date: date
    strategy_id: str
    signals: dict[str, float]          # {ticker: signal in [-1, +1]}
    signal_metadata: dict[str, dict]   # {ticker: {raw_inputs...}}
    is_valid: bool                     # False if data quality prevented computation
    invalid_tickers: list[str]         # Tickers excluded due to data issues

    def __post_init__(self) -> None:
        """Validate signal range after construction."""
        for ticker, signal in self.signals.items():
            if abs(signal) > 1.0 + 1e-9:
                raise ValueError(
                    f"Strategy {self.strategy_id}: signal for {ticker} = {signal:.4f} "
                    f"is outside [-1, +1] range. Normalisation failed."
                )


class BaseStrategy(ABC):
    """
    Abstract base for all metals strategies.

    Subclasses implement compute_signals() and validate_inputs().
    The base class handles logging and input validation scaffolding.
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

    @abstractmethod
    def compute_signals(
        self,
        data: dict[str, pd.DataFrame],
        as_of_date: date,
    ) -> SignalResult:
        """
        Generate normalised trading signals.

        Args:
            data:        {ticker: DataFrame} with required price/term structure data.
                         Each DataFrame is indexed by date with OHLCV + derived columns.
                         Strategy-specific columns documented in each subclass.
            as_of_date:  The date for which signals are being computed.
                         Data must only include information available ON this date.
                         No look-ahead bias.

        Returns:
            SignalResult with signals in [-1, +1].
            Positive = long; Negative = short; 0 = flat.
        """
        ...

    @abstractmethod
    def validate_inputs(
        self,
        data: dict[str, pd.DataFrame],
        as_of_date: date,
    ) -> tuple[bool, list[str]]:
        """
        Validate data quality before computing signals.

        Args:
            data:        Same structure as compute_signals().
            as_of_date:  Signal date.

        Returns:
            (all_valid, list_of_invalid_tickers)
            If any ticker is invalid, that ticker is excluded from signals.
            If critical data is invalid (e.g., all tickers), is_valid=False.
        """
        ...

    def run(
        self,
        data: dict[str, pd.DataFrame],
        as_of_date: date,
    ) -> SignalResult:
        """
        Main entry point. Validates inputs, then computes signals.
        Never call compute_signals() directly — always use run().

        Handles:
          - Input validation (data quality gate)
          - Logging with timing
          - Exception containment (returns flat signals on error)
        """
        import time
        start = time.monotonic()

        # Step 1: Validate inputs
        try:
            all_valid, invalid_tickers = self.validate_inputs(data, as_of_date)
        except Exception as e:
            self.logger.error(
                "Input validation raised exception on %s: %s. Returning flat signals.",
                as_of_date, e, exc_info=True,
            )
            return self._flat_result(as_of_date, is_valid=False, reason=str(e))

        if not all_valid and not any(
            t in self.universe for t in set(self.universe) - set(invalid_tickers)
        ):
            self.logger.error(
                "%s: All tickers invalid on %s. Cannot generate signals.",
                self.strategy_id, as_of_date,
            )
            return self._flat_result(as_of_date, is_valid=False)

        if invalid_tickers:
            self.logger.warning(
                "%s: Excluding %d tickers due to data issues: %s",
                self.strategy_id, len(invalid_tickers), invalid_tickers,
            )

        # Step 2: Compute signals
        try:
            result = self.compute_signals(data, as_of_date)
            elapsed = time.monotonic() - start
            self.logger.info(
                "%s signals computed for %s in %.1fms. Valid tickers: %d.",
                self.strategy_id, as_of_date, elapsed * 1000,
                len([v for v in result.signals.values() if v != 0]),
            )
            return result
        except Exception as e:
            self.logger.error(
                "%s: compute_signals raised exception on %s: %s. Returning flat.",
                self.strategy_id, as_of_date, e, exc_info=True,
            )
            return self._flat_result(as_of_date, is_valid=False, reason=str(e))

    def _flat_result(
        self,
        as_of_date: date,
        is_valid: bool = True,
        reason: str = "",
    ) -> SignalResult:
        """Return a zero-signal result for all universe members."""
        if reason:
            self.logger.warning(
                "%s: Flat result on %s. Reason: %s", self.strategy_id, as_of_date, reason
            )
        return SignalResult(
            as_of_date=as_of_date,
            strategy_id=self.strategy_id,
            signals={ticker: 0.0 for ticker in self.universe},
            signal_metadata={},
            is_valid=is_valid,
            invalid_tickers=self.universe if not is_valid else [],
        )

    def _check_min_history(
        self,
        data: dict[str, pd.DataFrame],
        as_of_date: date,
    ) -> list[str]:
        """
        Check that each ticker has sufficient price history.
        Returns list of tickers with insufficient history.
        """
        insufficient = []
        for ticker in self.universe:
            if ticker not in data:
                insufficient.append(ticker)
                continue
            df = data[ticker]
            available = len(df[df.index <= pd.Timestamp(as_of_date)])
            if available < self.config.min_history_days:
                insufficient.append(ticker)
                self.logger.warning(
                    "%s: %s has only %d days of history (needs %d).",
                    self.strategy_id, ticker, available, self.config.min_history_days,
                )
        return insufficient

    @staticmethod
    def cross_sectional_zscore(series: pd.Series, clip: float = 2.0) -> pd.Series:
        """
        Normalise a cross-sectional series to z-scores, clipped to ±clip.
        Maps ±clip sigma → ±1.

        Used by all strategies to produce final signals in [-1, +1].
        """
        if series.std() < 1e-10:
            return pd.Series(0.0, index=series.index)
        z = (series - series.mean()) / series.std()
        clipped = z.clip(-clip, clip)
        return clipped / clip   # normalise to [-1, +1]