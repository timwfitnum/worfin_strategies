"""
strategies/s1_carry.py
Strategy 1: Term Structure Carry

SIGNAL LOGIC:
  For each metal, compute annualised carry (Cash–3M basis):
    carry_i = (Cash_i - F3M_i) / Cash_i × (365 / DTE_i)

  Cross-sectionally rank all metals by carry.
  Long top tercile (deepest backwardation).
  Short bottom tercile (deepest contango).

  Final signal = cross_sectional_zscore(carry) — normalised to [-1, +1].

CRITICAL LME NOTE:
  The 3M prompt date rolls forward EVERY business day.
  DTE must be recalculated daily using get_lme_3m_dte().
  Never use a fixed 91-day assumption.

REBALANCE: Weekly (every 5 trading days)

ECONOMIC RATIONALE (Gorton & Rouwenhorst, 2006):
  Physical commodity producers need to hedge future production.
  They sell futures forward, creating a structural downward pressure on
  futures prices relative to spot (backwardation). Long carry strategies
  earn this risk premium by providing hedging capacity to producers.
  The signal is most reliable when combined with a momentum filter (see S4).
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone

import pandas as pd

from worfin.config.calendar import get_lme_3m_dte
from worfin.config.metals import S1_UNIVERSE, Exchange, get_metal
from worfin.data.pipeline.carry import compute_carry
from worfin.strategies.base import (
    BarSize,
    BaseStrategy,
    Frequency,
    SignalResult,
    StrategyConfig,
)

logger = logging.getLogger(__name__)

S1_CONFIG = StrategyConfig(
    strategy_id="S1",
    name="Term Structure Carry",
    universe=S1_UNIVERSE,
    frequency=Frequency.DAILY,
    bar_size=BarSize.DAILY,
    rebalance_freq="weekly",
    target_vol=0.09,
    max_drawdown_budget=0.12,
    min_history_bars=30,
    parameters={
        "zscore_clip":  2.0,
        "min_valid_tickers": 6,   # Need at least 6 for meaningful cross-sectional ranking
    },
)


class CarryStrategy(BaseStrategy):
    """
    Strategy S1: Term Structure Carry.

    Required DataFrame columns per ticker:
        cash_price  — LME Cash settlement price ($/tonne or $/oz)
        f3m_price   — LME 3-Month settlement price
        f3m_dte     — Actual DTE (for COMEX; LME computes from calendar)

    Index: pd.DatetimeIndex (timezone-aware, UTC)
    """

    def __init__(self, config: StrategyConfig = S1_CONFIG) -> None:
        super().__init__(config)
        self._params = config.parameters

    def validate_inputs(
        self,
        data: dict[str, pd.DataFrame],
        as_of: datetime,
    ) -> tuple[bool, list[str]]:
        invalid = self._check_min_history(data, as_of)
        required_cols = {"cash_price", "f3m_price"}

        for ticker in self.universe:
            if ticker in invalid:
                continue
            df = data.get(ticker)
            if df is None or df.empty:
                invalid.append(ticker)
                continue

            missing = required_cols - set(df.columns)
            if missing:
                logger.warning("S1: Missing columns %s for %s.", missing, ticker)
                invalid.append(ticker)
                continue

            as_of_ts = pd.Timestamp(as_of)
            df_to_date = df[df.index <= as_of_ts]
            if df_to_date.empty:
                invalid.append(ticker)
                continue

            row = df_to_date.iloc[-1]
            for col in required_cols:
                val = row[col]
                if pd.isna(val) or float(val) <= 0:
                    logger.warning("S1: Invalid %s for %s on %s.", col, ticker, as_of.date())
                    invalid.append(ticker)
                    break

        return len(invalid) == 0, list(set(invalid))

    def compute_signals(
        self,
        data: dict[str, pd.DataFrame],
        as_of: datetime,
    ) -> SignalResult:
        as_of_ts = pd.Timestamp(as_of)
        min_valid = self._params["min_valid_tickers"]

        _, invalid = self.validate_inputs(data, as_of)
        valid_tickers = [t for t in self.universe if t not in invalid]

        if len(valid_tickers) < min_valid:
            logger.error(
                "S1: Only %d valid tickers on %s — need ≥%d.",
                len(valid_tickers), as_of.date(), min_valid,
            )
            return self._flat_result(as_of, is_valid=False)

        raw_carry: dict[str, float] = {}
        metadata:  dict[str, dict]  = {}

        for ticker in valid_tickers:
            df = data[ticker]
            df_to_date = df[df.index <= as_of_ts]
            row = df_to_date.iloc[-1]

            cash_price = float(row["cash_price"])
            f3m_price  = float(row["f3m_price"])
            metal = get_metal(ticker)

            # CRITICAL: use actual DTE, not fixed 91 days
            if metal.exchange == Exchange.LME:
                dte = get_lme_3m_dte(as_of.date())
            else:
                dte = int(row.get("f3m_dte", 91))

            try:
                carry_value = compute_carry(cash_price, f3m_price, dte)
            except ValueError as e:
                logger.warning("S1: Carry computation failed for %s: %s", ticker, e)
                carry_value = 0.0

            raw_carry[ticker] = carry_value
            metadata[ticker] = {
                "cash_price":  cash_price,
                "f3m_price":   f3m_price,
                "dte":         dte,
                "raw_carry":   carry_value,
                "in_backwardation": carry_value > 0,
            }

        # Cross-sectional z-score → [-1, +1]
        carry_series = pd.Series(raw_carry)
        z_carry = self.cross_sectional_zscore(carry_series, clip=self._params["zscore_clip"])

        for ticker in valid_tickers:
            metadata[ticker]["z_carry"] = float(z_carry.get(ticker, 0.0))

        all_signals: dict[str, float] = {t: 0.0 for t in self.universe}
        for ticker in valid_tickers:
            all_signals[ticker] = float(z_carry.get(ticker, 0.0))

        return SignalResult(
            computed_at=as_of,
            valid_from=as_of,
            valid_until=as_of + self.config.signal_validity_window,
            strategy_id=self.strategy_id,
            bar_size=self.bar_size,
            signals=all_signals,
            signal_metadata=metadata,
            is_valid=True,
            invalid_tickers=invalid,
        )