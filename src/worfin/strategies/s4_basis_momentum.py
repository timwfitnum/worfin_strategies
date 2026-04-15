"""
strategies/s4_basis_momentum.py
Strategy 4: Basis-Momentum (BUILD FIRST — highest Sharpe, highest t-stat)

SIGNAL LOGIC:
  Two sub-signals, normalised cross-sectionally, combined with interaction term.

  Sub-signal 1 — Carry (Basis):
    carry_i = (Cash_i - F3M_i) / Cash_i × (365 / DTE_i)
    Positive = backwardation (earn roll yield long)
    Negative = contango (pay roll yield long)
    Uses ACTUAL calendar DTE — not fixed 91 days.

  Sub-signal 2 — Momentum:
    momentum_i = ln(P_t-5 / P_t-65)   ← 60-day return, skip last 5 days
    The 5-day skip avoids short-term reversal contaminating the signal.

  Composite:
    z_carry_i = cross_sectional_zscore(carry_i)
    z_mom_i   = cross_sectional_zscore(momentum_i)
    signal_i  = 0.5 × z_carry + 0.5 × z_mom + 0.25 × (z_carry × z_mom)

  The interaction term is the KEY INNOVATION:
    When carry and momentum AGREE:   |interaction| is POSITIVE → larger position
    When carry and momentum DISAGREE: |interaction| is NEGATIVE → smaller position
    This filters out situations where the two signals contradict each other.

REFERENCE: Bakshi, Gao & Rossi (2019) — t-stat 4.14 (highest in our universe)
REBALANCE: Bi-weekly (every 10 trading days)
ALLOCATION: 25% of total capital (largest single strategy)
"""

from __future__ import annotations

import logging
from datetime import date

import numpy as np
import pandas as pd

from worfin.config.calendar import get_lme_3m_dte
from worfin.config.metals import S4_UNIVERSE, Exchange, get_metal
from worfin.strategies.base import BaseStrategy, SignalResult, StrategyConfig

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# STRATEGY PARAMETERS
# These were set using IS data (2005–2017). Do NOT adjust on OOS data.
# ─────────────────────────────────────────────────────────────────────────────

S4_CONFIG = StrategyConfig(
    strategy_id="S4",
    name="Basis-Momentum",
    universe=S4_UNIVERSE,
    rebalance_freq="biweekly",
    target_vol=0.11,  # 11% annualised per-strategy vol target
    max_drawdown_budget=0.15,  # Suspend if 15% drawdown from HWM
    min_history_days=70,  # Need 65 days for momentum + 5-day skip
    parameters={
        "carry_weight": 0.50,  # Weight on carry sub-signal
        "momentum_weight": 0.50,  # Weight on momentum sub-signal
        "interaction_weight": 0.25,  # Weight on carry × momentum interaction
        "momentum_lookback": 60,  # Days for momentum lookback
        "momentum_skip": 5,  # Days to skip (short-term reversal avoidance)
        "zscore_clip": 2.0,  # Clip z-scores at ±2 before normalising
    },
)


class BasisMomentumStrategy(BaseStrategy):
    """
    Strategy S4: Basis-Momentum

    Required data columns per ticker DataFrame:
        - "close": Daily settlement price (front-month or Cash)
        - "cash_price": LME Cash price (for LME metals) OR front-month (COMEX)
        - "f3m_price": LME 3-Month price (for LME) OR second-month (COMEX)
        - "trade_date": Date of the price observation

    The DataFrame must be indexed by date (pd.DatetimeIndex).
    """

    def __init__(self, config: StrategyConfig = S4_CONFIG) -> None:
        super().__init__(config)
        self._params = config.parameters

    def validate_inputs(
        self,
        data: dict[str, pd.DataFrame],
        as_of_date: date,
    ) -> tuple[bool, list[str]]:
        """
        Validates:
          1. All required columns present
          2. Sufficient history for momentum lookback
          3. No NaN in cash/3M prices on as_of_date
          4. Positive prices (sanity check)
        """
        invalid = self._check_min_history(data, as_of_date)
        required_cols = {"close", "cash_price", "f3m_price"}

        for ticker in self.universe:
            if ticker in invalid:
                continue
            df = data.get(ticker)
            if df is None:
                invalid.append(ticker)
                continue

            # Check required columns
            missing = required_cols - set(df.columns)
            if missing:
                logger.warning("%s: Missing columns %s for %s.", self.strategy_id, missing, ticker)
                invalid.append(ticker)
                continue

            # Check as_of_date has data
            as_of_ts = pd.Timestamp(as_of_date)
            if as_of_ts not in df.index:
                logger.warning("%s: No data for %s on %s.", self.strategy_id, ticker, as_of_date)
                invalid.append(ticker)
                continue

            row = df.loc[as_of_ts]
            # Check prices are valid
            for col in required_cols:
                val = row[col]
                if pd.isna(val) or val <= 0:
                    logger.warning(
                        "%s: Invalid %s price for %s on %s: %s",
                        self.strategy_id,
                        col,
                        ticker,
                        as_of_date,
                        val,
                    )
                    invalid.append(ticker)
                    break

        all_valid = len(invalid) == 0
        return all_valid, list(set(invalid))

    def compute_signals(
        self,
        data: dict[str, pd.DataFrame],
        as_of_date: date,
    ) -> SignalResult:
        """
        Compute basis-momentum composite signals for all valid tickers.

        Returns SignalResult with signals in [-1, +1].
        """
        as_of_ts = pd.Timestamp(as_of_date)
        params = self._params

        # Only compute for valid tickers
        _, invalid = self.validate_inputs(data, as_of_date)
        valid_tickers = [t for t in self.universe if t not in invalid]

        if len(valid_tickers) < 4:
            logger.error(
                "%s: Only %d valid tickers on %s (need ≥4 for meaningful cross-sectional ranking).",
                self.strategy_id,
                len(valid_tickers),
                as_of_date,
            )
            return self._flat_result(as_of_date, is_valid=False)

        # ── Step 1: Compute raw sub-signals ──────────────────────────────────
        raw_carry: dict[str, float] = {}
        raw_momentum: dict[str, float] = {}
        metadata: dict[str, dict] = {}

        for ticker in valid_tickers:
            df = data[ticker]
            df_to_date = df[df.index <= as_of_ts]

            # ── Carry sub-signal ──────────────────────────────────────────────
            row = df_to_date.loc[as_of_ts]
            cash_price = float(row["cash_price"])
            f3m_price = float(row["f3m_price"])

            # Get actual DTE (calendar days between Cash and 3M settle)
            metal = get_metal(ticker)
            if metal.exchange == Exchange.LME:
                dte = get_lme_3m_dte(as_of_date)
            else:
                # COMEX: approximate with actual days to next contract expiry
                # If f3m_expiry column exists, use it; else default to 91
                dte = int(row.get("f3m_dte", 91))

            if dte <= 0:
                logger.warning(
                    "%s: DTE <= 0 for %s on %s — skipping carry.",
                    self.strategy_id,
                    ticker,
                    as_of_date,
                )
                raw_carry[ticker] = 0.0
            else:
                # Annualised carry = (Cash - 3M) / Cash × (365 / DTE)
                # Positive = backwardation, negative = contango
                raw_carry[ticker] = (cash_price - f3m_price) / cash_price * (365 / dte)

            # ── Momentum sub-signal ───────────────────────────────────────────
            skip = params["momentum_skip"]
            lookback = params["momentum_lookback"]

            # Need lookback + skip days of history
            min_idx = len(df_to_date) - lookback - skip
            if min_idx < 0:
                logger.warning(
                    "%s: Insufficient history for momentum on %s (%d days available).",
                    self.strategy_id,
                    ticker,
                    len(df_to_date),
                )
                raw_momentum[ticker] = 0.0
            else:
                # Price 65 days ago (skip last 5 to avoid short-term reversal)
                price_now = float(df_to_date["close"].iloc[-(skip + 1)])  # T-5
                price_then = float(df_to_date["close"].iloc[-(lookback + skip)])  # T-65

                if price_then <= 0:
                    raw_momentum[ticker] = 0.0
                else:
                    raw_momentum[ticker] = np.log(price_now / price_then)

            metadata[ticker] = {
                "cash_price": cash_price,
                "f3m_price": f3m_price,
                "dte": dte,
                "raw_carry": raw_carry[ticker],
                "raw_momentum": raw_momentum.get(ticker, 0.0),
            }

        # ── Step 2: Cross-sectional z-score normalisation ─────────────────────
        carry_series = pd.Series(raw_carry)
        momentum_series = pd.Series(raw_momentum)

        z_carry = self.cross_sectional_zscore(carry_series, clip=params["zscore_clip"])
        z_mom = self.cross_sectional_zscore(momentum_series, clip=params["zscore_clip"])

        # ── Step 3: Composite signal with interaction term ────────────────────
        # signal_i = 0.5×z_carry + 0.5×z_mom + 0.25×(z_carry × z_mom)
        #
        # The interaction term is positive when both signals agree on direction,
        # and negative when they disagree — naturally filters mixed signals.
        signals_raw = (
            params["carry_weight"] * z_carry
            + params["momentum_weight"] * z_mom
            + params["interaction_weight"] * (z_carry * z_mom)
        )

        # ── Step 4: Final normalisation to [-1, +1] ───────────────────────────
        # Re-normalise the composite (interaction can push it beyond ±1)
        final_signals = self.cross_sectional_zscore(signals_raw, clip=params["zscore_clip"])

        # Add composite signal data to metadata
        for ticker in valid_tickers:
            metadata[ticker].update(
                {
                    "z_carry": float(z_carry.get(ticker, 0.0)),
                    "z_momentum": float(z_mom.get(ticker, 0.0)),
                    "composite_raw": float(signals_raw.get(ticker, 0.0)),
                    "final_signal": float(final_signals.get(ticker, 0.0)),
                }
            )

        # Build final signals dict — flat (0.0) for any invalid/missing tickers
        all_signals: dict[str, float] = {t: 0.0 for t in self.universe}
        for ticker in valid_tickers:
            all_signals[ticker] = float(final_signals.get(ticker, 0.0))

        return SignalResult(
            as_of_date=as_of_date,
            strategy_id=self.strategy_id,
            signals=all_signals,
            signal_metadata=metadata,
            is_valid=True,
            invalid_tickers=invalid,
        )

    def get_top_longs_shorts(
        self,
        signal_result: SignalResult,
        n: int = 3,
    ) -> tuple[list[str], list[str]]:
        """
        Return the top N long and short tickers by signal strength.
        Useful for reporting and monitoring.
        """
        signals = signal_result.signals
        sorted_by_signal = sorted(signals.items(), key=lambda x: x[1], reverse=True)
        longs = [t for t, s in sorted_by_signal if s > 0][:n]
        shorts = [t for t, s in sorted_by_signal if s < 0][:n]
        return longs, shorts
