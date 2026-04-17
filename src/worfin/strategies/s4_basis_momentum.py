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
from datetime import datetime  # FIX: was `date` — all public methods need datetime

import numpy as np
import pandas as pd

from worfin.config.calendar import get_lme_3m_dte
from worfin.config.metals import S4_UNIVERSE, Exchange, get_metal
from worfin.strategies.base import (
    BarSize,          # FIX: was not imported
    BaseStrategy,
    Frequency,        # FIX: was not imported
    SignalResult,
    StrategyConfig,
)

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# STRATEGY PARAMETERS
# These were set using IS data (2005–2017). Do NOT adjust on OOS data.
# ─────────────────────────────────────────────────────────────────────────────

S4_CONFIG = StrategyConfig(
    strategy_id="S4",
    name="Basis-Momentum",
    universe=S4_UNIVERSE,
    frequency=Frequency.DAILY,      # FIX: was missing (required field)
    bar_size=BarSize.DAILY,         # FIX: was missing (required field)
    rebalance_freq="biweekly",
    target_vol=0.11,
    max_drawdown_budget=0.15,
    min_history_bars=70,            # FIX: was min_history_days (wrong field name)
    parameters={
        "carry_weight": 0.50,
        "momentum_weight": 0.50,
        "interaction_weight": 0.25,
        "momentum_lookback": 60,
        "momentum_skip": 5,
        "zscore_clip": 2.0,
    },
)


class BasisMomentumStrategy(BaseStrategy):
    """
    Strategy S4: Basis-Momentum

    Required data columns per ticker DataFrame:
        - "close":      Daily settlement price (front-month or Cash)
        - "cash_price": LME Cash price (for LME metals) OR front-month (COMEX)
        - "f3m_price":  LME 3-Month price (for LME) OR second-month (COMEX)
        - "f3m_dte":    Days-to-expiry for COMEX (optional; fallback is 91)

    The DataFrame must be indexed by DatetimeIndex (UTC-aware).
    """

    def __init__(self, config: StrategyConfig = S4_CONFIG) -> None:
        super().__init__(config)
        self._params = config.parameters

    def validate_inputs(
        self,
        data: dict[str, pd.DataFrame],
        as_of: datetime,              # FIX: was `as_of_date: date`
    ) -> tuple[bool, list[str]]:
        """
        Validates:
          1. All required columns present
          2. Sufficient history for momentum lookback (min_history_bars)
          3. No NaN in cash/3M/close prices on the last bar up to as_of
          4. Positive prices (sanity check)
        """
        invalid = self._check_min_history(data, as_of)   # FIX: was as_of_date
        required_cols = {"close", "cash_price", "f3m_price"}
        as_of_ts = pd.Timestamp(as_of)                   # FIX: now datetime → tz-aware Timestamp

        for ticker in self.universe:
            if ticker in invalid:
                continue
            df = data.get(ticker)
            if df is None:
                invalid.append(ticker)
                continue

            missing = required_cols - set(df.columns)
            if missing:
                logger.warning("%s: Missing columns %s for %s.", self.strategy_id, missing, ticker)
                invalid.append(ticker)
                continue

            # Use iloc[-1] on the slice up to as_of — avoids KeyError when
            # as_of is 14:00 UTC but index is midnight.
            df_to_date = df[df.index <= as_of_ts]
            if df_to_date.empty:
                logger.warning(
                    "%s: No data for %s on or before %s.", self.strategy_id, ticker, as_of.date()
                )
                invalid.append(ticker)
                continue

            row = df_to_date.iloc[-1]                     # FIX: was df.loc[as_of_ts] → KeyError risk
            for col in required_cols:
                val = row[col]
                if pd.isna(val) or float(val) <= 0:
                    logger.warning(
                        "%s: Invalid %s for %s on %s (value=%s).",
                        self.strategy_id, col, ticker, as_of.date(), val,
                    )
                    invalid.append(ticker)
                    break

        return len(invalid) == 0, list(set(invalid))

    def compute_signals(
        self,
        data: dict[str, pd.DataFrame],
        as_of: datetime,              # FIX: was `as_of_date: date`
    ) -> SignalResult:
        """
        Compute basis-momentum composite signals for all valid tickers.
        Returns SignalResult with signals in [-1, +1].
        """
        as_of_ts = pd.Timestamp(as_of)                   # FIX: datetime → tz-aware Timestamp
        params = self._params

        _, invalid = self.validate_inputs(data, as_of)   # FIX: was as_of_date
        valid_tickers = [t for t in self.universe if t not in invalid]

        if len(valid_tickers) < 4:
            logger.error(
                "%s: Only %d valid tickers on %s — need ≥4 for cross-sectional ranking.",
                self.strategy_id, len(valid_tickers), as_of.date(),
            )
            return self._flat_result(as_of, is_valid=False)   # FIX: was as_of_date

        # ── Step 1: Compute raw sub-signals ──────────────────────────────────
        raw_carry: dict[str, float] = {}
        raw_momentum: dict[str, float] = {}
        metadata: dict[str, dict] = {}

        for ticker in valid_tickers:
            df = data[ticker]
            df_to_date = df[df.index <= as_of_ts]

            # Last row up to as_of — same pattern as S1, avoids index KeyError
            row = df_to_date.iloc[-1]                     # FIX: was df_to_date.loc[as_of_ts]
            cash_price = float(row["cash_price"])
            f3m_price = float(row["f3m_price"])

            # ── Carry sub-signal ──────────────────────────────────────────────
            metal = get_metal(ticker)
            if metal.exchange == Exchange.LME:
                dte = get_lme_3m_dte(as_of.date())
            else:
                # COMEX: use f3m_dte if available, else 91-day approximation
                dte = int(row.get("f3m_dte", 91))

            if dte <= 0:
                logger.warning(
                    "%s: DTE ≤ 0 for %s on %s — carry set to 0.",
                    self.strategy_id, ticker, as_of.date(),
                )
                raw_carry[ticker] = 0.0
            else:
                # Annualised carry = (Cash - 3M) / Cash × (365 / DTE)
                raw_carry[ticker] = (cash_price - f3m_price) / cash_price * (365 / dte)

            # ── Momentum sub-signal ───────────────────────────────────────────
            skip = params["momentum_skip"]
            lookback = params["momentum_lookback"]

            if len(df_to_date) < lookback + skip + 1:
                logger.warning(
                    "%s: Insufficient history for momentum on %s (%d bars available, need %d).",
                    self.strategy_id, ticker, len(df_to_date), lookback + skip + 1,
                )
                raw_momentum[ticker] = 0.0
            else:
                price_now = float(df_to_date["close"].iloc[-(skip + 1)])    # T-5
                price_then = float(df_to_date["close"].iloc[-(lookback + skip)])  # T-65
                raw_momentum[ticker] = (
                    np.log(price_now / price_then) if price_then > 0 else 0.0
                )

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
        # Interaction is positive when both sub-signals agree on direction —
        # naturally amplifies high-conviction, attenuates conflicted signals.
        signals_raw = (
            params["carry_weight"] * z_carry
            + params["momentum_weight"] * z_mom
            + params["interaction_weight"] * (z_carry * z_mom)
        )

        # ── Step 4: Final normalisation to [-1, +1] ───────────────────────────
        # Re-normalise composite (interaction can push past ±1)
        final_signals = self.cross_sectional_zscore(signals_raw, clip=params["zscore_clip"])

        for ticker in valid_tickers:
            metadata[ticker].update(
                {
                    "z_carry": float(z_carry.get(ticker, 0.0)),
                    "z_momentum": float(z_mom.get(ticker, 0.0)),
                    "composite_raw": float(signals_raw.get(ticker, 0.0)),
                    "final_signal": float(final_signals.get(ticker, 0.0)),
                }
            )

        all_signals: dict[str, float] = {t: 0.0 for t in self.universe}
        for ticker in valid_tickers:
            all_signals[ticker] = float(final_signals.get(ticker, 0.0))

        # FIX: was SignalResult(as_of_date=...) — wrong field name, missing 4 required fields
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

    def get_top_longs_shorts(
        self,
        signal_result: SignalResult,
        n: int = 3,
    ) -> tuple[list[str], list[str]]:
        """
        Return the top N long and short tickers by signal strength.
        Useful for daily reporting and monitoring.
        """
        signals = signal_result.signals
        ranked = sorted(signals.items(), key=lambda x: x[1], reverse=True)
        longs = [t for t, s in ranked if s > 0][:n]
        shorts = [t for t, s in ranked if s < 0][:n]
        return longs, shorts