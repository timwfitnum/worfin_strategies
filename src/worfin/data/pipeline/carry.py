"""
data/pipeline/carry.py
Pre-compute carry signal inputs for S1 and S4.

Carry = (Cash - 3M) / Cash × (365 / DTE)
Positive = backwardation (earn roll yield going long)
Negative = contango (pay roll yield going long)

CRITICAL: DTE must be computed from ACTUAL calendar days between
Cash and 3M settle dates — never use a fixed 91-day assumption.
The 3M prompt rolls forward every LME business day.

Outputs feed into:
  clean_data.term_structure  — stored for audit and replay
  signals.computed_signals   — via strategy signal computation
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from worfin.config.calendar import get_lme_3m_dte
from worfin.config.metals import ALL_METALS, Exchange

logger = logging.getLogger(__name__)

TRADING_DAYS_PER_YEAR = 252


def compute_carry(
    cash_price: float,
    f3m_price: float,
    dte: int,
) -> float:
    """
    Annualised carry (basis) for a single metal on a single day.

    Args:
        cash_price: LME Cash settlement price ($/tonne or $/oz)
        f3m_price:  LME 3-Month settlement price
        dte:        Actual calendar days between Cash and 3M settle dates

    Returns:
        Annualised carry as decimal (e.g. 0.05 = 5% annualised)
        Positive = backwardation, Negative = contango

    Raises:
        ValueError: if inputs are invalid
    """
    if cash_price <= 0:
        raise ValueError(f"cash_price must be positive, got {cash_price}")
    if dte <= 0:
        raise ValueError(f"dte must be positive, got {dte}")

    return (cash_price - f3m_price) / cash_price * (365.0 / dte)


def compute_carry_series(
    cash_prices: pd.Series,
    f3m_prices: pd.Series,
    ticker: str,
) -> pd.Series:
    """
    Compute daily annualised carry for an LME metal over a price history.

    Handles the daily-rolling DTE calculation automatically.
    For COMEX metals, uses a fixed DTE approximation (no daily prompt system).

    Args:
        cash_prices:  Daily Cash settlement prices, DatetimeIndex (UTC)
        f3m_prices:   Daily 3-Month settlement prices, same index
        ticker:       Metal ticker (used to determine LME vs COMEX)

    Returns:
        Daily carry series, same index as inputs.
        NaN where prices are missing or DTE calculation fails.
    """
    if not cash_prices.index.equals(f3m_prices.index):
        raise ValueError("cash_prices and f3m_prices must have the same index")

    metal = ALL_METALS.get(ticker)
    if metal is None:
        raise ValueError(f"Unknown ticker: {ticker}")

    carry_values = []

    for ts in cash_prices.index:
        trade_date = ts.date() if hasattr(ts, "date") else ts

        cash = cash_prices.loc[ts]
        f3m = f3m_prices.loc[ts]

        if pd.isna(cash) or pd.isna(f3m) or cash <= 0 or f3m <= 0:
            carry_values.append(np.nan)
            continue

        try:
            if metal.exchange == Exchange.LME:
                # Use actual calendar DTE — recalculated every day
                dte = get_lme_3m_dte(trade_date)
            else:
                # COMEX: approximate with 91 days
                # Future improvement: use actual days to next expiry
                dte = 91

            carry_values.append(compute_carry(float(cash), float(f3m), dte))

        except (ValueError, Exception) as e:
            logger.warning("Carry computation failed for %s on %s: %s", ticker, trade_date, e)
            carry_values.append(np.nan)

    return pd.Series(carry_values, index=cash_prices.index, name=f"carry_{ticker}")


def compute_all_carry(
    term_structure_data: dict[str, dict[str, pd.Series]],
) -> dict[str, pd.Series]:
    """
    Compute carry for all metals in the universe.

    Args:
        term_structure_data: {ticker: {"cash": Series, "f3m": Series}}

    Returns:
        {ticker: carry_series}
    """
    results: dict[str, pd.Series] = {}

    for ticker, prices in term_structure_data.items():
        if "cash" not in prices or "f3m" not in prices:
            logger.warning("Missing cash or f3m prices for %s — skipping carry.", ticker)
            continue

        try:
            results[ticker] = compute_carry_series(
                cash_prices=prices["cash"],
                f3m_prices=prices["f3m"],
                ticker=ticker,
            )
            nan_count = results[ticker].isna().sum()
            if nan_count > 0:
                logger.warning(
                    "%s: %d NaN values in carry series (%.1f%% of %d observations).",
                    ticker,
                    nan_count,
                    nan_count / len(results[ticker]) * 100,
                    len(results[ticker]),
                )
        except Exception as e:
            logger.error("Failed to compute carry for %s: %s", ticker, e)

    return results


def cross_sectional_carry_zscore(
    carry_dict: dict[str, pd.Series],
    clip: float = 2.0,
) -> pd.DataFrame:
    """
    Compute cross-sectional z-score of carry across all metals for each date.

    This is the normalised carry sub-signal used in S1 and S4.

    Args:
        carry_dict: {ticker: carry_series} — must share the same date index
        clip:       Z-score clipping threshold (default 2.0)

    Returns:
        DataFrame with columns = tickers, values = z-scores clipped to [-1, +1]
    """
    df = pd.DataFrame(carry_dict)

    def _zscore_row(row: pd.Series) -> pd.Series:
        valid = row.dropna()
        if len(valid) < 4:
            return pd.Series(np.nan, index=row.index)
        std = valid.std()
        if std < 1e-10:
            return pd.Series(0.0, index=row.index)
        z = (row - valid.mean()) / std
        return z.clip(-clip, clip) / clip

    return df.apply(_zscore_row, axis=1)


def compute_carry_stats(carry_series: pd.Series) -> dict[str, float]:
    """
    Descriptive statistics for a carry series — used in backtesting reports.

    Returns percentiles and regime classification useful for understanding
    whether a metal is in normal/extreme contango or backwardation.
    """
    clean = carry_series.dropna()
    if len(clean) == 0:
        return {}

    return {
        "mean": float(clean.mean()),
        "std": float(clean.std()),
        "pct_10": float(clean.quantile(0.10)),
        "pct_25": float(clean.quantile(0.25)),
        "pct_50": float(clean.quantile(0.50)),
        "pct_75": float(clean.quantile(0.75)),
        "pct_90": float(clean.quantile(0.90)),
        "pct_backwardation": float((clean > 0).mean()),  # fraction of days in backwardation
        "sharpe_carry": float(clean.mean() / clean.std() * (252**0.5)) if clean.std() > 0 else 0.0,
    }
