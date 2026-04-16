"""
data/pipeline/volatility.py
Realised volatility computation for all metals.

Used as input to risk/sizing.py — the vol estimates here drive
every position size in the system. Accuracy is critical.

Convention: always annualised (× √252), always log-returns.
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from worfin.risk.limits import VOL_FLOOR

logger = logging.getLogger(__name__)

TRADING_DAYS_PER_YEAR: int = 252


def compute_log_returns(prices: pd.Series) -> pd.Series:
    """
    Compute daily log returns from a price series.
    Log returns are used throughout (symmetric, additive over time).

    Args:
        prices: Daily settlement prices, indexed by date.

    Returns:
        Log return series (NaN for first observation).
    """
    if prices.isnull().any():
        logger.warning(
            "Price series contains %d NaN values — forward-filling before return calculation.",
            prices.isnull().sum(),
        )
        prices = prices.ffill()

    return np.log(prices / prices.shift(1))


def compute_realised_vol(
    prices: pd.Series,
    window: int,
    annualise: bool = True,
) -> pd.Series:
    """
    Rolling realised volatility from log returns.

    Args:
        prices:    Daily settlement price series.
        window:    Lookback in trading days (typically 20 or 60).
        annualise: If True, multiply by √252 to get annualised vol.

    Returns:
        Volatility series (annualised if annualise=True).
        First (window) observations will be NaN.
    """
    log_returns = compute_log_returns(prices)
    rolling_vol = log_returns.rolling(window=window, min_periods=window).std()

    if annualise:
        rolling_vol = rolling_vol * np.sqrt(TRADING_DAYS_PER_YEAR)

    return rolling_vol


def compute_vol_estimates(
    prices: pd.Series,
    as_of_date: pd.Timestamp | None = None,
) -> dict[str, float]:
    """
    Compute the two vol estimates needed for position sizing.

    Returns both 20-day (primary) and 60-day (robustness cap) estimates
    for the most recent available date (or as_of_date if specified).

    Args:
        prices:      Daily price series.
        as_of_date:  Date to compute vol as of. Defaults to last available date.

    Returns:
        {"vol_20d": float, "vol_60d": float}
        Both are annualised and respect the VOL_FLOOR.
    """
    if len(prices) < 60:
        logger.warning(
            "Fewer than 60 observations for vol computation (%d available). "
            "60d vol will be NaN — 20d vol used as cap.",
            len(prices),
        )

    vol_20 = compute_realised_vol(prices, window=20)
    vol_60 = compute_realised_vol(prices, window=60)

    if as_of_date is not None:
        if as_of_date not in prices.index:
            raise ValueError(f"as_of_date {as_of_date} not found in price index.")
        vol_20_val = vol_20.loc[as_of_date]
        vol_60_val = vol_60.loc[as_of_date]
    else:
        vol_20_val = vol_20.iloc[-1]
        vol_60_val = vol_60.iloc[-1]

    # Handle NaN (insufficient history)
    if pd.isna(vol_20_val):
        logger.error("20d vol is NaN — returning VOL_FLOOR. Investigate data quality.")
        vol_20_val = VOL_FLOOR

    if pd.isna(vol_60_val):
        logger.warning("60d vol is NaN (insufficient history) — using 20d vol as substitute.")
        vol_60_val = vol_20_val

    return {
        "vol_20d": float(vol_20_val),
        "vol_60d": float(vol_60_val),
    }


def compute_all_vol_estimates(
    prices_dict: dict[str, pd.Series],
    as_of_date: pd.Timestamp | None = None,
) -> dict[str, dict[str, float]]:
    """
    Compute vol estimates for all metals in the universe.

    Args:
        prices_dict: {ticker: price_series}
        as_of_date:  Date to compute estimates as of.

    Returns:
        {ticker: {"vol_20d": float, "vol_60d": float}}
    """
    result: dict[str, dict[str, float]] = {}
    for ticker, prices in prices_dict.items():
        try:
            result[ticker] = compute_vol_estimates(prices, as_of_date)
        except Exception as e:
            logger.error("Failed to compute vol for %s: %s — using floor.", ticker, e)
            result[ticker] = {"vol_20d": VOL_FLOOR, "vol_60d": VOL_FLOOR}
    return result


def detect_vol_regime(
    ticker: str,
    vol_20d: float,
    strategy_target_vol: float,
) -> str:
    """
    Classify the current vol regime for a metal.
    Used for monitoring and alert generation.

    Returns: "normal" | "elevated" | "extreme"
    """
    ratio = vol_20d / strategy_target_vol
    if ratio < 1.5:
        return "normal"
    elif ratio < 2.0:
        return "elevated"  # Alert — approaching allocation reduction
    else:
        return "extreme"  # Halve allocation immediately


def flag_vol_spikes(
    prices: pd.Series,
    lookback: int = 60,
    spike_threshold: float = 4.0,
) -> pd.Series:
    """
    Identify days where daily return exceeds spike_threshold × rolling std.
    Returns a boolean series — True = spike day (investigate before using).

    This is for data quality monitoring, not trading signal generation.
    """
    log_returns = compute_log_returns(prices)
    rolling_mean = log_returns.rolling(lookback).mean()
    rolling_std = log_returns.rolling(lookback).std()
    z_score = (log_returns - rolling_mean) / rolling_std
    return z_score.abs() > spike_threshold
