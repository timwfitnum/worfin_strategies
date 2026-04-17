"""
risk/sizing.py
Volatility-targeting position sizing.

ALL position sizes flow through this module — strategies never size themselves.
The formula: Position_Notional = (target_vol × allocation) / vol_estimate

Key protections:
  - VOL_FLOOR: never use vol < 10% (prevents outsized positions in compressed vol)
  - 60d robustness cap: never exceed what 60d vol would produce
  - Liquidity discount: reduces exposure to illiquid metals
  - Min/max notional guardrails

FX RATE CONVENTION:
  usd_gbp_rate is REQUIRED on all public functions — no default.
  Callers must fetch a live rate from worfin.data.ingestion.fx_rates.get_usd_gbp().
  This prevents silently using a stale hardcoded rate in backtests or live trading.
"""

from __future__ import annotations

import logging
from decimal import Decimal

from worfin.config.metals import ALL_METALS, get_lots_for_notional
from worfin.risk.limits import (
    MAX_SINGLE_METAL_PCT,
    MIN_POSITION_NOTIONAL_GBP,
    STRATEGY_ALLOCATION,
    STRATEGY_TARGET_VOL,
    VOL_FLOOR,
)

logger = logging.getLogger(__name__)

# Liquidity discount factors: reduce target notional for illiquid metals
_LIQUIDITY_DISCOUNT: dict[int, float] = {
    1: 1.00,  # Tier 1: full allocation
    2: 0.75,  # Tier 2: 75% of computed notional
    3: 0.50,  # Tier 3: 50% of computed notional
}


def compute_position_notional(
    strategy_id: str,
    ticker: str,
    total_capital_gbp: float,
    realised_vol_20d: float,
    realised_vol_60d: float,
    signal: float,
    usd_gbp_rate: float,   # REQUIRED — fetch from data.ingestion.fx_rates.get_usd_gbp()
) -> Decimal:
    """
    Compute target position notional in GBP for a given metal/strategy.

    Args:
        strategy_id:       e.g. "S4"
        ticker:            Metal ticker, e.g. "CA"
        total_capital_gbp: Total portfolio NAV in GBP
        realised_vol_20d:  Trailing 20-day annualised vol (as decimal, e.g. 0.15)
        realised_vol_60d:  Trailing 60-day annualised vol (as decimal, e.g. 0.18)
        signal:            Normalised signal in [-1, +1]
        usd_gbp_rate:      USD/GBP FX rate (1 GBP = X USD).
                           Must be fetched fresh — no hardcoded default.

    Returns:
        Signed notional in GBP (positive = long, negative = short).
        Returns Decimal("0") if signal is effectively zero or notional < minimum.

    Raises:
        KeyError: If strategy_id or ticker is not in the defined universe.
        ValueError: If signal is outside [-1, +1] range.
    """
    if abs(signal) > 1.0 + 1e-9:
        raise ValueError(f"Signal must be in [-1, +1], got {signal:.4f}")
    if abs(signal) < 1e-6:
        return Decimal("0")

    target_vol = STRATEGY_TARGET_VOL[strategy_id]
    allocation_pct = STRATEGY_ALLOCATION[strategy_id]
    capital_allocated = total_capital_gbp * allocation_pct
    metal = ALL_METALS[ticker]

    # ── Step 1: VOL FLOOR ────────────────────────────────────────────────────
    # Never use vol below 10% — protects against extreme positions in
    # artificially compressed volatility regimes (e.g., pre-squeeze Nickel)
    effective_vol_20d = max(realised_vol_20d, VOL_FLOOR)
    effective_vol_60d = max(realised_vol_60d, VOL_FLOOR)

    if realised_vol_20d < VOL_FLOOR:
        logger.warning(
            "Vol floor applied: %s 20d vol %.1f%% < floor %.1f%%. Using floor.",
            ticker,
            realised_vol_20d * 100,
            VOL_FLOOR * 100,
        )

    # ── Step 2: PRIMARY SIZING ───────────────────────────────────────────────
    # notional = (target_vol × capital_allocated) / effective_vol
    notional_20d = (target_vol * capital_allocated) / effective_vol_20d

    # ── Step 3: 60d ROBUSTNESS CAP ───────────────────────────────────────────
    # Never exceed what 60d vol would produce.
    # Prevents outsizing during short-term vol compression.
    notional_60d_cap = (target_vol * capital_allocated) / effective_vol_60d
    notional = min(notional_20d, notional_60d_cap)

    # ── Step 4: SINGLE-METAL CAP ─────────────────────────────────────────────
    single_metal_cap = total_capital_gbp * MAX_SINGLE_METAL_PCT
    notional = min(notional, single_metal_cap)

    # ── Step 5: LIQUIDITY DISCOUNT ────────────────────────────────────────────
    tier = int(metal.liquidity_tier)
    discount = _LIQUIDITY_DISCOUNT.get(tier, 0.50)
    notional *= discount

    # ── Step 6: SIGNAL SCALING ────────────────────────────────────────────────
    # Scale by signal magnitude and apply direction
    notional *= abs(signal)
    signed_notional = notional if signal > 0 else -notional

    # ── Step 7: MINIMUM NOTIONAL CHECK ───────────────────────────────────────
    if abs(signed_notional) < MIN_POSITION_NOTIONAL_GBP:
        logger.debug(
            "%s/%s: notional £%.0f below minimum £%.0f — returning zero.",
            strategy_id,
            ticker,
            abs(signed_notional),
            MIN_POSITION_NOTIONAL_GBP,
        )
        return Decimal("0")

    return Decimal(str(round(signed_notional, 2)))


def compute_lots(
    strategy_id: str,
    ticker: str,
    total_capital_gbp: float,
    realised_vol_20d: float,
    realised_vol_60d: float,
    signal: float,
    current_price_usd: float,
    usd_gbp_rate: float,   # REQUIRED — fetch from data.ingestion.fx_rates.get_usd_gbp()
) -> int:
    """
    Compute the signed lot count for a proposed position.

    This is the function to call before placing orders.
    Always rounds toward zero (conservative — never over-sizes).

    Args:
        current_price_usd: Current front-month price in USD/unit
        usd_gbp_rate:      USD per GBP (e.g. 1.27).
                           Must be fetched fresh — no hardcoded default.

    Returns:
        Signed lot count (0 if position too small to be meaningful)
    """
    notional_gbp = compute_position_notional(
        strategy_id=strategy_id,
        ticker=ticker,
        total_capital_gbp=total_capital_gbp,
        realised_vol_20d=realised_vol_20d,
        realised_vol_60d=realised_vol_60d,
        signal=signal,
        usd_gbp_rate=usd_gbp_rate,
    )

    if notional_gbp == 0:
        return 0

    # Convert GBP notional to USD for lot calculation
    notional_usd = float(notional_gbp) * usd_gbp_rate

    # Compute unsigned lots (floor division — never over-size)
    abs_lots = get_lots_for_notional(ticker, current_price_usd, abs(notional_usd))

    # Apply direction
    direction = 1 if notional_gbp > 0 else -1
    return abs_lots * direction


def compute_portfolio_sizing(
    strategy_signals: dict[str, dict[str, float]],
    vol_estimates: dict[str, dict[str, float]],
    prices: dict[str, float],
    total_capital_gbp: float,
    usd_gbp_rate: float,   # REQUIRED — fetch from data.ingestion.fx_rates.get_usd_gbp()
) -> dict[str, dict[str, int]]:
    """
    Compute full portfolio sizing across all strategies and metals.

    Args:
        strategy_signals: {strategy_id: {ticker: signal}}
        vol_estimates:    {ticker: {"vol_20d": float, "vol_60d": float}}
        prices:           {ticker: current_price_usd}
        total_capital_gbp: Total NAV in GBP
        usd_gbp_rate:     USD per GBP. Must be fetched fresh.

    Returns:
        {strategy_id: {ticker: lots}}
    """
    result: dict[str, dict[str, int]] = {}

    for strategy_id, signals in strategy_signals.items():
        result[strategy_id] = {}
        for ticker, signal in signals.items():
            if ticker not in vol_estimates or ticker not in prices:
                logger.warning(
                    "Missing vol or price data for %s — skipping sizing.", ticker
                )
                result[strategy_id][ticker] = 0
                continue

            vols = vol_estimates[ticker]
            lots = compute_lots(
                strategy_id=strategy_id,
                ticker=ticker,
                total_capital_gbp=total_capital_gbp,
                realised_vol_20d=vols["vol_20d"],
                realised_vol_60d=vols["vol_60d"],
                signal=signal,
                current_price_usd=prices[ticker],
                usd_gbp_rate=usd_gbp_rate,
            )
            result[strategy_id][ticker] = lots

    return result