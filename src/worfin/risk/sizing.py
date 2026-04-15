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
"""
from __future__ import annotations

import logging
from decimal import ROUND_DOWN, Decimal

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
    1: 1.00,   # Tier 1: full allocation
    2: 0.75,   # Tier 2: 75% of computed notional
    3: 0.50,   # Tier 3: 50% of computed notional
}


def compute_position_notional(
    strategy_id: str,
    ticker: str,
    total_capital_gbp: float,
    realised_vol_20d: float,
    realised_vol_60d: float,
    signal: float,
    usd_gbp_rate: float = 1.27,
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
        usd_gbp_rate:      USD/GBP FX rate for notional conversion

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
            "Vol floor applied: %s 20d vol %.1f%% < floor %.1f%%. "
            "Using floor. Investigate if sustained.",
            ticker, realised_vol_20d * 100, VOL_FLOOR * 100,
        )

    # ── Step 2: PRIMARY SIZING FORMULA ──────────────────────────────────────
    # Position_Notional = (target_vol / effective_vol) × capital_allocated
    # This ensures each position contributes equally to risk budget
    raw_notional = (target_vol / effective_vol_20d) * capital_allocated

    # ── Step 3: ROBUSTNESS CAP (60-day) ─────────────────────────────────────
    # Never exceed what the 60-day vol estimate would produce.
    # Prevents oversizing when short-term vol is temporarily compressed
    # but the 60d estimate correctly captures longer-run risk.
    cap_notional_60d = (target_vol / effective_vol_60d) * capital_allocated
    notional = min(raw_notional, cap_notional_60d)

    if notional < raw_notional:
        logger.debug(
            "%s: 60d robustness cap applied (20d would have given %.0f GBP, "
            "capped to %.0f GBP)",
            ticker, raw_notional, notional,
        )

    # ── Step 4: LIQUIDITY DISCOUNT ───────────────────────────────────────────
    discount = _LIQUIDITY_DISCOUNT[metal.liquidity_tier]
    notional *= discount

    # ── Step 5: SIGNAL SCALING ───────────────────────────────────────────────
    # Scale by signal strength — a signal of 0.5 gives 50% of max notional
    notional *= abs(signal)

    # ── Step 6: NAV CONCENTRATION CAP ───────────────────────────────────────
    max_allowed = total_capital_gbp * MAX_SINGLE_METAL_PCT
    if notional > max_allowed:
        logger.warning(
            "%s: Notional £%.0f exceeds max single-metal limit £%.0f — capping.",
            ticker, notional, max_allowed,
        )
        notional = max_allowed

    # ── Step 7: MINIMUM SIZE CHECK ───────────────────────────────────────────
    if notional < MIN_POSITION_NOTIONAL_GBP:
        logger.debug(
            "%s: Notional £%.0f below minimum £%.0f — returning 0.",
            ticker, notional, MIN_POSITION_NOTIONAL_GBP,
        )
        return Decimal("0")

    # ── Step 8: APPLY DIRECTION FROM SIGNAL ─────────────────────────────────
    signed_notional = notional * (1 if signal > 0 else -1)

    return Decimal(str(round(signed_notional, 2)))


def compute_lots(
    strategy_id: str,
    ticker: str,
    total_capital_gbp: float,
    realised_vol_20d: float,
    realised_vol_60d: float,
    signal: float,
    current_price_usd: float,
    usd_gbp_rate: float = 1.27,
) -> int:
    """
    Compute integer number of lots (signed: positive=long, negative=short).

    This is the function to call before placing orders.
    Always rounds toward zero (conservative — never over-sizes).

    Args:
        current_price_usd: Current front-month price in USD/unit
        usd_gbp_rate:      To convert USD notional to GBP for NAV comparisons

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
    usd_gbp_rate: float = 1.27,
) -> dict[str, dict[str, int]]:
    """
    Compute full portfolio sizing across all strategies and metals.

    Args:
        strategy_signals: {strategy_id: {ticker: signal}}
        vol_estimates:    {ticker: {"vol_20d": float, "vol_60d": float}}
        prices:           {ticker: current_price_usd}
        total_capital_gbp: Total NAV in GBP

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