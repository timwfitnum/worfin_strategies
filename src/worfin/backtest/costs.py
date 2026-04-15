"""
backtest/costs.py
Transaction cost model — ALWAYS applied in backtests. No exceptions.

"A backtest without transaction costs is fiction." — Strategy document

This module implements the full cost model:
  1. Bid-ask spread cost (paid on every entry and exit)
  2. Commission (per contract, round-trip)
  3. Slippage (market impact + timing cost)
  4. Roll cost (for continuous position maintenance)

Cost budget rule: Always budget 30–50% HIGHER than baseline estimates.
Use COST_STRESS_MULTIPLIER for stress-test scenarios.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass

import pandas as pd

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# TRANSACTION COST PARAMETERS (round-trip, per trade)
# Source: Execution Playbook document + direct broker quotes
# ─────────────────────────────────────────────────────────────────────────────

TRANSACTION_COSTS: dict[str, dict] = {
    # Tier 1 — High Liquidity
    "CA": {"spread_bps": 3.0,  "commission_usd": 3.00, "slippage_bps": 0.75},  # LME Copper
    "AH": {"spread_bps": 3.0,  "commission_usd": 3.00, "slippage_bps": 0.75},  # LME Aluminium
    "GC": {"spread_bps": 2.0,  "commission_usd": 3.00, "slippage_bps": 0.75},  # COMEX Gold
    "SI": {"spread_bps": 3.0,  "commission_usd": 3.00, "slippage_bps": 1.00},  # COMEX Silver
    # Tier 2 — Medium Liquidity
    "ZS": {"spread_bps": 4.0,  "commission_usd": 3.00, "slippage_bps": 1.00},  # LME Zinc
    "NI": {"spread_bps": 8.0,  "commission_usd": 3.00, "slippage_bps": 2.00},  # LME Nickel
    "PB": {"spread_bps": 5.0,  "commission_usd": 3.00, "slippage_bps": 1.50},  # LME Lead
    "PL": {"spread_bps": 6.0,  "commission_usd": 3.00, "slippage_bps": 2.00},  # COMEX Platinum
    # Tier 3 — Low Liquidity
    "SN": {"spread_bps": 20.0, "commission_usd": 3.00, "slippage_bps": 5.00},  # LME Tin
    "PA": {"spread_bps": 10.0, "commission_usd": 3.00, "slippage_bps": 3.00},  # COMEX Palladium
}

# Roll costs — incurred when rolling from front to next contract
ROLL_COSTS: dict[str, dict] = {
    "LME":   {"typical_bps": 2.0,   "stressed_bps": 8.0},   # LME 3M roll
    "COMEX": {"typical_bps": 1.5,   "stressed_bps": 5.0},   # COMEX calendar spread
}

# Stress multiplier — budget 50% higher than baseline for safety
COST_STRESS_MULTIPLIER: float = 1.50

# Baseline multiplier for standard backtesting (use 1.2 = 20% buffer)
COST_BASELINE_MULTIPLIER: float = 1.20


@dataclass
class TradeCost:
    """Breakdown of all costs for a single round-trip trade."""
    ticker: str
    notional_usd: float
    lots: int

    spread_cost_usd: float    # Half-spread paid on entry + exit
    commission_usd: float     # Broker commission
    slippage_usd: float       # Market impact / timing cost
    roll_cost_usd: float      # Roll cost (if holding across roll date)

    @property
    def total_cost_usd(self) -> float:
        return self.spread_cost_usd + self.commission_usd + self.slippage_usd + self.roll_cost_usd

    @property
    def total_cost_bps(self) -> float:
        if self.notional_usd <= 0:
            return 0.0
        return (self.total_cost_usd / self.notional_usd) * 10_000

    @property
    def total_cost_pct(self) -> float:
        return self.total_cost_bps / 10_000


def compute_trade_cost(
    ticker: str,
    notional_usd: float,
    lots: int,
    include_roll: bool = False,
    multiplier: float = COST_BASELINE_MULTIPLIER,
) -> TradeCost:
    """
    Compute round-trip transaction costs for a single trade.

    Args:
        ticker:      Metal ticker (e.g., "GC")
        notional_usd: Trade notional in USD (unsigned)
        lots:        Number of contracts
        include_roll: If True, add one roll cost (for strategies holding across expiry)
        multiplier:  Cost multiplier (1.20 baseline, 1.50 for stress)

    Returns:
        TradeCost breakdown.
    """
    if ticker not in TRANSACTION_COSTS:
        logger.error("No cost data for ticker %s — using maximum cost (Tier 3).", ticker)
        costs = TRANSACTION_COSTS["SN"]   # Use worst case
    else:
        costs = TRANSACTION_COSTS[ticker]

    notional = abs(notional_usd)

    # Spread cost: half-spread paid at entry AND half at exit = full spread
    spread_cost = notional * (costs["spread_bps"] / 10_000) * multiplier

    # Commission: fixed per contract, round-trip
    commission = lots * costs["commission_usd"] * multiplier

    # Slippage: market impact, proportional to notional
    slippage = notional * (costs["slippage_bps"] / 10_000) * multiplier

    # Roll cost
    roll_cost = 0.0
    if include_roll:
        # Use average of LME and COMEX typical roll costs
        roll_bps = ROLL_COSTS["LME"]["typical_bps"]
        roll_cost = notional * (roll_bps / 10_000) * multiplier

    return TradeCost(
        ticker=ticker,
        notional_usd=notional,
        lots=lots,
        spread_cost_usd=spread_cost,
        commission_usd=commission,
        slippage_usd=slippage,
        roll_cost_usd=roll_cost,
    )


def apply_costs_to_returns(
    gross_returns: pd.Series,
    turnover: pd.Series,
    universe_avg_cost_bps: float = 6.0,
    multiplier: float = COST_BASELINE_MULTIPLIER,
) -> pd.Series:
    """
    Apply a simplified cost model to a gross return series.
    Used for quick backtesting before detailed position-level costing.

    Args:
        gross_returns:         Daily gross return series
        turnover:              Daily portfolio turnover (as fraction of NAV)
        universe_avg_cost_bps: Average round-trip cost across universe (default 6bps)
        multiplier:            Cost scaling multiplier

    Returns:
        Net return series after costs.
    """
    daily_cost = turnover * (universe_avg_cost_bps / 10_000) * multiplier
    net_returns = gross_returns - daily_cost

    total_cost_drag = daily_cost.sum()
    total_gross = gross_returns.sum()
    logger.info(
        "Cost drag: %.2f%% of gross returns. Gross: %.2f%%, Net: %.2f%%. "
        "Avg daily cost: %.1f bps.",
        (total_cost_drag / total_gross * 100) if total_gross != 0 else 0,
        total_gross * 100,
        net_returns.sum() * 100,
        daily_cost.mean() * 10_000,
    )

    return net_returns


def estimate_annual_cost_drag(
    strategy_id: str,
    avg_positions: int,
    avg_notional_per_position_usd: float,
    avg_holding_days: float,
    universe_mix: dict[str, float],  # {ticker: weight in portfolio}
) -> dict[str, float]:
    """
    Estimate the annual cost drag for a strategy.
    Use this to sanity-check whether a strategy can survive its own costs.

    Returns:
        {
          "annual_trades": int,
          "annual_cost_usd": float,
          "annual_cost_bps": float,
          "cost_hurdle_sharpe": float,  # Sharpe needed just to break even on costs
        }
    """
    annual_trades = (TRADING_DAYS_PER_YEAR / avg_holding_days) * avg_positions
    TRADING_DAYS_PER_YEAR = 252

    total_annual_cost = 0.0
    for ticker, weight in universe_mix.items():
        notional = avg_notional_per_position_usd * weight
        cost = compute_trade_cost(ticker, notional, lots=1)
        total_annual_cost += cost.total_cost_usd * (TRADING_DAYS_PER_YEAR / avg_holding_days)

    total_notional = avg_positions * avg_notional_per_position_usd
    cost_bps = (total_annual_cost / total_notional) * 10_000 if total_notional > 0 else 0.0

    # Cost hurdle: Sharpe needed just to cover transaction costs
    # Assumes cost vol ≈ strategy vol × 0.1 (rough approximation)
    cost_hurdle = cost_bps / 100  # Approximate annualised return needed

    return {
        "annual_trades": int(annual_trades),
        "annual_cost_usd": total_annual_cost,
        "annual_cost_bps": cost_bps,
        "cost_hurdle_return_pct": cost_hurdle,
    }