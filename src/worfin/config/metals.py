"""
config/metals.py
Metal universe definitions and contract specifications.
All values are constants — never modify at runtime.

Sources:
  LME contract specs: https://www.lme.com/metals
  COMEX specs: https://www.cmegroup.com/markets/metals
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class Exchange(str, Enum):
    LME = "LME"
    COMEX = "COMEX"


class LiquidityTier(int, Enum):
    HIGH = 1    # Tightest spreads, deepest book, max 5% ADV
    MEDIUM = 2  # Moderate spreads, max 3% ADV
    LOW = 3     # Widest spreads, max 2% ADV — extra caution


@dataclass(frozen=True)
class MetalSpec:
    """Immutable specification for a single metal futures contract."""

    ticker: str          # Internal ticker (e.g., "CA" for LME Copper)
    name: str            # Full name
    exchange: Exchange
    lot_size: float      # Contract lot size in base unit
    unit: str            # "tonnes", "oz", etc.
    quote_currency: str  # Always USD
    tick_size: float     # Minimum price move
    tick_value: float    # USD value of one tick per contract
    liquidity_tier: LiquidityTier
    ibkr_symbol: str     # Symbol as used in IBKR
    ibkr_exchange: str   # Exchange as IBKR expects it
    ibkr_currency: str   # Contract currency
    # Roll mechanics
    roll_days_before_fnc: int  # Days before First Notice/Cash prompt to roll
    # Execution costs (round-trip basis points)
    spread_bps: float
    commission_usd: float      # Per contract, round-trip
    slippage_bps: float


# ─────────────────────────────────────────────────────────────────────────────
# LME BASE METALS
# ─────────────────────────────────────────────────────────────────────────────
# LME uses a DAILY PROMPT DATE system — contracts mature on any business day.
# Cash = T+2; 3-Month = business day closest to 3 calendar months forward.
# There is NO single monthly expiry — the 3M prompt rolls forward every day.

COPPER = MetalSpec(
    ticker="CA",
    name="LME Copper",
    exchange=Exchange.LME,
    lot_size=25.0,
    unit="tonnes",
    quote_currency="USD",
    tick_size=0.50,
    tick_value=12.50,   # $0.50/tonne × 25 tonnes
    liquidity_tier=LiquidityTier.HIGH,
    ibkr_symbol="HG",
    ibkr_exchange="NYMEX",   # IBKR routes LME metals via CME equivalent
    ibkr_currency="USD",
    roll_days_before_fnc=3,
    spread_bps=3.0,
    commission_usd=3.00,
    slippage_bps=0.75,
)

ALUMINIUM = MetalSpec(
    ticker="AH",
    name="LME Aluminium",
    exchange=Exchange.LME,
    lot_size=25.0,
    unit="tonnes",
    quote_currency="USD",
    tick_size=0.50,
    tick_value=12.50,
    liquidity_tier=LiquidityTier.HIGH,
    ibkr_symbol="ALI",
    ibkr_exchange="NYMEX",
    ibkr_currency="USD",
    roll_days_before_fnc=3,
    spread_bps=3.0,
    commission_usd=3.00,
    slippage_bps=0.75,
)

ZINC = MetalSpec(
    ticker="ZS",
    name="LME Zinc",
    exchange=Exchange.LME,
    lot_size=25.0,
    unit="tonnes",
    quote_currency="USD",
    tick_size=0.50,
    tick_value=12.50,
    liquidity_tier=LiquidityTier.HIGH,
    ibkr_symbol="ZNC",
    ibkr_exchange="ICEEU",
    ibkr_currency="USD",
    roll_days_before_fnc=3,
    spread_bps=4.0,
    commission_usd=3.00,
    slippage_bps=1.00,
)

NICKEL = MetalSpec(
    ticker="NI",
    name="LME Nickel",
    exchange=Exchange.LME,
    lot_size=6.0,
    unit="tonnes",
    quote_currency="USD",
    tick_size=1.00,
    tick_value=6.00,   # $1/tonne × 6 tonnes
    liquidity_tier=LiquidityTier.MEDIUM,
    ibkr_symbol="NI",
    ibkr_exchange="ICEEU",
    ibkr_currency="USD",
    roll_days_before_fnc=3,
    spread_bps=8.0,
    commission_usd=3.00,
    slippage_bps=2.00,
    # ⚠️ NICKEL WARNING: March 2022 — LME suspended trading and cancelled
    # trades after a 250% intraday spike. Size conservatively.
    # The 10% vol FLOOR in risk/limits.py is critical for Nickel.
)

LEAD = MetalSpec(
    ticker="PB",
    name="LME Lead",
    exchange=Exchange.LME,
    lot_size=25.0,
    unit="tonnes",
    quote_currency="USD",
    tick_size=0.25,
    tick_value=6.25,
    liquidity_tier=LiquidityTier.MEDIUM,
    ibkr_symbol="LL",
    ibkr_exchange="ICEEU",
    ibkr_currency="USD",
    roll_days_before_fnc=3,
    spread_bps=5.0,
    commission_usd=3.00,
    slippage_bps=1.50,
)

TIN = MetalSpec(
    ticker="SN",
    name="LME Tin",
    exchange=Exchange.LME,
    lot_size=5.0,
    unit="tonnes",
    quote_currency="USD",
    tick_size=1.00,
    tick_value=5.00,
    liquidity_tier=LiquidityTier.LOW,
    ibkr_symbol="TIN",
    ibkr_exchange="ICEEU",
    ibkr_currency="USD",
    roll_days_before_fnc=5,   # Roll earlier — thin book
    spread_bps=20.0,
    commission_usd=3.00,
    slippage_bps=5.00,
    # ⚠️ TIN WARNING: Extremely thin order book (3–5 lots per side typical).
    # Always check book depth before executing. Use smallest lot clips.
)

# ─────────────────────────────────────────────────────────────────────────────
# COMEX PRECIOUS METALS
# ─────────────────────────────────────────────────────────────────────────────
# COMEX uses standard monthly expiry cycles.
# Roll before First Notice Day (FND) — typically last business day of month
# before contract month. Use IBKR calendar spread orders — never leg separately.

GOLD = MetalSpec(
    ticker="GC",
    name="COMEX Gold",
    exchange=Exchange.COMEX,
    lot_size=100.0,
    unit="oz",
    quote_currency="USD",
    tick_size=0.10,
    tick_value=10.00,   # $0.10/oz × 100 oz
    liquidity_tier=LiquidityTier.HIGH,
    ibkr_symbol="GC",
    ibkr_exchange="NYMEX",
    ibkr_currency="USD",
    roll_days_before_fnc=7,
    spread_bps=2.0,
    commission_usd=3.00,
    slippage_bps=0.75,
)

SILVER = MetalSpec(
    ticker="SI",
    name="COMEX Silver",
    exchange=Exchange.COMEX,
    lot_size=5000.0,
    unit="oz",
    quote_currency="USD",
    tick_size=0.005,
    tick_value=25.00,   # $0.005/oz × 5000 oz
    liquidity_tier=LiquidityTier.HIGH,
    ibkr_symbol="SI",
    ibkr_exchange="NYMEX",
    ibkr_currency="USD",
    roll_days_before_fnc=7,
    spread_bps=3.0,
    commission_usd=3.00,
    slippage_bps=1.00,
)

PLATINUM = MetalSpec(
    ticker="PL",
    name="COMEX Platinum",
    exchange=Exchange.COMEX,
    lot_size=50.0,
    unit="oz",
    quote_currency="USD",
    tick_size=0.10,
    tick_value=5.00,
    liquidity_tier=LiquidityTier.MEDIUM,
    ibkr_symbol="PL",
    ibkr_exchange="NYMEX",
    ibkr_currency="USD",
    roll_days_before_fnc=12,   # Roll earlier — thinner than Pd
    spread_bps=6.0,
    commission_usd=3.00,
    slippage_bps=2.00,
)

PALLADIUM = MetalSpec(
    ticker="PA",
    name="COMEX Palladium",
    exchange=Exchange.COMEX,
    lot_size=100.0,
    unit="oz",
    quote_currency="USD",
    tick_size=0.05,
    tick_value=5.00,
    liquidity_tier=LiquidityTier.LOW,
    ibkr_symbol="PA",
    ibkr_exchange="NYMEX",
    ibkr_currency="USD",
    roll_days_before_fnc=15,   # Roll earliest — least liquid precious metal
    spread_bps=10.0,
    commission_usd=3.00,
    slippage_bps=3.00,
    # ⚠️ PALLADIUM WARNING: Pt/Pd cointegration broke down 2017–2021.
    # Always run ADF test before trading S6 Pairs for this pair.
)

# ─────────────────────────────────────────────────────────────────────────────
# UNIVERSE COLLECTIONS
# ─────────────────────────────────────────────────────────────────────────────

ALL_METALS: dict[str, MetalSpec] = {
    "CA": COPPER,
    "AH": ALUMINIUM,
    "ZS": ZINC,
    "NI": NICKEL,
    "PB": LEAD,
    "SN": TIN,
    "GC": GOLD,
    "SI": SILVER,
    "PL": PLATINUM,
    "PA": PALLADIUM,
}

LME_METALS: dict[str, MetalSpec] = {
    k: v for k, v in ALL_METALS.items() if v.exchange == Exchange.LME
}

COMEX_METALS: dict[str, MetalSpec] = {
    k: v for k, v in ALL_METALS.items() if v.exchange == Exchange.COMEX
}

TIER_1_METALS: dict[str, MetalSpec] = {
    k: v for k, v in ALL_METALS.items() if v.liquidity_tier == LiquidityTier.HIGH
}

# Strategy-specific universes
S1_UNIVERSE = ["CA", "AH", "ZS", "NI", "PB", "SN", "GC", "SI", "PL", "PA"]
S2_UNIVERSE = ["CA", "AH", "ZS", "NI", "PB", "SN", "GC", "SI", "PL", "PA"]
S3_UNIVERSE = ["CA", "AH", "ZS", "NI", "PB", "SN", "GC", "SI", "PL", "PA"]
S4_UNIVERSE = ["CA", "AH", "ZS", "NI", "PB", "SN", "GC", "SI", "PL", "PA"]
S5_UNIVERSE = ["CA", "AH", "ZS", "NI", "PB", "SN"]  # LME only (inventory data)
S6_PAIRS = [
    ("GC", "SI"),   # Gold / Silver — monetary demand
    ("CA", "AH"),   # Copper / Aluminium — electrical conductors
    ("PL", "PA"),   # Platinum / Palladium — autocatalysts
    ("ZS", "PB"),   # Zinc / Lead — co-mined polymetallic
]

# ─────────────────────────────────────────────────────────────────────────────
# EXECUTION WINDOWS (London time)
# ─────────────────────────────────────────────────────────────────────────────

EXECUTION_WINDOWS = {
    "LME_BASE": {"start_hour": 14, "start_min": 0, "end_hour": 16, "end_min": 0},
    "COMEX_PRECIOUS": {"start_hour": 14, "start_min": 0, "end_hour": 16, "end_min": 0},
    "COMEX_PT_PD": {"start_hour": 14, "start_min": 30, "end_hour": 15, "end_min": 30},
}

# LME Ring closes (official settlement prices set)
LME_RING_CLOSE_HOUR = 13
LME_RING_CLOSE_MIN = 30

# Inventory report
LME_INVENTORY_REPORT_HOUR = 9
LME_INVENTORY_REPORT_MIN = 0

# S5 minimum delay after inventory release
S5_EXECUTION_DELAY_MINUTES = 30


def get_metal(ticker: str) -> MetalSpec:
    """Retrieve metal spec by ticker. Raises KeyError for unknown tickers."""
    if ticker not in ALL_METALS:
        raise KeyError(f"Unknown metal ticker: {ticker!r}. Valid tickers: {list(ALL_METALS)}")
    return ALL_METALS[ticker]


def get_lot_notional(ticker: str, price: float) -> float:
    """Calculate notional value of one contract in USD."""
    spec = get_metal(ticker)
    return spec.lot_size * price


def get_lots_for_notional(ticker: str, price: float, target_notional: float) -> int:
    """
    Calculate number of lots for a target notional.
    Always rounds DOWN to stay within risk limits.
    Returns 0 if notional is too small for even one lot.
    """
    if price <= 0 or target_notional <= 0:
        return 0
    lot_notional = get_lot_notional(ticker, price)
    return int(target_notional / lot_notional)   # floor division