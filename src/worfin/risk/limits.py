"""
risk/limits.py
All hard risk limits as constants.
NEVER modify these at runtime. NEVER use variables in their place.
These constants are safety-critical — a bug here risks real capital.

Any change to these values requires:
  1. Written justification
  2. Re-run of all stress tests
  3. Git commit with explicit message referencing the change
"""

from __future__ import annotations

# ─────────────────────────────────────────────────────────────────────────────
# VOLATILITY LIMITS
# ─────────────────────────────────────────────────────────────────────────────

# Minimum volatility estimate used in position sizing.
# Prevents absurdly large positions during artificially compressed vol regimes.
# Nickel March 2022: 20d vol was ~12% → moved 250% in 2 days.
VOL_FLOOR: float = 0.10  # 10% annualised — NEVER go below this

# Alert thresholds: actual vol vs strategy target vol
VOL_ALERT_MULTIPLIER: float = 1.5   # Alert if 20d vol > 1.5× target (Day 1/5)
VOL_REDUCE_MULTIPLIER: float = 2.0  # Halve allocation if > 2× target

# ─────────────────────────────────────────────────────────────────────────────
# POSITION LIMITS
# ─────────────────────────────────────────────────────────────────────────────

# Maximum single-metal notional as % of total NAV
MAX_SINGLE_METAL_PCT: float = 0.20  # 20%

# Maximum single-strategy gross exposure as % of NAV
MAX_SINGLE_STRATEGY_GROSS_PCT: float = 0.40  # 40%

# Maximum total portfolio gross exposure (sum of |notional| / NAV)
MAX_PORTFOLIO_GROSS: float = 2.50  # 250% of NAV

# Maximum total portfolio net exposure (net_long - net_short) / NAV
MAX_PORTFOLIO_NET: float = 0.80  # 80% of NAV (either direction)

# Minimum position size — positions smaller than this are not worth the friction
MIN_POSITION_NOTIONAL_GBP: float = 5_000.0  # £5,000

# Maximum single-metal gross % of total gross exposure
MAX_SINGLE_METAL_GROSS_PCT: float = 0.30  # 30% of gross

# ─────────────────────────────────────────────────────────────────────────────
# CIRCUIT BREAKERS (PORTFOLIO-LEVEL)
# ─────────────────────────────────────────────────────────────────────────────

# Daily loss limit — flatten ALL positions immediately if breached
DAILY_LOSS_LIMIT: float = 0.020  # 2% of NAV

# Weekly loss limit — reduce to 50% target, no new entries
WEEKLY_LOSS_LIMIT: float = 0.035  # 3.5% of NAV

# Monthly drawdown limit — reduce to 25% target, hold until next month
MONTHLY_DRAWDOWN_LIMIT: float = 0.050  # 5% from month-start NAV

# Peak drawdown — full suspension, formal review required
PEAK_DRAWDOWN_SUSPEND: float = 0.100  # 10% from all-time HWM

# Hard stop — full liquidation, 3-month paper trading before re-deployment
HARD_STOP_DRAWDOWN: float = 0.150  # 15% from HWM

# Reinstatement schedule after daily flatten
DAILY_FLATTEN_REINSTATEMENT_PCT: float = 0.75  # Restart at 75% next day

# Reinstatement schedule after weekly reduction
WEEKLY_SCALE_UP_PER_DAY: float = 0.10  # +10% per day back to 100%

# Reinstatement schedule after peak drawdown suspension
PEAK_DRAWDOWN_RESTART_PCT: float = 0.25  # Restart at 25%
PEAK_DRAWDOWN_SCALE_UP_DAYS: int = 40   # Scale over 40 trading days
PEAK_DRAWDOWN_COOL_OFF_DAYS: int = 10   # Min 10 days before restart

# ─────────────────────────────────────────────────────────────────────────────
# STRATEGY-LEVEL DRAWDOWN BUDGETS
# ─────────────────────────────────────────────────────────────────────────────
# If a strategy's drawdown exceeds this, it is suspended immediately.
# Close positions over max 3 trading days (no fire-sale).
# Restart at 50% after root cause analysis.

STRATEGY_DRAWDOWN_BUDGET: dict[str, float] = {
    "S1": 0.12,  # 12% — Carry
    "S2": 0.15,  # 15% — TSMOM
    "S3": 0.12,  # 12% — XS Momentum
    "S4": 0.15,  # 15% — Basis-Momentum
    "S5": 0.10,  # 10% — Inventory Surprise
    "S6": 0.10,  # 10% — Pairs
}

STRATEGY_RESTART_PCT: float = 0.50  # Restart at 50% of normal risk budget
STRATEGY_SCALE_UP_DAYS: int = 20    # Scale back to 100% over 20 trading days

# ─────────────────────────────────────────────────────────────────────────────
# LIQUIDITY LIMITS
# ─────────────────────────────────────────────────────────────────────────────
# Maximum percentage of average daily volume (ADV) per metal.
# Violating these risks moving the market against yourself.

MAX_ADV_PCT: dict[int, float] = {
    1: 0.05,  # Tier 1 (Cu, Al, Zn, Au, Ag): max 5% of ADV
    2: 0.03,  # Tier 2 (Ni, Pb, Pt): max 3% of ADV
    3: 0.02,  # Tier 3 (Sn, Pd): max 2% of ADV
}

# Maximum % of ADV in a single day (controls intraday impact)
MAX_SINGLE_DAY_ADV_PCT: float = 0.10

# Must be able to exit all positions within this many trading days
LIQUIDATION_HORIZON_DAYS: int = 3

# ─────────────────────────────────────────────────────────────────────────────
# CORRELATION LIMITS
# ─────────────────────────────────────────────────────────────────────────────

# Pairwise strategy correlation — if exceeded for 10+ days, reduce smaller strategy
PAIRWISE_CORRELATION_ALERT: float = 0.60
PAIRWISE_CORRELATION_REDUCTION: float = 0.50  # Reduce smaller strategy to 50%

# Average pairwise correlation — portfolio-level diversification alert
AVERAGE_CORRELATION_ALERT: float = 0.40
AVERAGE_CORRELATION_LEVERAGE_REDUCTION: float = 0.30  # Reduce overall leverage by 30%

# Rolling window for correlation calculation
CORRELATION_LOOKBACK_DAYS: int = 60
CORRELATION_ALERT_CONSECUTIVE_DAYS: int = 10  # Alert if exceeded for this many days

# ─────────────────────────────────────────────────────────────────────────────
# EXECUTION LIMITS
# ─────────────────────────────────────────────────────────────────────────────

# Fat-finger protection — reject orders >2% away from current mid
FAT_FINGER_PRICE_DEVIATION_PCT: float = 0.02  # 2%

# Maximum orders per day (across all strategies)
MAX_DAILY_ORDERS: int = 50

# Maximum time a signal can be old before execution is blocked
MAX_SIGNAL_AGE_HOURS: int = 24

# Market order rate alert — flag if >10% of executions use market orders
MARKET_ORDER_RATE_ALERT: float = 0.10  # 10%

# Slippage alert — flag if actual slippage > 2× model slippage
SLIPPAGE_ALERT_MULTIPLIER: float = 2.0

# ─────────────────────────────────────────────────────────────────────────────
# FX RATE STALENESS
# ─────────────────────────────────────────────────────────────────────────────

# Maximum calendar days the USD/GBP rate may be stale before raising
# FxRateUnavailable. FRED DEXUSUK has gaps on US bank holidays — 5 days
# covers a long weekend plus one contingency day without over-relaxing
# for genuine outages.
FX_RATE_MAX_STALENESS_DAYS: int = 5

# ─────────────────────────────────────────────────────────────────────────────
# DATA QUALITY THRESHOLDS
# ─────────────────────────────────────────────────────────────────────────────

# Price staleness — alert if price not updated for this many trading days
MAX_STALENESS_TRADING_DAYS: int = 1

# Outlier detection — flag if daily return exceeds this z-score
# NOTE: 4σ is a flag, not auto-discard — human investigation required
OUTLIER_THRESHOLD_SIGMA: float = 4.0

# Cross-source price discrepancy alert
PRICE_DISCREPANCY_THRESHOLD: float = 0.005  # 0.5%

# ─────────────────────────────────────────────────────────────────────────────
# RECONCILIATION THRESHOLDS
# ─────────────────────────────────────────────────────────────────────────────

# Block new orders if position discrepancy exceeds either threshold
RECONCILIATION_VALUE_THRESHOLD_GBP: float = 100.0  # £100
RECONCILIATION_PCT_THRESHOLD: float = 0.001         # 0.1% of position notional

# ─────────────────────────────────────────────────────────────────────────────
# STRATEGY TARGET VOLATILITIES
# ─────────────────────────────────────────────────────────────────────────────
# These drive: Position_Notional = (target_vol × allocation) / realised_vol

STRATEGY_TARGET_VOL: dict[str, float] = {
    "S1": 0.09,  # 9%  — Carry (lower — more stable)
    "S2": 0.11,  # 11% — TSMOM
    "S3": 0.09,  # 9%  — XS Momentum
    "S4": 0.11,  # 11% — Basis-Momentum (core strategy)
    "S5": 0.07,  # 7%  — Inventory (event-driven, shorter hold)
    "S6": 0.07,  # 7%  — Pairs (market-neutral, lower vol)
}

# ─────────────────────────────────────────────────────────────────────────────
# CAPITAL ALLOCATIONS
# ─────────────────────────────────────────────────────────────────────────────
# Percentage of total capital allocated to each strategy.
# Must sum to exactly 1.0.

STRATEGY_ALLOCATION: dict[str, float] = {
    "S1": 0.20,  # 20% — Carry
    "S2": 0.20,  # 20% — TSMOM
    "S3": 0.15,  # 15% — XS Momentum
    "S4": 0.25,  # 25% — Basis-Momentum (largest — highest confidence)
    "S5": 0.10,  # 10% — Inventory
    "S6": 0.10,  # 10% — Pairs
}

assert (
    abs(sum(STRATEGY_ALLOCATION.values()) - 1.0) < 1e-10
), "Strategy allocations must sum to 1.0"

# ─────────────────────────────────────────────────────────────────────────────
# KILL SWITCH
# ─────────────────────────────────────────────────────────────────────────────

# Maximum time to flatten all positions and cancel all orders
KILL_SWITCH_MAX_SECONDS: int = 60