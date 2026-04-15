# CLAUDE.md — /strategies
# Strategy Signal Logic | Part of: Metals Systematic Trading System

---

## SCOPE OF THIS DIRECTORY

This directory contains ONLY signal generation logic.
- Input: clean price data, term structure data, inventory data from `clean_data` schema
- Output: normalised signals in range [-1, +1] for each metal
- Position sizing: NOT here — that happens in `/risk/sizing.py`
- Order submission: NOT here — that happens in `/execution/`

**Golden rule:** Strategies are pure functions. Data in → signals out.
No side effects. No database writes (except via the signal store). No broker calls.

---

## BASE STRATEGY INTERFACE

Every strategy MUST inherit from `BaseStrategy` in `base.py`:

```python
class BaseStrategy(ABC):
    strategy_id: str          # e.g., "S4_BASIS_MOMENTUM"
    universe: list[str]       # metals in scope
    rebalance_freq: str       # "weekly", "biweekly", "monthly"
    target_vol: float         # annualised vol target
    max_drawdown_budget: float

    @abstractmethod
    def compute_signals(self, data: pd.DataFrame) -> pd.Series:
        """
        Returns a Series indexed by metal ticker,
        values in range [-1, +1]. Positive = long, negative = short.
        """

    @abstractmethod
    def validate_inputs(self, data: pd.DataFrame) -> bool:
        """Validate data quality before computing signals."""
```

---

## BUILD ORDER & STATUS

| Priority | Strategy | File | Status | Notes |
|----------|----------|------|--------|-------|
| 1 | S4 Basis-Momentum | s4_basis_momentum.py | 🔴 TODO | Start here — highest Sharpe (0.6–1.0) |
| 2 | S1 Carry | s1_carry.py | 🔴 TODO | Reliable carry premium |
| 3 | S2 TSMOM | s2_tsmom.py | 🔴 TODO | Crisis alpha, trend |
| 4 | S3 XS Momentum | s3_xsmom.py | 🔴 TODO | Relative metal rotation |
| 5 | S5 Inventory | s5_inventory.py | 🔴 TODO | Event-driven, infrequent |
| 6 | S6 Pairs | s6_pairs.py | 🔴 TODO | Cointegration, market-neutral |

---

## STRATEGY SPECIFICATIONS

### S4: Basis-Momentum (BUILD FIRST)
**Signal logic:**
```python
# Step 1: Carry sub-signal
carry = (F1 - F2) / F1 * (365 / dte_diff)  # annualised basis

# Step 2: Momentum sub-signal
momentum = front_month_60d_return  # 60-day lookback, skip last 5d

# Step 3: Normalise each cross-sectionally (z-score across universe)
z_carry = zscore_cross_sectional(carry)
z_mom = zscore_cross_sectional(momentum)

# Step 4: Composite with interaction term (KEY INNOVATION)
signal = 0.5 * z_carry + 0.5 * z_mom + 0.25 * (z_carry * z_mom)

# Interaction term: rewards alignment, penalises disagreement
# When carry and momentum agree → larger position
# When they disagree → smaller/zero position
```
**Rebalance:** Bi-weekly
**Reference:** Bakshi, Gao & Rossi (2019) — t-stat 4.14 — highest confidence strategy

---

### S1: Term Structure Carry
**Signal logic:**
```python
# LME: Cash–3M spread, annualised
carry = (F_cash - F_3m) / F_cash * (365 / actual_calendar_days)
# CRITICAL: use ACTUAL calendar days between Cash settle and 3M settle
# Recalculate daily — the 3M prompt rolls forward every business day

# Rank universe by carry signal
# Long top tercile (deepest backwardation)
# Short bottom tercile (deepest contango)
```
**Rebalance:** Weekly
**Key risk:** Nickel-type supply shocks — size via vol targeting protects against this

---

### S2: Time-Series Momentum
**Signal logic:**
```python
# EMA crossover, normalised by vol (EACH METAL INDEPENDENT)
signal_fast_slow = (ema(prices, 8) - ema(prices, 32)) / realised_vol_20d

# Blend multiple timeframes for robustness
signal = (
    signal_8_32 +    # fast
    signal_5_15 +    # medium-fast
    signal_16_64     # medium-slow
) / 3.0

# Each metal gets its own long/short signal independently
# Positive = long that metal; Negative = short that metal
```
**Rebalance:** When signal reverses direction
**Filter:** Only trade when |signal| > 0.5 (reduces whipsaw in choppy markets)

---

### S3: Cross-Sectional Momentum
**Signal logic:**
```python
# Formation period: 60 days, SKIP last 5 days (avoids short-term reversal)
formation_return = price_t_minus_65d_to_t_minus_5d_return

# Rank ALL metals by this return
# Long top 3, Short bottom 3 (out of 10-metal universe)
# Volatility-weight so each leg contributes equally to risk
```
**Rebalance:** Every 10–15 trading days
**Risk:** Momentum crashes — implement stop-loss at 2.5σ move against position

---

### S5: Inventory Surprise
**Signal logic:**
```python
# Z-score of daily LME on-warrant inventory change
z = (inventory_change_today - rolling_mean_60d) / rolling_std_60d

# Entry triggers
if z > +2.0:   signal = +1  # Large unexpected build → expect price recovery
if z < -2.0:   signal = -1  # Large unexpected drawdown → expect price pullback

# FILTER: Only trade when inventory surprise CONTRADICTS term structure
# Best setup: large build (bearish headline) in metal STILL in backwardation
# This improves hit rate from ~52% to ~58-62%

# Exit: 5-day target holding period OR trailing stop at 1.5× 20d ATR
#        OR z-score returns within ±0.5 (whichever first)
```
**Rebalance:** Event-driven (2–4 per metal per quarter)
**Critical:** Never trade mechanically without understanding warehouse context

---

### S6: Inter-Metal Spreads (Pairs)
**Signal logic:**
```python
# Cointegrating pairs:
PAIRS = [
    ("GC", "SI"),   # Gold/Silver — monetary/store-of-value demand
    ("CA", "AH"),   # Copper/Aluminium — electrical conductors, construction
    ("PL", "PA"),   # Platinum/Palladium — autocatalyst substitutes
    ("ZS", "PB"),   # Zinc/Lead — co-mined polymetallic deposits
]

# For each pair: log(Price_A) = α + β × log(Price_B) + ε
# Use 252-day expanding window for β
# Spread residual ε_t is the signal

z = (residual_t - mean_residual) / std_residual

# Entry: |z| > 2.0
# Exit: z crosses zero (mean reversion complete)
# Stop-loss: |z| > 3.5 (structural break)
# Max hold: 20 trading days

# REGIME CHECK: Run ADF test quarterly
# If ADF fails (p > 0.05) for 6-month window → SUSPEND that pair
```
**Structural break warning:** Pd/Pt broke cointegration for 3+ years (2017–2021)
Always check ADF before trading — cointegration is not permanent

---

## SIGNAL NORMALISATION STANDARDS

```python
def normalise_signal(raw_signal: pd.Series) -> pd.Series:
    """
    All signals must be normalised to [-1, +1] range.
    Uses cross-sectional z-score clamped at ±2.
    """
    z = (raw_signal - raw_signal.mean()) / raw_signal.std()
    return z.clip(-2, 2) / 2  # maps ±2σ to ±1
```

---

## CORRELATION MONITORING

Expected pairwise strategy correlations (from Risk Management Framework):
```
S1 (Carry) ↔ S2 (TSMOM):        +0.15 to +0.30
S1 (Carry) ↔ S4 (Basis-Mom):    +0.40 to +0.55  ← expected (S4 incorporates carry)
S2 (TSMOM) ↔ S3 (XS Mom):       +0.20 to +0.35
S5 (Inventory) ↔ all others:    -0.10 to +0.15
S6 (Pairs) ↔ all others:        -0.05 to +0.10
```

**Alert:** If any pair exceeds +0.60 for 10 consecutive days → reduce smaller strategy to 50%
**Alert:** If average pairwise > +0.40 → reduce portfolio leverage by 30%

---

## UNIVERSE & CONTRACT SPECS

| Metal | Exchange | Code | Lot Size | Quote | Tier |
|-------|----------|------|----------|-------|------|
| Copper | LME | CA | 25t | $/t | 1 |
| Aluminium | LME | AH | 25t | $/t | 1 |
| Zinc | LME | ZS | 25t | $/t | 1 |
| Nickel | LME | NI | 6t | $/t | 2 |
| Lead | LME | PB | 25t | $/t | 2 |
| Tin | LME | SN | 5t | $/t | 3 |
| Gold | COMEX | GC | 100 oz | $/oz | 1 |
| Silver | COMEX | SI | 5,000 oz | $/oz | 1 |
| Platinum | COMEX | PL | 50 oz | $/oz | 2 |
| Palladium | COMEX | PA | 100 oz | $/oz | 3 |

**LME critical note:** LME uses a DAILY PROMPT DATE system, not monthly expiry.
- Cash = T+2
- 3-Month = business day closest to 3 calendar months forward (rolls daily)
- Carry signal must use ACTUAL calendar days — recalculate daily

---

## WHAT I SHOULD ALERT YOU TO

When working in this directory, flag immediately if:
- Signal values are outside [-1, +1] range
- A strategy is trying to access position sizing or risk limits directly
- The OOS data is being used for parameter optimisation
- Transaction costs are missing from any backtest
- A strategy is reading from `raw_data` directly (should use `clean_data`)