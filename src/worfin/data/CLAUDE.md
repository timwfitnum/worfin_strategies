# CLAUDE.md — /data
# Data Pipeline | Part of: Metals Systematic Trading System

---

## SCOPE

This directory handles all data ingestion, transformation, validation, and storage.
It is the **foundation of the entire system** — bad data kills strategies silently.

```
/data/
├── ingestion/       ← Pull raw data from external sources (Layer 1)
├── pipeline/        ← Clean, roll-adjust, validate (Layer 2)
└── sources/         ← Per-source API connectors
```

**Principle:** Never transform data in the ingestion layer.
Store exactly what the source provides in `raw_data`.
All transformation happens in `pipeline/` and writes to `clean_data`.

---

## DATA ARCHITECTURE (3-LAYER)

### Layer 1: raw_data (IMMUTABLE — append-only, never modify)
```sql
raw_data.lme_prices         -- Direct from LME XML feed / Nasdaq Data Link
raw_data.comex_prices       -- Direct from yfinance / Nasdaq Data Link
raw_data.lme_inventory      -- Daily on-warrant stocks from LME.com
raw_data.cftc_cot           -- Weekly COT from CFTC.gov
raw_data.macro_indicators   -- PMI, DXY from FRED/Trading Economics
```

### Layer 2: clean_data (Validated, transformed, roll-adjusted)
```sql
clean_data.futures_prices       -- Normalised: date, metal, expiry, price, volume, OI
clean_data.continuous_series    -- Roll-adjusted continuous front-month series
clean_data.term_structure       -- Cash, 3M, 15M, 27M prices with DTE
clean_data.realised_vol         -- 20d and 60d rolling vol (annualised)
clean_data.carry_basis          -- Pre-computed carry signal inputs
```

### Layer 3: signals (Computed, ready for strategy consumption)
```sql
signals.carry             -- S1 inputs
signals.momentum          -- S2, S3 inputs
signals.basis_momentum    -- S4 inputs
signals.inventory         -- S5 inputs
signals.pairs_residuals   -- S6 cointegration residuals
```

---

## DATA SOURCES — IMPLEMENTATION PRIORITY

### Priority 1: Free (use for backtesting now)

**Nasdaq Data Link (Quandl)**
- What: Continuous futures, LME + COMEX historical
- How: `import nasdaqdatalink` or REST API
- Key: CHRIS/CME database for continuous contracts
- History: 20+ years — sufficient for IS (2005–2017) + OOS (2018–2022)
- Free tier: 50k API calls/day — more than enough

**yfinance**
- What: COMEX precious metals (GC, SI, PL, PA)
- How: `import yfinance as yf; yf.download("GC=F")`
- Caution: Quality inconsistent — always cross-validate
- Use for COMEX supplement only

**LME.com free feed**
- What: Official settlement prices (next-day delayed), daily inventory data
- How: Scrape or download CSV from website
- CRITICAL: These are the OFFICIAL Ring settlement prices — use for signal computation
- Inventory: Published ~09:00 London daily

**CFTC.gov + cot_reports library**
- What: Weekly Disaggregated COT (Managed Money positioning)
- How: `pip install cot_reports`
- Note: COMEX metals only (LME is UK-regulated, use LME COTR separately)
- Lag: 3 days (Tuesday data released Friday) — use as filter/confirmation only

**FRED API**
- What: ISM Manufacturing PMI, DXY, Industrial Production
- How: `pip install fredapi`
- Free with API key from fred.stlouisfed.org

---

### Priority 2: Live Trading (~£2–4k/yr)

**IBKR TWS API**
- What: Live prices + executed data (byproduct of brokerage)
- How: `ib_insync` (see `/execution/broker/ibkr.py`)
- Use: Real-time signal computation and live position valuation

**LME Off-Warrant API ($1,200/yr)**
- What: Programmatic access to full LME data including off-warrant stocks
- When: Worth it once deploying live capital

---

### Priority 3: Professional (~£15–25k/yr — Tier 2+)
- Refinitiv Workspace (~£8–15k/yr) — best balance of LME depth and API quality
- Bloomberg Terminal (~£20–25k/yr) — institutional gold standard

---

## ROLL METHODOLOGY

**LME (Base Metals):**
- 3M contract rolls daily — no active rolling needed if always trading 3M
- For S1 Carry (Cash-3M spread): recalculate DTE daily using actual calendar days
- Tom/Next rolls for overnight Cash positions: ~10–30 cents/tonne cost
- S1 positions: roll 2–3 business days before Cash prompt date
- ALWAYS execute as a single spread order (not two legs) — eliminates leg risk

**COMEX (Precious Metals):**
- Fixed monthly expiry — must actively roll before First Notice Day (FND)
- Roll windows:
  - Gold (GC): 5–10 business days before FND
  - Silver (SI): 5–10 business days before FND
  - Platinum (PL): 7–15 business days before FND (thin — roll early)
  - Palladium (PA): 10–15 business days before FND (least liquid — roll earliest)
- Always execute as calendar spread order on CME Globex

**Continuous series construction:**
- Back-adjusted (preferred for return calculations) — no level distortion
- Ratio-adjusted for S6 Pairs (preserves price ratios for cointegration)
- Log all roll dates in `audit.roll_log` with roll cost vs theoretical fair spread

---

## DATA QUALITY RULES (automated daily validation)

### Staleness Check
```python
# Flag if price has not updated for > 1 business day
# Exception: exchange holidays (maintain holiday calendar)
MAX_STALENESS_DAYS = 1

HOLIDAY_CALENDAR = {
    "LME": [...],    # LME holidays
    "COMEX": [...],  # CME Group holidays
    "UK": [...],     # UK bank holidays (affects LME)
    "US": [...],     # US holidays (affects COMEX)
}
```

### Outlier Detection
```python
# Flag if daily return > 4σ from 60-day rolling mean
# INVESTIGATE before signal layer processes — do not auto-discard
# Note: March 2022 Nickel moved 150% intraday — extreme moves do occur
OUTLIER_THRESHOLD_SIGMA = 4.0
```

### Cross-Source Validation
```python
# If using multiple sources, compare daily prices
# Discrepancy > 0.5% between LME direct and third-party → alert
PRICE_DISCREPANCY_THRESHOLD = 0.005  # 0.5%
```

### Continuity Check
```python
# Continuous futures series: no unexplained jumps exceeding known roll adjustment
# All roll dates logged and auditable
```

---

## REALISED VOLATILITY COMPUTATION

```python
def compute_realised_vol(prices: pd.Series, window: int) -> pd.Series:
    """
    Standard annualised realised volatility.
    Uses log returns for symmetry.

    Args:
        prices: Daily settlement prices
        window: Lookback period in trading days (20 or 60)

    Returns:
        Annualised volatility series
    """
    log_returns = np.log(prices / prices.shift(1))
    return log_returns.rolling(window).std() * np.sqrt(252)

# Always compute BOTH:
vol_20d = compute_realised_vol(prices, 20)  # Primary sizing input
vol_60d = compute_realised_vol(prices, 60)  # Robustness cap

# FLOOR enforcement happens in risk/sizing.py — not here
```

---

## CARRY SIGNAL INPUTS

```python
# LME Carry Signal Inputs (for S1 and S4)
def compute_lme_carry(
    cash_price: float,
    front_3m_price: float,
    cash_settle_date: date,
    three_month_settle_date: date,
) -> float:
    """
    Annualised carry (basis) for LME metals.

    CRITICAL: Use ACTUAL calendar days between settle dates.
    The 3M prompt rolls forward every business day, so DTE changes daily
    even if prices are unchanged.
    """
    dte_diff = (three_month_settle_date - cash_settle_date).days
    carry = (cash_price - front_3m_price) / cash_price * (365 / dte_diff)
    return carry
    # Positive = backwardation (earn roll yield long)
    # Negative = contango (pay roll yield long)
```

---

## DAILY OPERATIONAL TIMELINE

| Time (London) | Event | System |
|---------------|-------|--------|
| 09:00 | LME inventory report published | data/ingestion/lme_inventory.py |
| 09:05 | Inventory z-score computed | data/pipeline/inventory.py |
| 13:30–14:00 | LME official Ring prices published | data/ingestion/lme_prices.py |
| 14:00 | All clean data validated | data/pipeline/validate.py |
| 14:15 | Signals computed (all strategies) | /strategies/ |
| 18:00 | COMEX settlement prices pulled | data/ingestion/comex_prices.py |
| 18:30 | Full database backup | infrastructure/backup.py |

---

## WHAT I SHOULD FLAG

When working in this directory:
- Any transformation happening in `ingestion/` (should be in `pipeline/`)
- Any write to `raw_data` that isn't a pure insert (append-only)
- Missing holiday calendar check in staleness validation
- Carry signal using fixed 91-day DTE assumption instead of actual calendar days
- Continuous series construction without logging roll dates
- Any price data being used without passing through the quality validation layer
- LME inventory data being used without the warrant cancellation ratio
