# CLAUDE.md — /risk
# Risk Management Engine | Part of: Metals Systematic Trading System

---

## SCOPE & CRITICAL WARNING

This directory contains the **most important code in the entire system.**
A bug here can cause catastrophic financial loss. Treat every function as safety-critical.

**What lives here:**
- `limits.py` — All hard limits as constants (never modify at runtime)
- `sizing.py` — Volatility targeting and position sizing
- `monitor.py` — Real-time risk monitoring (runs as SEPARATE process)
- `circuit_breakers.py` — Automated drawdown triggers

**What does NOT live here:**
- Strategy logic → `/strategies/`
- Order submission → `/execution/`
- Signal computation → `/strategies/`

---

## ARCHITECTURE PRINCIPLE

The risk monitor runs as a **completely separate process** from the signal engine.
It cannot be blocked, overridden, or silenced by strategy code.
It enforces limits that CANNOT be bypassed — not even "just for testing."

```
Signal Engine (Process 1) ──signals──▶ Target Positions DB
Risk Monitor  (Process 2) ──reads──▶  Target Positions DB
                          ──enforces──▶ Hard Limits
                          ──writes──▶  Approved Positions DB
Execution     (Process 3) ──reads──▶  Approved Positions ONLY
```

---

## POSITION SIZING — THE VOLATILITY TARGETING FORMULA

```python
def compute_position_notional(
    target_vol: float,          # annualised, e.g. 0.10 (10%)
    capital_allocation: Decimal, # capital assigned to this strategy
    realised_vol_20d: float,    # trailing 20-day annualised vol
    realised_vol_60d: float,    # 60-day vol for robustness cap
    liquidity_tier: int,        # 1, 2, or 3
) -> Decimal:

    # FLOOR: Never use vol below 10% annualised — prevents oversizing
    # in artificially compressed vol regimes (see: Nickel March 2022)
    vol = max(realised_vol_20d, VOL_FLOOR)  # VOL_FLOOR = 0.10

    # Primary sizing formula
    notional = (target_vol * capital_allocation) / vol

    # ROBUSTNESS CAP: Never exceed what 60d vol would produce
    # Prevents oversizing during short-term vol compression
    notional_60d_cap = (target_vol * capital_allocation) / max(realised_vol_60d, VOL_FLOOR)
    notional = min(notional, notional_60d_cap)

    # LIQUIDITY DISCOUNT: Reduce exposure to illiquid metals
    liquidity_factors = {1: 1.0, 2: 0.75, 3: 0.50}
    notional *= liquidity_factors[liquidity_tier]

    return Decimal(str(notional))
```

**Why the 10% vol floor?**
The 2022 nickel short squeeze: 20-day realised vol was ~12% on March 1st.
The market moved 250% in 2 days. The floor prevents absurdly large positions
during compressed-vol regimes that often precede explosive moves.

---

## HARD LIMITS (constants — never change at runtime)

```python
# limits.py — these are CONSTANTS not configurations

# Position-level limits
MAX_SINGLE_METAL_PCT = 0.20        # 20% of total NAV
MAX_SINGLE_STRATEGY_GROSS_PCT = 0.40  # 40% of NAV
MAX_PORTFOLIO_GROSS = 2.50         # 250% of NAV
MAX_PORTFOLIO_NET = 0.80           # 80% of NAV (long or short)
MIN_POSITION_NOTIONAL = 5_000      # £5,000 minimum

# Volatility limits
VOL_FLOOR = 0.10                   # 10% minimum vol estimate
STRATEGY_VOL_ALERT_MULTIPLIER = 1.5   # alert if 20d vol > 1.5× target
STRATEGY_VOL_REDUCE_MULTIPLIER = 2.0  # halve allocation if > 2× target

# Circuit breaker thresholds
DAILY_LOSS_LIMIT = 0.020           # 2% of NAV
WEEKLY_LOSS_LIMIT = 0.035          # 3.5% of NAV
MONTHLY_DRAWDOWN_LIMIT = 0.050     # 5% from month-start
PEAK_DRAWDOWN_SUSPEND = 0.100      # 10% from all-time HWM → suspend
HARD_STOP = 0.150                  # 15% from HWM → full liquidation

# Liquidity limits (max % of average daily volume)
MAX_ADV_PCT = {1: 0.05, 2: 0.03, 3: 0.02}  # by tier
LIQUIDATION_HORIZON_DAYS = 3       # must be able to exit in 3 days
MAX_SINGLE_DAY_ADV_PCT = 0.10      # max 10% of ADV on any single day

# Correlation limits
PAIRWISE_CORRELATION_ALERT = 0.60  # reduce smaller strategy to 50%
AVERAGE_CORRELATION_ALERT = 0.40   # reduce overall leverage by 30%
CORRELATION_LOOKBACK_DAYS = 60     # rolling window for correlation calc

# Metal concentration
MAX_SINGLE_METAL_GROSS_PCT = 0.30  # 30% of total gross exposure

# Fat-finger protection
MAX_ORDER_PRICE_DEVIATION_PCT = 0.02  # order must be within 2% of mid

# Strategy drawdown budgets
MAX_DRAWDOWN_BUDGET = {
    "S1": 0.12,
    "S2": 0.15,
    "S3": 0.12,
    "S4": 0.15,
    "S5": 0.10,
    "S6": 0.10,
}
```

---

## CIRCUIT BREAKER LOGIC

```python
# circuit_breakers.py

class CircuitBreaker:
    """
    Runs continuously as a separate process.
    Checks P&L and positions every 60 seconds during trading hours.
    """

    def check_daily_loss(self, current_pnl_pct: float) -> Action:
        if current_pnl_pct <= -DAILY_LOSS_LIMIT:
            # FLATTEN ALL POSITIONS IMMEDIATELY
            # No new trades for remainder of day
            # Automatic reinstatement next day at 75% leverage
            return Action.FLATTEN_ALL

    def check_weekly_loss(self, weekly_pnl_pct: float) -> Action:
        if weekly_pnl_pct <= -WEEKLY_LOSS_LIMIT:
            # Reduce all positions to 50% of target
            # No new entries
            # Scale up +10% per day back to 100%
            return Action.REDUCE_50_PCT

    def check_monthly_drawdown(self, monthly_pnl_pct: float) -> Action:
        if monthly_pnl_pct <= -MONTHLY_DRAWDOWN_LIMIT:
            # Reduce to 25% of target across all strategies
            # Hold at 25% until next calendar month
            return Action.REDUCE_25_PCT

    def check_peak_drawdown(self, drawdown_from_hwm: float) -> Action:
        if drawdown_from_hwm >= PEAK_DRAWDOWN_SUSPEND:
            # Full suspension — flatten to cash
            # Formal review required before reinstatement
            # Min 10 trading day cooling period
            # Restart at 25%, scale over 40 days
            return Action.FULL_SUSPEND

        if drawdown_from_hwm >= HARD_STOP:
            # FULL LIQUIDATION — ALL SYSTEMS SHUTDOWN
            # Strategy redesign required
            # 3-month paper trading before redeployment
            return Action.HARD_STOP
```

---

## STRATEGY-LEVEL SUSPENSION

If a strategy's drawdown exceeds its `MAX_DRAWDOWN_BUDGET`:
1. Suspend the strategy immediately
2. Close ALL positions in that strategy over max 3 trading days
   (avoid fire-sale impact — spread the exit)
3. Strategy remains suspended until:
   a. Root cause analysis completed
   b. PM review and reinstatement approval
   c. Restart at 50% of normal risk budget
   d. Scale back to 100% over 20 trading days

If a strategy's realised vol exceeds 1.5× target for 5 consecutive days:
→ Reduce capital allocation by 25%

If a strategy's realised vol exceeds 2.0× target:
→ Halve allocation IMMEDIATELY

---

## FACTOR EXPOSURE MONITORING

Track these daily and alert if breached:

1. **Metal concentration:** No single metal > 30% of total gross exposure
   - Copper most likely to exceed (appears in multiple strategies)

2. **Directional tilt:** Monitor net beta to metals index (e.g., BCOM Industrial Metals)
   - If net beta > ±0.50 → add index hedge or reduce directional strategies

3. **USD/GBP exposure:** All LME/COMEX metals are USD-denominated
   - For capital < £500k: don't hedge (FX forward cost > benefit)
   - For capital > £500k: passive monthly FX hedge covering 50–75% of notional

---

## STRESS TEST SCENARIOS (run quarterly)

Historical scenarios to replay against current portfolio:

| Scenario | Date | Impact |
|----------|------|--------|
| Nickel short squeeze | Mar 2022 | +250% in 2 days, LME halted & cancelled trades |
| COVID crash | Mar 2020 | Base metals -15–25% in 3 weeks, correlation spike |
| China commodity rout | Nov 2018 | Cu -15%, Ni -20% in 6 weeks |
| SNB shock | Jan 2015 | Gold +5% in hours, vol spike |
| Aluminium warehouse crisis | 2013–14 | 18+ months of misleading inventory data |
| Lehman collapse | Sep–Oct 2008 | Base metals -40–60% in 8 weeks, liquidity evaporated |

Hypothetical scenarios:
- Both LME and COMEX halt for 48 hours: 10% adverse on reopening — does portfolio survive?
- Correlation shock: all 6 strategies at +0.80 for a month (effective diversification drops from 2.4× to 1.2×)
- Data feed failure: 3 days of stale signals — staleness detection must halt signal generation

**Stress test pass criteria:**
- No single-day loss > 2% of NAV
- Max drawdown through scenario ≤ 10% hard limit
- Kill switch would have triggered at correct threshold

---

## KILL SWITCH

The kill switch MUST:
- Flatten ALL positions and cancel ALL working orders
- Be triggerable within 60 seconds
- Work even if the main execution process is hung
- Be accessible via Telegram bot command (`/kill_all`) AND direct IBKR action
- Log every activation to `audit.system_events` with full context

When to use:
- Daily loss limit breached (automated trigger)
- Exchange trading halt announced
- Suspected system malfunction
- Broker margin call that cannot be met
- Any time risk of holding > risk of exiting at unfavourable prices

---

## WHAT I SHOULD FLAG

When working in this directory:
- Any attempt to use a variable instead of a constant for a hard limit
- Any code path where risk checks can be skipped
- Any direct comparison with `float` for monetary values (use `Decimal`)
- Missing audit log entries for risk events
- Circuit breaker logic that can be blocked by strategy code
- Missing staleness validation before using vol estimates