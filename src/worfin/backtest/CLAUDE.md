# CLAUDE.md — /backtest
# Backtesting & Validation Engine | Part of: Metals Systematic Trading System

---

## SCOPE

This directory contains the walk-forward backtesting engine, performance metrics,
transaction cost model, and all overfitting detection tools.

**The cardinal rule of backtesting:**
> A backtest is a hypothesis, not a result.
> The backtest tells you whether a strategy is worth investigating further.
> Only OOS performance and live paper trading confirm whether it works.

**Overfitting is the #1 threat.** More parameters = more danger.
The majority of published trading strategies fail to replicate OOS.

---

## DATA SPLITS (FIXED — NEVER ADJUST RETROACTIVELY)

```python
# These boundaries are SET ONCE before any analysis begins.
# They cannot be changed after the first look at any data.

IS_START  = date(2005, 1, 1)   # In-Sample start
IS_END    = date(2017, 12, 31) # In-Sample end       (~60% of data)
OOS_START = date(2018, 1, 1)   # Out-of-Sample start
OOS_END   = date(2022, 12, 31) # Out-of-Sample end   (~25% of data)
HOLDOUT_START = date(2023, 1, 1) # Holdout start     (~15% of data)
# HOLDOUT_END = today (rolling)

# OOS DATA IS SINGLE-USE.
# If you test on OOS, observe poor results, modify the strategy,
# and re-test on the same OOS data → IT IS NOW CONTAMINATED IN-SAMPLE.
# A failed strategy must either be discarded or wait for new data to accumulate.
```

---

## WALK-FORWARD METHODOLOGY

**Use anchored walk-forward (not rolling):**

```python
def anchored_walk_forward(strategy, all_data, step_size_months=6):
    """
    Anchored: always start from IS_START, expand window each step.

    Why anchored (not rolling):
    1. Uses all available data — more statistical power
    2. Tests parameter stability as window expands
    3. Better reflects real deployment (use ALL history to calibrate)

    Produces: Series of 6-month OOS return segments → concatenated equity curve
    Each segment: parameters fitted ONLY on data available at that time → no look-ahead
    """
    results = []
    is_end = IS_END

    while is_end < OOS_END:
        # Fit strategy on all IS data up to is_end
        params = strategy.optimise(all_data[IS_START:is_end])

        # Apply FIXED parameters to next step_size_months
        oos_start = is_end + 1 day
        oos_end = oos_start + step_size_months months
        oos_returns = strategy.apply_fixed_params(params, all_data[oos_start:oos_end])

        results.append(oos_returns)

        # Expand the IS window (anchored)
        is_end = oos_end

    return concatenate(results)  # Genuinely OOS equity curve
```

---

## REQUIRED PERFORMANCE METRICS (ALL STRATEGIES — BOTH IS AND OOS)

```python
# metrics.py — compute ALL of these before any strategy is considered

REQUIRED_METRICS = {
    "annualised_return":    "Geometric mean of daily returns × 252",
    "annualised_vol":       "Std dev of daily returns × √252",
    "sharpe_ratio":         "(Ann. Return - Risk-Free) / Ann. Vol",
    "sortino_ratio":        "Ann. Return / Downside Deviation",
    "max_drawdown":         "Largest peak-to-trough decline",
    "calmar_ratio":         "Ann. Return / Max Drawdown",
    "win_rate":             "% of trades with positive return",
    "profit_factor":        "Gross Profits / Gross Losses",
    "avg_trade_duration":   "Mean holding period in days",
    "annual_turnover":      "Total gross trades / average NAV",
    "t_stat_sharpe":        "Sharpe × √N (N = independent observations)",
    "wfer":                 "OOS Sharpe / IS Sharpe",
    "pbo":                  "Probability of Backtest Overfitting (CPCV)",
}

# MINIMUM THRESHOLDS (a strategy failing ANY of these is not deployed)
MINIMUM_THRESHOLDS = {
    "sharpe_ratio":   {"IS": 0.50, "OOS": 0.30},  # net of all costs
    "max_drawdown":   {"IS": 0.20, "OOS": 0.15},  # maximum acceptable
    "t_stat_sharpe":  {"IS": 3.0,  "OOS": 2.0},
    "wfer":           {"OOS": 0.50},
    "pbo":            {"OOS": 0.20},               # must be BELOW this
    "calmar_ratio":   {"OOS": 0.50},
}
```

---

## TRANSACTION COST MODEL (MANDATORY — NO EXCEPTIONS)

```python
# costs.py — always applied. A backtest without costs is fiction.

TRANSACTION_COSTS = {
    # Round-trip costs (entry + exit)
    "CA":  {"spread_bps": 3,  "commission": 3.00, "slippage_bps": 0.75},  # LME Cu
    "AH":  {"spread_bps": 3,  "commission": 3.00, "slippage_bps": 0.75},  # LME Al
    "ZS":  {"spread_bps": 4,  "commission": 3.00, "slippage_bps": 1.00},  # LME Zn
    "NI":  {"spread_bps": 8,  "commission": 3.00, "slippage_bps": 2.00},  # LME Ni
    "PB":  {"spread_bps": 5,  "commission": 3.00, "slippage_bps": 1.50},  # LME Pb
    "SN":  {"spread_bps": 20, "commission": 3.00, "slippage_bps": 5.00},  # LME Sn
    "GC":  {"spread_bps": 2,  "commission": 3.00, "slippage_bps": 0.75},  # COMEX Au
    "SI":  {"spread_bps": 3,  "commission": 3.00, "slippage_bps": 1.00},  # COMEX Ag
    "PL":  {"spread_bps": 6,  "commission": 3.00, "slippage_bps": 2.00},  # COMEX Pt
    "PA":  {"spread_bps": 10, "commission": 3.00, "slippage_bps": 3.00},  # COMEX Pd
}

ROLL_COSTS = {
    "LME_3M":     {"per_tonne": 0.75},   # $/tonne per roll, typical
    "COMEX_FRONT": {"per_oz_Au": 0.20},  # $/oz per roll, typical
}

# IMPORTANT: Budget 30–50% HIGHER than these estimates
# These are BASELINE — use upper end for stress testing
COST_STRESS_MULTIPLIER = 1.50
```

---

## OVERFITTING DETECTION

### 1. Multiple Testing Correction

```python
def adjust_significance_for_multiple_tests(n_combinations_tested: int) -> float:
    """
    If you tested 100 parameter combinations and pick the best,
    there's ~99.4% chance at least one will appear significant at 5% by chance.

    Always record total combinations tested (including discarded).
    Apply Bonferroni correction.
    """
    alpha_adjusted = 0.05 / n_combinations_tested
    required_t_stat = scipy.stats.t.ppf(1 - alpha_adjusted/2, df=N_OBSERVATIONS)
    return required_t_stat

# Harvey, Liu & Zhu (2016): minimum t-stat of 3.0 for any claim of significance
# (accounting for collective testing across industry)
MINIMUM_T_STAT = 3.0
```

### 2. Walk-Forward Efficiency Ratio (WFER)

```python
WFER = OOS_Sharpe / IS_Sharpe

# Interpretation:
# > 0.70: Excellent — strategy is robust, possibly under-fitted
# 0.50–0.70: Good — proceed to paper trading
# 0.30–0.50: Marginal — simplify parameters, re-test
# < 0.30: Poor — likely overfitted, discard or redesign

WFER_MINIMUM = 0.50
```

### 3. Combinatorial Purged Cross-Validation (CPCV)

```python
def compute_pbo(strategy, data, K=10):
    """
    de Prado (2018): Most rigorous overfitting detection.

    PBO = Probability of Backtest Overfitting
        = fraction of train/test combinations where OOS Sharpe is negative

    Target: PBO < 0.20
    Alert:  PBO > 0.40 (high overfitting probability)
    """
    blocks = split_into_K_contiguous_blocks(data, K)
    oos_sharpes = []

    for train_blocks, test_blocks in combinations(blocks, K-2):
        # PURGE: remove buffer zone between train and test (5 days)
        # EMBARGO: exclude first 5 days of test block after training
        train_data = purge_and_embargo(train_blocks)
        test_data = purge_and_embargo(test_blocks)

        params = strategy.fit(train_data)
        oos_return = strategy.evaluate(params, test_data)
        oos_sharpes.append(oos_return.sharpe)

    pbo = sum(1 for s in oos_sharpes if s < 0) / len(oos_sharpes)
    return pbo
```

### 4. Parameter Stability Analysis

```python
def check_parameter_stability(strategy, base_params):
    """
    Robust strategies show <20% Sharpe degradation for ±20% parameter shifts.
    Parameter-fragile strategies (>40% Sharpe degradation at ±20%) are likely overfitted.

    Vary each parameter ±30% from selected value.
    Flag if Sharpe degrades >40% when any parameter shifts by 20%.
    """
```

---

## STRATEGY GRADUATION PIPELINE

```
G0 → G1 → G2 → G3 → G4 → Live
```

| Gate | Stage | Duration | Pass Criteria |
|------|-------|----------|---------------|
| G0 | IS Backtest | Historical | IS Sharpe ≥ 0.50, t-stat ≥ 3.0, max DD ≤ 20%, parameter stability pass, clear economic rationale |
| G1 | Walk-Forward OOS | Historical | OOS Sharpe ≥ 0.30, WFER ≥ 0.50, PBO ≤ 0.20, max DD ≤ 15% |
| G2 | Holdout Validation | Recent history | Holdout Sharpe ≥ 0.20, consistent with OOS ±30%, no structural break |
| G3 | Paper Trading | Min 60 trading days | Paper Sharpe within 50% of OOS Sharpe, execution assumptions validated, data pipeline reliable |
| G4 | Live Ramp | 60 days at 25%→100% | Live Sharpe within 50% of paper Sharpe, all risk limits respected |

**Fail at any gate → back to previous gate or discard.**
**No strategy gets live capital without passing all 5 gates.**

**G4 Live Ramp schedule:**
- Days 1–20: 25% of target risk budget
- Days 21–40: 50%
- Days 41–60: 75%
- Day 61+: 100% (if no drawdown budget breach)

---

## HISTORICAL STRESS TEST PROCEDURE (QUARTERLY)

```python
STRESS_SCENARIOS = [
    {"name": "Nickel squeeze", "start": "2022-03-01", "end": "2022-03-15"},
    {"name": "COVID crash",    "start": "2020-02-24", "end": "2020-03-31"},
    {"name": "China rout",     "start": "2018-10-01", "end": "2018-12-31"},
    {"name": "SNB shock",      "start": "2015-01-15", "end": "2015-01-31"},
    {"name": "Aluminium warehouse", "start": "2013-06-01", "end": "2014-12-31"},
    {"name": "Lehman",         "start": "2008-09-15", "end": "2008-11-30"},
]

# For each scenario:
# 1. Replay actual daily returns through CURRENT portfolio
# 2. Use current position sizes and strategy allocations
# 3. Verify: no single-day loss > 2% NAV
# 4. Verify: max drawdown through scenario ≤ 10%
# 5. Verify: kill switch would have triggered at correct threshold
# If ANY check fails → tighten position limits or reduce allocations
```

---

## REGIME MONITORING

```python
# Classify metals market regime daily using 60-day rolling vol:
REGIME_VOL_THRESHOLDS = {
    "low_vol":    (0, 0.15),     # <15% annualised
    "normal_vol": (0.15, 0.25),  # 15–25%
    "high_vol":   (0.25, 9.99),  # >25%
}

# Strategy performance by regime:
# High-vol:    S2 TSMOM, S3 XS Mom outperform
# Low-to-norm: S1 Carry outperforms
# Correlation spike (avg >0.70): S3, S6 lose diversification benefit

# Structural break tests (quarterly on live return stream):
# 1. Chow test: parameter stability in most recent 6 months vs prior
# 2. CUSUM test: significant shift in cumulative return level?
# 3. Rolling Sharpe: if negative for 6 consecutive months → formal review
```

---

## BACKTEST REPORT TEMPLATE

Every strategy must have a formal report before passing G1:

```
Section 1: Strategy Description — hypothesis, signal, economic rationale
Section 2: Data & Methodology — sources, splits, cost model, roll method
Section 3: In-Sample Results — metrics table, equity curve, monthly heatmap,
           drawdown chart, parameter sensitivity surface
Section 4: Walk-Forward Results — OOS equity curve, WFER, IS vs OOS comparison
Section 5: Overfitting Analysis — combinations tested, t-stat w/ correction,
           PBO, parameter stability
Section 6: Risk Analysis — stress scenarios, factor exposure, strategy correlation
Section 7: Conclusion — pass/fail at each gate, recommended allocation,
           known risks, suspension conditions
Section 8: Appendix — parameters, Git commit hash, data source versions
```

---

## WHAT I SHOULD FLAG

When working in this directory:
- Any backtest not including full transaction costs
- IS/OOS/Holdout data splits being changed after first data inspection
- OOS data being used for parameter optimisation (contamination)
- Missing multiple testing correction when >1 parameter combination tested
- Sharpe presented without t-statistic and N clearly stated
- Parameter sensitivity analysis missing from any strategy report
- Stress tests showing daily loss > 2% NAV without flagging
- Walk-forward using rolling window instead of anchored (without good reason)
