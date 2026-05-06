# Plan 1 — S4 Basis-Momentum IS Backtest

**Sequenced position:** First. Must close before Plan 2 (refactor) starts.
**Goal:** First end-to-end S4 backtest on IS data 2005–2017, evaluated against G0.
**Owner:** Tim, with Claude Code in IDE for execution.
**Status:** State unknown — verification pass required before run.

---

## 1. G0 Pass Criteria (Hard)

The backtest passes G0 if and only if **all three** of these hold on IS data 2005–2017:

| Metric | Threshold | Source |
|---|---|---|
| IS Sharpe ratio (annualised, net of costs) | ≥ 0.50 | WorFIn validation framework |
| t-statistic of mean daily return | ≥ 3.0 | Harvey, Liu & Zhu (2016) — multiple-testing threshold |
| Maximum drawdown (peak-to-trough, daily NAV) | ≤ 20% | WorFIn risk framework |

A partial pass (2 of 3) is **not** a pass. Two of three is grounds for investigation, never for parameter tweaking.

Reference: Bakshi, Gao & Rossi (2019) reported a t-stat of 4.14 on the basis-momentum factor in their universe. We are aiming for materially below that — anything above 3.0 confirms the signal is alive in our specific implementation and metals universe. If we hit 4.14 or higher, that's a strong sign of overfitting and should trigger scrutiny, not celebration.

---

## 2. Pre-Flight Checklist

Do not press "run" until every box below is ticked. The order matters.

### 2.1 Code health
- [ ] `empyrical` removed from `pyproject.toml` dependencies
- [ ] Custom Sharpe/Sortino/Calmar/max-DD/t-stat functions implemented in `src/worfin/backtest/metrics.py`
- [ ] All existing tests pass after empyrical removal (target: 62+ tests, 0 failures)
- [ ] `pip install -e ".[dev]"` from a clean `.venv` succeeds on Python 3.11+
- [ ] `alembic upgrade head` applies cleanly on a fresh database
- [ ] Git working tree is clean and on a known commit (so the run is reproducible)

### 2.2 Step 0 verification (the four files)
Step 0 closes the gap between "Tim added a file" and "Claude has read the file with care." Each file gets a written verification note in your Claude Code session, not just a glance.

- [ ] `continuous.py` — verify the back-adjustment logic: which contract is the active month at each point, what are the roll dates, is the adjustment additive or proportional, is there look-ahead bias in the roll trigger
- [ ] `alembic/versions/002_pnl_accounting.py` — verify `environment` and `backtest_run_id` columns are present on P&L tables, verify TIMESTAMPTZ is used, verify `audit.roll_log` is included per memory
- [ ] `alembic/versions/003_fx_rates.py` — verify staleness flag (≥7 days old triggers audit), no silent fallback, FX rates stored as Decimal, source attribution column
- [ ] `pretrade_intergation.py` (rename to `pretrade_integration.py` while you're there) — verify all six pre-trade checks fire in order, verify they fail closed (refuse the trade) on any breach, verify each breach writes to `audit.risk_breaches` with `correlation_id` and `backtest_run_id`

For each file, the verification note should answer:
1. What does this file claim to do?
2. Does it do that?
3. What are the assumptions it makes about its inputs?
4. What edge cases does it not handle?
5. Are there any hardcoded values that should be config?

### 2.3 Data verification
- [ ] LME continuous series exists for all six base metals (CA, AH, ZS, NI, PB, SN) covering 2005-01-01 → 2017-12-31, daily, no gaps > 5 trading days
- [ ] COMEX continuous series exists for all four precious metals (GC, SI, PL, PA) covering same window
- [ ] Roll dates match exchange convention (LME 3-month rolling tenor; COMEX active month with FND offset)
- [ ] No future-dated rows leaked into the IS window
- [ ] Price units consistent (USD/tonne for LME; USD/oz for precious) and stored as Decimal
- [ ] At least one spot check per metal: pick a known date (e.g. LME copper close on 2008-09-15, Lehman day) and confirm the value matches an external source within 1%

### 2.4 Strategy implementation sanity
- [ ] `s4_basis_momentum.py` signal calculation matches Bakshi, Gao & Rossi (2019) exactly. Specifically:
  - 12-month formation window
  - Signal = average over [t-252, t-1] of (R_spot - R_basis), where R_spot is the front-nearby return and R_basis is the second-nearby return (or its reciprocal — confirm convention)
  - Cross-sectional rank monthly, long top tercile, short bottom tercile
  - Skip the most recent 1 month if implementing the "Jegadeesh skip" variant (note in code which variant is used)
- [ ] Position sizing happens in `risk/sizing.py`, NOT inside the strategy file (per CLAUDE.md)
- [ ] Inverse-volatility targeting at the position level (each leg sized to equal vol contribution)
- [ ] Strategy is stateless (no instance variables that persist across `generate_signals` calls)
- [ ] No `print()`, all logging via `structlog` with `correlation_id`

### 2.5 Cost model
- [ ] LME costs: bid-ask spread + commission + slippage. Use conservative figures (5–10 bps round-trip for liquid LME contracts; higher for nickel and tin).
- [ ] COMEX costs: similar, with NYMEX-specific commission tiers
- [ ] Roll costs explicitly modelled (S4 holds for ~10–20 days so monthly rebalance ≈ 1 roll per cycle per leg)
- [ ] Costs subtracted from gross P&L in `backtest/metrics.py`, not silently absorbed elsewhere

---

## 3. Backtest Configuration

Lock these in `config/backtest_runs/s4_is_v1.yaml` (or equivalent) before running. They are inputs to a reproducible run.

```yaml
run_id: s4_is_v1                       # written to audit.backtest_runs
strategy: s4_basis_momentum
universe: [CA, AH, ZS, NI, PB, SN, GC, SI, PL, PA]  # all 10 metals
in_sample:
  start: 2005-01-01
  end:   2017-12-31
out_of_sample: null                    # OOS is a separate run
holdout: null
frequency: daily
bar_size: 1d
rebalance: monthly                     # last business day, execute next open
target_vol_pct: 10                     # annualised, strategy-level (will scale to 25% in portfolio)
vol_lookback_days: 60                  # for vol targeting
formation_window_months: 12            # Bakshi-Gao-Rossi
skip_window_months: 0                  # set to 1 if testing the Jegadeesh-skip variant
top_quantile: 0.33
bottom_quantile: 0.33
costs:
  futures_cost_model: standard         # spread + commission + slippage
  bps_overrides:                       # only if standard model insufficient
    NI: 12                             # nickel needs higher cost
    SN: 15                             # tin too
fx_handling:
  base_currency: USD                   # P&L in USD; GBP conversion at portfolio level only
seeds:
  numpy: 42                            # for any stochastic component
environment: backtest                  # written to P&L rows
```

Anything not in the YAML is a magic number and should be lifted out before the run. If the run isn't fully reproducible from the YAML alone, it's not a real backtest.

---

## 4. Run Procedure

```bash
# Fresh terminal
cd worfin_strategies
source .venv/bin/activate
python --version  # confirm 3.11+

# Pull latest
git pull origin main
git status        # working tree clean?
git log --oneline -1  # record commit hash for the run

# Tests must be green before any backtest
pytest tests/ -v
# Expected: all pass, no warnings about empyrical or other deprecated deps

# Database is up to date
alembic current
alembic upgrade head

# Run S4 IS
python scripts/run_backtest.py \
  --config config/backtest_runs/s4_is_v1.yaml \
  --output runs/s4_is_v1/ \
  --log-level INFO
```

The runner writes:
- `runs/s4_is_v1/daily_pnl.parquet` — daily NAV and per-metal contribution
- `runs/s4_is_v1/positions.parquet` — daily target and actual positions
- `runs/s4_is_v1/trades.parquet` — every fill with cost breakdown
- `runs/s4_is_v1/metrics.json` — Sharpe, t-stat, max DD, hit rate, turnover
- `runs/s4_is_v1/run_metadata.json` — config snapshot, commit hash, dependency versions
- `runs/s4_is_v1/audit.log` — structured JSON log with `correlation_id`

---

## 5. Validation Steps After the Run

Do these in order. Do not skip to the metrics — the diagnostics matter as much as the numbers.

### 5.1 Sanity checks first (diagnostics before headline metrics)

These catch implementation bugs that produce real-looking but wrong P&L.

- **Position counts.** At each rebalance, do you hold roughly N/3 long and N/3 short? (For 10 metals, expect 3L/3S/4M with ties handled.)
- **Position sizing.** Plot per-position vol contribution. They should be flat — if one metal is 3x another, the inverse-vol sizing has a bug.
- **Turnover.** Monthly rebalance with 12-month formation should give modest turnover (~30–50% per month). >100% is a red flag.
- **Sign of P&L on known days.** During known commodity stress (2008-09 to 2009-03; Mar 2020), a momentum strategy should show specific behaviour. Verify directionally sensible.
- **Per-metal Sharpe distribution.** No single metal should contribute >40% of the strategy P&L. If one does, the strategy is concentration-risk in disguise.

### 5.2 Then headline metrics

Compute from `daily_pnl.parquet`:
- IS Sharpe (annualised, daily returns, 252-day convention)
- t-stat of mean daily return: `mean / (std / sqrt(N))`
- Max drawdown (peak-to-trough on cumulative NAV)
- Sortino (downside-deviation Sharpe)
- Calmar (annualised return / max DD)
- Hit rate (% of months with positive P&L)
- Average holding period
- Annual turnover

### 5.3 G0 evaluation

| Metric | Threshold | Result | Pass? |
|---|---|---|---|
| IS Sharpe | ≥ 0.50 | (fill in) | (fill in) |
| t-statistic | ≥ 3.0 | (fill in) | (fill in) |
| Max DD | ≤ 20% | (fill in) | (fill in) |

All three boxes ticked → G0 passes. Move to Plan 2.

---

## 6. Decision Tree

### 6.1 G0 passes
- Document IS results in `runs/s4_is_v1/G0_report.md`
- Update repo `README.md` to flag S4 IS as cleared
- **Do not** run OOS yet. OOS sits outside the metals refactor decision — we want clean OOS evaluation against the *refactored* code, so OOS comes after Plan 2.
- Move to Plan 2 (refactor).

### 6.2 G0 fails on Sharpe (< 0.50) or t-stat (< 3.0)
The temptation will be to tweak parameters. **Do not.** Tuning on IS data is the textbook overfitting trap. Instead:

1. Re-run sanity checks (5.1). Bug? Fix and re-run.
2. Verify signal calculation matches the paper exactly. Discrepancy? Fix and re-run.
3. Check cost model. Costs too high? Recalibrate to realistic levels (don't make them too low — that's overfitting in a different direction).
4. If 1–3 all clean and Sharpe is still <0.50: the strategy as-implemented does not have edge in our universe. Stop. Document. Decide whether to:
   - (a) Try a different signal variant from the paper (BGR test multiple specifications)
   - (b) Drop S4 from the universe and reweight the metals book

Option (a) is permitted **only** with a pre-registered variant (signed off in writing before the run). No fishing.

### 6.3 G0 fails on max DD (> 20%)
This is usually a position-sizing problem, not a signal problem.

1. Verify vol targeting is working. Plot rolling realised portfolio vol — is it stable around the 10% target?
2. Check for outsized positions during stress periods (2008, 2011, 2015 nickel squeeze, 2020 March).
3. Check vol-lookback parameter: 60 days may be too long during regime shifts. Consider EWMA with halflife 20–30 days as a sensitivity test (NOT as a tweak — flag as a structural decision).
4. If max DD remains > 20% with sensible sizing: the strategy is too risky for our portfolio limits and needs lower vol target (e.g. 6%) or smaller weight in portfolio.

### 6.4 Mixed result (e.g. Sharpe 0.55, t-stat 2.8)
Marginal pass on Sharpe but fail on t-stat usually means high noise / low sample size. The strategy might have edge but you can't statistically confirm it on this window. Treat as a fail — t-stat is the most reliable filter against overfitting at this stage. Investigate per 6.2.

---

## 7. Out of Scope for Plan 1

- OOS validation (deferred to post-refactor)
- Walk-forward optimisation (deferred; never run on IS)
- Live or paper trading (deferred)
- Other strategies (S1, S2, S3, S5, S6) — only after S4 IS clears
- FX (Plans 2 and 3)
- Refactor (Plan 2)

---

## 8. Definition of Done

Plan 1 is closed when:
- [ ] All Pre-Flight checklist boxes ticked
- [ ] Backtest run completed and outputs written to `runs/s4_is_v1/`
- [ ] G0 report written: pass or documented fail-with-investigation
- [ ] If pass: README.md updated; commit pushed; tag `s4-is-v1` created on the run commit
- [ ] If fail: investigation findings written; NEVER tuned on IS data; explicit go/no-go decision recorded

Only after this is closed do we open Plan 2.