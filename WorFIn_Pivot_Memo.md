# WorFIn Strategic Pivot Memo — FX Addition & Capital-Staged Live Deployment

**Status:** ACTIVE
**Decision date:** April 2026
**Authors:** Tim (CIO/CTO), Claude (advisor)
**Intended audience:** Future Claude sessions, future hires (James 2029), Jared (legal advisor)

---

## TL;DR

The metals strategy universe remains the primary thesis and long-term core of WorFIn. Nothing about the metals build, validation framework, or 5-year roadmap has been abandoned. A secondary FX strategy book has been added to handle the sub-£50k capital phase and serve as a permanent low-correlation diversifier thereafter. Module architecture has been generalised to be asset-class-agnostic to support both books cleanly.

---

## 1. Context

As of April 2026, Tim has approximately **£5,000 of live risk capital** available, with continuous additions planned over the following 6–12 months. The 5-year roadmap assumed £50–100k of live capital deployed in Year 2 (H2 2027 – H1 2028) against the metals strategy universe.

The capital gap is structural, not solvable by better strategy design:

- **One LME or COMEX contract's margin exceeds the entire live pot.** LME Copper ~$23k margin, COMEX Gold ~$14k margin, COMEX Silver ~$13k margin.
- **Minimum lot sizes preclude volatility targeting.** Vol targeting requires fractional positions. The smallest futures contract is one lot, and one lot has daily 1σ P&L of £1,000–£4,500 against a £5,000 account.
- **Micro futures do not solve the problem.** MGC, MES, etc. allow £5k to hold 2–3 positions. This is a directional bet, not a diversified systematic book. Commissions and slippage consume 50–100 bps per round-trip, which exceeds expected strategy edge at that size.

No amount of strategy redesign changes this. Metals futures have a capital threshold, and £5k is well below it.

---

## 2. The Decision

**Dual-track deployment.** The metals strategy universe and the FX strategy universe run in parallel indefinitely.

### Metals track
- **Status:** Primary thesis. Unchanged.
- **Phase:** Paper trading through 2027, per original roadmap.
- **Live deployment trigger:** Personal capital ≥ £50k AND metals paper Sharpe within 50% of OOS Sharpe for 60+ trading days.
- **Year 2 target (£50–100k):** Still the target. Expected hit window: mid-to-late 2027.

### FX track
- **Status:** Secondary book. New.
- **Phase:** Design → backtest → paper trading (alongside metals) → live at £5k when G3 paper criteria met.
- **Live deployment trigger:** FX paper Sharpe within 50% of OOS Sharpe for 60+ trading days. Can go live even if metals is still in paper.
- **Long-term role:** Permanent allocation. FX capital weight declines proportionally as metals capital becomes viable, but never drops to zero. FX carry and TSMOM have ~0.1–0.3 correlation to metals carry and TSMOM and retain permanent diversification value.

### Capital allocation ladder

| Live capital | FX book | Metals book |
|--------------|---------|-------------|
| £5k | 100% | 0% (paper) |
| £10–25k | 100% | 0% (paper) |
| £25–50k | ~50% | 0% (paper) |
| £50–100k | ~30% | ~70% |
| £100k+ | 20–25% | 75–80% |

---

## 3. What Did NOT Change

To be explicit, because future sessions should not re-open these questions without cause:

- **Metals thesis.** Unchanged. Bakshi, Gao & Rossi (2019) remains the core reference for S4. The six-strategy universe (S1 Carry, S2 TSMOM, S3 XS Mom, S4 Basis-Mom, S5 Inventory, S6 Pairs) is intact.
- **Metals strategy allocations.** S4 25%, S1 20%, S2 20%, S3 15%, S5 10%, S6 10% — unchanged. These are intra-metals weights.
- **5-year roadmap.** Year 2 live at £50–100k at metals. Year 4 fund launch. Year 5 Cape Town, £10–20m AUM. All intact.
- **Validation framework.** G0 → G1 → G2 → G3 → G4 gates apply to FX as well as metals, with identical thresholds.
- **Risk framework.** Volatility targeting, pre-trade risk checks, kill switch, reconciliation, drawdown limits — all applied to FX unchanged.
- **Infrastructure tier progression.** Tier 0 → 1 → 2 → 3 unchanged. Currently transitioning Tier 0 → 1.
- **Broker.** Interactive Brokers. IdealPro for FX, ICEEU for LME (when permissions come through), COMEX/NYMEX for precious metals.
- **Jared's role.** Legal advisor. Company incorporation still required before live capital deployment.
- **James joining 2029 as COO/Head of Capital.** Timeline unchanged.

---

## 4. What Changed Architecturally

Tim's decision: module layout is **asset-class-agnostic** rather than asset-class-specific.

### Before (metals-only)

```
src/worfin/
  config/
    metals.py           # MetalSpec, all metal constants
  data/ingestion/
    lme.py
    comex.py
    fx_rates.py         # Only for metals-to-GBP conversion
  strategies/
    s1_carry.py         # Hardcoded metals universe
    s4_basis_momentum.py
  execution/
    ibkr.py             # Futures-only
  risk/
    pretrade.py
```

### After (asset-class-agnostic)

```
src/worfin/
  config/
    instruments.py      # InstrumentSpec base + MetalSpec + FXSpec
    metals.py           # Metal instances (CU, AL, ...) using MetalSpec
    fx.py               # FX pair instances (EURUSD, GBPUSD, ...) using FXSpec
  data/
    ingestion/
      lme.py
      comex.py
      fred.py           # FX rates AND short rates (expanded role)
      ibkr_hist.py      # Unified historical data via IBKR
    models.py           # Asset-class-agnostic schema
  strategies/
    carry.py            # Parameterised over InstrumentUniverse
    tsmom.py            # Parameterised over InstrumentUniverse
    xs_momentum.py      # Parameterised over InstrumentUniverse
    basis_momentum.py   # Metals-specific (requires futures term structure)
    inventory.py        # Metals-specific (requires LME stocks data)
    pairs.py            # Metals-specific (cointegration within metals)
  execution/
    broker.py           # Broker interface
    ibkr.py             # IBKR adapter — handles futures AND FX
  costs/
    model.py            # CostModel interface
    futures.py          # FuturesCostModel (tick-based)
    fx.py               # FXCostModel (bps-based)
  risk/
    pretrade.py         # Asset-agnostic
```

### Migration plan

1. **Introduce the `InstrumentSpec` base class and polymorphism.** Existing `MetalSpec` becomes a subclass. No code using `MetalSpec` should break.
2. **Add `FXSpec` subclass.** Defines spread in bps, commission in bps, no lot size, no FND/roll logic, no margin tier.
3. **Refactor strategies S1, S2, S3 to accept a universe parameter.** FX1, FX2, FX3 become concrete instances of these strategies on the FX universe.
4. **Leave S4, S5, S6 metals-specific.** These require features (term structure, inventory, cointegration within a narrow complex) that do not generalise to FX cleanly.
5. **Introduce `CostModel` polymorphism.** Existing futures cost logic becomes `FuturesCostModel`. New `FXCostModel` for bps-based costs.
6. **Extend IBKR adapter to handle FX order types** (IdealPro-specific: fractional sizing, no expiry, different order codes).

This refactor is technical debt. It does not add new user-visible functionality by itself. It should be completed before or alongside the FX build, not deferred.

---

## 5. FX Strategy Universe Summary

Full spec lives in `FX_Strategy_Universe.md`. Short version:

**Universe:** 9 G10 pairs on IBKR IdealPro — EUR/USD, GBP/USD, USD/JPY, USD/CHF, AUD/USD, USD/CAD, NZD/USD, USD/SEK, USD/NOK.

**Strategies:**
- **FX1 Carry** ↔ maps to S1. Rate differential signal, cross-sectional rank, long top 3 / short bottom 3, rebalanced weekly. Expected Sharpe 0.4–0.6.
- **FX2 TSMOM** ↔ maps to S2. Blended EMA crossover per pair, rebalanced daily. Expected Sharpe 0.6–0.9.
- **FX3 XS Momentum** ↔ maps to S3. 60-day skip-5 return, rank, long top 3 / short bottom 3, rebalanced bi-weekly. Expected Sharpe 0.3–0.5.

**Allocation:** 40% FX1 / 40% FX2 / 20% FX3.
**Target portfolio vol:** 6–7% annualised (conservative; step to 10% after live validation).
**Expected portfolio Sharpe:** 0.8–1.0 IS, 0.5–0.7 OOS after costs.
**Rebalance cadence:** mixed per strategy (FX1 weekly, FX2 daily, FX3 bi-weekly) — accepted as intentional.
**Benchmark:** equal-weighted long basket of 9 normalised "foreign-vs-USD" pairs.

**Round-trip cost per trade:** ~1.2–2.5 bps (vs ~5–10 bps for metals futures). This is the quantitative reason FX works at £5k and metals does not.

---

## 6. Decisions Locked

| # | Decision | Resolution |
|---|----------|------------|
| 1 | Module architecture | Asset-class-agnostic (option 2) |
| 2 | Historical data for FX backtest | FRED daily close; IBKR for paper/live |
| 3 | Rebalance cadence across FX strategies | Mixed cadence accepted |
| 4 | FX benchmark | Equal-weighted basket of 9 normalised pairs |
| 5 | Continue metals paper trading during FX build | YES — do not pause metals |
| 6 | Live FX trigger | Paper Sharpe within 50% of OOS for 60+ days |
| 7 | Live metals trigger | Capital ≥ £50k AND paper gate met |

---

## 7. Sequencing (Implementation Order)

1. **Complete Step 0 verification** on metals files Tim added (`continuous.py`, `002_pnl_accounting.py`, `003_fx_rates.py`, `pretrade_intergation.py`).
2. **Complete first S4 Basis-Momentum IS backtest** on 2005–2017. Must clear G0 gate before opening the FX front.
3. **Module refactor: asset-class-agnostic.** `InstrumentSpec` hierarchy, `CostModel` polymorphism, strategy parameterisation.
4. **FX data layer.** Short rates pipeline (FRED), FX historicals (FRED + IBKR), `FXSpec` instances for the 9 pairs.
5. **FX backtests.** FX1, FX2, FX3 against IS 2005–2017, OOS 2018–present. Apply G0 and G1 gates.
6. **FX paper trading.** Runs alongside metals paper. Minimum 60 trading days per G3 gate.
7. **Live FX at £5k** when G3 cleared.
8. **Metals paper continues** throughout. When capital ≥ £50k and paper gates met, flip metals to live.

**Do not open the FX front until step 2 is complete.** The temptation to build the new shiny thing before finishing the metals validation is the #1 risk (per roadmap: "Infrastructure over-engineering — HIGH, #1 risk for engineer-founders").

---

## 8. Open Items & Watch List

- **IBKR paper permissions.** Support request sent April 2026 for LME (ICEEU), COMEX metals, and CME micros permissions. Awaiting response.
- **IBKR live LME minimum capital.** Critical data point. Drives metals live deployment timing.
- **IBKR market data subscriptions.** LME Real-Time (~$55/mo), NYMEX Real-Time (~$14/mo), CME Real-Time (~$14/mo). Paper accounts are billed.
- **CFDs as an alternative.** Deferred. Futures + FX cover the need.
- **Crypto.** Explicitly excluded per roadmap. Not reconsidered.
- **IBKR "professional" status check.** Macquarie tenure may flag professional classification. Worth checking before live permissions.
- **FX venue choice confirmed.** IdealPro (fractional, deep, tight). Not FXCM, not OANDA.
- **Company incorporation.** Still required before live capital deployment per roadmap. Jared to action when live deployment nears.
- **Intervention monitoring hook.** Need to flag SNB, BoJ, Norges Bank interventions before ingesting signals affected by them.

---

## 9. What Future Sessions Should Know

If you are a future Claude session picking up this project, the behavioural takeaways are:

1. **Do not treat FX as a replacement for metals.** It is a parallel, complementary track. If a user's request is ambiguous about which book they mean, ask.
2. **Search project knowledge first.** Before doing anything, run `project_knowledge_search` to load this memo and the strategy universe docs.
3. **Respect the sequencing discipline.** Do not build FX code ahead of metals S4 backtest completion unless Tim explicitly says to.
4. **Verify before building.** Apply the Step 0 discipline: any files Tim added between sessions must be verified before new work.
5. **Maintain the asset-class-agnostic architecture.** If you find yourself writing metals-specific code in a file that should be generic, stop and refactor.
6. **Do not relitigate the pivot.** The decision is made. Tim does not want to re-debate whether £5k could somehow work for metals.
7. **Keep CLAUDE.md files updated.** Every module directory has one. Add FX context to the relevant files as they evolve.
8. **Apply the same risk discipline to FX as to metals.** Vol targeting, pre-trade checks, kill switch, stress tests — FX is not a toy, it is a live book even at £5k.

---

## 10. Review Triggers

This memo should be reviewed and potentially updated when any of the following occurs:

- Live capital crosses a ladder threshold (£25k, £50k, £100k)
- IBKR responds on LME permissions
- FX G0 or G3 gate outcome (pass or fail)
- Metals G0 outcome on S4
- Any structural change to the 5-year roadmap
- New strategy added or removed from either universe
- James joins earlier or later than 2029

Attach a dated revision log at the end of this file when changes are made.

---

## Revision Log

- **April 2026** — Initial memo. FX book added alongside metals. Module architecture generalised. Capital ladder defined. Sequencing locked.
