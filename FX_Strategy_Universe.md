# WorFIn FX Strategy Universe

**Status:** Active — secondary book to the metals strategy universe
**Scope:** G10 cash FX on IBKR IdealPro
**Role in the firm:** Capital-efficient complement to the metals book; vehicle for early live deployment at sub-£50k capital; permanent diversifier thereafter.
**Last updated:** April 2026

---

## 1. Purpose

The FX strategy universe exists because systematic metals futures require capital that WorFIn does not yet have. One LME or COMEX contract's margin exceeds the entire starting live pot (£5k), and volatility targeting requires fractional positions that full-size futures contracts cannot provide. FX is the one asset class on IBKR where £5k can hold a properly vol-targeted, multi-position, multi-strategy book — because IdealPro offers deep liquidity at sub-dollar trade increments with 50:1 leverage, tight spreads (0.1–0.3 bps on G10), and minimal fixed costs.

The strategies below are deliberately chosen to be the FX analogues of S1 (Carry), S2 (TSMOM), and S3 (Cross-Sectional Momentum). Signal logic, vol targeting, and risk framework are reused from the metals universe with minimal modification. This is a universe swap, not a new strategy family.

**Relationship to the metals universe.** The metals thesis is unchanged. The 5-year roadmap, Year 2 target of £50–100k at metals, Cape Town fund launch, and all validation gates remain in force. FX runs alongside metals indefinitely: pre-£50k it is the live book while metals paper-trades; post-£50k it continues as a permanent low-correlation diversifier.

---

## 2. Universe

Nine G10 pairs, all with deep tier-1 liquidity on IdealPro and well-documented factor behaviour in the academic literature:

| Pair | Convention | Role |
|------|------------|------|
| EUR/USD | Quoted | Benchmark-grade liquidity; largest pair globally |
| GBP/USD | Quoted | Core cross |
| USD/JPY | Quoted | Carry funder (historically low rate) |
| USD/CHF | Quoted | Safe-haven carry funder |
| AUD/USD | Quoted | Commodity carry beneficiary |
| USD/CAD | Quoted | Oil-linked |
| NZD/USD | Quoted | Highest carry historically |
| USD/SEK | Quoted | Scandi cross, cyclical |
| USD/NOK | Quoted | Oil-linked Scandi |

**Normalisation convention.** For ranking and signal construction, all pairs are transformed to "foreign vs USD" (so USD/JPY is flipped to JPY/USD internally, USD/CHF to CHF/USD, etc.). This gives 9 consistent "long foreign" series against a common base, which is essential for cross-sectional ranking.

**Why not include USD/MXN, USD/ZAR, USD/TRY?** Emerging-market FX has higher carry but carries political and liquidity tail risk that is incompatible with the vol-targeting framework. Reserve for post-£500k deployment with proper tail hedges.

**Why only 9 pairs?** Cross-sectional strategies need ≥6 names for a tercile sort to be statistically meaningful; 9 gives clean 3L/3S/3M portfolios. More names increase monitoring burden without materially improving Sharpe at this capital level.

---

## 3. Strategy Overview

| # | Strategy | Type | Sharpe (academic) | Rebalance | Hold | Key driver |
|---|----------|------|--------------------|-----------|------|------------|
| FX1 | FX Carry | Carry | 0.4–0.6 | Weekly | 5–20d | Interest rate differential |
| FX2 | FX TSMOM | Trend | 0.6–0.9 | Daily | 5–15d | Price persistence, slow diffusion |
| FX3 | FX XS Momentum | RelStr | 0.3–0.5 | Bi-weekly | 10–30d | Relative currency strength |

**Target portfolio Sharpe:** 0.8–1.0 IS, 0.5–0.7 OOS after costs.
**Target portfolio volatility:** 6–7% annualised (conservative relative to metals' 10%; stepped up to 10% only after 60+ live trading days meet Sharpe ≥ 0.50).
**Expected internal correlation:** ~0.10 between FX1 and FX2; ~0.30 between FX2 and FX3; ~0.05 between FX1 and FX3.

**Allocation:**

| Strategy | Capital weight | Target vol |
|----------|---------------|------------|
| FX1 Carry | 40% | 6% |
| FX2 TSMOM | 40% | 8% |
| FX3 XS Mom | 20% | 6% |

---

## 4. FX1 — Carry

**Signal type:** Cross-sectional carry (rate differential)
**Direction:** Long/short
**Holding period:** 5–20 trading days
**Rebalance:** Weekly (every Friday close, execute Monday open)
**Expected Sharpe:** 0.4–0.6 standalone; higher combined with TSMOM

### Thesis

Currencies of countries with higher short-term interest rates earn a positive carry relative to funding currencies. The forward rate is a biased predictor of the future spot rate — this is the forward premium puzzle, one of the most robust anomalies in empirical finance. The carry premium exists because of (a) crash risk in high-yielders (investors demand compensation for tail events like 2008, 2015 CHF, 2020), (b) time-varying risk aversion, and (c) the persistent demand for safe-haven currencies by institutional investors.

### Statistical evidence

Koijen, Moskowitz, Pedersen & Vrugt (2018) formalised carry as a unified concept across asset classes. In G10 FX specifically, a carry-sorted portfolio earned approximately 5% annualised with Sharpe ~0.55 over 1983–2012. Lustig, Roussanov & Verdelhan (2011) documented the HML-FX factor (long high-carry, short low-carry) with an annualised Sharpe of ~0.60 pre-costs over 1983–2009. Both papers note that most of the return comes from cross-sectional differentials, not time-series predictability.

### Signal construction

For each pair, compute the carry as the annualised forward-spot differential:

```
carry_i = ((F_i - S_i) / S_i) × (365 / days_to_expiry)
```

where F is the 3-month forward rate and S is spot. This is approximately equal to the interest rate differential. Forward points are available from IBKR or computed from FRED interest rate data (EFFR, ESTR, SONIA, TONAR, SARON, RBA cash rate, BoC overnight, RBNZ OCR, Riksbank policy, Norges policy).

Rank the 9 pairs by carry. Go long the top 3, short the bottom 3, skip the middle 3.

### Position sizing

Inverse-volatility targeting at 6% annualised portfolio vol:

```
notional_i = (target_vol × capital) / (σ_20d_i × √252 × N_positions)
```

where N_positions = 6 (3L + 3S) and σ is 20-day realised vol annualised. Minimum vol floor 4% (FX has lower realised vol than metals; floor prevents absurd sizing during carry-compression periods).

### Risk considerations

- **Carry unwind risk:** High-yielders can crash sharply (JPY carry unwind 2008; CHF de-peg 2015; AUD/JPY 2020). Size stops at 2.5× realised vol per position.
- **Rate regime sensitivity:** Carry strategies performed poorly in 2014–2019 when rate differentials compressed. Monitor the cross-sectional dispersion of carry — if it falls below 1% annualised across the universe, halve the strategy allocation.
- **Central bank intervention:** Norges Bank, SNB, and BoJ have intervened in FX markets. Flag positions in intervened currencies for manual review.

---

## 5. FX2 — Time-Series Momentum

**Signal type:** Time-series momentum / trend
**Direction:** Long/short per pair, independently
**Holding period:** 5–15 trading days (dynamic, until signal reversal)
**Rebalance:** Daily
**Expected Sharpe:** 0.6–0.9 standalone

### Thesis

Each FX pair exhibits persistence in its own return — positive returns tend to be followed by positive returns over horizons of days to months. This is the same behavioural phenomenon underpinning S2 on metals: initial underreaction to fundamentals (rate changes, trade flows, commodity shocks) followed by delayed overreaction as systematic flows reinforce the trend. FX trends are particularly well-documented because currency markets are deep, liquid, and populated by non-speculative hedgers (exporters, importers, reserve managers) who provide the structural supply of alpha.

### Statistical evidence

Moskowitz, Ooi & Pedersen (2012) tested TSMOM across 58 futures markets including 10 currency pairs over 1985–2009. Currency TSMOM delivered a Sharpe of approximately 0.95 at the 12-month lookback / 1-month hold configuration, and 0.7 at shorter (1-month lookback) configurations. Baltas & Kosowski (2013) confirmed that blending multiple lookback windows in FX produces superior risk-adjusted returns compared to any single lookback.

### Signal construction

Blended EMA crossover per pair, identical to S2 metals logic:

```
signal_i = sign( Σ_k w_k × (EMA_fast_k - EMA_slow_k) / σ_20d )
```

with three timeframe blends:
- Fast: 5 / 15
- Medium: 8 / 32
- Slow: 16 / 64

Equal weight across the three timeframes initially. Normalise by 20-day realised vol for dimensionless comparability.

### Position sizing

Inverse-volatility targeting at 8% annualised per-strategy vol. Scale position by |signal| capped at 1.0 (so a very strong signal gets full size, a weak signal gets partial size). This acts as a built-in filter for choppy/sideways markets.

```
notional_i = min(|signal_i|, 1.0) × (target_vol × capital) / (σ_20d_i × √252 × N_pairs)
```

### Risk considerations

- **Whipsaw risk:** Range-bound regimes produce frequent signal reversals and cost drag. The signal magnitude cap acts as a soft filter; consider a hard filter (|signal| > 0.3) in subsequent iterations if whipsaw proves material in live data.
- **Crisis alpha:** TSMOM has historically performed well during equity drawdowns (2008, 2020). This is a feature, not a bug — do not suppress it.
- **Central bank regime shifts:** Major policy pivots (Fed 2022, BoJ 2024) can produce sharp reversals. Same stop logic as FX1 (2.5× realised vol per position).

---

## 6. FX3 — Cross-Sectional Momentum

**Signal type:** Cross-sectional relative strength
**Direction:** Long/short
**Holding period:** 10–30 trading days
**Rebalance:** Bi-weekly (every other Friday)
**Expected Sharpe:** 0.3–0.5 standalone

### Thesis

Within the G10 universe, currencies exhibiting recent relative strength tend to continue outperforming weaker currencies over horizons of 1–3 months. This is the currency analogue of cross-sectional equity momentum and is driven by slow information diffusion across global FX markets — trade flows, terms-of-trade shocks, and monetary policy divergences propagate unevenly across central bank reaction functions and investor attention.

### Statistical evidence

Asness, Moskowitz & Pedersen (2013) documented cross-sectional momentum in currencies as part of their "Value and Momentum Everywhere" study, with a Sharpe of approximately 0.50 over 1979–2011 for a G10 momentum portfolio. Menkhoff, Sarno, Schmeling & Schrimpf (2012) confirmed currency momentum with a 6-month formation / 1-month hold at Sharpe 0.71 over 1976–2010, with returns concentrated in currencies with higher idiosyncratic risk.

### Signal construction

For each pair, compute the 60-day total return, excluding the most recent 5 days:

```
signal_i = Σ_{t=T-65}^{T-5} log(S_i,t / S_i,t-1)
```

The 5-day skip mitigates short-term mean-reversion effects (Jegadeesh 1990 was originally documented in equities but applies in FX too).

Rank the 9 pairs by signal. Long top 3, short bottom 3, skip middle 3.

### Position sizing

Inverse-volatility targeting at 6% annualised. Equal risk contribution across the 6 held positions.

### Risk considerations

- **Momentum crashes:** Sharp reversals after extended trends produce left-tail losses (GBP post-Brexit 2016, AUD during commodity turns). Same 2.5× stop logic.
- **Correlation spikes:** During global risk-off events, G10 FX pairs can become highly correlated (all currencies weaken vs USD). When average pairwise correlation > 0.70, halve the FX3 allocation — the strategy degenerates to a USD-directional bet under these conditions.
- **Limited universe:** 9 names is tight. Idiosyncratic shocks to a single currency can swing the portfolio. Consider expanding to 12 pairs (add EUR/GBP, EUR/JPY, AUD/JPY crosses) once the base strategy is validated.

---

## 7. Portfolio Construction

**Allocation (of FX capital):**

| Strategy | Capital | Target vol | Expected contribution |
|----------|---------|------------|------------------------|
| FX1 Carry | 40% | 6% | ~2.4% portfolio vol |
| FX2 TSMOM | 40% | 8% | ~3.2% portfolio vol |
| FX3 XS Mom | 20% | 6% | ~1.2% portfolio vol |

Assuming ~0.1–0.3 inter-strategy correlations, total portfolio vol is ~6–7% annualised.

**Capital ladder.** FX capital allocation scales with the live pot:

| Live capital | FX book size | Metals book size |
|--------------|--------------|------------------|
| £5k | 100% (£5k) | 0% (paper only) |
| £10–25k | 100% (all FX) | 0% (paper only) |
| £25–50k | ~50% (£15–25k) | 0% (paper only, still sub-threshold) |
| £50–100k | ~30% (£15–30k) | ~70% (£35–70k) |
| £100k+ | 20–25% (permanent diversifier) | 75–80% |

FX allocation becomes proportionally smaller as metals capital becomes viable, but never drops to zero — FX carry and TSMOM have low correlation to commodity carry and TSMOM and retain permanent diversification value.

---

## 8. Benchmark

**Primary benchmark:** Equal-weighted long basket of the 9 foreign-currency-vs-USD series.

Construction: normalise all pairs to "foreign/USD" direction. Go long 1/9 in each normalised pair. Compute daily return of this basket. This represents a passive USD-weakness trade against the G10 complex. The portfolio's excess return over this benchmark is the alpha attributable to active strategy selection (long/short tilts, timing, ranking).

**Secondary benchmarks for individual strategies:**
- FX1 Carry: HML-FX factor return (long high-carry 3, short low-carry 3, equal-weighted)
- FX2 TSMOM: zero (market-neutral after vol targeting; long/short each pair independently)
- FX3 XS Mom: zero (market-neutral cross-sectional)

Report excess returns over benchmark on all monthly reports.

---

## 9. Costs

Cost model for backtest and live validation:

| Component | Assumption | Source |
|-----------|-----------|--------|
| Spread | 0.3 bps (EUR/USD, USD/JPY), 0.5 bps (GBP/USD, USD/CHF), 0.7 bps (AUD/USD, USD/CAD), 1.0 bps (NZD/USD, USD/SEK, USD/NOK) | IBKR IdealPro published |
| Commission | 0.2 bps per side | IBKR IdealPro tier 1 |
| Slippage model | 0.5 bps per trade (beyond spread) | Conservative assumption |
| Financing | Overnight swap points via IBKR; embedded in P&L | IBKR daily rollover |

Round-trip cost per trade: ~1.2–2.5 bps depending on pair. This is an order of magnitude lower than metals futures (5–10 bps+) and is the primary reason FX works at small capital.

**Conservative adjustment for backtest:** multiply all costs by 1.5× to account for live-vs-backtest decay. Validate against live trading and adjust.

---

## 10. Validation Framework

All six gates from the Metals Backtesting & Validation Protocol apply unchanged:

| Gate | Stage | Pass criteria |
|------|-------|---------------|
| G0 | IS Backtest | IS Sharpe ≥ 0.50, t-stat ≥ 3.0, max DD ≤ 15%, parameter stability pass |
| G1 | Walk-Forward OOS | OOS Sharpe ≥ 0.30, WFER ≥ 0.50, PBO ≤ 0.20 |
| G2 | Holdout Validation | Holdout Sharpe ≥ 0.20, consistent with OOS ±30% |
| G3 | Paper Trading | Min 60 trading days, paper Sharpe within 50% of OOS |
| G4 | Live Ramp | 60 days at 25% → 100% of target size |

**IS/OOS split:** IS = 2005–2017, OOS = 2018–present. Holdout = last 12 months rolling.

**Stress test scenarios for FX:**
- GBP Brexit referendum (Jun 2016)
- CHF de-peg (Jan 2015)
- JPY carry unwind (Aug 2015; Aug 2024)
- COVID dash for dollar (Mar 2020)
- Fed pivot / USD rally (Sep–Oct 2022)

Each scenario: verify no single-day loss > 2% of NAV and max drawdown through scenario ≤ 8%.

---

## 11. References

Asness, C., Moskowitz, T., & Pedersen, L. (2013). "Value and Momentum Everywhere." *Journal of Finance*, 68(3), 929–985.

Baltas, N., & Kosowski, R. (2013). "Momentum Strategies in Futures Markets and Trend-Following Funds." Working Paper, Imperial College London.

Jegadeesh, N. (1990). "Evidence of Predictable Behavior of Security Returns." *Journal of Finance*, 45(3), 881–898.

Koijen, R., Moskowitz, T., Pedersen, L., & Vrugt, E. (2018). "Carry." *Journal of Financial Economics*, 127(2), 197–225.

Lustig, H., Roussanov, N., & Verdelhan, A. (2011). "Common Risk Factors in Currency Markets." *Review of Financial Studies*, 24(11), 3731–3777.

Menkhoff, L., Sarno, L., Schmeling, M., & Schrimpf, A. (2012). "Currency Momentum Strategies." *Journal of Financial Economics*, 106(3), 660–684.

Moskowitz, T., Ooi, Y., & Pedersen, L. (2012). "Time Series Momentum." *Journal of Financial Economics*, 104(2), 228–250.

---

## 12. Open Decisions & Watch Items

- Awaiting IBKR response on live account permissions and LME minimum capital
- Interest rate ingestion pipeline: FRED for USD/EUR/GBP/JPY/CHF/AUD/CAD/NZD/SEK/NOK short rates
- FX historical data: FRED daily close for backtest; IBKR historical API for paper/live
- Benchmark calculation: implement equal-weighted normalised-direction basket in backtest engine
- Module architecture: asset-class-agnostic refactor ahead of FX implementation (see pivot memo)
- Intervention monitoring: hook to flag SNB, BoJ, Norges Bank interventions before ingesting signals

---

*This document lives alongside `Metals_Systematic_Trading_Strategies.pdf` as the second strategy universe spec for WorFIn. It is a living document — update as live data refines cost assumptions and strategy parameters.*
