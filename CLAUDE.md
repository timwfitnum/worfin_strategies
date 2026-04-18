# CLAUDE.md — Metals Systematic Trading System
# Master Project Brief | Updated: March 2026
# Tim — CIO/CTO | Systematic Commodity Trading

---

## 🎯 PROJECT IDENTITY

We are building a **production-grade systematic trading system** for metals futures.
Six statistically-validated strategies across LME base metals and COMEX precious metals.
Daily holding periods. Fully automated signal generation. Interactive Brokers execution.
This is a real trading business — every line of code has financial consequences.

**5-Year Roadmap:**
- **Now (Tier 0 → 1):** Backtesting infrastructure + paper trading
- **Year 2:** Live capital £50–100k personal
- **Year 4:** Fund launch £2–5m external AUM
- **Year 5:** Cape Town, £10–20m AUM, 3–4 person firm

---

## 🧠 CORE PHILOSOPHY — READ THIS FIRST

1. **Risk management IS the strategy.** Never treat risk as a constraint bolted onto alpha — it IS the central organising principle.
2. **A backtest is a hypothesis, not a result.** Only out-of-sample and live paper trading confirm edge.
3. **Overfitting is the #1 threat.** More parameters = more danger. Simpler is better.
4. **Infrastructure before capital.** Nothing goes live until paper-tested for 8–12 weeks minimum.
5. **One strategy done properly beats five done poorly.** S4 (Basis-Momentum) is the core. Build it first.
6. **Never size by conviction. Size by volatility.** Always inverse-vol targeting.
7. **The kill switch must always work.** Every component must be stoppable in <60 seconds.

---

## 📐 CURRENT PHASE & ACTIVE WORK

**Phase:** Tier 0 → Tier 1 transition
**Active build:** Data Layer → Backtest Engine → Execution Engine → Risk Monitor → Reconciliation
**Paper trading target:** 8–12 weeks before any live capital
**Priority order:** S4 Basis-Momentum first → S1 Carry → S2 TSMOM → S3 XS Mom → S5 Inventory → S6 Pairs

**When I ask you to build something, always check:**
- Does this serve the current phase (backtesting/paper trading)?
- Is it the simplest implementation that works?
- Does it respect all risk limits defined below?
- Is it testable and auditable?

---

## 🏗️ TECH STACK

| Component | Choice | Notes |
|-----------|--------|-------|
| Language | Python 3.11+ | Primary for all components |
| Database | PostgreSQL 18 | Run locally for dev; migrate to VPS for live |
| ORM | SQLAlchemy 2.0 | Async-capable; use for all DB interaction |
| Migrations | Alembic | All schema changes versioned — never raw ALTER TABLE |
| Broker API | ib_insync | IBKR connectivity; IB Gateway (not TWS) in production |
| Backtesting | vectorbt | Fast vectorised; custom event-driven layer on top |
| Data | pandas + numpy | Standard; use polars for large datasets if needed |
| Stats | scipy + statsmodels | ADF tests, cointegration, regression, GARCH |
| Vol modelling | arch | GARCH volatility estimation |
| Metrics | empyrical | Sharpe, Sortino, Calmar, drawdown |
| Scheduling | schedule (dev) → APScheduler (prod) → Airflow (Tier 2+) |
| Monitoring | Telegram bot + structured logging | JSON logs to file; alerts to Telegram |
| Testing | pytest + pytest-asyncio | All strategy logic must have unit tests |
| Environment | pyenv + venv | Never use conda |
| Secrets | python-dotenv (.env file) | Never hardcode. Never commit .env |
| Version control | Git + GitHub (private) | Feature branches; no direct commits to main |

---

## 🗄️ DATABASE SCHEMA — CANONICAL STRUCTURE

```sql
-- LAYER 1: Raw ingest (immutable — never modify after insert)
schema: raw_data
  tables: lme_prices, comex_prices, lme_inventory, cftc_cot, macro_indicators

-- LAYER 2: Clean, normalised, roll-adjusted
schema: clean_data
  tables: futures_prices, continuous_series, realised_vol, term_structure

-- LAYER 3: Computed signals
schema: signals
  tables: carry_signals, momentum_signals, basis_signals, inventory_signals, pairs_signals

-- LAYER 4: Portfolio & execution
schema: positions
  tables: target_positions, current_positions, position_history

schema: orders
  tables: order_log, fill_log, execution_quality

-- LAYER 5: Audit & monitoring
schema: audit
  tables: system_events, data_quality_flags, reconciliation_log, risk_breaches
```

**Rules:**
- Never query across schemas in strategy code — use views
- raw_data is append-only, never update or delete
- Every table has created_at and updated_at timestamps
- All prices stored in USD; GBP conversion applied at portfolio level only

---

## 📊 THE SIX STRATEGIES — QUICK REFERENCE

| ID | Name | Type | Sharpe | Hold | Allocation | Status |
|----|------|------|--------|------|------------|--------|
| S1 | Term Structure Carry | Carry | 0.4–0.8 | 5–20d | 20% | Build 2nd |
| S2 | Time-Series Momentum | Trend | 0.5–1.0 | 5–15d | 20% | Build 3rd |
| S3 | Cross-Sectional Momentum | RelStr | 0.3–0.6 | 10–30d | 15% | Build 4th |
| S4 | Basis-Momentum | Composite | 0.6–1.0 | 10–20d | 25% | **Build FIRST** |
| S5 | Inventory Surprise | Event | 0.3–0.5 | 3–10d | 10% | Build 5th |
| S6 | Inter-Metal Spreads | StatArb | 0.4–0.7 | 5–20d | 10% | Build 6th |

**Universe:**
- LME Base Metals: Copper (CA), Aluminium (AH), Zinc (ZS), Nickel (NI), Lead (PB), Tin (SN)
- COMEX Precious: Gold (GC), Silver (SI), Platinum (PL), Palladium (PA)

---

## ⚖️ RISK MANAGEMENT — HARD LIMITS (NEVER BYPASS THESE)

### Position Sizing (MANDATORY for every trade)
```
Position_Notional = (Target_Vol × Capital_Allocation) / (σ_asset × √252)

Where:
  Target_Vol = 0.10 (10% annualised per position)
  σ_asset = max(trailing_20d_realised_vol, 0.10)  ← FLOOR at 10%, always
  Robustness cap: never exceed what 60d vol estimate would produce
```

### Hard Position Limits
| Limit | Threshold | Action |
|-------|-----------|--------|
| Max single-metal notional | 20% of NAV | Reject order |
| Max single-strategy gross | 40% of NAV | Cap & reduce |
| Max portfolio gross | 250% of NAV | Scale all down |
| Max portfolio net | 80% of NAV | Hedge or reduce |
| Min position notional | £5,000 | Don't enter |

### Strategy Risk Budgets
| Strategy | Target Vol | Max Drawdown Budget |
|----------|-----------|-------------------|
| S1 Carry | 8–10% | 12% |
| S2 TSMOM | 10–12% | 15% |
| S3 XS Mom | 8–10% | 12% |
| S4 Basis-Mom | 10–12% | 15% |
| S5 Inventory | 6–8% | 10% |
| S6 Pairs | 6–8% | 10% |

### Portfolio Circuit Breakers (AUTOMATED, CANNOT BE OVERRIDDEN)
| Trigger | Threshold | Action |
|---------|-----------|--------|
| Daily loss | 2.0% NAV | Flatten ALL positions immediately |
| Weekly loss | 3.5% NAV | Reduce to 50%, no new entries |
| Monthly drawdown | 5.0% from month-start | Reduce to 25% |
| Peak drawdown | 10.0% from all-time HWM | Full suspension, formal review |
| Hard stop | 15.0% from HWM | Full liquidation, system shutdown |

### Liquidity Tiers
| Tier | Metals | Liquidity Factor | Max % of ADV |
|------|--------|-----------------|-------------|
| 1 (High) | Cu, Al, Zn, Au, Ag | 1.0× | 5% |
| 2 (Medium) | Ni, Pb, Pt | 0.75× | 3% |
| 3 (Low) | Sn, Pd | 0.50× | 2% |

---

## 📡 DATA SOURCES

| Source | What | Cost | Priority |
|--------|------|------|----------|
| Nasdaq Data Link | Continuous futures (LME+COMEX) | Free tier | Primary for backtest |
| yfinance | COMEX precious metals | Free | Supplement |
| LME.com | Official settlement prices + inventory | Free (next-day) | Primary for LME |
| IBKR TWS API | Live prices + execution | Free with account | Live trading |
| CFTC.gov + cot_reports | COT positioning | Free | Optional signal |
| FRED API | US macro (ISM PMI, DXY) | Free | Optional filter |
| SHFE | China inventory cross-reference | Free | S5 validation |

**Data engineering rules:**
- raw_data schema: store exactly what the source provides, never transform
- Always validate: staleness check (>1 business day = alert), outlier check (>4σ = flag & investigate)
- Cross-validate prices across sources when possible (>0.5% discrepancy = alert)
- Maintain a holiday calendar (LME + COMEX + UK bank holidays)

---

## 🔄 BACKTESTING RULES — NON-NEGOTIABLE

### Data Splits (FIXED — never adjust retroactively)
| Segment | Period | Purpose |
|---------|--------|---------|
| In-Sample (IS) | Jan 2005 – Dec 2017 | Signal discovery & parameter selection |
| Out-of-Sample (OOS) | Jan 2018 – Dec 2022 | Strategy validation (single-use) |
| Holdout | Jan 2023 – present | Final go/no-go only |

### Minimum Performance Thresholds
| Metric | IS Minimum | OOS Minimum |
|--------|-----------|-------------|
| Sharpe Ratio | ≥ 0.50 (net of costs) | ≥ 0.30 (net of costs) |
| Max Drawdown | ≤ 20% | ≤ 15% |
| t-statistic | ≥ 3.0 | ≥ 2.0 |
| Walk-Forward Efficiency (WFER) | — | ≥ 0.50 |
| Probability of Backtest Overfitting (PBO) | — | ≤ 0.20 |
| Calmar Ratio | — | ≥ 0.50 |

### Transaction Cost Model (ALWAYS include — no exceptions)
| Metal | Round-trip cost |
|-------|----------------|
| Cu, Al (LME Tier 1) | 3–5 bps |
| Sn (LME Tier 3) | 15–25 bps |
| Au (COMEX) | 2–4 bps |
| All costs | Budget 30–50% HIGHER than initial estimates |

### Strategy Graduation Gates
```
G0: IS Backtest → G1: Walk-Forward OOS → G2: Holdout → G3: Paper (60d) → G4: Live Ramp (60d)
```
No strategy receives live capital without passing all 5 gates.

---

## 🚀 EXECUTION RULES

**Exchange timing:**
- LME Base Metals: Execute 14:00–16:00 London (deepest liquidity)
- COMEX Precious: Execute 14:00–16:00 London (London/NY overlap)
- S1/S4: Execute AFTER LME official prices published (~13:30 London)
- S5: Execute 30min AFTER LME inventory report (~09:30 London)

**Order management (always follow this sequence):**
1. Passive limit at mid-price (60 seconds)
2. Aggressive limit at best bid/offer (60 seconds)
3. Market order fallback (should be <10% of executions)

**Pre-trade checks (MANDATORY before every order):**
- Order within position limits
- Won't breach gross/net exposure limits
- Within liquidity tier maximum
- Price within 2% of current mid (fat-finger protection)
- Daily order count within maximum
- Direction matches signal (prevents sign errors)

---

## 📁 PROJECT STRUCTURE

```
metals-trading/
├── CLAUDE.md                    ← This file (root)
├── .env                         ← Secrets (never commit)
├── .env.example                 ← Template (commit this)
├── requirements.txt
├── alembic/                     ← DB migrations
│   └── versions/
├── config/
│   ├── settings.py              ← All config via pydantic-settings
│   ├── metals.py                ← Contract specs, lot sizes, tick values
│   └── calendar.py              ← LME + COMEX holiday calendar
├── data/
│   ├── CLAUDE.md                ← Data-specific context
│   ├── ingestion/               ← Pull from sources
│   ├── pipeline/                ← Clean, roll-adjust, validate
│   └── sources/                 ← Per-source connectors
├── strategies/
│   ├── CLAUDE.md                ← Strategy-specific context
│   ├── base.py                  ← Abstract base strategy class
│   ├── s1_carry.py
│   ├── s2_tsmom.py
│   ├── s3_xsmom.py
│   ├── s4_basis_momentum.py     ← Build first
│   ├── s5_inventory.py
│   └── s6_pairs.py
├── risk/
│   ├── CLAUDE.md                ← Risk-specific context
│   ├── limits.py                ← All hard limits defined here
│   ├── sizing.py                ← Vol targeting, position sizing
│   ├── monitor.py               ← Real-time risk monitoring
│   └── circuit_breakers.py     ← Automated drawdown triggers
├── execution/
│   ├── CLAUDE.md                ← Execution-specific context
│   ├── engine.py                ← Core execution loop
│   ├── orders.py                ← Order management
│   ├── broker/
│   │   └── ibkr.py              ← IB Gateway connector
│   └── pretrade_checks.py       ← All pre-trade validations
├── backtest/
│   ├── CLAUDE.md                ← Backtesting-specific context
│   ├── engine.py                ← Walk-forward backtester
│   ├── metrics.py               ← All performance metrics
│   ├── costs.py                 ← Transaction cost model
│   └── validation.py           ← WFER, PBO, stress tests
├── monitoring/
│   ├── alerts.py                ← Telegram + email alerts
│   ├── reconciliation.py        ← Daily position reconciliation
│   ├── data_quality.py          ← Staleness, outlier detection
│   └── reporting.py             ← Daily P&L report generation
└── tests/
    ├── test_strategies/
    ├── test_risk/
    ├── test_execution/
    └── test_data/
```

---

## 🔑 CODING STANDARDS

**General:**
- Type hints on ALL functions — no exceptions
- Docstrings on all public functions and classes
- No magic numbers — all constants in `config/`
- All monetary values as `Decimal`, never `float`
- All dates as `datetime.date`, timezone-aware where needed (always UTC internally)

**Strategy code specifically:**
- Every strategy inherits from `BaseStrategy`
- Signals return values in range [-1, +1] (normalised z-score)
- Position sizing happens in `risk/sizing.py`, NEVER inside strategy code
- Strategies are stateless — they take data in, return signals out
- No `print()` statements — use `logging` module throughout

**Risk code specifically:**
- Hard limits in `risk/limits.py` are constants — never modify at runtime
- Circuit breakers run as a SEPARATE process from the signal engine
- Every risk breach is logged to `audit.risk_breaches` with full context
- Kill switch must work even if the main execution process is hung

**Testing:**
- Unit tests for every signal calculation
- Property-based tests for position sizing (always within limits)
- Integration tests for the full backtest pipeline
- Never mock the risk limits — test against real limits

---

## ⚠️ THINGS CLAUDE SHOULD NEVER DO

1. **Never bypass risk limits** — not even "just for testing"
2. **Never hardcode API keys, passwords, or credentials**
3. **Never modify the OOS/Holdout data splits** after they're set
4. **Never optimise parameters on OOS data** — it becomes contaminated IS data
5. **Never submit live orders without all pre-trade checks passing**
6. **Never assume a signal is correct** — validate data quality first
7. **Never delete from raw_data** — it's immutable
8. **Never add complexity** that the current phase doesn't require
9. **Never skip transaction costs** in backtests — "fiction without them"
10. **Never use `float`** for monetary calculations

---

## 📚 KEY REFERENCES (embedded in project docs)

- Gorton & Rouwenhorst (2006) — carry premium evidence
- Moskowitz, Ooi & Pedersen (2012) — time-series momentum
- Bakshi, Gao & Rossi (2019) — basis-momentum (t-stat 4.14)
- Harvey, Liu & Zhu (2016) — multiple testing, t-stat ≥ 3.0 threshold
- de Prado (2018) — CPCV, PBO methodology
- Figuerola-Ferretti & Gonzalo (2010) — LME cointegration

---

## 🧑‍💻 ABOUT THE DEVELOPER

- **Tim** — CIO/CTO. Front-office metals support background (Macquarie).
  ARPM-trained. Production engineering experience.
- Building this part-time (10–15 hours/week evenings and weekends)
- Solo until 2029 — every milestone must be achievable by one person
- **James** joins 2029 as COO/Head of Capital — investor relations, fundraising
- **Jared** — legal advisor (corporate structuring, FSCA)
- Target HQ: Cape Town by 2031

**Working style preferences:**
- Favour simple, correct code over clever, complex code
- Always explain the "why" behind implementation choices
- Flag when I'm about to build something the current phase doesn't need
- If something is risky or could lose money, say so explicitly
- When uncertain between two approaches, show both with trade-offs

---

*This file is the single source of truth for project context.
Update it whenever the phase, stack, or priorities change.*
