# WorFIn — Worthington-Fitnum Investments
## Systematic Metals Trading System

Private systematic trading system for LME base metals and COMEX precious metals.
Six statistically-validated strategies. Daily frequency. Python + PostgreSQL + Interactive Brokers.

---

## Strategy Universe

| ID | Strategy | Type | Target Sharpe | Allocation | Status |
|----|----------|------|--------------|------------|--------|
| S4 | Basis-Momentum | Composite | 0.6–1.0 | 25% | 🔴 Building |
| S1 | Term Structure Carry | Carry | 0.4–0.8 | 20% | 🔴 Pending |
| S2 | Time-Series Momentum | Trend | 0.5–1.0 | 20% | 🔴 Pending |
| S3 | Cross-Sectional Momentum | Rel. Strength | 0.3–0.6 | 15% | 🔴 Pending |
| S5 | Inventory Surprise | Event | 0.3–0.5 | 10% | 🔴 Pending |
| S6 | Inter-Metal Spreads | Stat. Arb | 0.4–0.7 | 10% | 🔴 Pending |

---

## Project Structure

```
worfin_strategies/
├── src/worfin/          # Main package
│   ├── config/          # Settings, metal specs, exchange calendars
│   ├── data/            # Ingestion + pipeline
│   ├── strategies/      # Signal generation (S1–S6)
│   ├── risk/            # Limits, sizing, circuit breakers
│   ├── backtest/        # Walk-forward engine, metrics, cost model
│   ├── execution/       # Pre-trade checks, order management
│   └── monitoring/      # Alerts, reconciliation, reporting
├── tests/               # pytest test suite (40 tests, 0 failures)
├── alembic/             # Database migrations
├── scripts/             # fetch_historical.py, run_backtest.py
├── .env.example         # Environment variable template
└── pyproject.toml       # Build + tool configuration
```

---

## Quick Start

```bash
# 1. Clone and install
git clone git@github.com:timwfitnum/worfin_strategies.git
cd worfin_strategies
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"

# 2. Configure environment
cp .env.example .env
# Edit .env with your API keys and database credentials

# 3. Set up database
createdb worfin
alembic upgrade head

# 4. Fetch historical data (2005–2022)
python scripts/fetch_historical.py

# 5. Run tests
pytest tests/ -v
```

---

## Current Phase

**Tier 0 → Tier 1:** Backtesting infrastructure and paper trading.

Validated alpha before capital deployment. No live orders until all six
strategies pass the full graduation pipeline: IS → Walk-Forward OOS →
Holdout → Paper Trading (60 days) → Live Ramp.

---

## Tech Stack

Python 3.11 · PostgreSQL 18 · SQLAlchemy 2.0 · Alembic · ib_insync ·
vectorbt · pandas · numpy · scipy · arch · pytest

---

*Confidential — not for distribution.*
