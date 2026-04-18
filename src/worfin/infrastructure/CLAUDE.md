# CLAUDE.md — /infrastructure
# Infrastructure & DevOps | Part of: Metals Systematic Trading System

---

## CURRENT TIER: 0 → 1

**Tier 0 (now):** Local dev on PC + Laptop. Research and backtesting only. No live capital.
**Tier 1 (Year 1):** VPS + paper trading + small live deployment.
**Tier 2 (Year 1–3):** Cloud-native production with redundancy.

**Core principle:** For daily-frequency strategies, the bottleneck is NEVER compute speed.
It is data quality, system reliability, and operational discipline.
A Raspberry Pi running flawlessly beats a £50k colocation that crashes once a month.

---

## LOCAL DEV SETUP (PC ↔ LAPTOP SYNC)

### Sync strategy
```
GitHub Private Repo (source of truth for ALL code)
         ↕                        ↕
      Your PC               Your Laptop
  (primary dev)           (secondary dev)
       |                        |
Local PostgreSQL           Local PostgreSQL
(same schema, full data) (same schema, subset data)
```

### What syncs how
| Artefact | Sync method |
|----------|-------------|
| Code | Git push/pull |
| DB schema | Alembic migrations (committed to Git) |
| DB data | pg_dump from PC → restore on laptop |
| Secrets | .env file — copy manually or use 1Password |
| Dependencies | pip install -r requirements.txt after pull |
| VS Code settings | VS Code Settings Sync (GitHub account) |

---

## VPS SPECIFICATION (Tier 1)

**Recommended: Hetzner Cloud CX22 (€5/month)**
- 2 vCPU, 4GB RAM, 40GB NVMe SSD
- Ubuntu 22.04 LTS
- Location: Falkenstein (DE) or Helsinki (FI)
- ~15ms ping to Interactive Brokers (irrelevant for daily strategies)

**Why Hetzner:** Best price-performance in Europe. Tier III equivalent datacentre.
Spending £30–60/month on a "trading VPS" buys nothing except a nicer marketing page.

**Architecture on VPS:**
```
VPS
├── IB Gateway (headless, port 4001 live / 4002 paper)
├── PostgreSQL 18 (production DB)
├── Python signal engine (cron/systemd)
├── Python execution engine (cron/systemd)
├── Risk monitor (systemd, separate process)
└── Telegram alerting bot
```

---

## ENVIRONMENT SETUP (RUN ON BOTH MACHINES)

```bash
# 1. Install pyenv
curl https://pyenv.run | bash
# Add to ~/.bashrc or ~/.zshrc:
export PATH="$HOME/.pyenv/bin:$PATH"
eval "$(pyenv init -)"

# 2. Install Python
pyenv install 3.11.9
pyenv global 3.11.9

# 3. Clone repo and set up venv
git clone git@github.com:YOUR_USERNAME/metals-trading.git
cd metals-trading
pyenv local 3.11.9
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
# .venv\Scripts\activate   # Windows

# 4. Install dependencies
pip install -r requirements.txt

# 5. Set up .env (copy from .env.example, fill in secrets)
cp .env.example .env
# Edit .env with your actual values

# 6. Set up PostgreSQL
createdb metals_trading
alembic upgrade head  # runs all migrations
```

---

## REPOSITORY STRUCTURE

```
metals-trading/
├── .github/
│   └── workflows/
│       └── tests.yml        ← CI: run tests on every push
├── CLAUDE.md                ← Root project brief (this system)
├── .env                     ← Secrets (NEVER commit)
├── .env.example             ← Template (commit this)
├── .gitignore
├── requirements.txt
├── requirements-dev.txt     ← pytest, black, ruff, mypy
├── pyproject.toml           ← Tool configuration
├── alembic.ini
├── alembic/
│   └── versions/            ← All DB migrations versioned
├── config/
│   ├── settings.py          ← Pydantic-settings, reads .env
│   ├── metals.py            ← Contract specs constants
│   └── calendar.py          ← Exchange holiday calendars
├── data/
│   ├── CLAUDE.md
│   ├── ingestion/
│   ├── pipeline/
│   └── sources/
├── strategies/
│   ├── CLAUDE.md
│   └── [strategy files]
├── risk/
│   ├── CLAUDE.md
│   └── [risk files]
├── execution/
│   ├── CLAUDE.md
│   └── [execution files]
├── backtest/
│   ├── CLAUDE.md
│   └── [backtest files]
├── monitoring/
│   ├── CLAUDE.md
│   └── [monitoring files]
└── tests/
    ├── conftest.py
    ├── test_strategies/
    ├── test_risk/
    ├── test_execution/
    └── test_data/
```

---

## .env TEMPLATE

```bash
# .env.example — commit this, not .env

# Database
DB_HOST=localhost
DB_PORT=5432
DB_NAME=metals_trading
DB_USER=metals_user
DB_PASSWORD=CHANGE_ME

# Interactive Brokers
IBKR_HOST=127.0.0.1
IBKR_PORT_LIVE=4001
IBKR_PORT_PAPER=4002
IBKR_CLIENT_ID=1
IBKR_ACCOUNT_ID=CHANGE_ME

# Data Sources
NASDAQ_DATA_LINK_API_KEY=CHANGE_ME
FRED_API_KEY=CHANGE_ME

# Monitoring
TELEGRAM_BOT_TOKEN=CHANGE_ME
TELEGRAM_CHAT_ID=CHANGE_ME
ALERT_EMAIL_FROM=CHANGE_ME
ALERT_EMAIL_TO=CHANGE_ME
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_PASSWORD=CHANGE_ME

# Environment
ENVIRONMENT=development  # development | paper | live
LOG_LEVEL=INFO
```

---

## .gitignore

```
# Environment
.env
.venv/
*.pyc
__pycache__/
.pytest_cache/
.mypy_cache/

# Data (too large for git)
data/raw/
data/downloads/
*.parquet
*.csv

# Database
*.db
*.sqlite

# OS
.DS_Store
Thumbs.db

# IDE
.vscode/settings.json  # sync this via VS Code Settings Sync instead
*.swp

# Logs
*.log
logs/

# Secrets (belt AND suspenders)
*.pem
*.key
secrets/
```

---

## pyproject.toml

```toml
[tool.black]
line-length = 100
target-version = ['py311']

[tool.ruff]
line-length = 100
select = ["E", "F", "I", "N", "W", "UP"]
ignore = []

[tool.mypy]
python_version = "3.11"
strict = true
ignore_missing_imports = true

[tool.pytest.ini_options]
testpaths = ["tests"]
asyncio_mode = "auto"
```

---

## GITHUB ACTIONS CI (tests.yml)

```yaml
name: Tests
on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    services:
      postgres:
        image: postgres:16
        env:
          POSTGRES_DB: metals_trading_test
          POSTGRES_USER: test_user
          POSTGRES_PASSWORD: test_password
        ports: ['5432:5432']

    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v4
        with:
          python-version: '3.11'
      - run: pip install -r requirements.txt -r requirements-dev.txt
      - run: pytest tests/ -v --tb=short
      - run: mypy .
      - run: ruff check .
```

---

## PROCESS MANAGEMENT (Tier 1 VPS)

```bash
# Use systemd to manage all services
# Auto-restarts on crash; logs to journald

# Example: /etc/systemd/system/metals-signal-engine.service
[Unit]
Description=Metals Trading Signal Engine
After=postgresql.service

[Service]
User=metals
WorkingDirectory=/home/metals/metals-trading
Environment=PYTHONPATH=/home/metals/metals-trading
ExecStart=/home/metals/.venv/bin/python -m monitoring.scheduler
Restart=always
RestartSec=30
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

---

## BACKUP STRATEGY

```bash
# Daily database backup (cron at 18:30 UTC)
# 0 18 * * 1-5 /home/metals/scripts/backup.sh

#!/bin/bash
DATE=$(date +%Y%m%d_%H%M%S)
pg_dump metals_trading | gzip > /backups/metals_trading_$DATE.sql.gz

# Upload to Backblaze B2 (~£1/month for 100GB)
# b2 upload-file metals-backups metals_trading_$DATE.sql.gz

# Keep last 30 days locally, 90 days in B2
find /backups -name "*.sql.gz" -mtime +30 -delete
```

---

## CHANGE MANAGEMENT PROCESS

Every production change follows this process:
1. Code change on feature branch
2. Peer review (or self-review with 24-hour cooling period for solo)
3. Deploy to staging → validate against yesterday's data
4. Deploy to production
5. Post-deployment monitoring for 24 hours

**No changes to production during trading hours** except emergency fixes
(which require the kill switch to be armed first).

---

## WHAT I SHOULD FLAG

When working in this directory:
- Any secret or credential appearing in code (should be in .env only)
- Skipping the staging step before production deployment
- Making changes during trading hours without arming kill switch
- Not pinning dependency versions in requirements.txt
- Missing tests in CI pipeline for critical risk code
- Backup not being tested (untested backup = no backup)
