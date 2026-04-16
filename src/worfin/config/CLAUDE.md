# CLAUDE.md — /config
# Configuration Layer | Part of: WorFIn Systematic Trading System

---

## SCOPE

This directory defines the immutable constants and configuration that the
entire system builds on. Changes here have system-wide consequences.

```
config/
├── settings.py   ← All env/secret config via pydantic-settings + .env
├── metals.py     ← All 10 contract specs as frozen dataclasses (constants)
└── calendar.py   ← LME + COMEX exchange holiday calendars + DTE utilities
```

---

## CRITICAL: LME DAILY PROMPT DATE SYSTEM

LME metals do NOT use monthly expiry like COMEX. Every business day, the
3-Month prompt date rolls forward. This means:

- **Cash = T+2 business days**
- **3-Month = business day closest to 3 calendar months from Cash settle**
- **DTE changes every single day** — even if prices are unchanged

This is implemented in `calendar.py` via `get_lme_3m_dte(trade_date)`.

**NEVER use a fixed 91-day assumption for LME carry calculations.**
The strategy CLAUDE.md and signals code must always call `get_lme_3m_dte()`.
Using 91 days introduces a systematic error in the carry signal that is
small on any given day but compounds over time in backtests.

---

## METALS UNIVERSE

All 10 metal contract specs live in `metals.py` as frozen `MetalSpec` dataclasses.
They are CONSTANTS — never modify at runtime.

| Ticker | Metal | Exchange | Tier | Key Risk |
|--------|-------|----------|------|----------|
| CA | Copper | LME | 1 | Most liquid LME metal |
| AH | Aluminium | LME | 1 | Electricity price exposure |
| ZS | Zinc | LME | 1 | — |
| NI | Nickel | LME | 2 | ⚠️ March 2022 — LME cancelled trades |
| PB | Lead | LME | 2 | — |
| SN | Tin | LME | 3 | ⚠️ Very thin book (3–5 lots/side) |
| GC | Gold | COMEX | 1 | Most liquid precious metal |
| SI | Silver | COMEX | 1 | — |
| PL | Platinum | COMEX | 2 | Roll early — thinner than Pd |
| PA | Palladium | COMEX | 3 | ⚠️ Pt/Pd cointegration broke 2017–2021 |

**IBKR routing:** LME metals accessed via IBKR as exchange = "ICEEU".
Always verify contract specs with IBKR before placing first live order.

---

## SETTINGS PATTERN

All configuration flows through `settings.py` via pydantic-settings.
Never access `os.environ` directly anywhere in the codebase.

```python
# Correct
from worfin.config.settings import get_settings
settings = get_settings()
db_url = settings.database_url

# Wrong — never do this
import os
db_url = os.environ["DATABASE_URL"]
```

`get_settings()` is a cached singleton — safe to call anywhere.

---

## WHAT I SHOULD FLAG

When working in this directory:
- Any attempt to hardcode API keys, connection strings, or credentials
- Use of fixed 91-day DTE for LME carry (must use `get_lme_3m_dte()`)
- Any modification to `MetalSpec` values at runtime (they are frozen)
- Direct `os.environ` access anywhere in the codebase
- Missing holiday in the exchange calendar (check LME/CME sites each December)
