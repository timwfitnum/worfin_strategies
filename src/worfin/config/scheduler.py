"""
config/scheduler.py
Frequency-agnostic scheduler configuration.

INTRADAY-READY DESIGN (Decision 5):
  Adding a new strategy at any frequency = one new entry in STRATEGY_SCHEDULES.
  No changes to the scheduler engine, execution engine, or any other code.

  Daily strategies: run once at a specific time each trading day
  Swing strategies: run at open and close of each session
  Intraday strategies: run every N minutes during trading hours

Current entries are all daily. Future intraday entries are shown as comments
to illustrate exactly how simple adding them will be.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from worfin.strategies.base import BarSize, Frequency


@dataclass(frozen=True)
class StrategySchedule:
    """
    Defines when and how often a strategy's signals are computed.

    For daily strategies: run_time_utc is the daily trigger time.
    For intraday strategies: interval_minutes is the recompute frequency.
    The scheduler engine reads these and routes accordingly — no hardcoding.
    """
    strategy_id: str
    frequency: str                      # Frequency.DAILY | INTRADAY | HOURLY etc.
    bar_size: str                       # BarSize.DAILY | HOURLY | MIN_5 etc.
    run_time_utc: Optional[str] = None  # "13:30" for daily strategies
    interval_minutes: Optional[int] = None  # For intraday: recompute every N minutes
    trading_days_only: bool = True      # Skip weekends and exchange holidays
    min_data_bars: int = 70             # Minimum bars before strategy can run
    notes: str = ""


# ─────────────────────────────────────────────────────────────────────────────
# CURRENT STRATEGIES — ALL DAILY FREQUENCY
# ─────────────────────────────────────────────────────────────────────────────
# Run after LME Ring close (13:30 UTC) so official settlement prices are used.
# Signals computed at 13:30, execution window opens 14:00–16:00 London.

STRATEGY_SCHEDULES: dict[str, StrategySchedule] = {

    "S4": StrategySchedule(
        strategy_id="S4",
        frequency=Frequency.DAILY,
        bar_size=BarSize.DAILY,
        run_time_utc="13:30",
        notes="Basis-Momentum. Run after LME Ring close. Bi-weekly rebalance.",
    ),

    "S1": StrategySchedule(
        strategy_id="S1",
        frequency=Frequency.DAILY,
        bar_size=BarSize.DAILY,
        run_time_utc="13:30",
        notes="Carry. Run after LME Ring close for official settlement prices.",
    ),

    "S2": StrategySchedule(
        strategy_id="S2",
        frequency=Frequency.DAILY,
        bar_size=BarSize.DAILY,
        run_time_utc="13:30",
        notes="TSMOM. Daily signal — rebalance when direction reverses.",
    ),

    "S3": StrategySchedule(
        strategy_id="S3",
        frequency=Frequency.DAILY,
        bar_size=BarSize.DAILY,
        run_time_utc="13:30",
        notes="XS Momentum. Rebalance every 10–15 trading days.",
    ),

    "S5": StrategySchedule(
        strategy_id="S5",
        frequency=Frequency.DAILY,
        bar_size=BarSize.DAILY,
        run_time_utc="09:05",   # 5 mins after LME inventory report (~09:00 London)
        notes="Inventory Surprise. Compute immediately after inventory report. "
              "Execute 30min after release (09:30 London).",
    ),

    "S6": StrategySchedule(
        strategy_id="S6",
        frequency=Frequency.DAILY,
        bar_size=BarSize.DAILY,
        run_time_utc="13:30",
        notes="Pairs / Stat Arb. Check ADF quarterly — suspend pair if p > 0.05.",
    ),
}


# ─────────────────────────────────────────────────────────────────────────────
# FUTURE INTRADAY STRATEGIES (examples — uncomment when building)
# Adding these requires ZERO changes to scheduler engine, execution engine,
# or any other code. Just add the entry here.
# ─────────────────────────────────────────────────────────────────────────────

# "S7": StrategySchedule(
#     strategy_id="S7",
#     frequency=Frequency.HOURLY,
#     bar_size=BarSize.HOURLY,
#     interval_minutes=60,
#     notes="Future: Hourly momentum on LME copper",
# ),

# "S8": StrategySchedule(
#     strategy_id="S8",
#     frequency=Frequency.INTRADAY,
#     bar_size=BarSize.MIN_5,
#     interval_minutes=5,
#     notes="Future: 5-min mean reversion on Gold/Silver ratio",
# ),

# ─────────────────────────────────────────────────────────────────────────────
# SYSTEM TASKS (non-strategy scheduled jobs)
# ─────────────────────────────────────────────────────────────────────────────

SYSTEM_TASKS: dict[str, dict] = {
    "data_ingestion_lme": {
        "run_time_utc": "13:35",   # After LME Ring close
        "description": "Fetch LME settlement prices + inventory",
    },
    "data_ingestion_comex": {
        "run_time_utc": "22:00",   # After COMEX close
        "description": "Fetch COMEX settlement prices",
    },
    "reconciliation": {
        "run_time_utc": "08:30",   # Before market open
        "description": "Daily position reconciliation vs IBKR",
    },
    "daily_report": {
        "run_time_utc": "16:30",   # After execution window
        "description": "Send daily P&L report to Telegram",
    },
    "risk_monitor": {
        "interval_seconds": 60,    # Every 60 seconds during trading hours
        "description": "Circuit breaker checks — runs as SEPARATE PROCESS",
    },
    "health_check": {
        "interval_seconds": 300,   # Every 5 minutes
        "description": "System health monitoring",
    },
    "db_backup": {
        "run_time_utc": "18:30",
        "description": "PostgreSQL backup to Backblaze B2",
    },
}


def get_schedule(strategy_id: str) -> StrategySchedule:
    """Get schedule config for a strategy. Raises KeyError if not found."""
    if strategy_id not in STRATEGY_SCHEDULES:
        raise KeyError(
            f"No schedule defined for strategy '{strategy_id}'. "
            f"Add an entry to STRATEGY_SCHEDULES in config/scheduler.py."
        )
    return STRATEGY_SCHEDULES[strategy_id]


def get_daily_strategies() -> list[StrategySchedule]:
    """Return all daily-frequency strategy schedules."""
    return [s for s in STRATEGY_SCHEDULES.values() if s.frequency == Frequency.DAILY]


def get_intraday_strategies() -> list[StrategySchedule]:
    """Return all intraday strategy schedules (empty until future strategies added)."""
    return [s for s in STRATEGY_SCHEDULES.values() if s.frequency not in (Frequency.DAILY, Frequency.SWING)]