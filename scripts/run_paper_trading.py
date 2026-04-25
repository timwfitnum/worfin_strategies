#!/usr/bin/env python3
"""
scripts/run_paper_trading.py
Cron-ready entry point for paper trading (Piece 4).

─── SCOPE ──────────────────────────────────────────────────────────────────
Wires together the pieces built in 1–3:
  • config/logging_config.configure_logging() — structured JSON logs
  • config/settings.get_settings()            — env/.env driven config
  • strategies/s4_basis_momentum.BasisMomentumStrategy
  • execution/engine.ExecutionEngine          — 8-step daily cycle
  • monitoring/alerts.get_alert_manager()     — Telegram + structured logs

One call per day via cron. Nothing in here schedules; nothing daemons; no REST.
Cron owns the when, this script owns the what.

─── KEY DECISIONS (with rationale) ─────────────────────────────────────────
  • Paper DB is hardcoded to "worfin_paper" and the URL is rebuilt from the
    component settings. The script NEVER uses settings.database_url directly
    because the research DB may live on the same server; a single DB_NAME env
    var cannot be trusted to route research vs paper correctly.
  • ENVIRONMENT=live → hard refuse with non-zero exit. Piece 4 is paper-only;
    live deploys will use a separate entry point with its own additional
    guards. This is a second guard (the broker has its own live-port refusal).
  • Dry-run stops after the engine constructs. No broker connect. No cycle run.
    Verifies: config loads, logging configures, paper DB reachable, migrations
    applied, strategy instantiates, engine constructs.
  • Migration check: alembic_version table must exist in worfin_paper. Catches
    the "forgot to migrate the new DB" foot-gun before a real cycle runs.
  • Execution-module log level is raised to INFO at startup. The shared
    logging_config silences worfin.execution at WARNING (for backtests) —
    that's wrong for paper, where we want the cycle narrative in the logs.
  • Startup ping uses AlertManager.startup_ping() (new, this piece) — pushes
    to Telegram regardless of severity so the phone gets proof-of-life even
    though severity is INFO.
  • Dependencies (db_engine, broker, alert_manager, strategy_factory) are
    injectable into run_paper_trading() so tests can mock them without the
    real venv, broker, or DB.
  • Top-level main() wraps asyncio.run in try/except — any uncaught exception
    becomes a CRITICAL alert + non-zero exit, so cron surfaces the failure.

─── EXIT CODES ─────────────────────────────────────────────────────────────
  0  — clean cycle (reconciliation mismatches do NOT promote to non-zero;
       engine already alerted)
  1  — any failure: live refusal, DB unreachable, migrations missing,
       unknown strategy, safe-state cycle, uncaught exception
  130 — interrupted (SIGINT / Ctrl-C)

─── USAGE ──────────────────────────────────────────────────────────────────
  python scripts/run_paper_trading.py                 # normal cron run
  python scripts/run_paper_trading.py --dry-run       # config + plumbing only
  python scripts/run_paper_trading.py --strategy S4   # (only S4 supported)
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine

    from worfin.execution.broker import IBKRBroker
    from worfin.monitoring.alerts import AlertManager
    from worfin.strategies.base import BaseStrategy

logger = logging.getLogger("worfin.scripts.run_paper_trading")


# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────

# Paper DB is hardcoded — NEVER read db_name from settings here.
PAPER_DB_NAME: str = "worfin_paper"

# ~/worfin/logs is consolidated across all worfin processes (backtest, paper,
# live). configure_logging will create the dir if missing.
LOG_DIR: Path = Path("~/worfin/logs").expanduser()


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse CLI args. Exposed for testing (argv kept defaultable)."""
    p = argparse.ArgumentParser(
        prog="run_paper_trading",
        description="WorFIn paper-trading cron entry point. Runs one cycle then exits.",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Load config, configure logging, verify paper DB + migrations, "
            "instantiate strategy + engine, then EXIT before touching the broker."
        ),
    )
    p.add_argument(
        "--strategy",
        choices=["S4"],
        default="S4",
        help="Strategy to run (only S4 is wired in Piece 4).",
    )
    return p.parse_args(argv)


# ─────────────────────────────────────────────────────────────────────────────
# SMALL PURE HELPERS (unit-tested directly)
# ─────────────────────────────────────────────────────────────────────────────


def mask_account_id(account_id: str) -> str:
    """Mask IBKR account ID for logs. Keeps first 2 + last 4 chars."""
    if not account_id:
        return "(not set)"
    if len(account_id) < 6:
        return "****"  # Too short to partially reveal; scrub entirely.
    return f"{account_id[:2]}****{account_id[-4:]}"


def build_paper_db_url(
    db_user: str,
    db_password: str,
    db_host: str,
    db_port: int,
) -> str:
    """
    Build a DB URL forced to the paper DB. The db_name is NEVER read from
    settings or env — it's hardcoded to PAPER_DB_NAME so this script cannot
    accidentally point at the research DB.
    """
    return f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{PAPER_DB_NAME}"


def _redact_password(url: str, password: str) -> str:
    """For log output only — never mutate the real URL."""
    if not password:
        return url
    return url.replace(password, "***")


# ─────────────────────────────────────────────────────────────────────────────
# DB PRE-FLIGHT
# ─────────────────────────────────────────────────────────────────────────────


def check_db_reachable(engine: Engine) -> bool:
    """True iff `SELECT 1` succeeds against the paper DB."""
    from sqlalchemy import text

    try:
        with engine.begin() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception as e:
        logger.critical("Paper DB unreachable: %s", e)
        return False


def check_migrations_applied(engine: Engine) -> bool:
    """
    True iff the alembic_version table exists (i.e. migrations have been run
    against worfin_paper). Missing table → fatal: run alembic first.
    """
    from sqlalchemy import text

    sql = text(
        """
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = 'alembic_version'
        )
        """
    )
    try:
        with engine.begin() as conn:
            row = conn.execute(sql).fetchone()
        return bool(row and row[0])
    except Exception as e:
        logger.critical("Migration check failed: %s", e)
        return False


# ─────────────────────────────────────────────────────────────────────────────
# STRATEGY FACTORY (one-strategy today; table-driven add later)
# ─────────────────────────────────────────────────────────────────────────────


def default_strategy_factory(strategy_id: str) -> BaseStrategy:
    """Map --strategy value to a concrete instance. argparse pre-filters."""
    if strategy_id == "S4":
        from worfin.strategies.s4_basis_momentum import BasisMomentumStrategy

        return BasisMomentumStrategy()
    raise ValueError(f"Unknown strategy: {strategy_id!r}")  # defensive — argparse gates


# ─────────────────────────────────────────────────────────────────────────────
# CORE FLOW (async, fully injectable for tests)
# ─────────────────────────────────────────────────────────────────────────────


async def run_paper_trading(
    args: argparse.Namespace,
    *,
    db_engine: Engine | None = None,
    broker: IBKRBroker | None = None,
    alert_manager: AlertManager | None = None,
    strategy_factory: Callable[[str], BaseStrategy] = default_strategy_factory,
    engine_factory: Callable[..., Any] | None = None,
) -> int:
    """
    Execute one paper-trading cycle. Returns the process exit code.

    All external dependencies are overridable so tests can drive this without
    a real DB, broker, Telegram, or strategy.
    """
    # ── Imports deferred so --help works without a full venv ───────────────
    from sqlalchemy import create_engine

    from worfin.config.logging_config import configure_logging
    from worfin.config.settings import Environment, get_settings
    from worfin.execution.engine import ExecutionEngine
    from worfin.monitoring.alerts import get_alert_manager

    # ── 1. Logging ─────────────────────────────────────────────────────────
    configure_logging(log_dir=LOG_DIR)
    # The shared logging_config silences worfin.execution at WARNING (a
    # backtest-era default). For paper we want the full cycle narrative.
    logging.getLogger("worfin.execution").setLevel(logging.INFO)

    # ── 2. Settings + environment guard ────────────────────────────────────
    settings = get_settings()
    if settings.environment == Environment.LIVE:
        # The broker has its own live-port guard; this is a second, process-
        # level guard. Piece 4 is paper only — live gets a separate entry.
        logger.critical(
            "Refusing to run: ENVIRONMENT=live. scripts/run_paper_trading.py is paper-only."
        )
        return 1

    acct_masked = mask_account_id(settings.ibkr_account_id)
    logger.info(
        "Paper-trading start: env=%s strategy=%s dry_run=%s account=%s capital_gbp=%.0f",
        settings.environment.value,
        args.strategy,
        args.dry_run,
        acct_masked,
        settings.trading_capital_gbp,
    )

    # ── 3. Paper DB engine ─────────────────────────────────────────────────
    if db_engine is None:
        db_url = build_paper_db_url(
            db_user=settings.db_user,
            db_password=settings.db_password,
            db_host=settings.db_host,
            db_port=settings.db_port,
        )
        logger.info("Paper DB target: %s", _redact_password(db_url, settings.db_password))
        db_engine = create_engine(db_url, pool_pre_ping=True, future=True)

    if not check_db_reachable(db_engine):
        return 1
    if not check_migrations_applied(db_engine):
        logger.critical(
            "alembic_version table missing in %s. Create the DB and run "
            "migrations first: `DB_NAME=%s alembic upgrade head`",
            PAPER_DB_NAME,
            PAPER_DB_NAME,
        )
        return 1

    # ── 4. Alert manager (Telegram auto-configures from settings) ──────────
    alert_mgr = alert_manager if alert_manager is not None else get_alert_manager()

    # ── 5. Strategy ────────────────────────────────────────────────────────
    try:
        strategy = strategy_factory(args.strategy)
    except Exception as e:
        logger.critical("Strategy construction failed: %s", e)
        return 1

    # ── 6. Engine ──────────────────────────────────────────────────────────
    if engine_factory is None:
        engine = ExecutionEngine(
            strategies=[strategy],
            db_engine=db_engine,
            broker=broker,  # None → ExecutionEngine uses singleton get_broker()
        )
    else:
        engine = engine_factory(
            strategies=[strategy],
            db_engine=db_engine,
            broker=broker,
        )

    # ── 7. Startup ping (Telegram regardless of severity) ──────────────────
    try:
        alert_mgr.startup_ping(
            environment=settings.environment.value,
            strategies=[strategy.strategy_id],
            account_id_masked=acct_masked,
        )
    except Exception as e:
        # Alert failures never propagate — cycle is more important than the ping
        logger.error("Startup ping failed (non-fatal): %s", e)

    # ── 8. Dry-run exit point ──────────────────────────────────────────────
    if args.dry_run:
        logger.info(
            "Dry-run complete: config loaded, paper DB reachable, migrations "
            "applied, strategy=%s instantiated, engine constructed. "
            "Exiting before run_cycle().",
            strategy.strategy_id,
        )
        return 0

    # ── 9. Run the cycle ───────────────────────────────────────────────────
    result = await engine.run_cycle()

    # ── 10. Post-run exit-code mapping ─────────────────────────────────────
    if result.safe_state:
        logger.critical(
            "Cycle %s ENTERED SAFE STATE: %s (duration=%.1fs)",
            result.correlation_id,
            result.safe_state_reason,
            result.duration_seconds or 0,
        )
        return 1

    if result.reconciliation is not None and not result.reconciliation.is_clean:
        # Engine already sent a CRITICAL alert per mismatch; don't duplicate.
        logger.warning(
            "Cycle %s completed with %d reconciliation mismatches (alerted by engine)",
            result.correlation_id,
            len(result.reconciliation.mismatches),
        )

    logger.info(
        "Cycle %s complete. duration=%.1fs strategies=%d safe_state=%s",
        result.correlation_id,
        result.duration_seconds or 0,
        len(result.strategy_results),
        result.safe_state,
    )
    return 0


# ─────────────────────────────────────────────────────────────────────────────
# TOP-LEVEL ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────


def main(argv: list[str] | None = None) -> int:
    """Synchronous wrapper. Owns top-level exception → alert → exit code."""
    args = parse_args(argv)
    try:
        return asyncio.run(run_paper_trading(args))
    except KeyboardInterrupt:
        logger.warning("Interrupted by user (SIGINT)")
        return 130
    except SystemExit:
        raise
    except Exception as e:
        # Any uncaught exception → critical alert → non-zero so cron surfaces it.
        # Alert itself is best-effort; never mask the real error.
        logger.critical(
            "Uncaught exception in paper trading run: %s: %s",
            type(e).__name__,
            e,
            exc_info=True,
        )
        try:
            from worfin.monitoring.alerts import AlertLevel, get_alert_manager

            get_alert_manager().send(
                AlertLevel.CRITICAL,
                (f"Paper trading run FAILED with uncaught exception: {type(e).__name__}: {e}"),
            )
        except Exception as alert_e:
            logger.error("Failure alert itself failed: %s", alert_e)
        return 1


if __name__ == "__main__":
    sys.exit(main())
