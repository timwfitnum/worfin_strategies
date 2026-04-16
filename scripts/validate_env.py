#!/usr/bin/env python3
"""
scripts/validate_env.py
Startup validation — run before any trading session.

Checks (in order):
  1. Required environment variables are present
  2. PostgreSQL is reachable and all expected schemas exist
  3. Nasdaq Data Link API key works (one tiny request)
  4. FRED API key works (one tiny request)
  5. Telegram bot credentials are valid (bot.get_me() — silent, no message)
  6. System clock is accurate (NTP comparison; warning-only)

Exit code:
  0 — everything OK
  1 — at least one critical check failed (warnings don't fail the run)

Flags:
  --send-telegram   Additionally send a real test message to your chat
                    (for first-time setup; default is silent validation)
  --skip-clock      Skip the NTP clock check (for offline/CI environments)

Usage:
  python scripts/validate_env.py
  python scripts/validate_env.py --send-telegram
"""

from __future__ import annotations

import argparse
import socket
import sys
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime

# ─────────────────────────────────────────────────────────────────────────────
# REQUIRED ENV VARS
# ─────────────────────────────────────────────────────────────────────────────

REQUIRED_ENV_VARS = [
    "DB_HOST",
    "DB_PORT",
    "DB_NAME",
    "DB_USER",
    "DB_PASSWORD",
    "NASDAQ_DATA_LINK_API_KEY",
    "FRED_API_KEY",
    "TELEGRAM_BOT_TOKEN",
    "TELEGRAM_CHAT_ID",
    "IBKR_HOST",
    "IBKR_PORT_LIVE",
    "IBKR_PORT_PAPER",
    "IBKR_CLIENT_ID",
    "ENVIRONMENT",
    "LOG_LEVEL",
]

EXPECTED_SCHEMAS = ["raw_data", "clean_data", "signals", "positions", "orders", "audit"]

NTP_DRIFT_WARNING_SECONDS = 2.0
NTP_DRIFT_FAILURE_SECONDS = 30.0


# ─────────────────────────────────────────────────────────────────────────────
# RESULT TYPES
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class CheckResult:
    name: str
    passed: bool
    is_warning: bool = False  # True = don't fail overall run
    detail: str = ""


@dataclass
class ValidationReport:
    results: list[CheckResult] = field(default_factory=list)

    def add(self, result: CheckResult) -> None:
        self.results.append(result)

    @property
    def critical_failures(self) -> list[CheckResult]:
        return [r for r in self.results if not r.passed and not r.is_warning]

    @property
    def warnings(self) -> list[CheckResult]:
        return [r for r in self.results if not r.passed and r.is_warning]

    def render(self) -> str:
        lines = []
        lines.append("=" * 78)
        lines.append("WorFIn Environment Validation")
        lines.append(f"Run at: {datetime.now(UTC).isoformat()}")
        lines.append("=" * 78)
        for r in self.results:
            if r.passed:
                icon = "✅"
            elif r.is_warning:
                icon = "⚠️ "
            else:
                icon = "❌"
            lines.append(f"{icon}  {r.name:<40} {r.detail}")
        lines.append("=" * 78)
        lines.append(
            f"Summary: {len([r for r in self.results if r.passed])} passed, "
            f"{len(self.warnings)} warning(s), "
            f"{len(self.critical_failures)} failure(s)."
        )
        return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# INDIVIDUAL CHECKS
# ─────────────────────────────────────────────────────────────────────────────


def check_env_vars() -> CheckResult:
    import os

    missing = [v for v in REQUIRED_ENV_VARS if not os.environ.get(v)]
    if missing:
        return CheckResult(
            "Environment variables",
            passed=False,
            detail=f"MISSING: {', '.join(missing)}",
        )
    return CheckResult(
        "Environment variables",
        passed=True,
        detail=f"all {len(REQUIRED_ENV_VARS)} present",
    )


def check_database() -> CheckResult:
    try:
        from sqlalchemy import create_engine, text

        from worfin.config.settings import get_settings

        settings = get_settings()
        engine = create_engine(settings.database_url, pool_pre_ping=True)
        with engine.connect() as conn:
            version = conn.execute(text("SELECT version()")).scalar()
            schemas = {
                r[0]
                for r in conn.execute(
                    text("SELECT schema_name FROM information_schema.schemata")
                ).all()
            }
        missing_schemas = [s for s in EXPECTED_SCHEMAS if s not in schemas]
        if missing_schemas:
            return CheckResult(
                "PostgreSQL",
                passed=False,
                detail=f"connected, but missing schemas: {missing_schemas}. "
                f"Run 'alembic upgrade head'",
            )
        # Truncate version string for display
        v = version.split(",")[0] if version else "unknown"
        return CheckResult(
            "PostgreSQL",
            passed=True,
            detail=f"{v}, all schemas present",
        )
    except Exception as exc:
        return CheckResult("PostgreSQL", passed=False, detail=f"connection failed: {exc}")


def check_nasdaq_data_link() -> CheckResult:
    try:
        import nasdaqdatalink

        from worfin.config.settings import get_settings

        settings = get_settings()
        if not settings.nasdaq_data_link_api_key:
            return CheckResult(
                "Nasdaq Data Link", passed=False, detail="NASDAQ_DATA_LINK_API_KEY not set"
            )
        nasdaqdatalink.ApiConfig.api_key = settings.nasdaq_data_link_api_key
        # Tiny request: just a few rows of a well-known series
        df = nasdaqdatalink.get(
            "CHRIS/CME_GC1",
            rows=3,
            returns="pandas",
        )
        if df is None or df.empty:
            return CheckResult("Nasdaq Data Link", passed=False, detail="request returned no data")
        return CheckResult(
            "Nasdaq Data Link", passed=True, detail=f"API key valid ({len(df)} rows fetched)"
        )
    except Exception as exc:
        return CheckResult("Nasdaq Data Link", passed=False, detail=f"request failed: {exc}")


def check_fred() -> CheckResult:
    try:
        from fredapi import Fred

        from worfin.config.settings import get_settings

        settings = get_settings()
        if not settings.fred_api_key:
            return CheckResult("FRED", passed=False, detail="FRED_API_KEY not set")
        fred = Fred(api_key=settings.fred_api_key)
        # Tiny request — one week of DEXUSUK
        series = fred.get_series("DEXUSUK", limit=5)
        if series is None or len(series) == 0:
            return CheckResult("FRED", passed=False, detail="DEXUSUK returned no data")
        latest_rate = series.dropna().iloc[-1]
        return CheckResult(
            "FRED", passed=True, detail=f"API key valid (latest DEXUSUK={latest_rate:.4f})"
        )
    except Exception as exc:
        return CheckResult("FRED", passed=False, detail=f"request failed: {exc}")


def check_telegram(send_test_message: bool = False) -> CheckResult:
    try:
        import asyncio

        import telegram

        from worfin.config.settings import get_settings

        settings = get_settings()
        if not settings.telegram_bot_token or not settings.telegram_chat_id:
            return CheckResult(
                "Telegram", passed=False, detail="TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID not set"
            )

        async def _validate() -> str:
            bot = telegram.Bot(token=settings.telegram_bot_token)
            me = await bot.get_me()
            detail = f"bot @{me.username} authenticated (silent)"
            if send_test_message:
                await bot.send_message(
                    chat_id=settings.telegram_chat_id,
                    text=(
                        f"🟢 WorFIn validate_env test — "
                        f"{datetime.now(UTC).isoformat()}"
                    ),
                )
                detail = f"bot @{me.username} authenticated; test message sent"
            return detail

        detail = asyncio.run(_validate())
        return CheckResult("Telegram", passed=True, detail=detail)
    except Exception as exc:
        return CheckResult("Telegram", passed=False, detail=f"auth failed: {exc}")


def check_system_clock() -> CheckResult:
    """Compare system clock to NTP. Warning-only unless drift > 30s."""
    try:
        import ntplib  # type: ignore
    except ImportError:
        return CheckResult(
            "System clock (NTP)",
            passed=False,
            is_warning=True,
            detail="ntplib not installed — skip (pip install ntplib)",
        )
    try:
        client = ntplib.NTPClient()
        response = client.request("pool.ntp.org", version=3, timeout=3)
        drift = abs(response.offset)
        if drift > NTP_DRIFT_FAILURE_SECONDS:
            return CheckResult(
                "System clock (NTP)",
                passed=False,
                is_warning=False,
                detail=f"clock off by {drift:.2f}s — CRITICAL, fix before trading",
            )
        if drift > NTP_DRIFT_WARNING_SECONDS:
            return CheckResult(
                "System clock (NTP)",
                passed=False,
                is_warning=True,
                detail=f"clock off by {drift:.2f}s — warning (fine for backtest)",
            )
        return CheckResult(
            "System clock (NTP)",
            passed=True,
            detail=f"drift {drift:+.3f}s (OK)",
        )
    except (TimeoutError, socket.gaierror, Exception) as exc:
        return CheckResult(
            "System clock (NTP)",
            passed=False,
            is_warning=True,
            detail=f"NTP query failed: {exc} — offline? Skip with --skip-clock.",
        )


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate WorFIn runtime environment.")
    parser.add_argument(
        "--send-telegram",
        action="store_true",
        help="Also send a live test message to TELEGRAM_CHAT_ID",
    )
    parser.add_argument(
        "--skip-clock",
        action="store_true",
        help="Skip the NTP clock check (useful offline / CI)",
    )
    args = parser.parse_args()

    report = ValidationReport()

    # 1. env vars — do this first because everything else depends on them
    env_result = check_env_vars()
    report.add(env_result)
    if not env_result.passed:
        # Without env vars we can't run other checks meaningfully
        print(report.render())
        return 1

    # 2–5. real checks
    checks: list[tuple[str, Callable[[], CheckResult]]] = [
        ("database", check_database),
        ("nasdaq", check_nasdaq_data_link),
        ("fred", check_fred),
        ("telegram", lambda: check_telegram(send_test_message=args.send_telegram)),
    ]
    for _name, fn in checks:
        try:
            report.add(fn())
        except Exception as exc:  # bullet-proof reporter
            report.add(CheckResult(_name, passed=False, detail=f"check raised: {exc}"))

    # 6. clock — last, and warning-only (unless very broken)
    if not args.skip_clock:
        report.add(check_system_clock())

    print(report.render())
    return 0 if not report.critical_failures else 1


if __name__ == "__main__":
    sys.exit(main())
