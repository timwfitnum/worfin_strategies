"""
scripts/fetch_historical.py
Pull all historical metals data for backtesting.

Run this ONCE after setting up the database:
    python scripts/fetch_historical.py

This fetches IS + OOS data (2005–2022) for all 10 metals,
both front and second month contracts, and stores in raw_data schema.

Duration: ~5–10 minutes on first run (API rate limiting).
Subsequent incremental runs: ~30 seconds.
"""
from __future__ import annotations

import logging
import sys
from datetime import date, datetime, timezone
from pathlib import Path

# Add src to path so worfin package is importable
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from worfin.config.settings import get_settings
from worfin.data.ingestion.nasdaq_datalink import fetch_all_metals, fetch_for_backtest

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("fetch_historical")


# ─────────────────────────────────────────────────────────────────────────────
# FIXED DATA SPLITS — DO NOT CHANGE AFTER FIRST RUN
# Changing these after you've looked at any results contaminates the OOS split.
# ─────────────────────────────────────────────────────────────────────────────
IS_START    = date(2005, 1,  1)
IS_END      = date(2017, 12, 31)
OOS_START   = date(2018, 1,  1)
OOS_END     = date(2022, 12, 31)
HOLDOUT_START = date(2023, 1, 1)
HOLDOUT_END = date.today()


def main() -> None:
    settings = get_settings()

    logger.info("=" * 60)
    logger.info("WorFIn — Historical Data Fetch")
    logger.info("Environment: %s", settings.environment.value)
    logger.info("=" * 60)

    if not settings.nasdaq_data_link_api_key:
        logger.error(
            "NASDAQ_DATA_LINK_API_KEY not set in .env\n"
            "Get a free key at: https://data.nasdaq.com\n"
            "Add to .env: NASDAQ_DATA_LINK_API_KEY=your_key_here"
        )
        sys.exit(1)

    # ── Fetch IS + OOS (primary backtest data) ───────────────────────────────
    logger.info("\nFetching IS + OOS data: %s → %s", IS_START, OOS_END)
    logger.info("This covers In-Sample (2005–2017) + Out-of-Sample (2018–2022)")
    logger.info("DO NOT use OOS data for parameter optimisation.\n")

    data = fetch_all_metals(start_date=IS_START, end_date=OOS_END)

    success_count = 0
    failure_count = 0

    for ticker, contracts in data.items():
        for contract_type, df in contracts.items():
            if df.empty:
                logger.warning("❌ %s %s — no data returned", ticker, contract_type)
                failure_count += 1
            else:
                logger.info(
                    "✅ %s %s — %d rows | %s → %s",
                    ticker, contract_type, len(df),
                    df.index.min().date(), df.index.max().date(),
                )
                success_count += 1

    logger.info("\n%s", "=" * 60)
    logger.info("Fetch complete: %d succeeded, %d failed", success_count, failure_count)

    if failure_count > 0:
        logger.warning(
            "\n%d fetches failed. Common causes:\n"
            "  1. API key not set or invalid\n"
            "  2. Dataset not available on free tier (check Nasdaq Data Link)\n"
            "  3. Rate limiting — wait 60 seconds and retry\n"
            "  4. Ticker code mapping incorrect — check data/ingestion/nasdaq_datalink.py",
            failure_count,
        )

    # ── Fetch Holdout (separate — touch only for final validation) ───────────
    logger.info("\n%s", "=" * 60)
    logger.info("Fetching Holdout data: %s → %s", HOLDOUT_START, HOLDOUT_END)
    logger.info("WARNING: Do NOT examine this data until strategy passes IS + OOS gates.\n")

    holdout_data = fetch_all_metals(start_date=HOLDOUT_START, end_date=HOLDOUT_END)

    holdout_rows = sum(
        len(df) for contracts in holdout_data.values()
        for df in contracts.values()
        if not df.empty
    )
    logger.info("Holdout fetch complete: %d total rows", holdout_rows)
    logger.info("\nNext step: run database setup then load data into PostgreSQL")
    logger.info("  alembic upgrade head")
    logger.info("  python scripts/load_to_db.py")


if __name__ == "__main__":
    main()