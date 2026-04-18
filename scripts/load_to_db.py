#!/usr/bin/env python3
"""
scripts/load_to_db.py
Load Nasdaq Data Link futures prices into raw_data.futures_prices.

DATA CONTRACT:
  Source: Nasdaq Data Link CHRIS/ dataset (continuous front-month series).
  Examples: CHRIS/CME_GC1 (Gold front), CHRIS/LME_CA1 (Copper front),
            CHRIS/LME_CA2 (Copper second month)

VALIDATION (pre-insert):
  - Settle price > 0  (CHRIS/LME sometimes has NaN rows for non-trading days)
  - not future-dated
  - ticker is a known WorFIn ticker

IDEMPOTENCY:
  Uses WHERE NOT EXISTS — safe to re-run. Rows already present are skipped.

USAGE:
  python scripts/load_to_db.py --ticker GC --start 2004-01-01 --end 2023-12-31
  python scripts/load_to_db.py --all --start 2004-01-01 --end 2023-12-31 --second
  python scripts/load_to_db.py --ticker GC --dry-run
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import UTC, date, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from worfin.config.logging_config import configure_logging

configure_logging()

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from worfin.config.metals import ALL_METALS
from worfin.config.settings import get_settings

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# NASDAQ DATA LINK SERIES MAPPING
# ─────────────────────────────────────────────────────────────────────────────

_NASDAQ_CODES: dict[str, dict[str, str]] = {
    "CA": {"front": "CHRIS/LME_CA1", "second": "CHRIS/LME_CA2"},
    "AH": {"front": "CHRIS/LME_AH1", "second": "CHRIS/LME_AH2"},
    "ZS": {"front": "CHRIS/LME_ZS1", "second": "CHRIS/LME_ZS2"},
    "NI": {"front": "CHRIS/LME_NI1", "second": "CHRIS/LME_NI2"},
    "PB": {"front": "CHRIS/LME_PB1", "second": "CHRIS/LME_PB2"},
    "SN": {"front": "CHRIS/LME_SN1", "second": "CHRIS/LME_SN2"},
    "GC": {"front": "CHRIS/CME_GC1", "second": "CHRIS/CME_GC2"},
    "SI": {"front": "CHRIS/CME_SI1", "second": "CHRIS/CME_SI2"},
    "PL": {"front": "CHRIS/CME_PL1", "second": "CHRIS/CME_PL2"},
    "PA": {"front": "CHRIS/CME_PA1", "second": "CHRIS/CME_PA2"},
}

SOURCE = "nasdaq_data_link_chris"
_MIN_PRICE = 0.01


# ─────────────────────────────────────────────────────────────────────────────
# FETCH
# ─────────────────────────────────────────────────────────────────────────────


def _fetch_nasdaq(nasdaq_code: str, start: date, end: date, api_key: str) -> pd.DataFrame:
    import nasdaqdatalink

    nasdaqdatalink.ApiConfig.api_key = api_key
    try:
        df: pd.DataFrame = nasdaqdatalink.get(
            nasdaq_code,
            start_date=start.isoformat(),
            end_date=end.isoformat(),
            returns="pandas",
        )
    except Exception as exc:
        logger.error("Nasdaq fetch failed for %s: %s", nasdaq_code, exc)
        return pd.DataFrame()
    if df is None or df.empty:
        logger.warning("Empty DataFrame for %s.", nasdaq_code)
        return pd.DataFrame()
    logger.info("Fetched %d rows for %s.", len(df), nasdaq_code)
    return df


# ─────────────────────────────────────────────────────────────────────────────
# VALIDATION
# ─────────────────────────────────────────────────────────────────────────────


def _validate_df(df, ticker, nasdaq_code, today, strict) -> tuple[pd.DataFrame, int]:
    original = len(df)
    issues: list[str] = []
    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index)
    if "Settle" not in df.columns:
        logger.warning(
            "%s (%s): 'Settle' missing. Available: %s", ticker, nasdaq_code, list(df.columns)
        )
        if strict:
            raise ValueError(f"Settle missing for {nasdaq_code}.")
        return pd.DataFrame(), original
    bad = df["Settle"].isna() | (df["Settle"] <= _MIN_PRICE)
    if bad.sum():
        issues.append(f"{bad.sum()} bad-price rows")
    df = df[~bad]
    future = df.index.normalize() > pd.Timestamp(today)
    if future.sum():
        issues.append(f"{future.sum()} future-dated rows")
    df = df[~future]
    if issues:
        msg = f"{ticker} ({nasdaq_code}): {'; '.join(issues)}"
        if strict:
            raise ValueError(msg)
        logger.warning(msg)
    return df, original - len(df)


# ─────────────────────────────────────────────────────────────────────────────
# INSERT
#
# raw_data.futures_prices columns (from 001_schema.py):
#   price_timestamp  — was named "timestamp" in old scripts (WRONG)
#   contract_type    — 'front' | 'second'   (was "contract_code" — WRONG)
#   bar_size         — 'daily'              (was "daily_front" etc — WRONG)
#   open, high, low  — short names         (was open_price etc — WRONG)
#   close            — Settle price        (no separate "settle_price" — WRONG)
#   fetched_at       — required, not nullable
#
# Idempotency: WHERE NOT EXISTS (table has no unique constraint).
# ─────────────────────────────────────────────────────────────────────────────

_INSERT_SQL = text(
    """
    INSERT INTO raw_data.futures_prices
        (price_timestamp, ticker, contract_type, bar_size,
         open, high, low, close,
         volume, open_interest, source, fetched_at)
    SELECT
        :price_timestamp, :ticker, :contract_type, :bar_size,
        :open, :high, :low, :close,
        :volume, :open_interest, :source, :fetched_at
    WHERE NOT EXISTS (
        SELECT 1 FROM raw_data.futures_prices
        WHERE ticker          = :ticker
          AND price_timestamp = :price_timestamp
          AND contract_type   = :contract_type
          AND source          = :source
    )
"""
)


def _safe_float(val: object) -> float | None:
    try:
        f = float(val)  # type: ignore[arg-type]
        return None if (f != f) else f
    except (TypeError, ValueError):
        return None


def _build_rows(
    df: pd.DataFrame, ticker: str, contract_type: str, fetched_at: datetime
) -> list[dict]:
    rows = []
    for ts, row in df.iterrows():
        settle = _safe_float(row.get("Settle"))
        if not settle or settle <= 0:
            continue
        ts_utc = (
            pd.Timestamp(ts).tz_localize("UTC")
            if ts.tzinfo is None
            else pd.Timestamp(ts).tz_convert("UTC")
        ).normalize()
        rows.append(
            {
                "price_timestamp": ts_utc.to_pydatetime(),
                "ticker": ticker,
                "contract_type": contract_type,
                "bar_size": "daily",
                "open": _safe_float(row.get("Open")),
                "high": _safe_float(row.get("High")),
                "low": _safe_float(row.get("Low")),
                "close": settle,
                "volume": _safe_float(row.get("Volume")),
                "open_interest": _safe_float(row.get("Previous Day Open Interest")),
                "source": SOURCE,
                "fetched_at": fetched_at,
            }
        )
    return rows


def _insert_rows(engine: Engine, rows: list[dict], dry_run: bool) -> tuple[int, int]:
    if not rows:
        return 0, 0
    if dry_run:
        logger.info("DRY RUN: would insert %d rows.", len(rows))
        return 0, len(rows)
    ins = skp = 0
    with engine.begin() as conn:
        for row in rows:
            r = conn.execute(_INSERT_SQL, row)
            if r.rowcount > 0:
                ins += 1
            else:
                skp += 1
    return ins, skp


# ─────────────────────────────────────────────────────────────────────────────
# ORCHESTRATOR
# ─────────────────────────────────────────────────────────────────────────────


def load_ticker(
    ticker, start, end, engine, api_key, include_second=False, dry_run=False, strict=False
) -> tuple[int, int]:
    codes = _NASDAQ_CODES.get(ticker)
    if not codes:
        logger.error("No Nasdaq code for %r.", ticker)
        return 0, 0

    today = date.today()
    fetched_at = datetime.now(UTC)
    total_ins = total_skp = 0

    series = [("front", codes["front"])]
    if include_second:
        series.append(("second", codes["second"]))

    for contract_type, nasdaq_code in series:
        logger.info("Loading %s (%s) %s → %s …", ticker, nasdaq_code, start, end)
        df = _fetch_nasdaq(nasdaq_code, start, end, api_key)
        if df.empty:
            continue
        df, n_dropped = _validate_df(df, ticker, nasdaq_code, today, strict)
        if df.empty:
            continue
        rows = _build_rows(df, ticker, contract_type, fetched_at)
        ins, skp = _insert_rows(engine, rows, dry_run)
        total_ins += ins
        total_skp += skp + n_dropped
        logger.info(
            "%s [%s]: %d inserted, %d skipped, %d validation drops.",
            ticker,
            contract_type,
            ins,
            skp,
            n_dropped,
        )

    return total_ins, total_skp


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Load Nasdaq Data Link futures into WorFIn DB.")
    tg = p.add_mutually_exclusive_group(required=True)
    tg.add_argument("--ticker", metavar="TICKER")
    tg.add_argument("--all", action="store_true", dest="all_tickers")
    p.add_argument("--start", default="2004-01-01", metavar="YYYY-MM-DD")
    p.add_argument("--end", default=date.today().isoformat(), metavar="YYYY-MM-DD")
    p.add_argument("--second", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--strict", action="store_true")
    p.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return p.parse_args()


def main() -> int:
    args = _parse_args()
    configure_logging(log_level=args.log_level, force=True)

    try:
        start = date.fromisoformat(args.start)
        end = date.fromisoformat(args.end)
    except ValueError as exc:
        logger.error("Invalid date: %s", exc)
        return 1

    settings = get_settings()
    if not settings.nasdaq_data_link_api_key:
        logger.error("NASDAQ_DATA_LINK_API_KEY not set.")
        return 1

    engine = create_engine(settings.database_url, pool_pre_ping=True)
    tickers = list(ALL_METALS.keys()) if args.all_tickers else [args.ticker.upper()]

    grand_ins = grand_skp = 0
    for ticker in tickers:
        if ticker not in _NASDAQ_CODES:
            logger.error("Unknown ticker %r.", ticker)
            if args.strict:
                return 1
            continue
        try:
            ins, skp = load_ticker(
                ticker,
                start,
                end,
                engine,
                settings.nasdaq_data_link_api_key,
                include_second=args.second,
                dry_run=args.dry_run,
                strict=args.strict,
            )
            grand_ins += ins
            grand_skp += skp
        except Exception:
            logger.exception("Error loading %s.", ticker)
            if args.strict:
                return 1

    action = "would insert" if args.dry_run else "inserted"
    print(
        f"\n{'[DRY RUN] ' if args.dry_run else ''}"
        f"Rows {action}: {grand_ins:,}   skipped: {grand_skp:,}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
