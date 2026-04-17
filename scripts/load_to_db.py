#!/usr/bin/env python3
"""
scripts/load_to_db.py
Load Nasdaq Data Link futures prices into raw_data.futures_prices.

DATA CONTRACT:
  Source: Nasdaq Data Link CHRIS/ dataset (continuous front-month series).
  Examples: CHRIS/CME_GC1 (Gold front), CHRIS/LME_CA1 (Copper front),
            CHRIS/LME_CA2 (Copper second month)

VALIDATION (pre-insert):
  - price > 0   (CHRIS/LME sometimes has NaN rows for non-trading days)
  - not stale   (date <= today; warns on futures dates from bad CSVs)
  - ticker is a known WORFIN ticker (optional soft check via --strict)

IDEMPOTENCY:
  Each row is identified by (ticker, timestamp, source).
  Rows already present are skipped — safe to re-run.
  Report: N rows inserted, M rows skipped.

USAGE:
  # Load Gold, 2005–2022
  python scripts/load_to_db.py --ticker GC --start 2005-01-01 --end 2022-12-31

  # Load all metals from Nasdaq CHRIS
  python scripts/load_to_db.py --all --start 2005-01-01 --end 2022-12-31

  # Load just front-month (default); add --second for second-month too
  python scripts/load_to_db.py --ticker CA --second

  # Dry run — validate without inserting
  python scripts/load_to_db.py --ticker GC --dry-run

FLAGS:
  --ticker TICKER    Single metal ticker (CA, AH, ZS, NI, PB, SN, GC, SI, PL, PA)
  --all              Load all 10 metals
  --start YYYY-MM-DD Start date (default: 2005-01-01)
  --end   YYYY-MM-DD End date   (default: today)
  --second           Also load second-month series (e.g. CHRIS/LME_CA2)
  --dry-run          Validate but do not insert
  --strict           Fail on unknown tickers / validation warnings
  --log-level LEVEL  Override log level (default: INFO)
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, datetime
from pathlib import Path

# Add src/ to path so worfin package is importable when run as a script
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
# Internal ticker → Nasdaq Data Link CHRIS dataset codes (front + second).

_NASDAQ_CODES: dict[str, dict[str, str]] = {
    "CA": {"front": "CHRIS/LME_CA1",  "second": "CHRIS/LME_CA2"},
    "AH": {"front": "CHRIS/LME_AH1",  "second": "CHRIS/LME_AH2"},
    "ZS": {"front": "CHRIS/LME_ZS1",  "second": "CHRIS/LME_ZS2"},
    "NI": {"front": "CHRIS/LME_NI1",  "second": "CHRIS/LME_NI2"},
    "PB": {"front": "CHRIS/LME_PB1",  "second": "CHRIS/LME_PB2"},
    "SN": {"front": "CHRIS/LME_SN1",  "second": "CHRIS/LME_SN2"},
    "GC": {"front": "CHRIS/CME_GC1",  "second": "CHRIS/CME_GC2"},
    "SI": {"front": "CHRIS/CME_SI1",  "second": "CHRIS/CME_SI2"},
    "PL": {"front": "CHRIS/CME_PL1",  "second": "CHRIS/CME_PL2"},
    "PA": {"front": "CHRIS/CME_PA1",  "second": "CHRIS/CME_PA2"},
}

SOURCE = "nasdaq_data_link_chris"

# Columns to extract from the CHRIS dataset
# CHRIS datasets have: Open, High, Low, Last, Change, Settle, Volume, Previous Day Open Interest
_SETTLE_COL = "Settle"
_VOLUME_COL = "Volume"
_OPEN_COL   = "Open"
_HIGH_COL   = "High"
_LOW_COL    = "Low"
_CLOSE_COL  = "Last"

# Minimum sensible settlement price — below this we treat the row as bad data
_MIN_PRICE = 0.01


# ─────────────────────────────────────────────────────────────────────────────
# FETCH
# ─────────────────────────────────────────────────────────────────────────────


def _fetch_nasdaq(
    nasdaq_code: str,
    start: date,
    end: date,
    api_key: str,
) -> pd.DataFrame:
    """
    Pull a Nasdaq Data Link CHRIS series and return a clean DataFrame
    indexed by date (pandas.Timestamp, tz-naive but representing UTC date).

    Returns empty DataFrame on any failure.
    """
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
        logger.error("Nasdaq Data Link fetch failed for %s: %s", nasdaq_code, exc)
        return pd.DataFrame()

    if df is None or df.empty:
        logger.warning("Nasdaq Data Link returned empty DataFrame for %s.", nasdaq_code)
        return pd.DataFrame()

    logger.info("Fetched %d rows for %s.", len(df), nasdaq_code)
    return df


# ─────────────────────────────────────────────────────────────────────────────
# VALIDATION
# ─────────────────────────────────────────────────────────────────────────────


def _validate_df(
    df: pd.DataFrame,
    ticker: str,
    nasdaq_code: str,
    today: date,
    strict: bool,
) -> tuple[pd.DataFrame, int]:
    """
    Validate the fetched DataFrame.

    Removes rows where:
      - Settle price is NaN or <= _MIN_PRICE
      - Date is in the future (beyond today)

    Returns (clean_df, n_dropped).
    """
    original_len = len(df)
    issues: list[str] = []

    # Ensure index is DatetimeIndex
    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index)

    # Drop rows with missing or zero settle
    if _SETTLE_COL not in df.columns:
        logger.warning(
            "%s (%s): 'Settle' column missing. Available: %s",
            ticker, nasdaq_code, list(df.columns),
        )
        if strict:
            raise ValueError(f"Settle column missing for {nasdaq_code}.")
        return pd.DataFrame(), original_len

    bad_price_mask = df[_SETTLE_COL].isna() | (df[_SETTLE_COL] <= _MIN_PRICE)
    n_bad_price = int(bad_price_mask.sum())
    if n_bad_price > 0:
        issues.append(f"{n_bad_price} rows with price <= {_MIN_PRICE} or NaN")
    df = df[~bad_price_mask]

    # Drop future-dated rows (data quality issue in some CHRIS series)
    future_mask = df.index.normalize() > pd.Timestamp(today)
    n_future = int(future_mask.sum())
    if n_future > 0:
        issues.append(f"{n_future} future-dated rows")
    df = df[~future_mask]

    if issues:
        msg = f"{ticker} ({nasdaq_code}): dropped — {'; '.join(issues)}."
        if strict:
            raise ValueError(msg)
        logger.warning(msg)

    n_dropped = original_len - len(df)
    return df, n_dropped


# ─────────────────────────────────────────────────────────────────────────────
# INSERT
# ─────────────────────────────────────────────────────────────────────────────


def _build_rows(
    df: pd.DataFrame,
    ticker: str,
    nasdaq_code: str,
    bar_size: str,
) -> list[dict]:
    """Convert a validated DataFrame into insert-ready row dicts."""
    rows = []
    for ts, row in df.iterrows():
        # Normalise timestamp to UTC midnight (daily bar)
        ts_utc = pd.Timestamp(ts).tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")
        ts_utc = ts_utc.normalize()  # midnight

        settle = float(row.get(_SETTLE_COL, float("nan")))
        volume = row.get(_VOLUME_COL)
        volume = float(volume) if volume is not None and not pd.isna(volume) else None

        rows.append(
            {
                "timestamp": ts_utc.to_pydatetime(),
                "ticker": ticker,
                "contract_code": nasdaq_code,
                "open_price": _safe_float(row.get(_OPEN_COL)),
                "high_price": _safe_float(row.get(_HIGH_COL)),
                "low_price": _safe_float(row.get(_LOW_COL)),
                "close_price": _safe_float(row.get(_CLOSE_COL)),
                "settle_price": settle,
                "volume": volume,
                "source": SOURCE,
                "bar_size": bar_size,
            }
        )
    return rows


def _safe_float(val: object) -> float | None:
    try:
        f = float(val)  # type: ignore[arg-type]
        return f if not pd.isna(f) else None
    except (TypeError, ValueError):
        return None


_INSERT_SQL = text(
    """
    INSERT INTO raw_data.futures_prices
        (timestamp, ticker, contract_code, open_price, high_price, low_price,
         close_price, settle_price, volume, source, bar_size)
    VALUES
        (:timestamp, :ticker, :contract_code, :open_price, :high_price, :low_price,
         :close_price, :settle_price, :volume, :source, :bar_size)
    ON CONFLICT (ticker, timestamp, source)
    DO NOTHING
    """
)


def _insert_rows(
    engine: Engine,
    rows: list[dict],
    dry_run: bool,
) -> tuple[int, int]:
    """
    Insert rows into raw_data.futures_prices.
    Returns (n_inserted, n_skipped).
    """
    if not rows:
        return 0, 0

    if dry_run:
        logger.info("DRY RUN: would attempt to insert %d rows (no DB writes).", len(rows))
        return 0, len(rows)

    inserted = 0
    skipped = 0
    try:
        with engine.begin() as conn:
            for row in rows:
                result = conn.execute(_INSERT_SQL, row)
                if result.rowcount > 0:
                    inserted += 1
                else:
                    skipped += 1
    except Exception:
        logger.exception("DB insert failed — partial data may be present.")
        raise

    return inserted, skipped


# ─────────────────────────────────────────────────────────────────────────────
# ORCHESTRATOR
# ─────────────────────────────────────────────────────────────────────────────


def load_ticker(
    ticker: str,
    start: date,
    end: date,
    engine: Engine,
    api_key: str,
    include_second: bool = False,
    dry_run: bool = False,
    strict: bool = False,
) -> tuple[int, int]:
    """
    Fetch and load one ticker's price data (front-month; optionally second-month).

    Returns (total_inserted, total_skipped).
    """
    codes = _NASDAQ_CODES.get(ticker)
    if codes is None:
        msg = f"No Nasdaq Data Link code for ticker {ticker!r}. Known: {list(_NASDAQ_CODES)}"
        if strict:
            raise KeyError(msg)
        logger.error(msg)
        return 0, 0

    today = date.today()
    total_inserted = 0
    total_skipped = 0

    series_to_load: list[tuple[str, str]] = [("front", codes["front"])]
    if include_second:
        series_to_load.append(("second", codes["second"]))

    for series_label, nasdaq_code in series_to_load:
        bar_size = f"daily_{series_label}"
        logger.info("Loading %s (%s) %s → %s …", ticker, nasdaq_code, start, end)

        df = _fetch_nasdaq(nasdaq_code, start, end, api_key)
        if df.empty:
            logger.warning("No data returned for %s — skipping.", nasdaq_code)
            continue

        df, n_dropped = _validate_df(df, ticker, nasdaq_code, today, strict)
        if df.empty:
            logger.warning("All rows failed validation for %s — skipping insert.", nasdaq_code)
            continue

        rows = _build_rows(df, ticker, nasdaq_code, bar_size)
        n_inserted, n_skipped = _insert_rows(engine, rows, dry_run)
        total_inserted += n_inserted
        total_skipped += n_skipped + n_dropped

        logger.info(
            "%s (%s): %d inserted, %d skipped (of %d total rows, %d dropped in validation).",
            ticker, nasdaq_code, n_inserted, n_skipped, len(df) + n_dropped, n_dropped,
        )

    return total_inserted, total_skipped


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Load Nasdaq Data Link futures prices into WorFIn DB.")

    ticker_group = p.add_mutually_exclusive_group(required=True)
    ticker_group.add_argument(
        "--ticker",
        metavar="TICKER",
        help="Single metal ticker (CA, AH, ZS, NI, PB, SN, GC, SI, PL, PA)",
    )
    ticker_group.add_argument(
        "--all",
        action="store_true",
        dest="all_tickers",
        help="Load all 10 metals",
    )

    p.add_argument(
        "--start",
        metavar="YYYY-MM-DD",
        default="2005-01-01",
        help="Start date (default: 2005-01-01)",
    )
    p.add_argument(
        "--end",
        metavar="YYYY-MM-DD",
        default=date.today().isoformat(),
        help="End date (default: today)",
    )
    p.add_argument(
        "--second",
        action="store_true",
        help="Also load the second-month series",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate but do not insert into the database",
    )
    p.add_argument(
        "--strict",
        action="store_true",
        help="Fail on any validation warning (default: warn and continue)",
    )
    p.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
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

    if start > end:
        logger.error("--start (%s) must be before --end (%s).", start, end)
        return 1

    settings = get_settings()
    if not settings.nasdaq_data_link_api_key:
        logger.error("NASDAQ_DATA_LINK_API_KEY not set. Cannot fetch data.")
        return 1

    engine = create_engine(settings.database_url, pool_pre_ping=True)

    tickers = list(ALL_METALS.keys()) if args.all_tickers else [args.ticker.upper()]

    for t in tickers:
        if t not in _NASDAQ_CODES:
            logger.error("Unknown ticker %r. Valid: %s", t, list(_NASDAQ_CODES))
            if args.strict:
                return 1
            continue

    grand_inserted = 0
    grand_skipped = 0

    for ticker in tickers:
        try:
            inserted, skipped = load_ticker(
                ticker=ticker,
                start=start,
                end=end,
                engine=engine,
                api_key=settings.nasdaq_data_link_api_key,
                include_second=args.second,
                dry_run=args.dry_run,
                strict=args.strict,
            )
            grand_inserted += inserted
            grand_skipped += skipped
        except Exception:
            logger.exception("Unhandled error loading %s — continuing.", ticker)
            if args.strict:
                return 1

    # ── Summary ──────────────────────────────────────────────────────────────
    action = "would insert" if args.dry_run else "inserted"
    logger.info(
        "=== LOAD COMPLETE ===  tickers=%s  %s=%d  skipped=%d",
        ", ".join(tickers),
        action,
        grand_inserted,
        grand_skipped,
    )
    print(f"\n{'[DRY RUN] ' if args.dry_run else ''}"
          f"Rows {action}: {grand_inserted:,}   Rows skipped: {grand_skipped:,}")
    return 0


if __name__ == "__main__":
    sys.exit(main())