#!/usr/bin/env python3
"""
scripts/load_market_data.py
Free market data loader — no API keys required.

SOURCES:
  Westmetall (westmetall.com)
    LME base metals: CA (Copper), AH (Aluminium), ZS (Zinc),
                     NI (Nickel), PB (Lead), SN (Tin)
    Data: official LME Cash + 3-Month settlement prices.
    Daily from January 2, 2008. Public HTML tables — no API key.
    robots.txt: no disallow rules.

  yfinance (finance.yahoo.com)
    COMEX precious metals: GC (Gold), SI (Silver), PL (Platinum), PA (Palladium)
    Data: continuous front-month contract.
    Daily from ~2000. Second-month approximated via carry offset.
    No API key.

WHY WESTMETALL IS GOOD FOR CARRY:
  Westmetall provides the official LME Cash and 3-Month settlement prices —
  the true LME term structure used in physical contracts.

  LME Cash (T+2 delivery)  → stored as contract_type='front'  → cash_price in S4
  LME 3M  (91-day forward) → stored as contract_type='second' → f3m_price in S4

  This gives S4 the REAL LME carry signal, not a generic contract approximation.
  The `get_lme_3m_dte()` function in calendar.py computes the exact daily DTE.

DATA COVERAGE vs NASDAQ CHRIS:
  Westmetall:  Jan 2008 → present   (IS period: 2008–2017, OOS: 2018–2022)
  Nasdaq CHRIS: Jan 2004 → present  (IS period: 2005–2017, OOS: 2018–2022)
  yfinance:    ~2000 → present

  The IS backtest will effectively start from 2008 when using Westmetall for
  LME metals. 10 years of IS data (2008–2017) is still statistically meaningful.

USAGE:
  python scripts/load_market_data.py --all
  python scripts/load_market_data.py --lme                # LME metals only
  python scripts/load_market_data.py --comex              # COMEX metals only
  python scripts/load_market_data.py --all --start 2008-01-01 --end 2023-12-31
  python scripts/load_market_data.py --all --dry-run      # validate, no DB writes

IDEMPOTENCY:
  Safe to re-run. Uses WHERE NOT EXISTS — already-present rows are skipped.
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import UTC, date, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from worfin.config.logging_config import configure_logging

configure_logging()

import pandas as pd
import requests
import yfinance as yf
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from worfin.config.settings import get_settings

logger = logging.getLogger("load_market_data")


# ─────────────────────────────────────────────────────────────────────────────
# METAL SPECS
# ─────────────────────────────────────────────────────────────────────────────

# Westmetall field names (confirmed by inspection April 2026)
_WESTMETALL_FIELDS: dict[str, str] = {
    "CA": "LME_Cu_cash",  # Copper
    "AH": "LME_Al_cash",  # Aluminium
    "ZS": "LME_Zn_cash",  # Zinc
    "NI": "LME_Ni_cash",  # Nickel
    "PB": "LME_Pb_cash",  # Lead
    "SN": "LME_Sn_cash",  # Tin
}

_WESTMETALL_BASE_URL = "https://www.westmetall.com/en/markdaten.php"
_WESTMETALL_SOURCE = "westmetall"

# yfinance continuous contract symbols + typical carry (annualised)
_YFINANCE_MAP: dict[str, dict] = {
    "GC": {"symbol": "GC=F", "carry_approx_annual": -0.008},  # Gold
    "SI": {"symbol": "SI=F", "carry_approx_annual": -0.010},  # Silver
    "PL": {"symbol": "PL=F", "carry_approx_annual": -0.012},  # Platinum
    "PA": {"symbol": "PA=F", "carry_approx_annual": -0.015},  # Palladium
}

_YFINANCE_SOURCE = "yfinance"

# Polite request headers — identify ourselves to Westmetall
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; WorFIn-Research-Bot/1.0; "
        "systematic-metals-research; contact: research@worfin.com)"
    ),
    "Accept-Language": "en-GB,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml",
}

# Seconds to wait between Westmetall requests (be a good citizen)
_WESTMETALL_REQUEST_DELAY = 2.0


# ─────────────────────────────────────────────────────────────────────────────
# DB INSERT
#
# Columns match raw_data.futures_prices from 001_schema.py:
#   price_timestamp, ticker, contract_type, bar_size,
#   open, high, low, close, volume, open_interest, source, fetched_at
#
# Idempotency: WHERE NOT EXISTS (no unique constraint on the table).
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


def _insert_rows(
    engine: Engine,
    rows: list[dict],
    dry_run: bool,
    label: str,
) -> tuple[int, int]:
    """Insert a list of row dicts. Returns (inserted, skipped)."""
    if not rows:
        return 0, 0
    if dry_run:
        logger.info("DRY RUN: %d rows for %s — no DB writes.", len(rows), label)
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


def _safe_float(val: object) -> float | None:
    try:
        f = float(val)  # type: ignore[arg-type]
        return None if f != f else f  # NaN → None
    except (TypeError, ValueError):
        return None


# ─────────────────────────────────────────────────────────────────────────────
# WESTMETALL SCRAPER
# ─────────────────────────────────────────────────────────────────────────────


def _fetch_westmetall_html(field: str) -> str | None:
    """
    Fetch the full HTML page for one Westmetall field.
    Returns raw HTML string, or None on failure.
    """
    url = f"{_WESTMETALL_BASE_URL}?action=table&field={field}"
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=30)
        resp.raise_for_status()
        return resp.text
    except requests.RequestException as exc:
        logger.error("Westmetall fetch failed for field=%s: %s", field, exc)
        return None


def _parse_westmetall_html(html: str, ticker: str) -> pd.DataFrame:
    """
    Parse all annual tables from a Westmetall HTML page.

    The page contains one <table> per calendar year (newest first).
    Each table has columns: date | {Metal} Cash-Settlement | {Metal} 3-month | {Metal} stock

    Returns a clean DataFrame with columns:
        date        — pd.Timestamp (UTC, midnight)
        cash_price  — float (USD/tonne, LME Cash settlement)
        f3m_price   — float (USD/tonne, LME 3-Month settlement)
        inventory   — float (tonnes on warrant, informational only)

    Rows are sorted oldest-first.
    """
    try:
        # pandas.read_html returns one DataFrame per <table>
        # thousands=',' strips the comma thousands separator from numbers
        tables = pd.read_html(html, thousands=",", flavor="html5lib")
    except Exception as exc:
        logger.error("%s: failed to parse Westmetall HTML: %s", ticker, exc)
        return pd.DataFrame()

    # Filter to tables with exactly 4 columns and a date-like first column
    valid_tables: list[pd.DataFrame] = []
    for tbl in tables:
        if tbl.shape[1] != 4:
            continue
        # Rename columns by position — headers vary by metal but structure is fixed
        tbl.columns = ["date_raw", "cash_price", "f3m_price", "inventory"]
        # Keep only rows where date_raw looks like a date (contains a digit and a dot)
        date_mask = tbl["date_raw"].astype(str).str.match(r"^\d{1,2}\. \w+ \d{4}$")
        tbl = tbl[date_mask].copy()
        if len(tbl) > 0:
            valid_tables.append(tbl)

    if not valid_tables:
        logger.error("%s: no valid annual tables found in Westmetall page.", ticker)
        return pd.DataFrame()

    df = pd.concat(valid_tables, ignore_index=True)

    # Parse dates — format: "17. April 2026" (English month names)
    def _parse_date(s: str) -> pd.Timestamp | None:
        try:
            return pd.Timestamp(datetime.strptime(str(s).strip(), "%d. %B %Y"), tz="UTC")
        except ValueError:
            try:
                # Handle abbreviated months if they appear
                return pd.Timestamp(pd.to_datetime(str(s).strip(), dayfirst=True, utc=True))
            except Exception:
                return None

    df["date"] = df["date_raw"].map(_parse_date)
    df = df.dropna(subset=["date"])

    # Coerce price columns to numeric (read_html with thousands=',' should handle this,
    # but belt-and-braces for any edge cases)
    for col in ["cash_price", "f3m_price", "inventory"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Drop rows with missing prices
    df = df.dropna(subset=["cash_price", "f3m_price"])
    df = df[df["cash_price"] > 0]

    # Deduplicate (same date might appear in multiple yearly tables at year boundaries)
    df = df.sort_values("date").drop_duplicates(subset=["date"]).reset_index(drop=True)

    logger.info(
        "%s: parsed %d rows from Westmetall (%s → %s)",
        ticker,
        len(df),
        df["date"].min().date() if len(df) else "?",
        df["date"].max().date() if len(df) else "?",
    )
    return df[["date", "cash_price", "f3m_price", "inventory"]]


def _westmetall_to_rows(
    df: pd.DataFrame,
    ticker: str,
    start: date,
    end: date,
    fetched_at: datetime,
) -> tuple[list[dict], list[dict]]:
    """
    Convert Westmetall DataFrame into two lists of DB row dicts:
        (front_rows, second_rows)

    LME Cash  → contract_type='front'  (the carry numerator / momentum series)
    LME 3M    → contract_type='second' (the carry denominator)
    """
    start_ts = pd.Timestamp(start, tz="UTC")
    end_ts = pd.Timestamp(end, tz="UTC") + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)

    mask = (df["date"] >= start_ts) & (df["date"] <= end_ts)
    df_range = df[mask].copy()

    if df_range.empty:
        return [], []

    front_rows: list[dict] = []
    second_rows: list[dict] = []

    for _, row in df_range.iterrows():
        ts = row["date"].to_pydatetime()
        base = {
            "ticker": ticker,
            "bar_size": "daily",
            "open": None,  # Westmetall provides settlement only, no OHLC
            "high": None,
            "low": None,
            "volume": None,
            "open_interest": None,
            "source": _WESTMETALL_SOURCE,
            "fetched_at": fetched_at,
        }

        cash = _safe_float(row["cash_price"])
        f3m = _safe_float(row["f3m_price"])

        if cash and cash > 0:
            front_rows.append(
                {
                    **base,
                    "price_timestamp": ts,
                    "contract_type": "front",
                    "close": cash,
                }
            )

        if f3m and f3m > 0:
            second_rows.append(
                {
                    **base,
                    "price_timestamp": ts,
                    "contract_type": "second",
                    "close": f3m,
                }
            )

    return front_rows, second_rows


def load_lme_metals(
    tickers: list[str],
    start: date,
    end: date,
    engine: Engine,
    dry_run: bool = False,
) -> tuple[int, int]:
    """
    Scrape Westmetall and load LME metals into the DB.
    Returns (total_inserted, total_skipped).
    """
    grand_ins = grand_skp = 0
    fetched_at = datetime.now(UTC)

    for i, ticker in enumerate(tickers):
        field = _WESTMETALL_FIELDS.get(ticker)
        if not field:
            logger.error("No Westmetall field for %s — skipping.", ticker)
            continue

        logger.info(
            "[%d/%d] Fetching %s from Westmetall (field=%s) …", i + 1, len(tickers), ticker, field
        )

        html = _fetch_westmetall_html(field)
        if not html:
            logger.warning("%s: no HTML returned — skipping.", ticker)
            continue

        df = _parse_westmetall_html(html, ticker)
        if df.empty:
            logger.warning("%s: no data parsed — skipping.", ticker)
            continue

        front_rows, second_rows = _westmetall_to_rows(df, ticker, start, end, fetched_at)

        fi, fs = _insert_rows(engine, front_rows, dry_run, f"{ticker}[front]")
        si, ss = _insert_rows(engine, second_rows, dry_run, f"{ticker}[second]")

        logger.info(
            "%s: front=%d inserted/%d skipped | second=%d inserted/%d skipped",
            ticker,
            fi,
            fs,
            si,
            ss,
        )
        grand_ins += fi + si
        grand_skp += fs + ss

        # Be polite to Westmetall — small delay between requests
        if i < len(tickers) - 1:
            logger.debug(
                "Waiting %.1fs before next Westmetall request …", _WESTMETALL_REQUEST_DELAY
            )
            time.sleep(_WESTMETALL_REQUEST_DELAY)

    return grand_ins, grand_skp


# ─────────────────────────────────────────────────────────────────────────────
# YFINANCE LOADER (COMEX PRECIOUS METALS)
# ─────────────────────────────────────────────────────────────────────────────


def _fetch_yfinance(symbol: str, start: date, end: date) -> pd.DataFrame:
    """
    Fetch daily OHLCV from yfinance for a continuous futures symbol.
    Returns empty DataFrame on failure.
    """
    try:
        df = yf.Ticker(symbol).history(
            start=start.isoformat(),
            end=end.isoformat(),
            interval="1d",
            auto_adjust=True,
            back_adjust=False,
        )
        if df.empty:
            logger.warning("%s: yfinance returned empty DataFrame.", symbol)
            return pd.DataFrame()

        if df.index.tz is None:
            df.index = df.index.tz_localize("UTC")
        else:
            df.index = df.index.tz_convert("UTC")
        df.index = df.index.normalize()

        logger.info(
            "%s: fetched %d rows (%s → %s)",
            symbol,
            len(df),
            df.index.min().date(),
            df.index.max().date(),
        )
        return df
    except Exception as exc:
        logger.error("%s: yfinance fetch failed: %s", symbol, exc)
        return pd.DataFrame()


def _yfinance_to_rows(
    df: pd.DataFrame,
    ticker: str,
    carry_approx_annual: float,
    fetched_at: datetime,
) -> tuple[list[dict], list[dict]]:
    """
    Convert yfinance DataFrame to (front_rows, second_rows).

    Front:  yfinance Close → contract_type='front'
    Second: Close × (1 - carry_annual × 91/365) → contract_type='second'
    """
    front_rows: list[dict] = []
    second_rows: list[dict] = []

    for ts, row in df.iterrows():
        close = _safe_float(row.get("Close"))
        if close is None or close <= 0:
            continue

        ts_dt = ts.to_pydatetime()
        base = {
            "ticker": ticker,
            "bar_size": "daily",
            "open": _safe_float(row.get("Open")),
            "high": _safe_float(row.get("High")),
            "low": _safe_float(row.get("Low")),
            "volume": _safe_float(row.get("Volume")),
            "open_interest": None,
            "source": _YFINANCE_SOURCE,
            "fetched_at": fetched_at,
        }

        front_rows.append(
            {
                **base,
                "price_timestamp": ts_dt,
                "contract_type": "front",
                "close": close,
            }
        )

        # Approximate second-month via carry offset
        # carry_approx_annual < 0 → contango (f2 slightly above front)
        second_close = close * (1.0 - carry_approx_annual * (91.0 / 365.0))
        second_rows.append(
            {
                **base,
                "price_timestamp": ts_dt,
                "contract_type": "second",
                "close": second_close,
                "open": None,
                "high": None,
                "low": None,  # no OHLC for approximated second
            }
        )

    return front_rows, second_rows


def load_comex_metals(
    tickers: list[str],
    start: date,
    end: date,
    engine: Engine,
    dry_run: bool = False,
) -> tuple[int, int]:
    """
    Fetch yfinance data and load COMEX metals into the DB.
    Returns (total_inserted, total_skipped).
    """
    grand_ins = grand_skp = 0
    fetched_at = datetime.now(UTC)

    for i, ticker in enumerate(tickers):
        spec = _YFINANCE_MAP.get(ticker)
        if not spec:
            logger.error("No yfinance spec for %s — skipping.", ticker)
            continue

        logger.info(
            "[%d/%d] Fetching %s (%s) from yfinance …", i + 1, len(tickers), ticker, spec["symbol"]
        )

        df = _fetch_yfinance(spec["symbol"], start, end)
        if df.empty:
            logger.warning("%s: no data from yfinance — skipping.", ticker)
            continue

        front_rows, second_rows = _yfinance_to_rows(
            df, ticker, spec["carry_approx_annual"], fetched_at
        )

        fi, fs = _insert_rows(engine, front_rows, dry_run, f"{ticker}[front]")
        si, ss = _insert_rows(engine, second_rows, dry_run, f"{ticker}[second]")

        logger.info(
            "%s: front=%d inserted/%d skipped | second=%d inserted/%d skipped",
            ticker,
            fi,
            fs,
            si,
            ss,
        )
        grand_ins += fi + si
        grand_skp += fs + ss

    return grand_ins, grand_skp


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Load free market data: Westmetall (LME) + yfinance (COMEX).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python scripts/load_market_data.py --all\n"
            "  python scripts/load_market_data.py --lme\n"
            "  python scripts/load_market_data.py --comex --start 2010-01-01\n"
            "  python scripts/load_market_data.py --all --dry-run\n"
        ),
    )
    source_group = p.add_mutually_exclusive_group(required=True)
    source_group.add_argument(
        "--all",
        action="store_true",
        help="Load both LME metals (Westmetall) and COMEX metals (yfinance)",
    )
    source_group.add_argument(
        "--lme",
        action="store_true",
        help="Load LME metals only (CA, AH, ZS, NI, PB, SN) via Westmetall",
    )
    source_group.add_argument(
        "--comex",
        action="store_true",
        help="Load COMEX metals only (GC, SI, PL, PA) via yfinance",
    )
    p.add_argument(
        "--start",
        default="2008-01-01",
        metavar="YYYY-MM-DD",
        help=(
            "Start date (default: 2008-01-01 — Westmetall's earliest date). "
            "Westmetall data before 2008-01-01 does not exist. "
            "yfinance goes back further if needed."
        ),
    )
    p.add_argument(
        "--end",
        default=date.today().isoformat(),
        metavar="YYYY-MM-DD",
        help="End date (default: today)",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse and validate data without writing to the DB",
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
        logger.error("Invalid date format: %s", exc)
        return 1

    if start > end:
        logger.error("--start must be before --end")
        return 1

    # ── Database connection ────────────────────────────────────────────────
    settings = get_settings()
    engine = create_engine(settings.database_url, pool_pre_ping=True)
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("Database connected: %s", settings.database_url.split("@")[-1])
    except Exception as exc:
        logger.error("Cannot connect to database: %s", exc)
        return 2

    grand_ins = grand_skp = 0

    # ── LME via Westmetall ─────────────────────────────────────────────────
    if args.all or args.lme:
        lme_tickers = list(_WESTMETALL_FIELDS.keys())
        logger.info(
            "Loading %d LME metals from Westmetall: %s",
            len(lme_tickers),
            ", ".join(lme_tickers),
        )
        if start < date(2008, 1, 2):
            logger.warning(
                "Westmetall data starts 2008-01-02. Rows before that date will simply not exist."
            )

        lme_ins, lme_skp = load_lme_metals(
            tickers=lme_tickers,
            start=start,
            end=end,
            engine=engine,
            dry_run=args.dry_run,
        )
        grand_ins += lme_ins
        grand_skp += lme_skp
        logger.info("LME total: %d inserted, %d skipped", lme_ins, lme_skp)

    # ── COMEX via yfinance ─────────────────────────────────────────────────
    if args.all or args.comex:
        comex_tickers = list(_YFINANCE_MAP.keys())
        logger.info(
            "Loading %d COMEX metals from yfinance: %s",
            len(comex_tickers),
            ", ".join(comex_tickers),
        )
        comex_ins, comex_skp = load_comex_metals(
            tickers=comex_tickers,
            start=start,
            end=end,
            engine=engine,
            dry_run=args.dry_run,
        )
        grand_ins += comex_ins
        grand_skp += comex_skp
        logger.info("COMEX total: %d inserted, %d skipped", comex_ins, comex_skp)

    # ── Summary ────────────────────────────────────────────────────────────
    action = "would insert" if args.dry_run else "inserted"
    print(f"\n{'─' * 60}")
    print(f"  {'[DRY RUN] ' if args.dry_run else ''}Load complete")
    print(f"  Rows {action}: {grand_ins:,}")
    print(f"  Rows skipped:  {grand_skp:,}")
    print(f"{'─' * 60}")

    if not args.dry_run:
        print("\nVerify the data landed:")
        print(
            "  psql -U worfin -d worfin_research -h localhost -c \\\n"
            '  "SELECT ticker, contract_type, source, COUNT(*), '
            "MIN(price_timestamp)::date, MAX(price_timestamp)::date\n"
            "   FROM raw_data.futures_prices\n"
            "   GROUP BY ticker, contract_type, source\n"
            '   ORDER BY source, ticker, contract_type;"'
        )
        print(
            "\nThen run the backtest:\n"
            "  python scripts/run_backtest.py --strategy S4 --period IS --no-pretrade --plot"
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())
