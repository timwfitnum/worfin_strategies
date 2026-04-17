"""
data/ingestion/fx_rates.py
USD/GBP FX rate fetcher — primary source: FRED DEXUSUK.

DESIGN:
  The FRED public CSV endpoint requires no API key:
    https://fred.stlouisfed.org/graph/fredgraph.csv?id=DEXUSUK
  DEXUSUK is expressed as USD per 1 GBP, matching our internal convention.

LOOKUP CHAIN (get_usd_gbp):
  1. In-process cache (LRU, per-date, cleared at process start)
  2. DB  — raw_data.fx_rates  (idempotent insert; shared across processes)
  3. FRED API (public CSV endpoint, no key needed)
  4. Prior-business-day fallback — logs INFO and writes to
     audit.data_quality_flags so the dashboard can surface it.
  Raises FxRateUnavailable if nothing found within FX_RATE_MAX_STALENESS_DAYS.

CONVENTIONS:
  - pair = "USDGBP"  meaning "1 GBP = X USD"
  - rate > 0 always  (FRED sometimes emits a '.' for holidays → skipped)
  - All DB writes use NUMERIC(14,8) — never store float

USAGE:
  from worfin.data.ingestion.fx_rates import get_usd_gbp
  rate = get_usd_gbp(date.today(), engine=engine)
"""

from __future__ import annotations

import logging
from datetime import UTC, date, datetime, timedelta
from functools import lru_cache

import pandas as pd
import requests
from sqlalchemy import text
from sqlalchemy.engine import Engine

from worfin.risk.limits import FX_RATE_MAX_STALENESS_DAYS

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────

FRED_CSV_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv"
FRED_SERIES = "DEXUSUK"
PAIR = "USDGBP"
SOURCE = "FRED"
_REQUEST_TIMEOUT_SECONDS = 10
_FRED_MISSING_SENTINEL = "."   # FRED emits '.' for days it has no fixing


# ─────────────────────────────────────────────────────────────────────────────
# EXCEPTIONS
# ─────────────────────────────────────────────────────────────────────────────


class FxRateUnavailable(RuntimeError):
    """
    Raised when no USD/GBP rate can be found within the staleness window.

    Callers MUST handle this — a missing FX rate means all GBP/USD conversions
    are wrong. Block the trade and alert.
    """

    def __init__(self, for_date: date, staleness_days: int) -> None:
        super().__init__(
            f"No USD/GBP rate available for {for_date} or within the prior "
            f"{staleness_days} calendar days. "
            f"Check FRED connectivity and audit.data_quality_flags."
        )
        self.for_date = for_date
        self.staleness_days = staleness_days


# ─────────────────────────────────────────────────────────────────────────────
# IN-PROCESS CACHE
# ─────────────────────────────────────────────────────────────────────────────
# LRU keyed on date string — stays warm for the lifetime of the process.
# Each backtest bar only hits the DB once per date; live trading hits FRED once
# per day. Cache is automatically wiped when the process restarts.


@lru_cache(maxsize=2000)  # 2000 trading days ≈ 8 years — ample for backtest
def _cached_rate(date_iso: str) -> float | None:
    """
    Internal: returns the cached rate for a date-string, or None if not seen.
    Populated lazily by get_usd_gbp — do NOT call directly.
    """
    return None  # initial miss; filled by the cache-aside pattern below


def _set_cache(date_iso: str, rate: float) -> None:
    """Store rate in the LRU cache by clearing the stale entry first."""
    # lru_cache has no direct setter; we use a side-channel dict for writes.
    _rate_store[date_iso] = rate


# Side-channel dict that backs the cache (lru_cache cannot be written to directly)
_rate_store: dict[str, float] = {}


def _get_from_cache(for_date: date) -> float | None:
    return _rate_store.get(for_date.isoformat())


# ─────────────────────────────────────────────────────────────────────────────
# DB HELPERS
# ─────────────────────────────────────────────────────────────────────────────


def _get_from_db(for_date: date, engine: Engine) -> float | None:
    """
    Look up the most-recent available rate on or before `for_date`.
    Returns None if no row found.
    """
    sql = text(
        """
        SELECT rate
        FROM raw_data.fx_rates
        WHERE pair = :pair
          AND as_of_date <= :d
        ORDER BY as_of_date DESC
        LIMIT 1
        """
    )
    with engine.connect() as conn:
        row = conn.execute(sql, {"pair": PAIR, "d": for_date}).fetchone()
    if row is None:
        return None
    rate = float(row[0])
    if rate <= 0:
        logger.warning("DB returned non-positive FX rate %.6f for %s — ignoring.", rate, for_date)
        return None
    return rate


def _write_to_db(engine: Engine, as_of_date: date, rate: float) -> None:
    """
    Idempotent upsert of a single FX rate into raw_data.fx_rates.
    On conflict (pair, as_of_date, source) — do nothing (source-of-truth wins).
    """
    sql = text(
        """
        INSERT INTO raw_data.fx_rates
            (as_of_date, pair, rate, source, source_series_id, bar_size)
        VALUES
            (:d, :pair, :rate, :source, :series, 'daily')
        ON CONFLICT (pair, as_of_date, source) DO NOTHING
        """
    )
    try:
        with engine.begin() as conn:
            conn.execute(
                sql,
                {
                    "d": as_of_date,
                    "pair": PAIR,
                    "rate": rate,
                    "source": SOURCE,
                    "series": FRED_SERIES,
                },
            )
    except Exception:
        logger.exception("Failed to persist FX rate %.6f for %s to DB — continuing.", rate, as_of_date)


def _write_data_quality_flag(
    engine: Engine,
    for_date: date,
    fallback_date: date,
    rate: float,
) -> None:
    """
    Record a prior-day fallback in audit.data_quality_flags so the monitoring
    dashboard can surface it. Best-effort — failures are logged but not raised.
    """
    sql = text(
        """
        INSERT INTO audit.data_quality_flags
            (flagged_at, flag_type, ticker, as_of_date, detail)
        VALUES
            (NOW(), 'fx_rate_fallback', :pair, :d,
             :detail)
        ON CONFLICT DO NOTHING
        """
    )
    detail = (
        f"FRED DEXUSUK unavailable for {for_date}. "
        f"Using prior business day {fallback_date} rate={rate:.6f}."
    )
    try:
        with engine.begin() as conn:
            conn.execute(sql, {"pair": PAIR, "d": for_date, "detail": detail})
        logger.debug("data_quality_flag written for fx_rate_fallback on %s.", for_date)
    except Exception:
        # The audit table may not exist yet (pre-migration) — don't crash the caller.
        logger.warning(
            "Could not write data_quality_flag for %s (table may not exist). Detail: %s",
            for_date,
            detail,
        )


# ─────────────────────────────────────────────────────────────────────────────
# FRED FETCHER
# ─────────────────────────────────────────────────────────────────────────────


def _fetch_from_fred(for_date: date) -> dict[date, float]:
    """
    Fetch the most-recent FRED DEXUSUK observations via the public CSV endpoint.
    Returns a dict of {date: rate}. Empty on any failure (caller handles).

    Uses the public endpoint — no API key required:
      https://fred.stlouisfed.org/graph/fredgraph.csv?id=DEXUSUK
    Filters to the 30 days surrounding `for_date` so the response is small.
    """
    start = (for_date - timedelta(days=30)).isoformat()
    params = {
        "id": FRED_SERIES,
        "vintage_date": for_date.isoformat(),  # as-of-date snapshot
        "observation_start": start,
        "observation_end": for_date.isoformat(),
    }
    try:
        resp = requests.get(
            FRED_CSV_URL,
            params=params,
            timeout=_REQUEST_TIMEOUT_SECONDS,
            headers={"User-Agent": "WorFIn-MetalsTrading/1.0"},
        )
        resp.raise_for_status()
    except requests.RequestException as exc:
        logger.warning("FRED request failed for %s: %s", for_date, exc)
        return {}

    lines = resp.text.strip().splitlines()
    if len(lines) < 2:
        logger.warning("FRED response too short for %s (lines=%d).", for_date, len(lines))
        return {}

    result: dict[date, float] = {}
    # First line is header "DATE,DEXUSUK"
    for line in lines[1:]:
        parts = line.strip().split(",")
        if len(parts) != 2:
            continue
        date_str, value_str = parts[0].strip(), parts[1].strip()
        if value_str == _FRED_MISSING_SENTINEL or not value_str:
            continue  # holiday or missing observation
        try:
            obs_date = date.fromisoformat(date_str)
            rate = float(value_str)
            if rate > 0:
                result[obs_date] = rate
        except (ValueError, TypeError):
            continue

    logger.debug("FRED returned %d valid DEXUSUK observations for window ending %s.", len(result), for_date)
    return result


# ─────────────────────────────────────────────────────────────────────────────
# PUBLIC API
# ─────────────────────────────────────────────────────────────────────────────


def get_usd_gbp(
    for_date: date,
    engine: Engine | None = None,
) -> float:
    """
    Return the USD/GBP rate for `for_date` (1 GBP = X USD, e.g. 1.27).

    Lookup chain:
      1. In-process LRU cache (free, instant)
      2. DB — raw_data.fx_rates  (fast, shared between processes)
      3. FRED public CSV API     (network; no API key required)
      4. Prior-business-day fallback within FX_RATE_MAX_STALENESS_DAYS

    On fallback: logs INFO and writes to audit.data_quality_flags.
    Raises FxRateUnavailable if nothing found within the staleness window.

    Args:
        for_date:  The date the rate is needed for (typically bar date).
        engine:    SQLAlchemy engine. If None, skips DB layers (cache+FRED only).

    Returns:
        float — rate > 0 (1 GBP = X USD)
    """
    # ── 1. In-process cache ──────────────────────────────────────────────────
    cached = _get_from_cache(for_date)
    if cached is not None:
        return cached

    # ── 2. DB lookup ─────────────────────────────────────────────────────────
    if engine is not None:
        db_rate = _get_from_db(for_date, engine)
        if db_rate is not None:
            _set_cache(for_date.isoformat(), db_rate)
            logger.debug("FX rate for %s: %.6f (from DB).", for_date, db_rate)
            return db_rate

    # ── 3. FRED fetch ─────────────────────────────────────────────────────────
    fred_obs = _fetch_from_fred(for_date)

    if for_date in fred_obs:
        rate = fred_obs[for_date]
        _set_cache(for_date.isoformat(), rate)
        if engine is not None:
            _write_to_db(engine, for_date, rate)
        logger.debug("FX rate for %s: %.6f (from FRED).", for_date, rate)
        return rate

    # ── 4. Prior-business-day fallback ────────────────────────────────────────
    # Walk backwards through FRED results (or DB) up to MAX_STALENESS days.
    # Prefer results already fetched from FRED (no extra network call).
    staleness = FX_RATE_MAX_STALENESS_DAYS
    for days_back in range(1, staleness + 1):
        fallback_date = for_date - timedelta(days=days_back)

        # Check FRED results we already have
        if fallback_date in fred_obs:
            rate = fred_obs[fallback_date]
            logger.info(
                "USD/GBP rate unavailable for %s — using prior day %s (%.6f). "
                "days_back=%d/%d.",
                for_date,
                fallback_date,
                rate,
                days_back,
                staleness,
            )
            _set_cache(for_date.isoformat(), rate)
            if engine is not None:
                _write_to_db(engine, fallback_date, rate)
                _write_data_quality_flag(engine, for_date, fallback_date, rate)
            return rate

        # Check DB for this fallback date
        if engine is not None:
            db_fallback = _get_from_db(fallback_date, engine)
            if db_fallback is not None:
                logger.info(
                    "USD/GBP rate unavailable for %s — using DB prior day %s (%.6f). "
                    "days_back=%d/%d.",
                    for_date,
                    fallback_date,
                    db_fallback,
                    days_back,
                    staleness,
                )
                _set_cache(for_date.isoformat(), db_fallback)
                _write_data_quality_flag(engine, for_date, fallback_date, db_fallback)
                return db_fallback

    # Nothing found within the staleness window
    raise FxRateUnavailable(for_date, staleness)


def prefetch_fx_rates(
    start_date: date,
    end_date: date,
    engine: Engine | None = None,
) -> dict[date, float]:
    """
    Bulk-fetch USD/GBP rates for a date range and warm the cache.

    Useful at the start of a backtest run to avoid per-bar network calls.
    Fetches the entire range from FRED in one request, persists to DB, and
    populates the in-process cache.

    Args:
        start_date: First date in range (inclusive)
        end_date:   Last date in range (inclusive)
        engine:     SQLAlchemy engine for DB persistence (optional)

    Returns:
        {date: rate} for all dates where FRED had a value.
    """
    params = {
        "id": FRED_SERIES,
        "observation_start": start_date.isoformat(),
        "observation_end": end_date.isoformat(),
    }
    try:
        resp = requests.get(
            FRED_CSV_URL,
            params=params,
            timeout=30,
            headers={"User-Agent": "WorFIn-MetalsTrading/1.0"},
        )
        resp.raise_for_status()
    except requests.RequestException as exc:
        logger.error("FRED bulk fetch failed: %s. Cache not warmed.", exc)
        return {}

    lines = resp.text.strip().splitlines()
    result: dict[date, float] = {}
    for line in lines[1:]:
        parts = line.strip().split(",")
        if len(parts) != 2:
            continue
        date_str, value_str = parts[0].strip(), parts[1].strip()
        if value_str == _FRED_MISSING_SENTINEL or not value_str:
            continue
        try:
            obs_date = date.fromisoformat(date_str)
            rate = float(value_str)
            if rate > 0:
                result[obs_date] = rate
                _set_cache(obs_date.isoformat(), rate)
        except (ValueError, TypeError):
            continue

    if engine is not None:
        _bulk_write_to_db(engine, result)

    logger.info(
        "Prefetched %d USD/GBP rates for %s → %s.",
        len(result),
        start_date,
        end_date,
    )
    return result


def _bulk_write_to_db(engine: Engine, rates: dict[date, float]) -> None:
    """Idempotent bulk insert of multiple FX rates."""
    if not rates:
        return
    rows = [
        {"d": d, "pair": PAIR, "rate": r, "source": SOURCE, "series": FRED_SERIES}
        for d, r in rates.items()
    ]
    sql = text(
        """
        INSERT INTO raw_data.fx_rates
            (as_of_date, pair, rate, source, source_series_id, bar_size)
        VALUES
            (:d, :pair, :rate, :source, :series, 'daily')
        ON CONFLICT (pair, as_of_date, source) DO NOTHING
        """
    )
    try:
        with engine.begin() as conn:
            conn.execute(sql, rows)
        logger.debug("Bulk-inserted %d FX rows into raw_data.fx_rates.", len(rows))
    except Exception:
        logger.exception("Bulk DB write for FX rates failed — rates still in cache.")