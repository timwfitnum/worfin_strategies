"""
data/ingestion/fred.py
FRED fetcher for macro + FX data.

Primary use: DEXUSUK — U.S. Dollars to One British Pound (daily fixing).

Writes to raw_data.fx_rates (append-only; never modifies historical rows).
"""

from __future__ import annotations

import logging
from datetime import date
from decimal import Decimal

import pandas as pd
from fredapi import Fred
from sqlalchemy import text
from sqlalchemy.engine import Engine

from worfin.config.settings import get_settings

logger = logging.getLogger(__name__)

# FRED series IDs
DEXUSUK = "DEXUSUK"  # USD per GBP — daily fixing, noon NY time


def _get_fred_client() -> Fred:
    settings = get_settings()
    if not settings.fred_api_key:
        raise ValueError(
            "FRED_API_KEY not set in .env — register at "
            "https://fred.stlouisfed.org/docs/api/api_key.html"
        )
    return Fred(api_key=settings.fred_api_key)


def fetch_usd_gbp(
    start_date: date,
    end_date: date | None = None,
) -> pd.DataFrame:
    """
    Fetch USD/GBP daily series from FRED.

    DEXUSUK is quoted as 'US Dollars to One British Pound' — so a value of
    1.27 means 1 GBP = 1.27 USD. This is the rate we multiply GBP amounts
    by to get USD, or divide USD amounts by to get GBP.

    Args:
        start_date: First date to fetch
        end_date:   Last date to fetch (defaults to today)

    Returns:
        DataFrame with columns [as_of_date, pair, rate, source, source_series_id]
        Empty DataFrame on fetch failure.
    """
    if end_date is None:
        end_date = date.today()

    try:
        fred = _get_fred_client()
        logger.info("Fetching DEXUSUK %s → %s", start_date, end_date)
        series = fred.get_series(
            DEXUSUK,
            observation_start=start_date.isoformat(),
            observation_end=end_date.isoformat(),
        )
        series = series.dropna()
        if series.empty:
            logger.warning("FRED returned empty series for DEXUSUK %s → %s", start_date, end_date)
            return pd.DataFrame()

        df = pd.DataFrame(
            {
                "as_of_date": [
                    d.date() if isinstance(d, pd.Timestamp) else d for d in series.index
                ],
                "pair": "USDGBP",
                "rate": series.values.astype(float),
                "source": "FRED",
                "source_series_id": DEXUSUK,
            }
        )
        logger.info(
            "Fetched %d FX observations (%s → %s)",
            len(df),
            df["as_of_date"].min(),
            df["as_of_date"].max(),
        )
        return df
    except Exception as exc:
        logger.error("FRED fetch failed for DEXUSUK: %s", exc)
        return pd.DataFrame()


def store_fx_rates(engine: Engine, df: pd.DataFrame) -> int:
    """
    Upsert FX rates into raw_data.fx_rates.

    Uses ON CONFLICT (pair, as_of_date, source) DO NOTHING — raw_data is
    append-only, we never modify historical observations. To correct a
    bad rate, load from a different source and use the lookup fallback.

    Returns number of rows inserted.
    """
    if df.empty:
        return 0

    rows = [
        {
            "as_of_date": r["as_of_date"],
            "pair": r["pair"],
            "rate": Decimal(str(r["rate"])),
            "source": r["source"],
            "source_series_id": r.get("source_series_id"),
            "bar_size": "daily",
        }
        for _, r in df.iterrows()
    ]
    stmt = text(
        """
        INSERT INTO raw_data.fx_rates
          (as_of_date, pair, rate, source, source_series_id, bar_size)
        VALUES
          (:as_of_date, :pair, :rate, :source, :source_series_id, :bar_size)
        ON CONFLICT (pair, as_of_date, source) DO NOTHING
    """
    )
    with engine.begin() as conn:
        result = conn.execute(stmt, rows)
    inserted = result.rowcount if result.rowcount is not None else len(rows)
    logger.info("Inserted %d new FX rows into raw_data.fx_rates", inserted)
    return inserted


def fetch_and_store_usd_gbp(
    engine: Engine,
    start_date: date,
    end_date: date | None = None,
) -> int:
    """Convenience: fetch from FRED and persist in one call."""
    df = fetch_usd_gbp(start_date, end_date)
    return store_fx_rates(engine, df)
