"""
data/ingestion/nasdaq_datalink.py
Pull historical metals futures data from Nasdaq Data Link (formerly Quandl).

This is the PRIMARY source for backtesting (2005–present).
Free tier supports the full dataset needed for IS + OOS periods.

Data stored in: raw_data schema (append-only — never modify after insert).
"""

from __future__ import annotations

import logging
from datetime import date, datetime

import nasdaqdatalink
import pandas as pd

from worfin.config.settings import get_settings

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# DATASET MAPPINGS
# Nasdaq Data Link / Quandl continuous futures codes
# ─────────────────────────────────────────────────────────────────────────────

# CHRIS database: continuous (non-expired) futures contracts
# Format: CHRIS/{EXCHANGE}_{SYMBOL}{CONTRACT_NUMBER}
# CONTRACT_NUMBER: 1 = front month, 2 = second month, etc.

NASDAQ_CODES: dict[str, dict[str, str]] = {
    # LME Base Metals (via LME database or CHRIS)
    "CA": {
        "front": "CHRIS/LME_CA1",  # LME Copper, 1st month
        "second": "CHRIS/LME_CA2",  # LME Copper, 2nd month
    },
    "AH": {
        "front": "CHRIS/LME_AH1",
        "second": "CHRIS/LME_AH2",
    },
    "ZS": {
        "front": "CHRIS/LME_ZS1",
        "second": "CHRIS/LME_ZS2",
    },
    "NI": {
        "front": "CHRIS/LME_NI1",
        "second": "CHRIS/LME_NI2",
    },
    "PB": {
        "front": "CHRIS/LME_PB1",
        "second": "CHRIS/LME_PB2",
    },
    "SN": {
        "front": "CHRIS/LME_SN1",
        "second": "CHRIS/LME_SN2",
    },
    # COMEX Precious Metals
    "GC": {
        "front": "CHRIS/CME_GC1",  # COMEX Gold
        "second": "CHRIS/CME_GC2",
    },
    "SI": {
        "front": "CHRIS/CME_SI1",  # COMEX Silver
        "second": "CHRIS/CME_SI2",
    },
    "PL": {
        "front": "CHRIS/CME_PL1",  # COMEX Platinum
        "second": "CHRIS/CME_PL2",
    },
    "PA": {
        "front": "CHRIS/CME_PA1",  # COMEX Palladium
        "second": "CHRIS/CME_PA2",
    },
}


def configure_api() -> None:
    """Configure Nasdaq Data Link with API key from settings."""
    settings = get_settings()
    if not settings.nasdaq_data_link_api_key:
        raise ValueError(
            "NASDAQ_DATA_LINK_API_KEY not set in .env — "
            "get a free key at https://data.nasdaq.com"
        )
    nasdaqdatalink.ApiConfig.api_key = settings.nasdaq_data_link_api_key


def fetch_continuous_futures(
    ticker: str,
    start_date: date,
    end_date: date,
    contract: str = "front",
) -> pd.DataFrame:
    """
    Fetch continuous futures data for a single metal.

    Args:
        ticker:     Metal ticker (e.g., "GC" for gold)
        start_date: First date to retrieve
        end_date:   Last date to retrieve
        contract:   "front" or "second"

    Returns:
        DataFrame with columns: [open, high, low, close, volume, open_interest]
        Indexed by date. Empty DataFrame if fetch fails.

    Note:
        Data goes into raw_data schema unchanged.
        Do not transform here — transformation happens in data/pipeline/.
    """
    configure_api()

    if ticker not in NASDAQ_CODES:
        raise ValueError(f"Unknown ticker: {ticker}. Valid: {list(NASDAQ_CODES)}")

    dataset_code = NASDAQ_CODES[ticker][contract]

    try:
        breakpoint()
        logger.info("Fetching %s (%s) from %s to %s ...", ticker, contract, start_date, end_date)
        raw = nasdaqdatalink.get(
            dataset_code,
            api_key=nasdaqdatalink.ApiConfig.api_key,
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
            returns="pandas",
        )

        # Standardise column names
        raw.columns = [c.lower().replace(" ", "_") for c in raw.columns]
        raw.index = pd.to_datetime(raw.index)
        raw.index.name = "date"

        # Ensure we have at least a close column
        if "settle" in raw.columns and "close" not in raw.columns:
            raw = raw.rename(columns={"settle": "close"})
        if "last" in raw.columns and "close" not in raw.columns:
            raw = raw.rename(columns={"last": "close"})

        # Add metadata columns
        raw["ticker"] = ticker
        raw["contract_type"] = contract
        raw["source"] = "nasdaq_data_link"
        raw["fetched_at"] = datetime.utcnow()

        logger.info(
            "Fetched %d rows for %s %s (%s → %s)",
            len(raw),
            ticker,
            contract,
            raw.index.min().date(),
            raw.index.max().date(),
        )
        return raw

    except Exception as e:
        logger.error("Failed to fetch %s %s: %s", ticker, contract, e)
        return pd.DataFrame()


def fetch_all_metals(
    start_date: date,
    end_date: date,
    tickers: list[str] | None = None,
) -> dict[str, dict[str, pd.DataFrame]]:
    """
    Fetch front-month and second-month data for all (or specified) metals.

    Args:
        start_date: Start of date range
        end_date:   End of date range
        tickers:    Specific tickers to fetch; defaults to all 10

    Returns:
        {ticker: {"front": DataFrame, "second": DataFrame}}
        Missing data is represented as empty DataFrames (never raises).
    """
    if tickers is None:
        tickers = list(NASDAQ_CODES.keys())

    result: dict[str, dict[str, pd.DataFrame]] = {}
    errors: list[str] = []

    for ticker in tickers:
        result[ticker] = {}
        for contract in ("front", "second"):
            df = fetch_continuous_futures(ticker, start_date, end_date, contract)
            result[ticker][contract] = df
            if df.empty:
                errors.append(f"{ticker}:{contract}")

    if errors:
        logger.warning(
            "Fetch completed with %d failures: %s. " "Check API key and dataset availability.",
            len(errors),
            errors,
        )
    else:
        logger.info(
            "All metals fetched successfully. Date range: %s to %s.",
            start_date,
            end_date,
        )

    return result


def fetch_for_backtest(
    tickers: list[str] | None = None,
) -> dict[str, dict[str, pd.DataFrame]]:
    """
    Convenience function: fetch the full IS + OOS backtest history (2005–2022).

    Uses the fixed data splits defined in the backtest protocol.
    """
    IS_START = date(2005, 1, 1)
    OOS_END = date(2022, 12, 31)

    logger.info("Fetching full backtest history: %s → %s", IS_START, OOS_END)
    return fetch_all_metals(IS_START, OOS_END, tickers)
