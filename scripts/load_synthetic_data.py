#!/usr/bin/env python3
"""
scripts/load_synthetic_data.py
Generate and load synthetic price data for all 10 metals — pipeline validation only.

DO NOT use results from this data for strategy decisions.
Source is tagged 'synthetic' in the DB — cannot be confused with real data.

USAGE:
  python scripts/load_synthetic_data.py
  python scripts/load_synthetic_data.py --start 2004-01-01 --end 2023-12-31
  python scripts/load_synthetic_data.py --seed 99 --dry-run
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

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from worfin.config.settings import get_settings

logger = logging.getLogger("load_synthetic")

# ─────────────────────────────────────────────────────────────────────────────
# METAL PARAMETERS  (calibrated to approximate historical regimes)
# ─────────────────────────────────────────────────────────────────────────────

METAL_PARAMS: dict[str, dict] = {
    "CA": {
        "start_price": 2800.0,
        "annual_vol": 0.22,
        "annual_drift": 0.04,
        "carry_mean": 0.02,
        "carry_vol": 0.03,
    },
    "AH": {
        "start_price": 1600.0,
        "annual_vol": 0.20,
        "annual_drift": 0.03,
        "carry_mean": -0.01,
        "carry_vol": 0.03,
    },
    "ZS": {
        "start_price": 1100.0,
        "annual_vol": 0.25,
        "annual_drift": 0.04,
        "carry_mean": 0.01,
        "carry_vol": 0.04,
    },
    "NI": {
        "start_price": 12000.0,
        "annual_vol": 0.35,
        "annual_drift": 0.03,
        "carry_mean": 0.00,
        "carry_vol": 0.06,
    },
    "PB": {
        "start_price": 850.0,
        "annual_vol": 0.22,
        "annual_drift": 0.03,
        "carry_mean": 0.01,
        "carry_vol": 0.03,
    },
    "SN": {
        "start_price": 8500.0,
        "annual_vol": 0.28,
        "annual_drift": 0.04,
        "carry_mean": 0.02,
        "carry_vol": 0.04,
    },
    "GC": {
        "start_price": 410.0,
        "annual_vol": 0.16,
        "annual_drift": 0.06,
        "carry_mean": -0.008,
        "carry_vol": 0.005,
    },
    "SI": {
        "start_price": 6.5,
        "annual_vol": 0.28,
        "annual_drift": 0.04,
        "carry_mean": -0.010,
        "carry_vol": 0.008,
    },
    "PL": {
        "start_price": 840.0,
        "annual_vol": 0.22,
        "annual_drift": 0.02,
        "carry_mean": -0.012,
        "carry_vol": 0.010,
    },
    "PA": {
        "start_price": 250.0,
        "annual_vol": 0.35,
        "annual_drift": 0.06,
        "carry_mean": -0.015,
        "carry_vol": 0.015,
    },
}

_TICKERS_ORDERED = ["CA", "AH", "ZS", "NI", "PB", "SN", "GC", "SI", "PL", "PA"]

# Approximate cross-metal correlation matrix
_CORR = np.array(
    [
        [1.00, 0.55, 0.60, 0.40, 0.50, 0.35, 0.25, 0.25, 0.20, 0.15],
        [0.55, 1.00, 0.55, 0.35, 0.45, 0.30, 0.20, 0.22, 0.18, 0.12],
        [0.60, 0.55, 1.00, 0.38, 0.50, 0.32, 0.22, 0.24, 0.20, 0.14],
        [0.40, 0.35, 0.38, 1.00, 0.38, 0.28, 0.18, 0.20, 0.16, 0.12],
        [0.50, 0.45, 0.50, 0.38, 1.00, 0.30, 0.20, 0.22, 0.18, 0.12],
        [0.35, 0.30, 0.32, 0.28, 0.30, 1.00, 0.18, 0.20, 0.16, 0.10],
        [0.25, 0.20, 0.22, 0.18, 0.20, 0.18, 1.00, 0.70, 0.60, 0.50],
        [0.25, 0.22, 0.24, 0.20, 0.22, 0.20, 0.70, 1.00, 0.55, 0.45],
        [0.20, 0.18, 0.20, 0.16, 0.18, 0.16, 0.60, 0.55, 1.00, 0.55],
        [0.15, 0.12, 0.14, 0.12, 0.12, 0.10, 0.50, 0.45, 0.55, 1.00],
    ]
)

SOURCE = "synthetic"


# ─────────────────────────────────────────────────────────────────────────────
# PRICE PATH GENERATION
# ─────────────────────────────────────────────────────────────────────────────


def generate_price_paths(start: date, end: date, seed: int = 42) -> dict[str, pd.DataFrame]:
    """Correlated GBM price paths for all 10 metals with mean-reverting carry."""
    rng = np.random.default_rng(seed)
    bdays = pd.bdate_range(start=start, end=end, freq="B", tz="UTC")
    n = len(bdays)
    dt = 1.0 / 252.0

    L = np.linalg.cholesky(_CORR)
    z = rng.standard_normal((n, 10)) @ L.T  # correlated shocks

    result: dict[str, pd.DataFrame] = {}
    for i, ticker in enumerate(_TICKERS_ORDERED):
        p = METAL_PARAMS[ticker]
        prices = p["start_price"] * np.exp(
            np.cumsum(
                (p["annual_drift"] - 0.5 * p["annual_vol"] ** 2) * dt
                + p["annual_vol"] * np.sqrt(dt) * z[:, i]
            )
        )
        prices = np.maximum(prices, p["start_price"] * 0.01)

        # Mean-reverting carry process
        carry = np.zeros(n)
        carry[0] = p["carry_mean"]
        for j in range(1, n):
            carry[j] = (
                carry[j - 1]
                + 2.0 * (p["carry_mean"] - carry[j - 1]) * dt
                + p["carry_vol"] * np.sqrt(dt) * rng.standard_normal()
            )

        second = np.maximum(prices * (1.0 - carry * 91.0 / 365.0), prices * 0.5)
        result[ticker] = pd.DataFrame({"front": prices, "second": second}, index=bdays)
    return result


# ─────────────────────────────────────────────────────────────────────────────
# DB INSERT
# Columns match raw_data.futures_prices as defined in 001_schema.py:
#   price_timestamp, ticker, contract_type, bar_size,
#   open, high, low, close, volume, open_interest, source, fetched_at
#
# Idempotency via WHERE NOT EXISTS (no unique constraint on the table).
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


def _insert_series(
    engine: Engine,
    ticker: str,
    index: pd.DatetimeIndex,
    prices: np.ndarray,
    contract_type: str,  # 'front' | 'second'
    dry_run: bool,
) -> tuple[int, int]:
    now = datetime.now(UTC)
    rows = []
    for ts, px in zip(index, prices, strict=False):
        if px <= 0 or np.isnan(px):
            continue
        sp = px * 0.001  # small OHLC spread
        rows.append(
            {
                "price_timestamp": ts.to_pydatetime(),
                "ticker": ticker,
                "contract_type": contract_type,
                "bar_size": "daily",
                "open": float(px - sp * 0.5),
                "high": float(px + sp),
                "low": float(px - sp),
                "close": float(px),
                "volume": None,
                "open_interest": None,
                "source": SOURCE,
                "fetched_at": now,
            }
        )

    if not rows:
        return 0, 0
    if dry_run:
        logger.info("DRY RUN: %d rows for %s [%s]", len(rows), ticker, contract_type)
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
# CLI
# ─────────────────────────────────────────────────────────────────────────────


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Load SYNTHETIC price data — pipeline validation only.")
    p.add_argument("--start", default="2004-01-01")
    p.add_argument("--end", default="2023-12-31")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return p.parse_args()


def main() -> int:
    args = _parse_args()
    configure_logging(log_level=args.log_level, force=True)

    print("\n" + "⚠️  " * 20)
    print("  WARNING: SYNTHETIC DATA — NOT REAL MARKET DATA")
    print("  Pipeline validation only. Do not interpret results as strategy performance.")
    print("⚠️  " * 20 + "\n")

    try:
        start = date.fromisoformat(args.start)
        end = date.fromisoformat(args.end)
    except ValueError as exc:
        logger.error("Invalid date: %s", exc)
        return 1

    settings = get_settings()
    engine = create_engine(settings.database_url, pool_pre_ping=True)
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("Database connected.")
    except Exception as exc:
        logger.error("Cannot connect: %s", exc)
        return 2

    paths = generate_price_paths(start, end, seed=args.seed)
    grand_ins = grand_skp = 0

    for ticker, df in paths.items():
        fi, fs = _insert_series(engine, ticker, df.index, df["front"].values, "front", args.dry_run)
        si, ss = _insert_series(
            engine, ticker, df.index, df["second"].values, "second", args.dry_run
        )
        logger.warning("⚠ SYNTHETIC %s: front=%d, second=%d inserted", ticker, fi, si)
        grand_ins += fi + si
        grand_skp += fs + ss

    action = "would insert" if args.dry_run else "inserted"
    print(
        f"\n{'[DRY RUN] ' if args.dry_run else ''}Rows {action}: {grand_ins:,}  skipped: {grand_skp:,}"
    )
    print("All rows tagged source='synthetic' in raw_data.futures_prices.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
