"""
scripts/run_backtest.py
Run walk-forward backtest for a single strategy.

Usage:
    python scripts/run_backtest.py --strategy S4 --period IS
    python scripts/run_backtest.py --strategy S4 --period OOS
    python scripts/run_backtest.py --strategy S4 --period all

This script is the primary entry point for strategy validation.
It enforces the IS/OOS data splits and prints a full performance report.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("run_backtest")


def main() -> None:
    parser = argparse.ArgumentParser(description="WorFIn Strategy Backtester")
    parser.add_argument("--strategy", required=True, choices=["S1", "S2", "S3", "S4", "S5", "S6"])
    parser.add_argument("--period", required=True, choices=["IS", "OOS", "Holdout", "all"])
    parser.add_argument(
        "--costs", action="store_true", default=True, help="Include transaction costs (always True)"
    )
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("WorFIn Backtester | Strategy: %s | Period: %s", args.strategy, args.period)
    logger.info("Transaction costs: ALWAYS INCLUDED")
    logger.info("=" * 60)

    # Backtest engine is built next session — placeholder for now
    logger.info(
        "\nBacktest engine (backtest/engine.py) is the next build priority.\n"
        "This script will call it once it exists.\n\n"
        "Current status: data layer and signal layer ready.\n"
        "Next: build backtest/engine.py to wire them together."
    )


if __name__ == "__main__":
    main()
