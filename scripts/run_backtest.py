#!/usr/bin/env python3
"""
scripts/run_backtest.py
End-to-end walk-forward backtest runner.

USAGE:
  python scripts/run_backtest.py --strategy S4 --period IS
  python scripts/run_backtest.py --strategy S4 --period OOS
  python scripts/run_backtest.py --strategy S4 --period all
  python scripts/run_backtest.py --strategy S4 --period all --capital 500000
  python scripts/run_backtest.py --strategy S4 --period IS --no-pretrade

DATA REQUIREMENTS:
  raw_data.futures_prices must contain front and second month close prices.
  Load them first:
    python scripts/load_to_db_yfinance.py   (COMEX 4 metals — pipeline test)
    python scripts/load_synthetic_data.py   (all 10 metals — pipeline test)
    python scripts/load_to_db.py --all --start 2004-01-01 --end 2023-12-31 --second
                                            (real data — when Nasdaq is back)

IS GATES (must all pass to run OOS):
  Sharpe ≥ 0.50 | t-stat ≥ 3.0 | Max DD ≤ 20%

OOS GATES (run only if IS passes):
  Sharpe ≥ 0.30 | WFER ≥ 0.50

EXIT CODES:
  0 — all periods passed their gates
  1 — IS or OOS gate failure
  2 — data / configuration error
"""

from __future__ import annotations

import argparse
import logging
import sys
import uuid
from datetime import UTC, date, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from worfin.backtest.engine import (
    IS_END,
    IS_START,
    OOS_END,
    OOS_START,
    BacktestConfig,
    BacktestResult,
    WalkForwardEngine,
)
from worfin.backtest.metrics import (
    compute_wfer,
)
from worfin.config.calendar import get_lme_3m_dte
from worfin.config.metals import ALL_METALS, Exchange
from worfin.config.settings import get_settings
from worfin.data.pipeline.continuous import build_continuous_series
from worfin.strategies.base import BaseStrategy

logger = logging.getLogger("run_backtest")

_SEP = "=" * 72


# ─────────────────────────────────────────────────────────────────────────────
# STRATEGY FACTORY
# ─────────────────────────────────────────────────────────────────────────────


def _make_strategy(strategy_id: str) -> BaseStrategy:
    if strategy_id == "S4":
        from worfin.strategies.s4_basis_momentum import BasisMomentumStrategy

        return BasisMomentumStrategy()
    raise NotImplementedError(
        f"Strategy {strategy_id!r} not yet implemented. Only S4 is available."
    )


# ─────────────────────────────────────────────────────────────────────────────
# DATA LOADING
# ─────────────────────────────────────────────────────────────────────────────
#
# raw_data.futures_prices schema (from 001_schema.py):
#   price_timestamp  — TIMESTAMPTZ   (not "timestamp")
#   ticker           — VARCHAR(4)
#   contract_type    — VARCHAR(10)   values: 'front' | 'second' | 'cash' | '3m'
#   bar_size         — VARCHAR(10)   values: 'daily' | '1h' etc.
#   open             — NUMERIC       (not "open_price")
#   high             — NUMERIC       (not "high_price")
#   low              — NUMERIC       (not "low_price")
#   close            — NUMERIC       (not "close_price", no separate "settle_price")
#   volume           — NUMERIC
#   open_interest    — NUMERIC
#   source           — VARCHAR(50)
#   fetched_at       — TIMESTAMPTZ
#   created_at       — TIMESTAMPTZ
#
# Source preference order (DISTINCT ON ensures no duplicates):
#   1. nasdaq_data_link_chris  (real, most accurate)
#   2. yfinance                (real COMEX, approximate carry)
#   3. synthetic               (generated, pipeline validation only)


def _load_raw_series(
    engine: Engine,
    ticker: str,
    contract_type: str,  # 'front' | 'second'
    start: date,
    end: date,
) -> pd.Series:
    """
    Pull close prices from raw_data.futures_prices.

    Uses DISTINCT ON (price_timestamp) ordered by source preference so real
    data is always chosen over synthetic/yfinance when multiple sources exist
    for the same date.

    Returns a tz-aware (UTC) DatetimeIndex Series, or empty Series if no rows.
    """
    sql = text(
        """
        SELECT DISTINCT ON (price_timestamp)
               price_timestamp,
               close
        FROM raw_data.futures_prices
        WHERE ticker        = :ticker
          AND contract_type = :contract_type
          AND bar_size      = 'daily'
          AND price_timestamp >= :start
          AND price_timestamp <= :end
          AND close IS NOT NULL
          AND close > 0
        ORDER BY price_timestamp,
                 CASE source
                     WHEN 'nasdaq_data_link_chris' THEN 1
                     WHEN 'yfinance'               THEN 2
                     WHEN 'synthetic'              THEN 3
                     ELSE 4
                 END
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(
            sql,
            {
                "ticker": ticker,
                "contract_type": contract_type,
                "start": datetime(start.year, start.month, start.day, tzinfo=UTC),
                "end": datetime(end.year, end.month, end.day, 23, 59, 59, tzinfo=UTC),
            },
        ).fetchall()

    if not rows:
        return pd.Series(dtype=float, name="close")

    idx = pd.DatetimeIndex(
        [r[0] if r[0].tzinfo else r[0].replace(tzinfo=UTC) for r in rows],
        tz=UTC,
        name="price_timestamp",
    )
    series = pd.Series([float(r[1]) for r in rows], index=idx, name="close")
    series = series[~series.index.duplicated(keep="last")].sort_index()
    return series


def _compute_f3m_dte_series(index: pd.DatetimeIndex, exchange: Exchange) -> pd.Series:
    """Actual DTE per day — LME uses rolling prompt; COMEX approximates 91."""
    if exchange == Exchange.LME:
        dtes = [get_lme_3m_dte(ts.date()) for ts in index]
    else:
        dtes = [91] * len(index)
    return pd.Series(dtes, index=index, name="f3m_dte", dtype=float)


def load_price_data(
    tickers: list[str],
    start: date,
    end: date,
    engine: Engine,
    run_id: str | None = None,
) -> dict[str, pd.DataFrame]:
    """
    Load and process price data for the backtest engine.

    For each ticker:
      1. Fetch front-month close prices  → build back-adjusted series
      2. Fetch second-month close prices → use as carry denominator (f3m_price)
      3. Compute actual DTE per bar
      4. Return engine-ready DataFrame with columns:
           close       — back-adjusted close (for momentum signal + P&L)
           cash_price  — unadjusted front close (for carry numerator)
           f3m_price   — second-month close (for carry denominator)
           f3m_dte     — actual DTE from Cash to 3M prompt
    """
    # Pull one extra year before start to warm vol lookbacks (60-day window)
    warm_start = date(max(start.year - 1, 2004), 1, 1)

    price_data: dict[str, pd.DataFrame] = {}
    missing: list[str] = []

    for ticker in tickers:
        spec = ALL_METALS[ticker]

        front = _load_raw_series(engine, ticker, "front", warm_start, end)
        second = _load_raw_series(engine, ticker, "second", warm_start, end)

        if front.empty:
            logger.warning(
                "%s: no front-month data found (contract_type='front', bar_size='daily'). "
                "Load data first with load_to_db_yfinance.py or load_synthetic_data.py.",
                ticker,
            )
            missing.append(ticker)
            continue

        if second.empty:
            logger.warning(
                "%s: no second-month data — carry signal will be flat (front ≈ second).", ticker
            )
            second = front.copy()

        # Build back-adjusted continuous series for momentum signal
        result = build_continuous_series(
            ticker=ticker,
            front=front,
            second=second,
            method="back_adjusted",
            environment="backtest",
            backtest_run_id=run_id,
            engine=None,  # skip per-ticker DB audit writes during load
        )

        adj = result.adjusted_series  # back-adjusted → momentum
        raw = result.unadjusted_front  # unadjusted    → carry numerator

        second_aligned = second.reindex(raw.index).ffill()
        dte = _compute_f3m_dte_series(raw.index, spec.exchange)

        df = pd.DataFrame(
            {
                "close": adj,
                "cash_price": raw,
                "f3m_price": second_aligned,
                "f3m_dte": dte,
            },
            index=raw.index,
        )
        df = df.dropna(subset=["close", "cash_price", "f3m_price"])

        if len(df) < 100:
            logger.warning(
                "%s: only %d clean rows after processing — excluding from universe.",
                ticker,
                len(df),
            )
            missing.append(ticker)
            continue

        price_data[ticker] = df
        logger.info(
            "%s: %d rows loaded (%s → %s, %d rolls)",
            ticker,
            len(df),
            df.index.min().date(),
            df.index.max().date(),
            len(result.roll_events),
        )

    if missing:
        logger.warning(
            "Excluded %d tickers: %s. Proceeding with %d/%d.",
            len(missing),
            missing,
            len(price_data),
            len(tickers),
        )

    return price_data


# ─────────────────────────────────────────────────────────────────────────────
# REPORT PRINTING
# ─────────────────────────────────────────────────────────────────────────────


def _print_report(result: BacktestResult, wfer: float | None = None) -> None:
    m = result.metrics
    if m is None:
        logger.error("No metrics computed — cannot print report.")
        return

    if wfer is not None:
        m.wfer = wfer
        m.gate_results = m._evaluate_gates()

    lines = [
        "",
        _SEP,
        f"  WorFIn Backtest Report — {m.strategy_id} — {m.period}",
        _SEP,
        f"  Period   : {m.start_date}  →  {m.end_date}",
        f"  Days     : {m.n_trading_days:,}  ({m.n_trading_days / 252:.1f} years)",
        "",
        "  ── Returns ──────────────────────────────────────────────────",
        f"  Total return        : {m.total_return:>+8.1%}",
        f"  Annualised return   : {m.annualised_return:>+8.1%}",
        f"  Annualised vol      : {m.annualised_vol:>8.1%}",
        "",
        "  ── Risk-adjusted ────────────────────────────────────────────",
        f"  Sharpe ratio        : {m.sharpe_ratio:>8.2f}",
        f"  Sortino ratio       : {m.sortino_ratio:>8.2f}",
        f"  Calmar ratio        : {m.calmar_ratio:>8.2f}",
        f"  t-statistic (Sharpe): {m.t_stat_sharpe:>8.2f}",
        "",
        "  ── Drawdown ─────────────────────────────────────────────────",
        f"  Max drawdown        : {m.max_drawdown:>8.1%}",
        f"  Avg drawdown        : {m.avg_drawdown:>8.1%}",
        f"  Max DD duration     : {m.max_drawdown_duration_days:>8,} days",
        "",
        "  ── Trade stats ──────────────────────────────────────────────",
        f"  Win rate            : {m.win_rate:>8.1%}",
        f"  Profit factor       : {m.profit_factor:>8.2f}",
        f"  Annual turnover     : {m.annual_turnover:>8.1f}×",
    ]

    if wfer is not None:
        lines += [
            "",
            "  ── Walk-Forward ─────────────────────────────────────────────",
            f"  WFER (OOS/IS Sharpe): {wfer:>8.2f}",
        ]

    lines += ["", "  ── Gates ────────────────────────────────────────────────────"]
    for gate, passed in m.gate_results.items():
        lines.append(f"  {'✅' if passed else '❌'}  {gate}")

    overall = (
        "✅  ALL GATES PASSED — advance to next phase"
        if m.passes_all_gates
        else "❌  GATE FAILURE — do not advance"
    )
    lines += ["", f"  {overall}", _SEP, ""]
    print("\n".join(lines))

    if result.warnings:
        logger.warning("%d backtest warning(s):", len(result.warnings))
        for w in result.warnings:
            logger.warning("  %s", w)


# ─────────────────────────────────────────────────────────────────────────────
# ARGUMENT PARSING
# ─────────────────────────────────────────────────────────────────────────────


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="WorFIn walk-forward backtest runner.")
    p.add_argument("--strategy", required=True, choices=["S1", "S2", "S3", "S4", "S5", "S6"])
    p.add_argument(
        "--period",
        required=True,
        choices=["IS", "OOS", "all"],
        help="IS=2005-2017 | OOS=2018-2022 | all=IS then OOS",
    )
    p.add_argument("--capital", type=float, default=100_000.0, metavar="GBP")
    p.add_argument(
        "--no-pretrade",
        action="store_true",
        help="Disable pre-trade checks (faster, not recommended for final validation)",
    )
    p.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    p.add_argument(
        "--plot",
        action="store_true",
        help="Generate and display performance charts after the backtest completes",
    )
    p.add_argument(
        "--save-plots",
        metavar="DIR",
        nargs="?",
        const="reports",
        default=None,
        help="Save chart PNGs to DIR (default: reports/). Implies --plot.",
    )
    return p.parse_args()


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────


def main() -> int:
    args = _parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    logger.info(_SEP)
    logger.info("  WorFIn Backtester — %s — %s", args.strategy, args.period)
    logger.info(
        "  Capital: £{:,.0f}   Pre-trade: {}".format(
            args.capital, "disabled" if args.no_pretrade else "enabled"
        )
    )
    logger.info(_SEP)

    # ── Database ──────────────────────────────────────────────────────────────
    try:
        settings = get_settings()
        db_engine = create_engine(settings.database_url, pool_pre_ping=True)
        with db_engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("Database connected: %s", settings.database_url.split("@")[-1])
    except Exception as exc:
        logger.error("Cannot connect to database: %s", exc)
        return 2

    # ── Strategy ──────────────────────────────────────────────────────────────
    try:
        strategy = _make_strategy(args.strategy)
    except NotImplementedError as exc:
        logger.error("%s", exc)
        return 2

    universe = strategy.universe
    logger.info("Universe (%d metals): %s", len(universe), ", ".join(universe))

    run_is = args.period in ("IS", "all")
    run_oos = args.period in ("OOS", "all")
    data_end = OOS_END if run_oos else IS_END

    # ── Load price data ───────────────────────────────────────────────────────
    logger.info("Loading price data from DB (%s → %s) …", IS_START, data_end)
    try:
        run_id = str(uuid.uuid4())
        price_data = load_price_data(
            tickers=universe,
            start=IS_START,
            end=data_end,
            engine=db_engine,
            run_id=run_id,
        )
    except Exception as exc:
        logger.error("Data loading failed: %s", exc, exc_info=True)
        return 2

    if len(price_data) < 4:
        logger.error(
            "Only %d tickers loaded — need ≥4 for cross-sectional signals.\n"
            "Run one of the data loaders first:\n"
            "  python scripts/load_to_db_yfinance.py\n"
            "  python scripts/load_synthetic_data.py",
            len(price_data),
        )
        return 2

    # ── Engine ────────────────────────────────────────────────────────────────
    bt_engine = WalkForwardEngine(
        strategy=strategy,
        price_data=price_data,
        db_engine=db_engine,
    )

    is_result: BacktestResult | None = None
    oos_result: BacktestResult | None = None
    exit_code = 0

    # ── IS run ────────────────────────────────────────────────────────────────
    if run_is:
        logger.info("Running IS backtest: %s → %s …", IS_START, IS_END)
        try:
            is_result = bt_engine.run(
                BacktestConfig(
                    strategy_id=args.strategy,
                    start_date=IS_START,
                    end_date=IS_END,
                    period_label="IS",
                    total_capital_gbp=args.capital,
                    enable_pretrade_checks=not args.no_pretrade,
                    db_engine=db_engine,
                )
            )
        except Exception as exc:
            logger.error("IS backtest failed: %s", exc, exc_info=True)
            return 2

        _print_report(is_result)

        if not is_result.metrics or not is_result.metrics.passes_all_gates:
            logger.warning("IS GATES FAILED — do not run OOS. Review signal logic and parameters.")
            if run_oos:
                logger.warning("OOS skipped to preserve data split integrity.")
            return 1

        logger.info("IS GATES PASSED ✅")

    # ── OOS run ───────────────────────────────────────────────────────────────
    if run_oos:
        if args.period == "OOS" and is_result is None:
            logger.warning("Running OOS standalone — ensure IS was validated previously.")

        logger.info("Running OOS backtest: %s → %s …", OOS_START, OOS_END)
        try:
            oos_result = bt_engine.run(
                BacktestConfig(
                    strategy_id=args.strategy,
                    start_date=OOS_START,
                    end_date=OOS_END,
                    period_label="OOS",
                    total_capital_gbp=args.capital,
                    enable_pretrade_checks=not args.no_pretrade,
                    db_engine=db_engine,
                )
            )
        except Exception as exc:
            logger.error("OOS backtest failed: %s", exc, exc_info=True)
            return 2

        wfer: float | None = None
        if is_result and is_result.metrics and oos_result.metrics:
            wfer = compute_wfer(
                is_sharpe=is_result.metrics.sharpe_ratio,
                oos_sharpe=oos_result.metrics.sharpe_ratio,
            )

        _print_report(oos_result, wfer=wfer)

        if not oos_result.metrics or not oos_result.metrics.passes_all_gates:
            logger.warning("OOS GATES FAILED — do not deploy to paper trading.")
            exit_code = 1
        else:
            logger.info("OOS GATES PASSED ✅")

    # ── Combined summary ──────────────────────────────────────────────────────
    if is_result and oos_result and is_result.metrics and oos_result.metrics:
        wfer_val = compute_wfer(is_result.metrics.sharpe_ratio, oos_result.metrics.sharpe_ratio)
        print(f"\n{'─' * 72}")
        print("  COMBINED SUMMARY")
        print(f"{'─' * 72}")
        print(
            f"  IS  Sharpe: {is_result.metrics.sharpe_ratio:+.2f}  "
            f"| MaxDD: {is_result.metrics.max_drawdown:.1%}  "
            f"| t-stat: {is_result.metrics.t_stat_sharpe:.1f}"
        )
        print(
            f"  OOS Sharpe: {oos_result.metrics.sharpe_ratio:+.2f}  "
            f"| MaxDD: {oos_result.metrics.max_drawdown:.1%}  "
            f"| WFER: {wfer_val:.2f}"
        )
        print(f"{'─' * 72}")
        if wfer_val >= 0.70:
            interp = "Excellent — possibly under-fitted. High OOS confidence."
        elif wfer_val >= 0.50:
            interp = "Good — proceed to 60-day paper trading."
        elif wfer_val >= 0.30:
            interp = "Marginal — simplify parameters and re-test."
        else:
            interp = "Poor — likely overfitted. Discard or redesign."
        print(f"  WFER interpretation: {interp}")
        print(f"{'─' * 72}\n")

    # ── Charts ────────────────────────────────────────────────────────────────
    wants_charts = args.plot or args.save_plots is not None
    if wants_charts and is_result is not None:
        try:
            from worfin.backtest.charts import generate_report

            chart_dir = Path(args.save_plots) if args.save_plots else Path("reports")
            # show=True only when --plot is passed; --save-plots alone saves without popping a window
            show_window = args.plot
            generate_report(
                is_result=is_result,
                oos_result=oos_result,
                output_dir=chart_dir,
                show=show_window,
            )
        except Exception as exc:
            logger.warning("Chart generation failed: %s", exc, exc_info=True)
            logger.warning("Install matplotlib if missing: pip install matplotlib")

    return exit_code


if __name__ == "__main__":
    sys.exit(main())
