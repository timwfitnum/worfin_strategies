"""
backtest/engine.py
Walk-forward backtesting engine for WorFIn strategies.

ARCHITECTURE:
  The engine is the integration layer that wires together:
    data → signals → sizing → costs → positions → metrics

  This is where subtle bugs hide. Every design decision here
  must be justified against look-ahead bias and cost accuracy.

KEY DESIGN DECISIONS:
  1. Strict date isolation: on each bar, only data up to and including
     that bar's timestamp is visible to the strategy. No look-ahead.
  2. Signals computed BEFORE position sizing (signals drive size).
  3. Transaction costs applied on the day of trade execution, not signal.
  4. Positions carried at yesterday's close until rebalance date.
  5. Anchored walk-forward: IS always starts from IS_START.

LOOK-AHEAD BIAS PREVENTION (critical):
  - Data sliced with df[df.index <= current_date] — strict <=
  - Vol estimates use only data available at signal time
  - Carry DTE uses trade date, not settlement date
  - No future prices used for any calculation
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Optional

import numpy as np
import pandas as pd

from worfin.backtest.costs import apply_costs_to_returns, COST_BASELINE_MULTIPLIER
from worfin.backtest.metrics import PerformanceMetrics, compute_metrics, compute_wfer
from worfin.config.calendar import is_lme_trading_day, trading_days_between
from worfin.data.pipeline.volatility import compute_vol_estimates
from worfin.risk.limits import STRATEGY_ALLOCATION, STRATEGY_TARGET_VOL, VOL_FLOOR
from worfin.risk.sizing import compute_lots
from worfin.strategies.base import BaseStrategy, SignalResult

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# FIXED DATA SPLITS — SET ONCE, NEVER CHANGE
# ─────────────────────────────────────────────────────────────────────────────
IS_START      = date(2005, 1,  1)
IS_END        = date(2017, 12, 31)
OOS_START     = date(2018, 1,  1)
OOS_END       = date(2022, 12, 31)
HOLDOUT_START = date(2023, 1,  1)


@dataclass
class BacktestConfig:
    """Configuration for a single backtest run."""
    strategy_id: str
    start_date: date
    end_date: date
    period_label: str                     # "IS", "OOS", "Holdout"
    total_capital_gbp: float = 100_000.0
    usd_gbp_rate: float = 1.27
    cost_multiplier: float = COST_BASELINE_MULTIPLIER
    walk_forward_step_months: int = 6    # Anchored walk-forward step size
    rebalance_every_n_days: int = 10     # Bi-weekly for S4


@dataclass
class DailyState:
    """State of the portfolio on a single day."""
    date: date
    positions: dict[str, int]            # {ticker: lots (signed)}
    nav_gbp: float
    daily_return: float
    turnover: float
    signals: dict[str, float]
    transaction_costs_gbp: float


@dataclass
class BacktestResult:
    """Full results from a backtest run."""
    config: BacktestConfig
    daily_returns: pd.Series             # Indexed by date
    daily_nav: pd.Series
    daily_states: list[DailyState] = field(default_factory=list)
    metrics: Optional[PerformanceMetrics] = None
    warnings: list[str] = field(default_factory=list)

    def compute_metrics(self) -> PerformanceMetrics:
        """Compute and cache performance metrics."""
        self.metrics = compute_metrics(
            returns=self.daily_returns,
            period=self.config.period_label,
            strategy_id=self.config.strategy_id,
        )
        return self.metrics


class WalkForwardEngine:
    """
    Anchored walk-forward backtesting engine.

    Anchored = IS always starts from IS_START, window expands each step.
    This is preferred over rolling because it uses all available IS data
    and better reflects how the strategy is deployed in practice.

    Usage:
        engine = WalkForwardEngine(strategy, price_data)
        is_result  = engine.run(BacktestConfig("S4", IS_START, IS_END, "IS"))
        oos_result = engine.run(BacktestConfig("S4", OOS_START, OOS_END, "OOS"))
    """

    def __init__(
        self,
        strategy: BaseStrategy,
        price_data: dict[str, pd.DataFrame],
    ) -> None:
        """
        Args:
            strategy:   Strategy instance to backtest
            price_data: {ticker: DataFrame} with full history
                        DataFrame must have DatetimeIndex (UTC) and columns:
                        close, cash_price, f3m_price, f3m_dte
        """
        self.strategy = strategy
        self.price_data = price_data
        self._validate_data()

    def _validate_data(self) -> None:
        """Check data quality before running backtest."""
        required_cols = {"close", "cash_price", "f3m_price"}
        for ticker, df in self.price_data.items():
            missing = required_cols - set(df.columns)
            if missing:
                raise ValueError(f"{ticker}: missing columns {missing}")
            if not isinstance(df.index, pd.DatetimeIndex):
                raise ValueError(f"{ticker}: index must be DatetimeIndex")
            nan_pct = df["close"].isna().mean()
            if nan_pct > 0.05:
                logger.warning("%s: %.1f%% NaN in close prices.", ticker, nan_pct * 100)

    def run(self, config: BacktestConfig) -> BacktestResult:
        """
        Run a backtest over the specified date range.

        This is the main entry point. It iterates day by day through the
        trading calendar, computes signals on rebalance days, sizes positions,
        and tracks P&L. All look-ahead bias prevention happens here.

        Args:
            config: BacktestConfig specifying the period and parameters

        Returns:
            BacktestResult with daily returns and computed metrics
        """
        logger.info(
            "Starting %s backtest: %s | %s → %s",
            config.strategy_id, config.period_label,
            config.start_date, config.end_date,
        )

        trading_days = trading_days_between(config.start_date, config.end_date)

        if len(trading_days) < 60:
            raise ValueError(
                f"Only {len(trading_days)} trading days in range — need ≥60."
            )

        # Portfolio state
        current_positions: dict[str, int] = {t: 0 for t in self.strategy.universe}
        nav = config.total_capital_gbp
        daily_returns: list[float] = []
        daily_nav: list[float] = []
        daily_states: list[DailyState] = []
        last_rebalance: Optional[date] = None

        for i, today in enumerate(trading_days):
            today_ts = pd.Timestamp(today, tz="UTC")

            # ── Get today's prices ───────────────────────────────────────────
            prices = self._get_prices_on_date(today)
            if not prices:
                logger.warning("No prices available on %s — skipping.", today)
                daily_returns.append(0.0)
                daily_nav.append(nav)
                continue

            # ── Compute P&L from yesterday's positions ───────────────────────
            prev_day = trading_days[i - 1] if i > 0 else today
            prev_prices = self._get_prices_on_date(prev_day) if i > 0 else prices
            daily_pnl_gbp = self._compute_daily_pnl(
                current_positions, prices, prev_prices,
                config.usd_gbp_rate,
            )
            daily_return = daily_pnl_gbp / nav if nav > 0 else 0.0
            nav += daily_pnl_gbp

            # ── Rebalance check ──────────────────────────────────────────────
            should_rebalance = self._should_rebalance(
                today, last_rebalance, config.rebalance_every_n_days, i,
            )

            transaction_costs_gbp = 0.0
            new_signals: dict[str, float] = {}

            if should_rebalance:
                # ── Get data available UP TO today (strict <=, no look-ahead)
                data_to_date = {
                    ticker: df[df.index <= today_ts].copy()
                    for ticker, df in self.price_data.items()
                }

                # ── Compute signals ──────────────────────────────────────────
                as_of = datetime(today.year, today.month, today.day, 14, 0, tzinfo=timezone.utc)
                signal_result: SignalResult = self.strategy.run(data_to_date, as_of=as_of)
                new_signals = signal_result.signals

                if signal_result.is_valid:
                    # ── Compute vol estimates (only data up to today) ────────
                    vol_estimates = self._compute_vols(data_to_date, today_ts)

                    # ── Size new target positions ────────────────────────────
                    target_positions = self._compute_target_positions(
                        signals=new_signals,
                        vol_estimates=vol_estimates,
                        prices=prices,
                        nav_gbp=nav,
                        strategy_id=config.strategy_id,
                        usd_gbp_rate=config.usd_gbp_rate,
                    )

                    # ── Compute trades needed and their costs ────────────────
                    trades = {
                        t: target_positions.get(t, 0) - current_positions.get(t, 0)
                        for t in self.strategy.universe
                    }
                    transaction_costs_gbp = self._compute_trade_costs(
                        trades=trades,
                        prices=prices,
                        usd_gbp_rate=config.usd_gbp_rate,
                        multiplier=config.cost_multiplier,
                    )

                    # ── Apply costs ──────────────────────────────────────────
                    nav -= transaction_costs_gbp
                    daily_return -= transaction_costs_gbp / (nav + transaction_costs_gbp)
                    current_positions = target_positions
                    last_rebalance = today

            # ── Record daily state ───────────────────────────────────────────
            daily_returns.append(daily_return)
            daily_nav.append(nav)
            daily_states.append(DailyState(
                date=today,
                positions=current_positions.copy(),
                nav_gbp=nav,
                daily_return=daily_return,
                turnover=sum(abs(t) for t in (trades.values() if should_rebalance and signal_result.is_valid else {}.values())),
                signals=new_signals,
                transaction_costs_gbp=transaction_costs_gbp,
            ))

        # ── Build result ─────────────────────────────────────────────────────
        idx = pd.DatetimeIndex([pd.Timestamp(d, tz="UTC") for d in trading_days[:len(daily_returns)]])
        returns_series = pd.Series(daily_returns, index=idx, name="daily_return")
        nav_series     = pd.Series(daily_nav,     index=idx, name="nav_gbp")

        result = BacktestResult(
            config=config,
            daily_returns=returns_series,
            daily_nav=nav_series,
            daily_states=daily_states,
        )
        result.compute_metrics()

        logger.info("\n%s", result.metrics.summary() if result.metrics else "No metrics")
        return result

    def run_walk_forward(
        self,
        capital_gbp: float = 100_000.0,
    ) -> tuple[BacktestResult, BacktestResult]:
        """
        Run the full anchored walk-forward: IS then OOS.

        Returns (is_result, oos_result).
        Computes WFER automatically.
        """
        sid = self.strategy.strategy_id

        is_result = self.run(BacktestConfig(
            strategy_id=sid, start_date=IS_START, end_date=IS_END,
            period_label="IS", total_capital_gbp=capital_gbp,
        ))
        oos_result = self.run(BacktestConfig(
            strategy_id=sid, start_date=OOS_START, end_date=OOS_END,
            period_label="OOS", total_capital_gbp=capital_gbp,
        ))

        is_sharpe  = is_result.metrics.sharpe_ratio  if is_result.metrics  else 0.0
        oos_sharpe = oos_result.metrics.sharpe_ratio if oos_result.metrics else 0.0
        wfer = compute_wfer(is_sharpe, oos_sharpe)

        logger.info(
            "\n%s Walk-Forward Results:\n"
            "  IS Sharpe:  %.2f\n"
            "  OOS Sharpe: %.2f\n"
            "  WFER:       %.2f %s\n"
            "  Verdict:    %s",
            sid, is_sharpe, oos_sharpe, wfer,
            "(≥0.50 target)" ,
            "✅ PASS" if wfer >= 0.50 and oos_sharpe >= 0.30 else "❌ FAIL",
        )

        return is_result, oos_result

    # ─────────────────────────────────────────────────────────────────────────
    # PRIVATE HELPERS
    # ─────────────────────────────────────────────────────────────────────────

    def _get_prices_on_date(self, d: date) -> dict[str, float]:
        """Get close prices for all tickers on a specific date."""
        ts = pd.Timestamp(d, tz="UTC")
        prices: dict[str, float] = {}
        for ticker, df in self.price_data.items():
            if ts in df.index:
                val = df.loc[ts, "close"]
                if not pd.isna(val) and float(val) > 0:
                    prices[ticker] = float(val)
        return prices

    def _compute_daily_pnl(
        self,
        positions: dict[str, int],
        today_prices: dict[str, float],
        prev_prices: dict[str, float],
        usd_gbp_rate: float,
    ) -> float:
        """
        P&L from price moves on existing positions.
        Positions are in lots (integer), price moves are per unit × lot_size.
        """
        from worfin.config.metals import get_metal
        pnl_usd = 0.0
        for ticker, lots in positions.items():
            if lots == 0:
                continue
            if ticker not in today_prices or ticker not in prev_prices:
                continue
            metal = get_metal(ticker)
            price_move = today_prices[ticker] - prev_prices[ticker]
            pnl_usd += lots * metal.lot_size * price_move
        return pnl_usd / usd_gbp_rate

    def _compute_vols(
        self,
        data_to_date: dict[str, pd.DataFrame],
        as_of_ts: pd.Timestamp,
    ) -> dict[str, dict[str, float]]:
        """Compute 20d and 60d vol for each ticker using only available data."""
        vols: dict[str, dict[str, float]] = {}
        for ticker, df in data_to_date.items():
            if len(df) < 20:
                vols[ticker] = {"vol_20d": VOL_FLOOR, "vol_60d": VOL_FLOOR}
                continue
            try:
                vols[ticker] = compute_vol_estimates(df["close"], as_of_date=as_of_ts)
            except Exception as e:
                logger.warning("Vol computation failed for %s: %s", ticker, e)
                vols[ticker] = {"vol_20d": VOL_FLOOR, "vol_60d": VOL_FLOOR}
        return vols

    def _compute_target_positions(
        self,
        signals: dict[str, float],
        vol_estimates: dict[str, dict[str, float]],
        prices: dict[str, float],
        nav_gbp: float,
        strategy_id: str,
        usd_gbp_rate: float,
    ) -> dict[str, int]:
        """Size positions using vol-targeting formula."""
        target: dict[str, int] = {}
        for ticker in self.strategy.universe:
            signal = signals.get(ticker, 0.0)
            if signal == 0.0 or ticker not in prices or ticker not in vol_estimates:
                target[ticker] = 0
                continue
            vols = vol_estimates[ticker]
            lots = compute_lots(
                strategy_id=strategy_id,
                ticker=ticker,
                total_capital_gbp=nav_gbp,
                realised_vol_20d=vols["vol_20d"],
                realised_vol_60d=vols["vol_60d"],
                signal=signal,
                current_price_usd=prices[ticker],
                usd_gbp_rate=usd_gbp_rate,
            )
            target[ticker] = lots
        return target

    def _compute_trade_costs(
        self,
        trades: dict[str, int],
        prices: dict[str, float],
        usd_gbp_rate: float,
        multiplier: float,
    ) -> float:
        """Compute total transaction costs for a set of trades in GBP."""
        from worfin.backtest.costs import compute_trade_cost
        from worfin.config.metals import get_metal
        total_cost_usd = 0.0
        for ticker, lots_delta in trades.items():
            if lots_delta == 0 or ticker not in prices:
                continue
            metal = get_metal(ticker)
            notional_usd = abs(lots_delta) * metal.lot_size * prices[ticker]
            cost = compute_trade_cost(
                ticker=ticker,
                notional_usd=notional_usd,
                lots=abs(lots_delta),
                multiplier=multiplier,
            )
            total_cost_usd += cost.total_cost_usd
        return total_cost_usd / usd_gbp_rate

    def _should_rebalance(
        self,
        today: date,
        last_rebalance: Optional[date],
        every_n_days: int,
        day_index: int,
    ) -> bool:
        """True if today is a rebalance day."""
        if last_rebalance is None:
            return day_index >= 60  # Wait for minimum history
        days_since = (today - last_rebalance).days
        return days_since >= every_n_days