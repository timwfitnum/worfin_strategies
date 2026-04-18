"""
backtest/engine.py
Walk-forward backtesting engine for WorFIn strategies.

ARCHITECTURE:
  The engine is the integration layer that wires together:
    data → signals → sizing → costs → pre-trade checks → positions → metrics

  This is where subtle bugs hide. Every design decision here
  must be justified against look-ahead bias and cost accuracy.

KEY DESIGN DECISIONS:
  1. Strict date isolation: on each bar, only data up to and including
     that bar's timestamp is visible to the strategy. No look-ahead.
  2. Signals computed BEFORE position sizing (signals drive size).
  3. Transaction costs applied on the day of trade execution, not signal.
  4. Positions carried at yesterday's close until rebalance date.
  5. Anchored walk-forward: IS always starts from IS_START.
  6. Pre-trade checks applied on every rebalance — same 8 checks as live.
  7. FX rate fetched from DB/FRED per run — never hardcoded.

LOOK-AHEAD BIAS PREVENTION (critical):
  - Data sliced with df[df.index <= current_date] — strict <=
  - Vol estimates use only data available at signal time
  - Carry DTE uses trade date, not settlement date
  - No future prices used for any calculation
  - FX rates prefetched using data up to the bar date
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field
from datetime import UTC, date, datetime

import pandas as pd
from sqlalchemy.engine import Engine

from worfin.backtest.costs import COST_BASELINE_MULTIPLIER
from worfin.backtest.metrics import PerformanceMetrics, compute_metrics
from worfin.backtest.pretrade_integration import (  # ← correct spelling
    build_portfolio_state,
    compute_adv,
    log_rejections_to_audit,
    run_pretrade_checks,
)
from worfin.config.calendar import trading_days_between
from worfin.data.ingestion.fx_rates import FxRateUnavailable, get_usd_gbp, prefetch_fx_rates
from worfin.execution.pretrade_checks import PreTradeChecker
from worfin.risk.limits import VOL_FLOOR
from worfin.risk.sizing import compute_lots
from worfin.strategies.base import BaseStrategy, SignalResult

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# FIXED DATA SPLITS — SET ONCE, NEVER CHANGE
# ─────────────────────────────────────────────────────────────────────────────
IS_START = date(2005, 1, 1)
IS_END = date(2017, 12, 31)
OOS_START = date(2018, 1, 1)
OOS_END = date(2022, 12, 31)
HOLDOUT_START = date(2023, 1, 1)


@dataclass
class BacktestConfig:
    """Configuration for a single backtest run."""

    strategy_id: str
    start_date: date
    end_date: date
    period_label: str  # "IS", "OOS", "Holdout"
    total_capital_gbp: float = 100_000.0
    # usd_gbp_rate is intentionally NOT a field here.
    # The engine fetches a live rate from raw_data.fx_rates / FRED for
    # each bar. Hardcoding 1.27 would produce systematically wrong sizing
    # across decades of backtest data. See WalkForwardEngine._get_fx_rate().
    cost_multiplier: float = COST_BASELINE_MULTIPLIER
    walk_forward_step_months: int = 6  # Anchored walk-forward step size
    rebalance_every_n_days: int = 10  # Bi-weekly for S4
    enable_pretrade_checks: bool = True  # Wire in production checks
    db_engine: Engine | None = field(default=None, repr=False)
    # Set at run-start; groups all DB writes for this simulation
    backtest_run_id: str = field(default_factory=lambda: str(uuid.uuid4()))


@dataclass
class DailyState:
    """State of the portfolio on a single day."""

    date: date
    positions: dict[str, int]  # {ticker: lots (signed)}
    nav_gbp: float
    daily_return: float
    turnover: float
    signals: dict[str, float]
    transaction_costs_gbp: float
    usd_gbp_rate: float  # actual rate used on this bar


@dataclass
class BacktestResult:
    """Full results from a backtest run."""

    config: BacktestConfig
    daily_returns: pd.Series  # Indexed by date
    daily_nav: pd.Series
    daily_states: list[DailyState] = field(default_factory=list)
    metrics: PerformanceMetrics | None = None
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
        engine = WalkForwardEngine(strategy, price_data, db_engine=engine)
        is_result  = engine.run(BacktestConfig("S4", IS_START, IS_END, "IS"))
        oos_result = engine.run(BacktestConfig("S4", OOS_START, OOS_END, "OOS"))
    """

    def __init__(
        self,
        strategy: BaseStrategy,
        price_data: dict[str, pd.DataFrame],
        db_engine: Engine | None = None,
    ) -> None:
        """
        Args:
            strategy:   Strategy instance to backtest
            price_data: {ticker: DataFrame} with full history
                        DataFrame must have DatetimeIndex (UTC) and columns:
                        close, cash_price, f3m_price, f3m_dte
            db_engine:  SQLAlchemy engine for FX rate lookup and audit writes.
                        If None, FX rates are fetched from FRED directly (no caching).
        """
        self.strategy = strategy
        self.price_data = price_data
        self.db_engine = db_engine
        self._pretrade_checker = PreTradeChecker()
        self._validate_data()
        # FX rate cache: {date: rate} prefetched at run start
        self._fx_cache: dict[date, float] = {}

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

    # ─────────────────────────────────────────────────────────────────────────
    # FX RATE HELPERS
    # ─────────────────────────────────────────────────────────────────────────

    def _prefetch_fx_rates(self, start: date, end: date) -> None:
        """
        Bulk-warm the FX cache for the backtest date range.
        Called once at run() start — avoids per-bar FRED calls.
        """
        try:
            self._fx_cache = prefetch_fx_rates(start, end, engine=self.db_engine)
            logger.info(
                "Prefetched %d USD/GBP rates for %s → %s.",
                len(self._fx_cache),
                start,
                end,
            )
        except Exception:
            logger.warning("FX prefetch failed — will fall back to per-bar lookup.", exc_info=True)

    def _get_fx_rate(self, bar_date: date) -> float:
        """
        Return the USD/GBP rate for a single bar.

        Uses the prefetched cache first (populated by _prefetch_fx_rates).
        Falls back to get_usd_gbp() (DB → FRED → prior-day fallback).
        Raises FxRateUnavailable if nothing found.
        """
        if bar_date in self._fx_cache:
            return self._fx_cache[bar_date]
        # Miss — hit get_usd_gbp which has its own cache + fallback
        rate = get_usd_gbp(bar_date, engine=self.db_engine)
        self._fx_cache[bar_date] = rate
        return rate

    # ─────────────────────────────────────────────────────────────────────────
    # MAIN RUN LOOP
    # ─────────────────────────────────────────────────────────────────────────

    def run(self, config: BacktestConfig) -> BacktestResult:
        """
        Run a backtest over the specified date range.

        This is the main entry point. It iterates day by day through the
        trading calendar, computes signals on rebalance days, sizes positions,
        runs pre-trade checks, and tracks P&L.
        All look-ahead bias prevention happens here.

        Args:
            config: BacktestConfig specifying the period and parameters

        Returns:
            BacktestResult with daily returns and computed metrics
        """
        logger.info(
            "Starting %s backtest: %s | %s → %s  run_id=%s",
            config.strategy_id,
            config.period_label,
            config.start_date,
            config.end_date,
            config.backtest_run_id,
        )

        trading_days = trading_days_between(config.start_date, config.end_date)

        if len(trading_days) < 60:
            raise ValueError(f"Only {len(trading_days)} trading days in range — need ≥60.")

        # Prefetch FX rates for the entire date range (one FRED call)
        self._prefetch_fx_rates(config.start_date, config.end_date)

        # Portfolio state
        current_positions: dict[str, int] = dict.fromkeys(self.strategy.universe, 0)
        nav = config.total_capital_gbp
        daily_returns: list[float] = []
        daily_nav: list[float] = []
        daily_states: list[DailyState] = []
        warnings: list[str] = []
        last_rebalance: date | None = None
        orders_today_count: int = 0

        for i, today in enumerate(trading_days):
            today_ts = pd.Timestamp(today, tz="UTC")

            # ── Fetch FX rate for this bar ────────────────────────────────────
            try:
                usd_gbp = self._get_fx_rate(today)
            except FxRateUnavailable as exc:
                msg = f"{today}: FX rate unavailable — {exc}. Skipping bar."
                logger.error(msg)
                warnings.append(msg)
                daily_returns.append(0.0)
                daily_nav.append(nav)
                continue

            # ── Get today's prices ────────────────────────────────────────────
            prices = self._get_prices_on_date(today)
            if not prices:
                logger.warning("No prices available on %s — skipping.", today)
                daily_returns.append(0.0)
                daily_nav.append(nav)
                continue

            # ── Compute P&L from yesterday's positions ────────────────────────
            prev_day = trading_days[i - 1] if i > 0 else today
            prev_prices = self._get_prices_on_date(prev_day) if i > 0 else prices
            daily_pnl_gbp = self._compute_daily_pnl(
                current_positions,
                prices,
                prev_prices,
                usd_gbp,
            )
            daily_return = daily_pnl_gbp / nav if nav > 0 else 0.0
            nav += daily_pnl_gbp

            # ── Rebalance check ───────────────────────────────────────────────
            should_rebalance = self._should_rebalance(
                today,
                last_rebalance,
                config.rebalance_every_n_days,
                i,
            )

            transaction_costs_gbp = 0.0
            new_signals: dict[str, float] = {}
            orders_today_count = 0 if should_rebalance else orders_today_count

            if should_rebalance:
                # ── Get data available UP TO today (strict <=, no look-ahead)
                data_to_date = {
                    ticker: df[df.index <= today_ts].copy()
                    for ticker, df in self.price_data.items()
                }

                # ── Compute signals ──────────────────────────────────────────
                as_of = datetime(today.year, today.month, today.day, 14, 0, tzinfo=UTC)
                signal_result: SignalResult = self.strategy.run(data_to_date, as_of=as_of)
                new_signals = signal_result.signals

                if signal_result.is_valid:
                    # ── Compute vol estimates (only data up to today) ─────────
                    vol_estimates = self._compute_vols(data_to_date, today_ts)

                    # ── Size new target positions ─────────────────────────────
                    target_positions = self._compute_target_positions(
                        signals=new_signals,
                        vol_estimates=vol_estimates,
                        prices=prices,
                        nav_gbp=nav,
                        strategy_id=config.strategy_id,
                        usd_gbp_rate=usd_gbp,
                    )

                    # ── Compute deltas (what we actually need to trade) ────────
                    trades = {
                        t: target_positions.get(t, 0) - current_positions.get(t, 0)
                        for t in self.strategy.universe
                    }

                    # ── Pre-trade checks ──────────────────────────────────────
                    if config.enable_pretrade_checks:
                        adv = compute_adv(data_to_date, today_ts)
                        portfolio_state = build_portfolio_state(
                            nav_gbp=nav,
                            current_lots=current_positions,
                            current_prices_usd=prices,
                            usd_gbp_rate=usd_gbp,
                            orders_today=orders_today_count,
                            adv_by_ticker=adv,
                        )
                        decisions = run_pretrade_checks(
                            checker=self._pretrade_checker,
                            proposed_deltas=trades,
                            current_lots=current_positions,
                            prices_usd=prices,
                            signals=new_signals,
                            signal_timestamp=as_of,
                            strategy_id=config.strategy_id,
                            portfolio=portfolio_state,
                            usd_gbp_rate=usd_gbp,
                        )
                        # Apply decisions: use approved trades, hold rejected
                        approved_trades: dict[str, int] = {}
                        for d in decisions:
                            if d.proposed_lots == 0:
                                continue
                            approved_trades[d.ticker] = d.proposed_lots if d.approved else 0
                        # Write rejections to audit
                        log_rejections_to_audit(
                            engine=config.db_engine,
                            decisions=decisions,
                            as_of_ts=as_of,
                            backtest_run_id=config.backtest_run_id,
                        )
                        trades = approved_trades
                        orders_today_count = sum(1 for v in trades.values() if v != 0)

                    # ── Compute costs ─────────────────────────────────────────
                    transaction_costs_gbp = self._compute_trade_costs(
                        trades=trades,
                        prices=prices,
                        usd_gbp_rate=usd_gbp,
                        multiplier=config.cost_multiplier,
                    )

                    # ── Apply trades and costs ────────────────────────────────
                    nav -= transaction_costs_gbp
                    if nav > 0:
                        daily_return -= transaction_costs_gbp / (nav + transaction_costs_gbp)
                    for ticker, delta in trades.items():
                        current_positions[ticker] = current_positions.get(ticker, 0) + delta
                    last_rebalance = today

            # ── Record daily state ────────────────────────────────────────────
            daily_returns.append(daily_return)
            daily_nav.append(nav)

            turnover = self._compute_turnover(
                trades if should_rebalance else {}, prices, nav, usd_gbp
            )

            daily_states.append(
                DailyState(
                    date=today,
                    positions=dict(current_positions),
                    nav_gbp=nav,
                    daily_return=daily_return,
                    turnover=turnover,
                    signals=new_signals,
                    transaction_costs_gbp=transaction_costs_gbp,
                    usd_gbp_rate=usd_gbp,
                )
            )

        # ── Build result ──────────────────────────────────────────────────────
        date_index = pd.DatetimeIndex([pd.Timestamp(d) for d in trading_days[: len(daily_returns)]])
        result = BacktestResult(
            config=config,
            daily_returns=pd.Series(daily_returns, index=date_index, name="returns"),
            daily_nav=pd.Series(daily_nav, index=date_index, name="nav_gbp"),
            daily_states=daily_states,
            warnings=warnings,
        )
        result.compute_metrics()

        logger.info(
            "Backtest complete: %s %s | Sharpe=%.2f | MaxDD=%.1f%% | n_days=%d",
            config.strategy_id,
            config.period_label,
            result.metrics.sharpe_ratio if result.metrics else float("nan"),
            (result.metrics.max_drawdown * 100) if result.metrics else float("nan"),
            len(daily_returns),
        )
        return result

    # ─────────────────────────────────────────────────────────────────────────
    # PRIVATE HELPERS
    # ─────────────────────────────────────────────────────────────────────────

    def _get_prices_on_date(self, bar_date: date) -> dict[str, float]:
        """Return {ticker: close_price} for bar_date. Skips tickers with no data."""
        prices: dict[str, float] = {}
        bar_ts = pd.Timestamp(bar_date, tz="UTC")
        for ticker, df in self.price_data.items():
            if bar_ts in df.index:
                val = df.loc[bar_ts, "close"]
                if pd.notna(val) and float(val) > 0:
                    prices[ticker] = float(val)
        return prices

    def _compute_daily_pnl(
        self,
        positions: dict[str, int],
        today_prices: dict[str, float],
        prev_prices: dict[str, float],
        usd_gbp_rate: float,
    ) -> float:
        """Compute total portfolio P&L in GBP for one bar."""
        from worfin.config.metals import get_metal

        pnl_usd = 0.0
        for ticker, lots in positions.items():
            if lots == 0:
                continue
            t_price = today_prices.get(ticker)
            p_price = prev_prices.get(ticker)
            if t_price is None or p_price is None:
                continue
            metal = get_metal(ticker)
            pnl_usd += lots * metal.lot_size * (t_price - p_price)
        return pnl_usd / usd_gbp_rate

    def _compute_vols(
        self,
        data: dict[str, pd.DataFrame],
        as_of_ts: pd.Timestamp,
    ) -> dict[str, dict[str, float]]:
        """Compute 20d and 60d vol estimates per ticker. No look-ahead."""

        result: dict[str, dict[str, float]] = {}
        for ticker, df in data.items():
            window = df[df.index <= as_of_ts]["close"].dropna()
            if len(window) < 21:
                result[ticker] = {"vol_20d": VOL_FLOOR, "vol_60d": VOL_FLOOR}
                continue
            log_rets = (window / window.shift(1)).apply(
                lambda x: float("nan") if x <= 0 else __import__("math").log(x)
            )
            vol_20 = float(log_rets.rolling(20).std().iloc[-1]) * (252**0.5)
            vol_60 = float(log_rets.rolling(60).std().iloc[-1]) * (252**0.5)
            result[ticker] = {
                "vol_20d": max(
                    vol_20 if not __import__("math").isnan(vol_20) else VOL_FLOOR, VOL_FLOOR
                ),
                "vol_60d": max(
                    vol_60 if not __import__("math").isnan(vol_60) else VOL_FLOOR, VOL_FLOOR
                ),
            }
        return result

    def _compute_target_positions(
        self,
        signals: dict[str, float],
        vol_estimates: dict[str, dict[str, float]],
        prices: dict[str, float],
        nav_gbp: float,
        strategy_id: str,
        usd_gbp_rate: float,
    ) -> dict[str, int]:
        """Compute target lot count for each ticker in the universe."""
        result: dict[str, int] = {}
        for ticker in self.strategy.universe:
            signal = signals.get(ticker, 0.0)
            if ticker not in vol_estimates or ticker not in prices:
                result[ticker] = 0
                continue
            vols = vol_estimates[ticker]
            result[ticker] = compute_lots(
                strategy_id=strategy_id,
                ticker=ticker,
                total_capital_gbp=nav_gbp,
                realised_vol_20d=vols["vol_20d"],
                realised_vol_60d=vols["vol_60d"],
                signal=signal,
                current_price_usd=prices[ticker],
                usd_gbp_rate=usd_gbp_rate,
            )
        return result

    def _compute_trade_costs(
        self,
        trades: dict[str, int],
        prices: dict[str, float],
        usd_gbp_rate: float,
        multiplier: float,
    ) -> float:
        """Total transaction costs for a set of trades, converted to GBP."""
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

    def _compute_turnover(
        self,
        trades: dict[str, int],
        prices: dict[str, float],
        nav_gbp: float,
        usd_gbp_rate: float,
    ) -> float:
        """Single-trip turnover as fraction of NAV."""
        from worfin.config.metals import get_metal

        if nav_gbp <= 0:
            return 0.0
        total_usd = 0.0
        for ticker, lots in trades.items():
            if lots == 0 or ticker not in prices:
                continue
            metal = get_metal(ticker)
            total_usd += abs(lots) * metal.lot_size * prices[ticker]
        return (total_usd / usd_gbp_rate) / nav_gbp

    def _should_rebalance(
        self,
        today: date,
        last_rebalance: date | None,
        every_n_days: int,
        day_index: int,
    ) -> bool:
        """True if today is a rebalance day."""
        if last_rebalance is None:
            return day_index >= 60  # Wait for minimum history
        days_since = (today - last_rebalance).days
        return days_since >= every_n_days
