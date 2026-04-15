"""
backtest/metrics.py
Comprehensive performance metrics for strategy validation.

Every strategy MUST pass the minimum thresholds defined here
before advancing through the graduation pipeline.

Key statistical note:
  Sharpe = annualised_return / annualised_vol is a POINT ESTIMATE.
  Always compute and report the t-statistic:
    t = Sharpe × √N   (where N = independent observations)
  A Sharpe of 0.8 on 3 years of data (t≈1.4) is not statistically significant.
  Minimum t-stat: 3.0 IS, 2.0 OOS (Harvey, Liu & Zhu, 2016).
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field

import pandas as pd

logger = logging.getLogger(__name__)

TRADING_DAYS_PER_YEAR = 252
RISK_FREE_RATE = 0.04  # 4% — approximate UK base rate; update annually

# ─────────────────────────────────────────────────────────────────────────────
# MINIMUM THRESHOLDS (from Backtesting & Validation Protocol)
# ─────────────────────────────────────────────────────────────────────────────

THRESHOLDS = {
    "IS": {
        "sharpe_ratio": 0.50,
        "max_drawdown": 0.20,  # Max acceptable (lower is better)
        "t_stat_sharpe": 3.00,
    },
    "OOS": {
        "sharpe_ratio": 0.30,
        "max_drawdown": 0.15,
        "t_stat_sharpe": 2.00,
        "wfer": 0.50,
        "pbo": 0.20,  # Must be BELOW this (lower = less overfitted)
        "calmar_ratio": 0.50,
    },
}


@dataclass
class PerformanceMetrics:
    """Full set of performance metrics for one strategy / one period."""

    period: str  # "IS", "OOS", "Holdout", "Paper", "Live"
    strategy_id: str
    start_date: str
    end_date: str
    n_trading_days: int

    # Return metrics
    total_return: float
    annualised_return: float
    annualised_vol: float

    # Risk-adjusted metrics
    sharpe_ratio: float
    sortino_ratio: float
    calmar_ratio: float

    # Drawdown
    max_drawdown: float
    avg_drawdown: float
    max_drawdown_duration_days: int

    # Trade statistics
    win_rate: float
    profit_factor: float
    avg_trade_return: float
    avg_win: float
    avg_loss: float
    avg_holding_days: float
    annual_turnover: float

    # Statistical significance
    t_stat_sharpe: float  # Sharpe × √N_independent

    # Overfitting metrics (OOS only)
    wfer: float | None = None  # Walk-Forward Efficiency Ratio
    pbo: float | None = None  # Probability of Backtest Overfitting

    # Gate pass/fail
    gate_results: dict[str, bool] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.gate_results = self._evaluate_gates()

    def _evaluate_gates(self) -> dict[str, bool]:
        """Evaluate pass/fail for each threshold criterion."""
        gates: dict[str, bool] = {}
        thresholds = THRESHOLDS.get(self.period, {})

        if "sharpe_ratio" in thresholds:
            gates["sharpe_ratio"] = self.sharpe_ratio >= thresholds["sharpe_ratio"]
        if "max_drawdown" in thresholds:
            gates["max_drawdown"] = self.max_drawdown <= thresholds["max_drawdown"]
        if "t_stat_sharpe" in thresholds:
            gates["t_stat_sharpe"] = self.t_stat_sharpe >= thresholds["t_stat_sharpe"]
        if "wfer" in thresholds and self.wfer is not None:
            gates["wfer"] = self.wfer >= thresholds["wfer"]
        if "pbo" in thresholds and self.pbo is not None:
            gates["pbo"] = self.pbo <= thresholds["pbo"]  # BELOW threshold
        if "calmar_ratio" in thresholds:
            gates["calmar_ratio"] = self.calmar_ratio >= thresholds["calmar_ratio"]

        return gates

    @property
    def passes_all_gates(self) -> bool:
        return all(self.gate_results.values())

    def summary(self) -> str:
        """Human-readable performance summary for reports."""
        gate_str = " | ".join(f"{'✅' if v else '❌'} {k}" for k, v in self.gate_results.items())
        return (
            f"[{self.period}] {self.strategy_id} | {self.start_date} → {self.end_date}\n"
            f"  Return: {self.annualised_return:.1%} | Vol: {self.annualised_vol:.1%} "
            f"| Sharpe: {self.sharpe_ratio:.2f} (t={self.t_stat_sharpe:.1f})\n"
            f"  Max DD: {self.max_drawdown:.1%} | Calmar: {self.calmar_ratio:.2f} "
            f"| Win rate: {self.win_rate:.1%}\n"
            f"  Gates: {gate_str}\n"
            f"  {'✅ PASS' if self.passes_all_gates else '❌ FAIL — do not advance'}"
        )


def compute_metrics(
    returns: pd.Series,
    period: str,
    strategy_id: str,
    trade_log: pd.DataFrame | None = None,
    wfer: float | None = None,
    pbo: float | None = None,
) -> PerformanceMetrics:
    """
    Compute the full set of performance metrics from a daily return series.

    Args:
        returns:     Daily return series (as decimal fractions, e.g. 0.01 = 1%)
                     Must be net of ALL transaction costs.
        period:      "IS", "OOS", "Holdout", "Paper", or "Live"
        strategy_id: e.g. "S4"
        trade_log:   Optional DataFrame with individual trade returns and holding periods
        wfer:        Walk-Forward Efficiency Ratio (OOS only)
        pbo:         Probability of Backtest Overfitting (OOS only)

    Returns:
        PerformanceMetrics with all metrics computed and gates evaluated.
    """
    returns = returns.dropna()

    if len(returns) < 20:
        logger.error(
            "Cannot compute metrics for %s %s: only %d observations (need ≥20).",
            strategy_id,
            period,
            len(returns),
        )
        raise ValueError(f"Insufficient data: {len(returns)} observations")

    n = len(returns)
    ann_factor = TRADING_DAYS_PER_YEAR

    # ── Return metrics ────────────────────────────────────────────────────────
    total_return = float((1 + returns).prod() - 1)
    years = n / ann_factor
    annualised_return = float((1 + total_return) ** (1 / years) - 1) if years > 0 else 0.0
    annualised_vol = float(returns.std() * math.sqrt(ann_factor))

    # ── Risk-adjusted ─────────────────────────────────────────────────────────
    excess_return = annualised_return - RISK_FREE_RATE
    sharpe = float(excess_return / annualised_vol) if annualised_vol > 0 else 0.0

    # Sortino: uses downside deviation (below 0, not below risk-free rate)
    downside_returns = returns[returns < 0]
    downside_vol = (
        float(downside_returns.std() * math.sqrt(ann_factor))
        if len(downside_returns) > 1
        else annualised_vol
    )
    sortino = float(annualised_return / downside_vol) if downside_vol > 0 else 0.0

    # ── Drawdown ──────────────────────────────────────────────────────────────
    cum_returns = (1 + returns).cumprod()
    rolling_max = cum_returns.cummax()
    drawdown_series = (cum_returns - rolling_max) / rolling_max

    max_drawdown = float(drawdown_series.min()) * -1  # Positive number for display
    avg_drawdown = (
        float(drawdown_series[drawdown_series < 0].mean()) * -1
        if (drawdown_series < 0).any()
        else 0.0
    )

    calmar = float(annualised_return / max_drawdown) if max_drawdown > 0 else 0.0

    # Max drawdown duration
    max_dd_duration = _compute_max_drawdown_duration(drawdown_series)

    # ── Statistical significance ──────────────────────────────────────────────
    # Use √(N_trading_days / 5) as independent observations (weekly decorrelation)
    # Conservative — daily returns are autocorrelated
    n_independent = n / 5
    t_stat = float(sharpe * math.sqrt(n_independent))

    # ── Trade statistics (if trade log provided) ──────────────────────────────
    if trade_log is not None and len(trade_log) > 0:
        win_rate = float((trade_log["return"] > 0).mean())
        profit_factor = _compute_profit_factor(trade_log["return"])
        avg_trade_return = float(trade_log["return"].mean())
        avg_win = (
            float(trade_log.loc[trade_log["return"] > 0, "return"].mean())
            if (trade_log["return"] > 0).any()
            else 0.0
        )
        avg_loss = (
            float(trade_log.loc[trade_log["return"] < 0, "return"].mean())
            if (trade_log["return"] < 0).any()
            else 0.0
        )
        avg_holding_days = (
            float(trade_log["holding_days"].mean()) if "holding_days" in trade_log.columns else 0.0
        )
        annual_turnover = float(len(trade_log) / years)
    else:
        # Approximate from return series
        sign_changes = (returns.shift(1) * returns < 0).sum()
        win_rate = float((returns > 0).mean())
        profit_factor = _compute_profit_factor(returns)
        avg_trade_return = float(returns.mean())
        avg_win = float(returns[returns > 0].mean()) if (returns > 0).any() else 0.0
        avg_loss = float(returns[returns < 0].mean()) if (returns < 0).any() else 0.0
        avg_holding_days = 0.0
        annual_turnover = float(sign_changes / years)

    return PerformanceMetrics(
        period=period,
        strategy_id=strategy_id,
        start_date=(
            str(returns.index[0].date())
            if hasattr(returns.index[0], "date")
            else str(returns.index[0])
        ),
        end_date=(
            str(returns.index[-1].date())
            if hasattr(returns.index[-1], "date")
            else str(returns.index[-1])
        ),
        n_trading_days=n,
        total_return=total_return,
        annualised_return=annualised_return,
        annualised_vol=annualised_vol,
        sharpe_ratio=sharpe,
        sortino_ratio=sortino,
        calmar_ratio=calmar,
        max_drawdown=max_drawdown,
        avg_drawdown=avg_drawdown,
        max_drawdown_duration_days=max_dd_duration,
        win_rate=win_rate,
        profit_factor=profit_factor,
        avg_trade_return=avg_trade_return,
        avg_win=avg_win,
        avg_loss=avg_loss,
        avg_holding_days=avg_holding_days,
        annual_turnover=annual_turnover,
        t_stat_sharpe=t_stat,
        wfer=wfer,
        pbo=pbo,
    )


def compute_wfer(is_sharpe: float, oos_sharpe: float) -> float:
    """
    Walk-Forward Efficiency Ratio = OOS Sharpe / IS Sharpe.

    Interpretation:
      > 0.70: Excellent — strategy is robust, possibly under-fitted
      0.50–0.70: Good — proceed to paper trading
      0.30–0.50: Marginal — simplify parameters, re-test
      < 0.30: Poor — likely overfitted, discard or redesign

    Minimum acceptable: 0.50
    """
    if is_sharpe <= 0:
        logger.warning(
            "IS Sharpe is <= 0 (%.2f) — WFER undefined. Strategy failed IS gate.", is_sharpe
        )
        return 0.0
    return oos_sharpe / is_sharpe


def _compute_max_drawdown_duration(drawdown_series: pd.Series) -> int:
    """Return the longest consecutive period spent in drawdown (days)."""
    in_drawdown = drawdown_series < 0
    max_duration = 0
    current_duration = 0
    for in_dd in in_drawdown:
        if in_dd:
            current_duration += 1
            max_duration = max(max_duration, current_duration)
        else:
            current_duration = 0
    return max_duration


def _compute_profit_factor(returns: pd.Series) -> float:
    """Gross profits / Gross losses. > 1.0 required for positive expectancy."""
    gains = returns[returns > 0].sum()
    losses = abs(returns[returns < 0].sum())
    if losses == 0:
        return float("inf") if gains > 0 else 0.0
    return float(gains / losses)


def monthly_return_heatmap(returns: pd.Series) -> pd.DataFrame:
    """
    Pivot daily returns into a year × month heatmap.
    Standard format for performance reports.
    """
    monthly = returns.resample("M").apply(lambda x: (1 + x).prod() - 1)
    df = monthly.to_frame("return")
    df["year"] = df.index.year
    df["month"] = df.index.month
    heatmap = df.pivot(index="year", columns="month", values="return")
    heatmap.columns = [
        "Jan",
        "Feb",
        "Mar",
        "Apr",
        "May",
        "Jun",
        "Jul",
        "Aug",
        "Sep",
        "Oct",
        "Nov",
        "Dec",
    ]
    return heatmap


def print_metrics_report(metrics: PerformanceMetrics) -> None:
    """Print formatted metrics report to logger."""
    logger.info("\n" + "=" * 70)
    logger.info(metrics.summary())
    logger.info("=" * 70)
