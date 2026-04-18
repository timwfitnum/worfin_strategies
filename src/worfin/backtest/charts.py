"""
backtest/charts.py
Performance visualisation for WorFIn backtest results.

CHARTS PRODUCED:
  Figure 1 — Main performance dashboard (2×2 grid):
    Top-left:     Equity curve (NAV over time)
    Bottom-left:  Underwater / drawdown chart
    Top-right:    Monthly returns heatmap
    Bottom-right: Rolling 12-month Sharpe ratio

  Figure 2 — Returns analysis:
    Left:   Annual returns bar chart with risk-free hurdle line
    Centre: Return distribution histogram with normal overlay
    Right:  Win/loss distribution (daily returns split by sign)

USAGE:
  from worfin.backtest.charts import generate_report

  # After running backtest:
  generate_report(is_result, output_dir=Path("reports/"))

  # With IS + OOS on same equity curve:
  generate_report(is_result, oos_result=oos_result, output_dir=Path("reports/"))

OUTPUT:
  reports/
    S4_IS_dashboard.png   — main 2×2 dashboard
    S4_IS_returns.png     — returns analysis
"""

from __future__ import annotations

import logging
import math
from pathlib import Path

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# COLOUR PALETTE  — consistent, professional, colour-blind friendly
# ─────────────────────────────────────────────────────────────────────────────

COLOURS = {
    "is_line": "#2563EB",  # Blue   — IS equity curve
    "oos_line": "#16A34A",  # Green  — OOS equity curve
    "drawdown": "#DC2626",  # Red    — drawdown fill
    "positive": "#16A34A",  # Green  — positive returns
    "negative": "#DC2626",  # Red    — negative returns
    "neutral": "#6B7280",  # Grey   — neutral elements
    "grid": "#E5E7EB",  # Light grey grid
    "background": "#FAFAFA",  # Near-white background
    "hurdle": "#D97706",  # Amber  — risk-free rate line
    "text": "#111827",  # Near-black text
    "sharpe_pos": "#2563EB",  # Blue   — positive rolling Sharpe
    "sharpe_neg": "#DC2626",  # Red    — negative rolling Sharpe
}

RISK_FREE_RATE = 0.04  # 4% — must match metrics.py


def _apply_style(ax, title: str = "", xlabel: str = "", ylabel: str = "") -> None:
    """Apply consistent visual style to an axes object."""
    ax.set_facecolor(COLOURS["background"])
    ax.grid(True, color=COLOURS["grid"], linewidth=0.5, alpha=0.8)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_color(COLOURS["grid"])
    ax.spines["bottom"].set_color(COLOURS["grid"])
    ax.tick_params(colors=COLOURS["text"], labelsize=8)
    if title:
        ax.set_title(title, fontsize=10, fontweight="bold", color=COLOURS["text"], pad=8)
    if xlabel:
        ax.set_xlabel(xlabel, fontsize=8, color=COLOURS["neutral"])
    if ylabel:
        ax.set_ylabel(ylabel, fontsize=8, color=COLOURS["neutral"])


# ─────────────────────────────────────────────────────────────────────────────
# INDIVIDUAL CHART FUNCTIONS
# ─────────────────────────────────────────────────────────────────────────────


def plot_equity_curve(
    ax,
    is_result,
    oos_result=None,
    initial_nav: float = 100_000.0,
) -> None:
    """
    NAV equity curve. IS in blue, OOS in green if provided.
    Marks the IS/OOS boundary with a dashed vertical line.
    """
    is_nav = is_result.daily_nav

    # Normalise to starting value for clean comparison
    ax.plot(
        is_nav.index,
        is_nav.values,
        color=COLOURS["is_line"],
        linewidth=1.5,
        label=f"IS ({is_result.config.start_date.year}–{is_result.config.end_date.year})",
    )

    if oos_result is not None:
        oos_nav = oos_result.daily_nav
        ax.plot(
            oos_nav.index,
            oos_nav.values,
            color=COLOURS["oos_line"],
            linewidth=1.5,
            label=f"OOS ({oos_result.config.start_date.year}–{oos_result.config.end_date.year})",
        )
        # IS/OOS boundary
        boundary = pd.Timestamp(is_result.config.end_date)
        ax.axvline(
            boundary,
            color=COLOURS["neutral"],
            linewidth=1,
            linestyle="--",
            alpha=0.6,
            label="IS/OOS split",
        )

    # Starting capital line
    ax.axhline(
        initial_nav,
        color=COLOURS["neutral"],
        linewidth=0.8,
        linestyle=":",
        alpha=0.5,
    )

    ax.yaxis.set_major_formatter(
        __import__("matplotlib").ticker.FuncFormatter(lambda x, _: f"£{x:,.0f}")
    )
    ax.legend(fontsize=7, framealpha=0.8)
    _apply_style(ax, title="NAV Equity Curve", ylabel="Portfolio Value (GBP)")


def plot_drawdown(ax, is_result, oos_result=None) -> None:
    """Underwater equity chart — shows time spent and depth in drawdown."""

    def _drawdown_series(returns: pd.Series) -> pd.Series:
        cum = (1 + returns).cumprod()
        return (cum - cum.cummax()) / cum.cummax() * 100  # in percent

    is_dd = _drawdown_series(is_result.daily_returns)
    ax.fill_between(
        is_dd.index,
        is_dd.values,
        0,
        color=COLOURS["drawdown"],
        alpha=0.4,
        label="IS Drawdown",
    )
    ax.plot(is_dd.index, is_dd.values, color=COLOURS["drawdown"], linewidth=0.8)

    if oos_result is not None:
        oos_dd = _drawdown_series(oos_result.daily_returns)
        ax.fill_between(
            oos_dd.index,
            oos_dd.values,
            0,
            color=COLOURS["oos_line"],
            alpha=0.25,
            label="OOS Drawdown",
        )
        ax.plot(oos_dd.index, oos_dd.values, color=COLOURS["oos_line"], linewidth=0.8)
        boundary = pd.Timestamp(is_result.config.end_date)
        ax.axvline(boundary, color=COLOURS["neutral"], linewidth=1, linestyle="--", alpha=0.6)

    ax.axhline(0, color=COLOURS["neutral"], linewidth=0.8)
    ax.set_ylim(top=5)  # Cap top at +5% so flat periods look flat
    ax.yaxis.set_major_formatter(
        __import__("matplotlib").ticker.FuncFormatter(lambda x, _: f"{x:.0f}%")
    )
    ax.legend(fontsize=7, framealpha=0.8)
    _apply_style(ax, title="Drawdown (Underwater Equity)", ylabel="Drawdown %")


def plot_monthly_heatmap(ax, returns: pd.Series) -> None:
    """Monthly returns heatmap — year × month grid, red/green colour scale."""

    from worfin.backtest.metrics import monthly_return_heatmap

    heatmap = monthly_return_heatmap(returns)

    # Use a symmetric diverging colormap centred on 0
    vmax = max(abs(heatmap.values[~np.isnan(heatmap.values)]).max(), 0.01)
    cmap = __import__("matplotlib").cm.RdYlGn

    im = ax.imshow(
        heatmap.values,
        cmap=cmap,
        vmin=-vmax,
        vmax=vmax,
        aspect="auto",
    )

    # Labels
    ax.set_xticks(range(len(heatmap.columns)))
    ax.set_xticklabels(heatmap.columns, fontsize=7)
    ax.set_yticks(range(len(heatmap.index)))
    ax.set_yticklabels(heatmap.index, fontsize=7)

    # Annotate each cell with the return value
    for i in range(len(heatmap.index)):
        for j in range(len(heatmap.columns)):
            val = heatmap.iloc[i, j]
            if not np.isnan(val):
                text_color = "white" if abs(val) > vmax * 0.6 else COLOURS["text"]
                ax.text(
                    j,
                    i,
                    f"{val * 100:.1f}%",
                    ha="center",
                    va="center",
                    fontsize=5.5,
                    color=text_color,
                )

    ax.set_title(
        "Monthly Returns Heatmap",
        fontsize=10,
        fontweight="bold",
        color=COLOURS["text"],
        pad=8,
    )
    ax.tick_params(colors=COLOURS["text"], labelsize=7)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)


def plot_rolling_sharpe(ax, returns: pd.Series, window_months: int = 12) -> None:
    """Rolling annualised Sharpe ratio. Blue above 0, red below."""
    window = window_months * 21  # approximate trading days per month
    if len(returns) < window:
        ax.text(
            0.5,
            0.5,
            "Insufficient data for rolling Sharpe",
            transform=ax.transAxes,
            ha="center",
            va="center",
            fontsize=9,
        )
        _apply_style(ax, title=f"Rolling {window_months}-Month Sharpe")
        return

    rolling_mean = returns.rolling(window).mean() * 252
    rolling_std = returns.rolling(window).std() * math.sqrt(252)
    rolling_sharpe = (rolling_mean - RISK_FREE_RATE) / rolling_std.replace(0, np.nan)

    # Split into positive / negative for two-colour fill
    pos = rolling_sharpe.clip(lower=0)
    neg = rolling_sharpe.clip(upper=0)

    ax.fill_between(
        rolling_sharpe.index, pos, 0, color=COLOURS["sharpe_pos"], alpha=0.4, label="Positive"
    )
    ax.fill_between(
        rolling_sharpe.index, neg, 0, color=COLOURS["sharpe_neg"], alpha=0.4, label="Negative"
    )
    ax.plot(rolling_sharpe.index, rolling_sharpe, color=COLOURS["text"], linewidth=0.8, alpha=0.7)

    ax.axhline(0, color=COLOURS["neutral"], linewidth=1)
    ax.axhline(
        0.5,
        color=COLOURS["hurdle"],
        linewidth=0.8,
        linestyle="--",
        alpha=0.7,
        label="IS gate (0.50)",
    )
    ax.legend(fontsize=7, framealpha=0.8)
    _apply_style(
        ax,
        title=f"Rolling {window_months}-Month Sharpe",
        ylabel="Sharpe Ratio",
    )


def plot_annual_returns(ax, returns: pd.Series) -> None:
    """Annual returns bar chart with risk-free rate hurdle line."""
    annual = returns.resample("YE").apply(lambda r: (1 + r).prod() - 1)
    years = [str(d.year) for d in annual.index]
    values = annual.values * 100  # in percent

    colours = [COLOURS["positive"] if v >= 0 else COLOURS["negative"] for v in values]
    bars = ax.bar(years, values, color=colours, alpha=0.8, width=0.6)

    # Value labels on each bar
    for bar, val in zip(bars, values, strict=False):
        ypos = bar.get_height() + 0.3 if val >= 0 else bar.get_height() - 1.5
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            ypos,
            f"{val:+.1f}%",
            ha="center",
            va="bottom",
            fontsize=6.5,
            color=COLOURS["text"],
        )

    ax.axhline(0, color=COLOURS["neutral"], linewidth=0.8)
    ax.axhline(
        RISK_FREE_RATE * 100,
        color=COLOURS["hurdle"],
        linewidth=1,
        linestyle="--",
        alpha=0.8,
        label=f"Risk-free {RISK_FREE_RATE * 100:.0f}%",
    )
    ax.tick_params(axis="x", rotation=45, labelsize=7)
    ax.yaxis.set_major_formatter(
        __import__("matplotlib").ticker.FuncFormatter(lambda x, _: f"{x:.0f}%")
    )
    ax.legend(fontsize=7, framealpha=0.8)
    _apply_style(ax, title="Annual Returns", ylabel="Return %")


def plot_return_distribution(ax, returns: pd.Series) -> None:
    """Daily return histogram with fitted normal distribution overlay."""
    r = returns.dropna() * 100  # in percent

    # Histogram
    n_bins = min(60, max(20, len(r) // 50))
    ax.hist(
        r,
        bins=n_bins,
        color=COLOURS["is_line"],
        alpha=0.6,
        edgecolor="white",
        linewidth=0.3,
        density=True,
        label="Daily returns",
    )

    # Normal distribution overlay
    mu, sigma = r.mean(), r.std()
    x = np.linspace(r.min(), r.max(), 200)
    normal_pdf = np.exp(-0.5 * ((x - mu) / sigma) ** 2) / (sigma * math.sqrt(2 * math.pi))
    ax.plot(
        x,
        normal_pdf,
        color=COLOURS["negative"],
        linewidth=1.5,
        linestyle="--",
        label=f"Normal (μ={mu:.2f}%, σ={sigma:.2f}%)",
    )

    ax.axvline(0, color=COLOURS["neutral"], linewidth=1)
    ax.axvline(
        mu, color=COLOURS["hurdle"], linewidth=1, linestyle=":", alpha=0.8, label=f"Mean={mu:.2f}%"
    )
    ax.legend(fontsize=7, framealpha=0.8)
    _apply_style(ax, title="Daily Return Distribution", xlabel="Daily Return %", ylabel="Density")


def plot_win_loss(ax, returns: pd.Series) -> None:
    """Box plot comparing distributions of winning vs losing days."""
    r = returns.dropna() * 100
    wins = r[r > 0].values
    losses = r[r < 0].values

    if len(wins) == 0 or len(losses) == 0:
        ax.text(
            0.5,
            0.5,
            "Insufficient data",
            transform=ax.transAxes,
            ha="center",
            va="center",
            fontsize=9,
        )
        _apply_style(ax, title="Win / Loss Distribution")
        return

    bp = ax.boxplot(
        [wins, losses],
        labels=["Winning days", "Losing days"],
        patch_artist=True,
        widths=0.4,
        medianprops={"color": COLOURS["text"], "linewidth": 1.5},
        whiskerprops={"color": COLOURS["neutral"]},
        capprops={"color": COLOURS["neutral"]},
        flierprops={"marker": ".", "markersize": 2, "alpha": 0.4},
    )
    bp["boxes"][0].set_facecolor(COLOURS["positive"])
    bp["boxes"][0].set_alpha(0.5)
    bp["boxes"][1].set_facecolor(COLOURS["negative"])
    bp["boxes"][1].set_alpha(0.5)

    ax.axhline(0, color=COLOURS["neutral"], linewidth=0.8)
    n_wins = len(wins)
    n_losses = len(losses)
    win_rate = n_wins / (n_wins + n_losses)
    ax.set_title(
        f"Win / Loss Distribution  (win rate: {win_rate:.1%})",
        fontsize=10,
        fontweight="bold",
        color=COLOURS["text"],
        pad=8,
    )
    ax.yaxis.set_major_formatter(
        __import__("matplotlib").ticker.FuncFormatter(lambda x, _: f"{x:.1f}%")
    )
    _apply_style(ax, ylabel="Daily Return %")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN REPORT GENERATOR
# ─────────────────────────────────────────────────────────────────────────────


def generate_report(
    is_result,
    oos_result=None,
    output_dir: Path | None = None,
    show: bool = True,
) -> list[Path]:
    """
    Generate and save all performance charts.

    Args:
        is_result:   BacktestResult for the IS period (required)
        oos_result:  BacktestResult for OOS period (optional — overlaid on equity curve)
        output_dir:  Directory to save PNGs. Defaults to reports/ in repo root.
        show:        Whether to call plt.show() (True in interactive sessions)

    Returns:
        List of Path objects for saved chart files.
    """
    try:
        import matplotlib
        import matplotlib.pyplot as plt
        import matplotlib.ticker
    except ImportError:
        logger.error(
            "matplotlib not installed. Run: pip install matplotlib\n"
            "Or: pip install -e '.[dev]' if matplotlib is in dev extras."
        )
        return []

    matplotlib.rcParams.update(
        {
            "font.family": "DejaVu Sans",
            "font.size": 9,
            "figure.dpi": 150,
            "savefig.dpi": 150,
            "savefig.bbox": "tight",
            "figure.facecolor": COLOURS["background"],
        }
    )

    strategy_id = is_result.config.strategy_id
    period = is_result.config.period_label

    if output_dir is None:
        # Default to reports/ next to the repo root
        output_dir = Path(__file__).parents[3] / "reports"
    output_dir.mkdir(parents=True, exist_ok=True)

    saved: list[Path] = []

    # ── Combine IS + OOS returns for single-period charts ──────────────────────
    all_returns = is_result.daily_returns
    if oos_result is not None:
        all_returns = pd.concat([is_result.daily_returns, oos_result.daily_returns])
        all_returns = all_returns[~all_returns.index.duplicated()]

    m = is_result.metrics

    # ─────────────────────────────────────────────────────────────────────────
    # FIGURE 1 — Main Dashboard (2×2)
    # ─────────────────────────────────────────────────────────────────────────
    fig1, axes = plt.subplots(2, 2, figsize=(14, 9))
    fig1.set_facecolor(COLOURS["background"])

    # Title block
    sharpe_str = f"{m.sharpe_ratio:.2f}" if m else "N/A"
    maxdd_str = f"{m.max_drawdown:.1%}" if m else "N/A"
    tstat_str = f"{m.t_stat_sharpe:.1f}" if m else "N/A"
    gate_str = "✅ PASS" if (m and m.passes_all_gates) else "❌ FAIL"
    data_notice = " ⚠ SYNTHETIC DATA" if _is_synthetic(is_result) else ""

    fig1.suptitle(
        f"WorFIn — {strategy_id} Strategy — {period} Backtest{data_notice}\n"
        f"Sharpe: {sharpe_str}  |  Max DD: {maxdd_str}  |  "
        f"t-stat: {tstat_str}  |  {gate_str}",
        fontsize=11,
        fontweight="bold",
        color=COLOURS["text"] if not _is_synthetic(is_result) else COLOURS["negative"],
        y=0.98,
    )

    plot_equity_curve(
        axes[0, 0], is_result, oos_result, initial_nav=is_result.config.total_capital_gbp
    )
    plot_drawdown(axes[1, 0], is_result, oos_result)
    plot_monthly_heatmap(axes[0, 1], all_returns)
    plot_rolling_sharpe(axes[1, 1], all_returns)

    fig1.tight_layout(rect=[0, 0, 1, 0.95])

    p1 = output_dir / f"{strategy_id}_{period}_dashboard.png"
    fig1.savefig(p1, facecolor=COLOURS["background"])
    saved.append(p1)
    logger.info("Saved dashboard chart: %s", p1)

    # ─────────────────────────────────────────────────────────────────────────
    # FIGURE 2 — Returns Analysis (1×3)
    # ─────────────────────────────────────────────────────────────────────────
    fig2, axes2 = plt.subplots(1, 3, figsize=(15, 5))
    fig2.set_facecolor(COLOURS["background"])
    fig2.suptitle(
        f"WorFIn — {strategy_id} — Returns Analysis ({period}){data_notice}",
        fontsize=11,
        fontweight="bold",
        color=COLOURS["text"],
        y=1.02,
    )

    plot_annual_returns(axes2[0], all_returns)
    plot_return_distribution(axes2[1], all_returns)
    plot_win_loss(axes2[2], all_returns)

    fig2.tight_layout()

    p2 = output_dir / f"{strategy_id}_{period}_returns.png"
    fig2.savefig(p2, facecolor=COLOURS["background"])
    saved.append(p2)
    logger.info("Saved returns chart: %s", p2)

    if show:
        try:
            plt.show()
        except Exception:
            pass  # Non-interactive environment (e.g. server) — skip show()

    plt.close("all")

    print(f"\nCharts saved to: {output_dir}/")
    for p in saved:
        print(f"  {p.name}")

    return saved


def _is_synthetic(result) -> bool:
    """Heuristic: flag if likely run on synthetic data (no real data source tag)."""
    # We can't inspect the source directly from BacktestResult, but we can
    # check if turnover is unrealistically high (synthetic has no ADV filtering)
    if result.metrics and result.metrics.annual_turnover > 100:
        return True
    return False
