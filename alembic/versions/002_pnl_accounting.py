"""
alembic/versions/002_pnl_accounting.py
P&L accounting tables + audit.roll_log.

Adds:
  positions.trades           — unified strategy-attributed trade ledger
                               (backtest + paper + live, distinguished by environment)
  positions.daily_pnl        — P&L attribution with scope column
                               (portfolio | strategy | ticker)
  positions.account_summary  — point-in-time account state
                               (NAV, margin, leverage, exposure)
  audit.roll_log             — every continuous-series roll event
                               (added here because continuous.py writes to it)

DESIGN:
  - All tables support environment ∈ {"backtest", "paper", "live"}
  - backtest_run_id UUID groups rows belonging to a single backtest run
  - TIMESTAMPTZ everywhere (intraday-ready)
  - All monetary values NUMERIC (never FLOAT) — Decimal-compatible

Revision ID: 002
Revises: 001
"""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "002"
down_revision = "001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ─────────────────────────────────────────────────────────────────────────
    # positions.trades
    # Unified trade ledger. Both backtest engine and live execution engine
    # write identical row structures. environment column distinguishes them.
    # ─────────────────────────────────────────────────────────────────────────
    op.create_table(
        "trades",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        # When the trade executed (fill timestamp in live; bar timestamp in backtest)
        sa.Column("trade_timestamp", sa.DateTime(timezone=True), nullable=False),
        # Environment discriminator
        sa.Column("environment", sa.String(10), nullable=False),  # "backtest"|"paper"|"live"
        sa.Column("backtest_run_id", postgresql.UUID(as_uuid=True), nullable=True),
        # Strategy attribution
        sa.Column("strategy_id", sa.String(4), nullable=False),
        sa.Column("ticker", sa.String(4), nullable=False),
        sa.Column("exchange", sa.String(10), nullable=False),  # "LME"|"COMEX"
        # Trade details (signed: lots > 0 buy, lots < 0 sell)
        sa.Column("side", sa.String(10), nullable=False),  # "buy"|"sell"
        sa.Column("lots", sa.Integer, nullable=False),
        sa.Column("price_usd", sa.Numeric(18, 6), nullable=False),
        sa.Column("notional_usd", sa.Numeric(18, 2), nullable=False),  # |lots| * lot_size * price
        # FX snapshot at trade time
        sa.Column("fx_rate_usd_gbp", sa.Numeric(12, 6), nullable=False),
        sa.Column("notional_gbp", sa.Numeric(18, 2), nullable=False),
        # Cost breakdown (always in USD — convert to GBP at reporting time)
        sa.Column("commission_usd", sa.Numeric(12, 4), nullable=False, server_default="0"),
        sa.Column("spread_cost_usd", sa.Numeric(12, 4), nullable=False, server_default="0"),
        sa.Column("slippage_usd", sa.Numeric(12, 4), nullable=False, server_default="0"),
        sa.Column("total_cost_usd", sa.Numeric(12, 4), nullable=False, server_default="0"),
        # Realised P&L — populated only for closing/reducing trades (NULL otherwise)
        sa.Column("realised_pnl_usd", sa.Numeric(18, 2), nullable=True),
        sa.Column("realised_pnl_gbp", sa.Numeric(18, 2), nullable=True),
        # Signal traceability
        sa.Column("signal_timestamp", sa.DateTime(timezone=True), nullable=True),
        sa.Column("signal_value", sa.Numeric(10, 6), nullable=True),
        # Linkage to order lifecycle (live only; NULL in backtest)
        sa.Column("order_id", sa.BigInteger, nullable=True),
        sa.Column("fill_id", sa.BigInteger, nullable=True),
        sa.Column("broker_trade_id", sa.String(50), nullable=True),
        # Free-form
        sa.Column("notes", sa.Text, nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("NOW()"),
            nullable=False,
        ),
        schema="positions",
    )
    op.create_check_constraint(
        "ck_trades_environment",
        "trades",
        "environment IN ('backtest', 'paper', 'live')",
        schema="positions",
    )
    op.create_check_constraint(
        "ck_trades_side",
        "trades",
        "side IN ('buy', 'sell')",
        schema="positions",
    )
    op.create_check_constraint(
        "ck_trades_lots_nonzero",
        "trades",
        "lots <> 0",
        schema="positions",
    )
    op.create_index(
        "ix_trades_env_ts", "trades", ["environment", "trade_timestamp"], schema="positions"
    )
    op.create_index(
        "ix_trades_strategy_ts", "trades", ["strategy_id", "trade_timestamp"], schema="positions"
    )
    op.create_index(
        "ix_trades_ticker_ts", "trades", ["ticker", "trade_timestamp"], schema="positions"
    )
    op.create_index(
        "ix_trades_run",
        "trades",
        ["backtest_run_id"],
        schema="positions",
        postgresql_where=sa.text("backtest_run_id IS NOT NULL"),
    )

    # ─────────────────────────────────────────────────────────────────────────
    # positions.daily_pnl
    # Three-level P&L attribution with scope column:
    #   scope='portfolio' → strategy_id IS NULL, ticker IS NULL
    #   scope='strategy'  → strategy_id NOT NULL, ticker IS NULL
    #   scope='ticker'    → strategy_id NOT NULL, ticker NOT NULL
    # ─────────────────────────────────────────────────────────────────────────
    op.create_table(
        "daily_pnl",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        # End-of-period mark timestamp (EOD for daily, end-of-bar for intraday later)
        sa.Column("as_of_timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("scope", sa.String(20), nullable=False),  # "portfolio"|"strategy"|"ticker"
        sa.Column("strategy_id", sa.String(4), nullable=True),
        sa.Column("ticker", sa.String(4), nullable=True),
        sa.Column("environment", sa.String(10), nullable=False),
        sa.Column("backtest_run_id", postgresql.UUID(as_uuid=True), nullable=True),
        # P&L measurements
        sa.Column("nav_gbp", sa.Numeric(18, 2), nullable=False),
        sa.Column("daily_pnl_gbp", sa.Numeric(18, 2), nullable=False),
        sa.Column("daily_pnl_usd", sa.Numeric(18, 2), nullable=True),
        sa.Column("daily_return", sa.Numeric(12, 8), nullable=False),
        sa.Column("cumulative_pnl_gbp", sa.Numeric(18, 2), nullable=False),
        # Exposure (portfolio + strategy scope only)
        sa.Column("gross_exposure_gbp", sa.Numeric(18, 2), nullable=True),
        sa.Column("net_exposure_gbp", sa.Numeric(18, 2), nullable=True),
        # Drawdown tracking (portfolio scope only)
        sa.Column("high_water_mark_gbp", sa.Numeric(18, 2), nullable=True),
        sa.Column("current_drawdown", sa.Numeric(10, 6), nullable=True),
        # Activity
        sa.Column("num_trades", sa.Integer, nullable=True),
        sa.Column("realised_pnl_gbp", sa.Numeric(18, 2), nullable=True),
        sa.Column("unrealised_pnl_gbp", sa.Numeric(18, 2), nullable=True),
        sa.Column("total_cost_gbp", sa.Numeric(18, 2), nullable=True),
        # FX snapshot
        sa.Column("fx_rate_usd_gbp", sa.Numeric(12, 6), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("NOW()"),
            nullable=False,
        ),
        schema="positions",
    )
    op.create_check_constraint(
        "ck_daily_pnl_scope",
        "daily_pnl",
        "scope IN ('portfolio', 'strategy', 'ticker')",
        schema="positions",
    )
    op.create_check_constraint(
        "ck_daily_pnl_environment",
        "daily_pnl",
        "environment IN ('backtest', 'paper', 'live')",
        schema="positions",
    )
    # Scope consistency: portfolio rows have no strategy/ticker, strategy rows have
    # strategy but no ticker, ticker rows have both
    op.create_check_constraint(
        "ck_daily_pnl_scope_consistency",
        "daily_pnl",
        """
        (scope = 'portfolio' AND strategy_id IS NULL AND ticker IS NULL)
        OR (scope = 'strategy' AND strategy_id IS NOT NULL AND ticker IS NULL)
        OR (scope = 'ticker'   AND strategy_id IS NOT NULL AND ticker IS NOT NULL)
        """,
        schema="positions",
    )
    # Uniqueness: one row per (period, scope, strategy, ticker, environment, run)
    # NULL values in strategy_id/ticker/backtest_run_id are treated as distinct in
    # PostgreSQL default behaviour — use COALESCE for a deterministic unique index.
    op.create_index(
        "ux_daily_pnl_unique",
        "daily_pnl",
        [
            "as_of_timestamp",
            "scope",
            sa.text("COALESCE(strategy_id, '')"),
            sa.text("COALESCE(ticker, '')"),
            "environment",
            sa.text("COALESCE(backtest_run_id::text, '')"),
        ],
        unique=True,
        schema="positions",
    )
    op.create_index(
        "ix_daily_pnl_ts_scope", "daily_pnl", ["as_of_timestamp", "scope"], schema="positions"
    )
    op.create_index(
        "ix_daily_pnl_run",
        "daily_pnl",
        ["backtest_run_id"],
        schema="positions",
        postgresql_where=sa.text("backtest_run_id IS NOT NULL"),
    )

    # ─────────────────────────────────────────────────────────────────────────
    # positions.account_summary
    # Point-in-time account state. In live mode populated from IBKR account
    # snapshots. In backtest populated at end-of-bar from simulated state.
    # ─────────────────────────────────────────────────────────────────────────
    op.create_table(
        "account_summary",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("as_of_timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("environment", sa.String(10), nullable=False),
        sa.Column("backtest_run_id", postgresql.UUID(as_uuid=True), nullable=True),
        # NAV (both currencies tracked from day one — GBP is primary reporting)
        sa.Column("nav_gbp", sa.Numeric(18, 2), nullable=False),
        sa.Column("nav_usd", sa.Numeric(18, 2), nullable=False),
        sa.Column("cash_gbp", sa.Numeric(18, 2), nullable=True),
        sa.Column("cash_usd", sa.Numeric(18, 2), nullable=True),
        # Margin (live only — NULL in backtest unless simulated)
        sa.Column("initial_margin_usd", sa.Numeric(18, 2), nullable=True),
        sa.Column("maintenance_margin_usd", sa.Numeric(18, 2), nullable=True),
        sa.Column("excess_liquidity_usd", sa.Numeric(18, 2), nullable=True),
        sa.Column("buying_power_usd", sa.Numeric(18, 2), nullable=True),
        # Exposure & leverage
        sa.Column("gross_exposure_gbp", sa.Numeric(18, 2), nullable=False),
        sa.Column("net_exposure_gbp", sa.Numeric(18, 2), nullable=False),
        sa.Column("gross_leverage", sa.Numeric(10, 4), nullable=False),  # gross / nav
        sa.Column("net_leverage", sa.Numeric(10, 4), nullable=False),  # |net| / nav
        # Activity
        sa.Column("num_open_positions", sa.Integer, nullable=False, server_default="0"),
        sa.Column("num_strategies_active", sa.Integer, nullable=False, server_default="0"),
        # FX + HWM
        sa.Column("fx_rate_usd_gbp", sa.Numeric(12, 6), nullable=False),
        sa.Column("all_time_hwm_gbp", sa.Numeric(18, 2), nullable=False),
        sa.Column("peak_drawdown_pct", sa.Numeric(10, 6), nullable=False, server_default="0"),
        # Source of snapshot
        sa.Column("source", sa.String(20), nullable=False),  # "ibkr"|"backtest"|"manual"
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("NOW()"),
            nullable=False,
        ),
        schema="positions",
    )
    op.create_check_constraint(
        "ck_account_summary_environment",
        "account_summary",
        "environment IN ('backtest', 'paper', 'live')",
        schema="positions",
    )
    op.create_index(
        "ix_account_summary_ts",
        "account_summary",
        ["environment", "as_of_timestamp"],
        schema="positions",
    )
    op.create_index(
        "ix_account_summary_run",
        "account_summary",
        ["backtest_run_id"],
        schema="positions",
        postgresql_where=sa.text("backtest_run_id IS NOT NULL"),
    )

    # ─────────────────────────────────────────────────────────────────────────
    # audit.roll_log
    # Every continuous-series roll event. Auditable record of how the
    # roll-adjusted series was constructed — essential for reproducibility.
    # ─────────────────────────────────────────────────────────────────────────
    op.create_table(
        "roll_log",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("roll_timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("ticker", sa.String(4), nullable=False),
        sa.Column("exchange", sa.String(10), nullable=False),
        # Contract identifiers (may be Nasdaq CHRIS codes or actual contract months)
        sa.Column("from_contract", sa.String(40), nullable=True),
        sa.Column("to_contract", sa.String(40), nullable=True),
        # Prices observed on the last day the old front was front
        sa.Column("old_front_price_usd", sa.Numeric(18, 6), nullable=False),
        sa.Column("new_front_price_usd", sa.Numeric(18, 6), nullable=False),
        # Adjustment magnitude
        sa.Column("gap_absolute", sa.Numeric(18, 6), nullable=False),  # old - new
        sa.Column("gap_pct", sa.Numeric(12, 8), nullable=False),  # gap / old
        sa.Column("roll_method", sa.String(20), nullable=False),  # "back_adjusted"|"ratio_adjusted"
        # Fair-value comparison (optional — for roll cost monitoring)
        sa.Column("theoretical_fair_spread", sa.Numeric(18, 6), nullable=True),
        sa.Column("roll_cost_vs_fair_bps", sa.Numeric(10, 4), nullable=True),
        sa.Column("days_before_fnd", sa.Integer, nullable=True),
        # Context
        sa.Column("environment", sa.String(10), nullable=False, server_default="'backtest'"),
        sa.Column("backtest_run_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("series_id", sa.String(50), nullable=True),  # groups rolls for one series build
        sa.Column("detection_method", sa.String(30), nullable=True),  # "data"|"calendar"|"explicit"
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("NOW()"),
            nullable=False,
        ),
        schema="audit",
    )
    op.create_check_constraint(
        "ck_roll_log_method",
        "roll_log",
        "roll_method IN ('back_adjusted', 'ratio_adjusted')",
        schema="audit",
    )
    op.create_index(
        "ix_roll_log_ticker_ts", "roll_log", ["ticker", "roll_timestamp"], schema="audit"
    )
    op.create_index(
        "ix_roll_log_series",
        "roll_log",
        ["series_id"],
        schema="audit",
        postgresql_where=sa.text("series_id IS NOT NULL"),
    )


def downgrade() -> None:
    op.drop_table("roll_log", schema="audit")
    op.drop_table("account_summary", schema="positions")
    op.drop_table("daily_pnl", schema="positions")
    op.drop_table("trades", schema="positions")
