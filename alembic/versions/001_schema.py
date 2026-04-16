"""
alembic/versions/001_initial_schema.py
Initial database schema for WorFIn.

INTRADAY-READY DESIGN DECISIONS (all applied here):
  1. TIMESTAMPTZ everywhere — no DATE columns that would require migration later
  2. bar_size column on all price and signal tables
  3. valid_from / valid_until on signal tables — execution engine is frequency-agnostic
  4. All 5 schemas created: raw_data, clean_data, signals, positions, audit

Revision ID: 001
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:

    # ─────────────────────────────────────────────────────────────────────────
    # CREATE ALL SCHEMAS
    # ─────────────────────────────────────────────────────────────────────────
    op.execute("CREATE SCHEMA IF NOT EXISTS raw_data")
    op.execute("CREATE SCHEMA IF NOT EXISTS clean_data")
    op.execute("CREATE SCHEMA IF NOT EXISTS signals")
    op.execute("CREATE SCHEMA IF NOT EXISTS positions")
    op.execute("CREATE SCHEMA IF NOT EXISTS orders")
    op.execute("CREATE SCHEMA IF NOT EXISTS audit")

    # ─────────────────────────────────────────────────────────────────────────
    # SCHEMA: raw_data (IMMUTABLE — append-only, never UPDATE or DELETE)
    # Stores exactly what sources provide — no transformation
    # ─────────────────────────────────────────────────────────────────────────

    op.create_table(
        "futures_prices",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        # DECISION 1: TIMESTAMPTZ — not DATE. Works for daily AND intraday.
        sa.Column("price_timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("ticker", sa.String(4), nullable=False),
        sa.Column(
            "contract_type", sa.String(10), nullable=False
        ),  # "front", "second", "cash", "3m"
        # DECISION 2: bar_size — same table works for daily bars and future intraday bars
        sa.Column("bar_size", sa.String(10), nullable=False, server_default="daily"),
        sa.Column("open", sa.Numeric(18, 6), nullable=True),
        sa.Column("high", sa.Numeric(18, 6), nullable=True),
        sa.Column("low", sa.Numeric(18, 6), nullable=True),
        sa.Column("close", sa.Numeric(18, 6), nullable=False),
        sa.Column("volume", sa.Numeric(18, 2), nullable=True),
        sa.Column("open_interest", sa.Numeric(18, 2), nullable=True),
        sa.Column(
            "source", sa.String(50), nullable=False
        ),  # "nasdaq_data_link", "ibkr", "yfinance"
        sa.Column("fetched_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("NOW()"),
            nullable=False,
        ),
        schema="raw_data",
    )
    op.create_index(
        "ix_raw_futures_ticker_ts",
        "futures_prices",
        ["ticker", "price_timestamp"],
        schema="raw_data",
    )
    op.create_index("ix_raw_futures_bar_size", "futures_prices", ["bar_size"], schema="raw_data")

    op.create_table(
        "lme_inventory",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("report_timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("ticker", sa.String(4), nullable=False),
        sa.Column("on_warrant", sa.Numeric(18, 2), nullable=True),  # tonnes
        sa.Column("cancelled", sa.Numeric(18, 2), nullable=True),  # cancelled warrants (tonnes)
        sa.Column("total_stocks", sa.Numeric(18, 2), nullable=True),
        sa.Column("cancellation_ratio", sa.Numeric(8, 6), nullable=True),  # cancelled / total
        sa.Column("source", sa.String(50), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("NOW()"),
            nullable=False,
        ),
        schema="raw_data",
    )
    op.create_index(
        "ix_raw_inventory_ticker_ts",
        "lme_inventory",
        ["ticker", "report_timestamp"],
        schema="raw_data",
    )

    op.create_table(
        "cftc_cot",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("report_date", sa.DateTime(timezone=True), nullable=False),
        sa.Column("ticker", sa.String(4), nullable=False),
        sa.Column("mm_long", sa.Numeric(18, 2), nullable=True),  # Managed Money long
        sa.Column("mm_short", sa.Numeric(18, 2), nullable=True),
        sa.Column("mm_net", sa.Numeric(18, 2), nullable=True),
        sa.Column("mm_net_pct_oi", sa.Numeric(8, 6), nullable=True),  # as % of open interest
        sa.Column("source", sa.String(50), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("NOW()"),
            nullable=False,
        ),
        schema="raw_data",
    )

    op.create_table(
        "macro_indicators",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("observation_ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("indicator", sa.String(50), nullable=False),  # "ISM_PMI", "DXY", "CAIXIN_PMI"
        sa.Column("value", sa.Numeric(18, 6), nullable=False),
        sa.Column("source", sa.String(50), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("NOW()"),
            nullable=False,
        ),
        schema="raw_data",
    )

    # ─────────────────────────────────────────────────────────────────────────
    # SCHEMA: clean_data (validated, roll-adjusted, derived)
    # ─────────────────────────────────────────────────────────────────────────

    op.create_table(
        "continuous_prices",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("price_timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("ticker", sa.String(4), nullable=False),
        sa.Column("bar_size", sa.String(10), nullable=False, server_default="daily"),
        sa.Column("close", sa.Numeric(18, 6), nullable=False),
        sa.Column("open", sa.Numeric(18, 6), nullable=True),
        sa.Column("high", sa.Numeric(18, 6), nullable=True),
        sa.Column("low", sa.Numeric(18, 6), nullable=True),
        sa.Column("volume", sa.Numeric(18, 2), nullable=True),
        sa.Column("roll_adjusted", sa.Boolean, nullable=False, server_default="true"),
        sa.Column("log_return", sa.Numeric(18, 10), nullable=True),
        sa.Column("is_roll_date", sa.Boolean, nullable=False, server_default="false"),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("NOW()"),
            nullable=False,
        ),
        schema="clean_data",
    )
    op.create_index(
        "ix_clean_prices_ticker_ts",
        "continuous_prices",
        ["ticker", "price_timestamp"],
        schema="clean_data",
    )
    op.create_index(
        "ix_clean_prices_bar_size", "continuous_prices", ["bar_size"], schema="clean_data"
    )

    op.create_table(
        "term_structure",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("price_timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("ticker", sa.String(4), nullable=False),
        sa.Column("cash_price", sa.Numeric(18, 6), nullable=True),
        sa.Column("f3m_price", sa.Numeric(18, 6), nullable=True),
        sa.Column("f15m_price", sa.Numeric(18, 6), nullable=True),
        sa.Column("f27m_price", sa.Numeric(18, 6), nullable=True),
        sa.Column("cash_settle_date", sa.DateTime(timezone=True), nullable=True),
        sa.Column("f3m_settle_date", sa.DateTime(timezone=True), nullable=True),
        sa.Column("dte_cash_3m", sa.Integer, nullable=True),  # actual calendar days
        sa.Column("carry_annualised", sa.Numeric(18, 10), nullable=True),  # pre-computed
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("NOW()"),
            nullable=False,
        ),
        schema="clean_data",
    )
    op.create_index(
        "ix_clean_term_ticker_ts",
        "term_structure",
        ["ticker", "price_timestamp"],
        schema="clean_data",
    )

    op.create_table(
        "realised_vol",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("computed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("ticker", sa.String(4), nullable=False),
        sa.Column("bar_size", sa.String(10), nullable=False, server_default="daily"),
        sa.Column("window", sa.Integer, nullable=False),  # lookback in bars (20, 60)
        sa.Column("vol_annualised", sa.Numeric(18, 10), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("NOW()"),
            nullable=False,
        ),
        schema="clean_data",
    )
    op.create_index(
        "ix_clean_vol_ticker_ts", "realised_vol", ["ticker", "computed_at"], schema="clean_data"
    )

    # ─────────────────────────────────────────────────────────────────────────
    # SCHEMA: signals
    # DECISION 3: valid_from / valid_until — execution engine is frequency-agnostic
    # Daily: valid_until = computed_at + 24h
    # Hourly: valid_until = computed_at + 1h
    # 5-min: valid_until = computed_at + 5min
    # ─────────────────────────────────────────────────────────────────────────

    op.create_table(
        "computed_signals",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("computed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("valid_from", sa.DateTime(timezone=True), nullable=False),
        sa.Column("valid_until", sa.DateTime(timezone=True), nullable=False),
        sa.Column("strategy_id", sa.String(4), nullable=False),
        sa.Column("ticker", sa.String(4), nullable=False),
        sa.Column("bar_size", sa.String(10), nullable=False, server_default="daily"),
        sa.Column("frequency", sa.String(20), nullable=False, server_default="daily"),
        sa.Column("signal_value", sa.Numeric(8, 6), nullable=False),  # [-1, +1]
        sa.Column("carry_component", sa.Numeric(8, 6), nullable=True),
        sa.Column("momentum_component", sa.Numeric(8, 6), nullable=True),
        sa.Column("composite_raw", sa.Numeric(8, 6), nullable=True),
        sa.Column("is_valid", sa.Boolean, nullable=False, server_default="true"),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("NOW()"),
            nullable=False,
        ),
        schema="signals",
    )
    op.create_index(
        "ix_signals_strategy_ticker_ts",
        "computed_signals",
        ["strategy_id", "ticker", "computed_at"],
        schema="signals",
    )
    op.create_index("ix_signals_valid_until", "computed_signals", ["valid_until"], schema="signals")
    op.create_index(
        "ix_signals_frequency", "computed_signals", ["frequency", "bar_size"], schema="signals"
    )

    # ─────────────────────────────────────────────────────────────────────────
    # SCHEMA: positions
    # ─────────────────────────────────────────────────────────────────────────

    op.create_table(
        "target_positions",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("computed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("strategy_id", sa.String(4), nullable=False),
        sa.Column("ticker", sa.String(4), nullable=False),
        sa.Column("target_lots", sa.Integer, nullable=False),
        sa.Column("target_notional_gbp", sa.Numeric(18, 2), nullable=False),
        sa.Column("signal_value", sa.Numeric(8, 6), nullable=False),
        sa.Column("vol_20d", sa.Numeric(8, 6), nullable=False),
        sa.Column("vol_60d", sa.Numeric(8, 6), nullable=False),
        sa.Column("approved", sa.Boolean, nullable=False, server_default="false"),
        sa.Column("approved_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("NOW()"),
            nullable=False,
        ),
        schema="positions",
    )

    op.create_table(
        "current_positions",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("as_of", sa.DateTime(timezone=True), nullable=False),
        sa.Column("strategy_id", sa.String(4), nullable=False),
        sa.Column("ticker", sa.String(4), nullable=False),
        sa.Column("current_lots", sa.Integer, nullable=False),
        sa.Column("avg_entry_price", sa.Numeric(18, 6), nullable=True),
        sa.Column("current_notional_gbp", sa.Numeric(18, 2), nullable=True),
        sa.Column("unrealised_pnl_gbp", sa.Numeric(18, 2), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("NOW()"),
            nullable=False,
        ),
        schema="positions",
    )
    op.create_index("ix_positions_current_ts", "current_positions", ["as_of"], schema="positions")

    # ─────────────────────────────────────────────────────────────────────────
    # SCHEMA: orders
    # ─────────────────────────────────────────────────────────────────────────

    op.create_table(
        "order_log",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("order_timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("strategy_id", sa.String(4), nullable=False),
        sa.Column("ticker", sa.String(4), nullable=False),
        sa.Column(
            "order_type", sa.String(20), nullable=False
        ),  # "limit_passive", "limit_aggressive", "market"
        sa.Column("side", sa.String(5), nullable=False),  # "buy", "sell"
        sa.Column("lots", sa.Integer, nullable=False),
        sa.Column("limit_price", sa.Numeric(18, 6), nullable=True),
        sa.Column("ibkr_order_id", sa.String(50), nullable=True),
        sa.Column(
            "status", sa.String(20), nullable=False
        ),  # "pending", "filled", "cancelled", "rejected"
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("NOW()"),
            nullable=False,
        ),
        schema="orders",
    )

    op.create_table(
        "fill_log",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("fill_timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("order_id", sa.BigInteger, nullable=False),
        sa.Column("ticker", sa.String(4), nullable=False),
        sa.Column("strategy_id", sa.String(4), nullable=False),
        sa.Column("fill_price", sa.Numeric(18, 6), nullable=False),
        sa.Column("lots_filled", sa.Integer, nullable=False),
        sa.Column("commission_usd", sa.Numeric(10, 4), nullable=True),
        sa.Column("slippage_bps", sa.Numeric(8, 4), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("NOW()"),
            nullable=False,
        ),
        schema="orders",
    )

    # ─────────────────────────────────────────────────────────────────────────
    # SCHEMA: audit (immutable event log — never delete)
    # ─────────────────────────────────────────────────────────────────────────

    op.create_table(
        "system_events",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("event_timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "event_type", sa.String(50), nullable=False
        ),  # "startup", "kill_switch", "circuit_breaker"
        sa.Column(
            "severity", sa.String(20), nullable=False
        ),  # "info", "warning", "critical", "kill"
        sa.Column("strategy_id", sa.String(4), nullable=True),
        sa.Column("ticker", sa.String(4), nullable=True),
        sa.Column("message", sa.Text, nullable=False),
        sa.Column("context_json", sa.Text, nullable=True),  # JSON blob of additional context
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("NOW()"),
            nullable=False,
        ),
        schema="audit",
    )
    op.create_index("ix_audit_events_ts", "system_events", ["event_timestamp"], schema="audit")
    op.create_index("ix_audit_events_type", "system_events", ["event_type"], schema="audit")

    op.create_table(
        "risk_breaches",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("breach_timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("breach_type", sa.String(50), nullable=False),
        sa.Column("action_taken", sa.String(50), nullable=False),
        sa.Column("threshold", sa.Numeric(10, 6), nullable=True),
        sa.Column("actual_value", sa.Numeric(10, 6), nullable=True),
        sa.Column("strategy_id", sa.String(4), nullable=True),
        sa.Column("ticker", sa.String(4), nullable=True),
        sa.Column("message", sa.Text, nullable=False),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("NOW()"),
            nullable=False,
        ),
        schema="audit",
    )

    op.create_table(
        "reconciliation_log",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("reconciled_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("status", sa.String(20), nullable=False),  # "clean", "mismatch", "blocked"
        sa.Column("discrepancies", sa.Integer, nullable=False, server_default="0"),
        sa.Column("total_value_diff_gbp", sa.Numeric(18, 2), nullable=True),
        sa.Column("details_json", sa.Text, nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("NOW()"),
            nullable=False,
        ),
        schema="audit",
    )

    op.create_table(
        "data_quality_flags",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("flagged_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("ticker", sa.String(4), nullable=True),
        sa.Column(
            "flag_type", sa.String(50), nullable=False
        ),  # "staleness", "outlier", "price_discrepancy"
        sa.Column("bar_size", sa.String(10), nullable=True),
        sa.Column("description", sa.Text, nullable=False),
        sa.Column("auto_resolved", sa.Boolean, nullable=False, server_default="false"),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("NOW()"),
            nullable=False,
        ),
        schema="audit",
    )


def downgrade() -> None:
    # Drop all tables in reverse order
    for schema in ["audit", "orders", "positions", "signals", "clean_data", "raw_data"]:
        op.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
