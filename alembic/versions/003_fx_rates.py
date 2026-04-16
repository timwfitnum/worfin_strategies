"""
alembic/versions/003_fx_rates.py
Adds raw_data.fx_rates for persistent FX rate storage.

Primary source: FRED DEXUSUK (USD per GBP, daily).
Schema supports multiple sources and currencies for future expansion.

Revision ID: 003
Revises: 002
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "003"
down_revision = "002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "fx_rates",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        # The date the FX rate applies to (market convention: FRED DEXUSUK is a
        # daily fixing, so DATE is correct for the raw observation). `fetched_at`
        # below captures when we pulled it from the source.
        sa.Column("as_of_date", sa.Date, nullable=False),
        # Currency pair in ISO 4217 concatenation — "USDGBP" means USD per 1 GBP
        sa.Column("pair", sa.String(10), nullable=False),
        # Rate. DEXUSUK ≈ 1.27 → 1 GBP = 1.27 USD
        sa.Column("rate", sa.Numeric(14, 8), nullable=False),
        # Source attribution
        sa.Column("source", sa.String(20), nullable=False),            # "FRED"|"manual"|...
        sa.Column("source_series_id", sa.String(40), nullable=True),   # "DEXUSUK"
        sa.Column("bar_size", sa.String(10), nullable=False, server_default="'daily'"),
        # When we fetched — for raw_data audit trail
        sa.Column("fetched_at", sa.DateTime(timezone=True),
                  server_default=sa.text("NOW()"), nullable=False),
        schema="raw_data",
    )
    op.create_check_constraint(
        "ck_fx_rates_rate_positive",
        "fx_rates",
        "rate > 0",
        schema="raw_data",
    )
    # One rate per (pair, date, source)
    op.create_index(
        "ux_fx_rates_pair_date_source",
        "fx_rates",
        ["pair", "as_of_date", "source"],
        unique=True,
        schema="raw_data",
    )
    op.create_index(
        "ix_fx_rates_pair_date",
        "fx_rates",
        ["pair", "as_of_date"],
        schema="raw_data",
    )


def downgrade() -> None:
    op.drop_table("fx_rates", schema="raw_data")