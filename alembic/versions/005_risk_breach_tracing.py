"""
alembic/versions/005_risk_breach_tracing.py
Add backtest_run_id and correlation_id to audit.risk_breaches.

Without these columns, pre-trade rejections in backtests cannot be
traced to the specific run that generated them, making audit logs
useless for multi-run comparisons.
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "005"
down_revision = "004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "risk_breaches",
        sa.Column("backtest_run_id", postgresql.UUID(as_uuid=True), nullable=True),
        schema="audit",
    )
    op.add_column(
        "risk_breaches",
        sa.Column("correlation_id", sa.String(36), nullable=True),
        schema="audit",
    )
    op.create_index(
        "ix_audit_risk_breaches_backtest_run_id",
        "risk_breaches",
        ["backtest_run_id"],
        schema="audit",
    )


def downgrade() -> None:
    op.drop_index(
        "ix_audit_risk_breaches_backtest_run_id",
        table_name="risk_breaches",
        schema="audit",
    )
    op.drop_column("risk_breaches", "correlation_id", schema="audit")
    op.drop_column("risk_breaches", "backtest_run_id", schema="audit")
