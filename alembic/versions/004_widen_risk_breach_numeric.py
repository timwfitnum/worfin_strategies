"""
alembic/versions/004_widen_risk_breaches_numeric.py
Widen audit.risk_breaches threshold and actual_value columns.

NUMERIC(10, 6) → NUMERIC(18, 4)
  Old max: 9,999.999999
  New max: 99,999,999,999,999.9999

These columns store heterogeneous values: GBP notionals, hours,
percentages, lot counts — so they need room for large numbers.
"""

from alembic import op

revision = "004"
down_revision = "003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE audit.risk_breaches
            ALTER COLUMN threshold    TYPE NUMERIC(18, 4),
            ALTER COLUMN actual_value TYPE NUMERIC(18, 4)
    """
    )


def downgrade() -> None:
    op.execute(
        """
        ALTER TABLE audit.risk_breaches
            ALTER COLUMN threshold    TYPE NUMERIC(10, 6),
            ALTER COLUMN actual_value TYPE NUMERIC(10, 6)
    """
    )
