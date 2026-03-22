"""add disbalance fields to job transactions

Revision ID: 20260316_000007
Revises: 20260316_000006
Create Date: 2026-03-16 00:00:07

"""

from alembic import op
import sqlalchemy as sa


revision = "20260316_000007"
down_revision = "20260316_000006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "job_transactions",
        sa.Column("is_disbalanced", sa.Boolean(), nullable=False, server_default=sa.false()),
    )
    op.add_column(
        "job_transactions",
        sa.Column("disbalance_expected_balance", sa.Numeric(18, 2), nullable=True),
    )
    op.add_column(
        "job_transactions",
        sa.Column("disbalance_delta", sa.Numeric(18, 2), nullable=True),
    )
    op.alter_column("job_transactions", "is_disbalanced", server_default=None)


def downgrade() -> None:
    op.drop_column("job_transactions", "disbalance_delta")
    op.drop_column("job_transactions", "disbalance_expected_balance")
    op.drop_column("job_transactions", "is_disbalanced")
