"""add job transactions table

Revision ID: 20260302_000002
Revises: 20260219_000001
Create Date: 2026-03-02 00:00:02

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "20260302_000002"
down_revision = "20260219_000001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "job_transactions",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("job_id", sa.String(length=36), nullable=False),
        sa.Column("page_key", sa.String(length=32), nullable=False),
        sa.Column("row_index", sa.Integer(), nullable=False),
        sa.Column("row_id", sa.String(length=64), nullable=False),
        sa.Column("rownumber", sa.Integer(), nullable=True),
        sa.Column("row_number", sa.String(length=32), nullable=True),
        sa.Column("date", sa.String(length=32), nullable=True),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("debit", sa.Numeric(18, 2), nullable=True),
        sa.Column("credit", sa.Numeric(18, 2), nullable=True),
        sa.Column("balance", sa.Numeric(18, 2), nullable=True),
        sa.Column("row_type", sa.String(length=32), nullable=False),
        sa.Column("x1", sa.Numeric(10, 6), nullable=True),
        sa.Column("y1", sa.Numeric(10, 6), nullable=True),
        sa.Column("x2", sa.Numeric(10, 6), nullable=True),
        sa.Column("y2", sa.Numeric(10, 6), nullable=True),
        sa.Column("is_manual_edit", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("job_id", "page_key", "row_index", name="uq_job_transactions_job_page_row_index"),
    )
    op.create_index("ix_job_transactions_job_id", "job_transactions", ["job_id"], unique=False)
    op.create_index("ix_job_transactions_job_page", "job_transactions", ["job_id", "page_key"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_job_transactions_job_page", table_name="job_transactions")
    op.drop_index("ix_job_transactions_job_id", table_name="job_transactions")
    op.drop_table("job_transactions")
