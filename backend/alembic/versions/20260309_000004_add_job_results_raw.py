"""add job results raw table

Revision ID: 20260309_000004
Revises: 20260309_000003
Create Date: 2026-03-09 00:00:04

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = "20260309_000004"
down_revision = "20260309_000003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "job_results_raw",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("job_id", sa.String(length=36), nullable=False),
        sa.Column("is_ocr", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("raw_xml", sa.Text(), nullable=True),
        sa.Column("raw_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("job_id", name="uq_job_results_raw_job_id"),
    )
    op.create_index("ix_job_results_raw_job_id", "job_results_raw", ["job_id"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_job_results_raw_job_id", table_name="job_results_raw")
    op.drop_table("job_results_raw")
