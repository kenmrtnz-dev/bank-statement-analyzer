"""normalize jobs schema for current runtime model

Revision ID: 20260310_000005
Revises: 20260309_000004
Create Date: 2026-03-10 00:00:05

"""

from alembic import op
import sqlalchemy as sa


revision = "20260310_000005"
down_revision = "20260309_000004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("DROP TABLE IF EXISTS submission_pages CASCADE")
    op.execute("DROP TABLE IF EXISTS transactions CASCADE")

    op.add_column("jobs", sa.Column("job_id", sa.String(length=36), nullable=True))
    op.add_column("jobs", sa.Column("file_name", sa.String(length=512), nullable=False, server_default=""))
    op.add_column("jobs", sa.Column("file_size", sa.BigInteger(), nullable=False, server_default="0"))
    op.add_column("jobs", sa.Column("processing_status", sa.String(length=32), nullable=False, server_default="queued"))
    op.add_column("jobs", sa.Column("process_started", sa.DateTime(timezone=True), nullable=True))
    op.add_column("jobs", sa.Column("process_end", sa.DateTime(timezone=True), nullable=True))
    op.add_column("jobs", sa.Column("is_reversed", sa.Boolean(), nullable=False, server_default=sa.false()))

    op.execute("UPDATE jobs SET job_id = CAST(id AS TEXT) WHERE job_id IS NULL")
    op.execute("UPDATE jobs SET processing_status = COALESCE(NULLIF(status, ''), processing_status)")

    op.alter_column("jobs", "job_id", nullable=False)
    op.drop_constraint("jobs_pkey", "jobs", type_="primary")
    op.create_primary_key("jobs_pkey", "jobs", ["job_id"])
    op.create_index("ix_jobs_processing_status", "jobs", ["processing_status"], unique=False)

    op.alter_column("jobs", "file_name", server_default=None)
    op.alter_column("jobs", "file_size", server_default=None)
    op.alter_column("jobs", "processing_status", server_default=None)
    op.alter_column("jobs", "is_reversed", server_default=None)


def downgrade() -> None:
    op.drop_index("ix_jobs_processing_status", table_name="jobs")
    op.drop_constraint("jobs_pkey", "jobs", type_="primary")
    op.create_primary_key("jobs_pkey", "jobs", ["id"])
    op.drop_column("jobs", "is_reversed")
    op.drop_column("jobs", "process_end")
    op.drop_column("jobs", "process_started")
    op.drop_column("jobs", "processing_status")
    op.drop_column("jobs", "file_size")
    op.drop_column("jobs", "file_name")
    op.drop_column("jobs", "job_id")
