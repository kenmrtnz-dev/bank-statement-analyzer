"""baseline schema

Revision ID: 20260219_000001
Revises: 
Create Date: 2026-02-19 00:00:01

"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = "20260219_000001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "users",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("email", sa.String(length=255), nullable=False),
        sa.Column("password_hash", sa.String(length=255), nullable=False),
        sa.Column("role", sa.String(length=32), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("email", name="uq_users_email"),
    )
    op.create_index("ix_users_email", "users", ["email"], unique=False)
    op.create_index("ix_users_role", "users", ["role"], unique=False)

    op.create_table(
        "submissions",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("agent_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("users.id"), nullable=False),
        sa.Column("assigned_evaluator_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("users.id"), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("borrower_name", sa.String(length=255), nullable=True),
        sa.Column("lead_reference", sa.String(length=255), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("input_pdf_key", sa.String(length=1024), nullable=False),
        sa.Column("current_job_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("summary_snapshot_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_submissions_agent_id", "submissions", ["agent_id"], unique=False)
    op.create_index("ix_submissions_assigned_evaluator_id", "submissions", ["assigned_evaluator_id"], unique=False)
    op.create_index("ix_submissions_current_job_id", "submissions", ["current_job_id"], unique=False)
    op.create_index("ix_submissions_lead_reference", "submissions", ["lead_reference"], unique=False)
    op.create_index("ix_submissions_status", "submissions", ["status"], unique=False)

    op.create_table(
        "jobs",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("submission_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("submissions.id"), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("step", sa.String(length=64), nullable=True),
        sa.Column("progress", sa.Integer(), nullable=False),
        sa.Column("parse_mode", sa.String(length=16), nullable=True),
        sa.Column("ocr_backend", sa.String(length=64), nullable=True),
        sa.Column("diagnostics_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("ix_jobs_submission_id", "jobs", ["submission_id"], unique=False)
    op.create_index("ix_jobs_status", "jobs", ["status"], unique=False)

    op.create_table(
        "transactions",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("submission_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("submissions.id"), nullable=False),
        sa.Column("job_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("jobs.id"), nullable=True),
        sa.Column("page", sa.String(length=32), nullable=True),
        sa.Column("row_index", sa.Integer(), nullable=False),
        sa.Column("date", sa.String(length=32), nullable=True),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("debit", sa.Numeric(18, 2), nullable=True),
        sa.Column("credit", sa.Numeric(18, 2), nullable=True),
        sa.Column("balance", sa.Numeric(18, 2), nullable=True),
        sa.Column("x1", sa.Numeric(10, 6), nullable=True),
        sa.Column("y1", sa.Numeric(10, 6), nullable=True),
        sa.Column("x2", sa.Numeric(10, 6), nullable=True),
        sa.Column("y2", sa.Numeric(10, 6), nullable=True),
        sa.Column("is_manual_edit", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_transactions_submission_id", "transactions", ["submission_id"], unique=False)
    op.create_index("ix_transactions_job_id", "transactions", ["job_id"], unique=False)
    op.create_index("ix_transactions_date", "transactions", ["date"], unique=False)

    op.create_table(
        "submission_pages",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("submission_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("submissions.id"), nullable=False),
        sa.Column("job_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("jobs.id"), nullable=True),
        sa.Column("page_key", sa.String(length=32), nullable=False),
        sa.Column("page_index", sa.Integer(), nullable=False),
        sa.Column("parse_status", sa.String(length=24), nullable=False),
        sa.Column("review_status", sa.String(length=24), nullable=False),
        sa.Column("rows_count", sa.Integer(), nullable=False),
        sa.Column("has_unsaved", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("parse_error", sa.Text(), nullable=True),
        sa.Column("last_parsed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_saved_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("submission_id", "page_key", name="uq_submission_page_key"),
    )
    op.create_index("ix_submission_pages_submission_id", "submission_pages", ["submission_id"], unique=False)
    op.create_index("ix_submission_pages_job_id", "submission_pages", ["job_id"], unique=False)
    op.create_index("ix_submission_pages_page_key", "submission_pages", ["page_key"], unique=False)
    op.create_index("ix_submission_pages_parse_status", "submission_pages", ["parse_status"], unique=False)
    op.create_index("ix_submission_pages_review_status", "submission_pages", ["review_status"], unique=False)

    op.create_table(
        "reports",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("submission_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("submissions.id"), nullable=False),
        sa.Column("generated_by", postgresql.UUID(as_uuid=True), sa.ForeignKey("users.id"), nullable=False),
        sa.Column("report_type", sa.String(length=64), nullable=False),
        sa.Column("blob_key", sa.String(length=1024), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_reports_submission_id", "reports", ["submission_id"], unique=False)
    op.create_index("ix_reports_generated_by", "reports", ["generated_by"], unique=False)

    op.create_table(
        "audit_log",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("actor_user_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("users.id"), nullable=True),
        sa.Column("submission_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("submissions.id"), nullable=True),
        sa.Column("action", sa.String(length=128), nullable=False),
        sa.Column("before_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("after_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_audit_log_actor_user_id", "audit_log", ["actor_user_id"], unique=False)
    op.create_index("ix_audit_log_submission_id", "audit_log", ["submission_id"], unique=False)
    op.create_index("ix_audit_log_action", "audit_log", ["action"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_audit_log_action", table_name="audit_log")
    op.drop_index("ix_audit_log_submission_id", table_name="audit_log")
    op.drop_index("ix_audit_log_actor_user_id", table_name="audit_log")
    op.drop_table("audit_log")

    op.drop_index("ix_reports_generated_by", table_name="reports")
    op.drop_index("ix_reports_submission_id", table_name="reports")
    op.drop_table("reports")

    op.drop_index("ix_submission_pages_review_status", table_name="submission_pages")
    op.drop_index("ix_submission_pages_parse_status", table_name="submission_pages")
    op.drop_index("ix_submission_pages_page_key", table_name="submission_pages")
    op.drop_index("ix_submission_pages_job_id", table_name="submission_pages")
    op.drop_index("ix_submission_pages_submission_id", table_name="submission_pages")
    op.drop_table("submission_pages")

    op.drop_index("ix_transactions_date", table_name="transactions")
    op.drop_index("ix_transactions_job_id", table_name="transactions")
    op.drop_index("ix_transactions_submission_id", table_name="transactions")
    op.drop_table("transactions")

    op.drop_index("ix_jobs_status", table_name="jobs")
    op.drop_index("ix_jobs_submission_id", table_name="jobs")
    op.drop_table("jobs")

    op.drop_index("ix_submissions_status", table_name="submissions")
    op.drop_index("ix_submissions_lead_reference", table_name="submissions")
    op.drop_index("ix_submissions_current_job_id", table_name="submissions")
    op.drop_index("ix_submissions_assigned_evaluator_id", table_name="submissions")
    op.drop_index("ix_submissions_agent_id", table_name="submissions")
    op.drop_table("submissions")

    op.drop_index("ix_users_role", table_name="users")
    op.drop_index("ix_users_email", table_name="users")
    op.drop_table("users")
