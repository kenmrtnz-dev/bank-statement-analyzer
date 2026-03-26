"""add strict intake fields to job pages

Revision ID: 20260326_000011
Revises: 20260318_000010
Create Date: 2026-03-26 00:00:11

"""

from alembic import op
import sqlalchemy as sa


revision = "20260326_000011"
down_revision = "20260318_000010"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    columns = {column.get("name") for column in inspector.get_columns("job_pages")}

    if "page_type" not in columns:
        op.add_column(
            "job_pages",
            sa.Column("page_type", sa.String(length=16), nullable=False, server_default=sa.text("'digital'")),
        )
    if "raw_text" not in columns:
        op.add_column("job_pages", sa.Column("raw_text", sa.Text(), nullable=True))
    if "processing_status" not in columns:
        op.add_column(
            "job_pages",
            sa.Column("processing_status", sa.String(length=32), nullable=False, server_default=sa.text("'pending'")),
        )

    op.execute(
        """
        UPDATE job_pages
        SET page_type = CASE
            WHEN COALESCE(is_digital, FALSE) THEN 'digital'
            ELSE 'scanned'
        END
        """
    )
    op.execute(
        """
        UPDATE job_pages
        SET raw_text = COALESCE(raw_text, NULLIF(raw_result ->> 'text', ''))
        WHERE raw_text IS NULL OR raw_text = ''
        """
    )
    op.execute(
        """
        UPDATE job_pages
        SET processing_status = CASE
            WHEN raw_result IS NOT NULL OR raw_text IS NOT NULL THEN 'done'
            ELSE COALESCE(NULLIF(processing_status, ''), 'pending')
        END
        """
    )


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    columns = {column.get("name") for column in inspector.get_columns("job_pages")}

    if "processing_status" in columns:
        op.drop_column("job_pages", "processing_status")
    if "raw_text" in columns:
        op.drop_column("job_pages", "raw_text")
    if "page_type" in columns:
        op.drop_column("job_pages", "page_type")
