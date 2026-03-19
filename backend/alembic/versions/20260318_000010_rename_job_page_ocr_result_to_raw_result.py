"""rename job page ocr_result to raw_result

Revision ID: 20260318_000010
Revises: 20260318_000009
Create Date: 2026-03-18 00:00:10

"""

from alembic import op
import sqlalchemy as sa


revision = "20260318_000010"
down_revision = "20260318_000009"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    columns = {column.get("name") for column in inspector.get_columns("job_pages")}
    if "ocr_result" in columns and "raw_result" not in columns:
        op.execute("ALTER TABLE job_pages RENAME COLUMN ocr_result TO raw_result")


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    columns = {column.get("name") for column in inspector.get_columns("job_pages")}
    if "raw_result" in columns and "ocr_result" not in columns:
        op.execute("ALTER TABLE job_pages RENAME COLUMN raw_result TO ocr_result")
