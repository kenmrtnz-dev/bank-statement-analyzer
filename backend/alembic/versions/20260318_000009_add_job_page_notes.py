"""add notes to job pages

Revision ID: 20260318_000009
Revises: 20260318_000008
Create Date: 2026-03-18 00:00:09

"""

from alembic import op
import sqlalchemy as sa


revision = "20260318_000009"
down_revision = "20260318_000008"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    columns = {column.get("name") for column in inspector.get_columns("job_pages")}
    if "notes" not in columns:
        op.add_column("job_pages", sa.Column("notes", sa.Text(), nullable=True))


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    columns = {column.get("name") for column in inspector.get_columns("job_pages")}
    if "notes" in columns:
        op.drop_column("job_pages", "notes")
