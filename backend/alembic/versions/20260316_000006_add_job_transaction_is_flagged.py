"""add is_flagged to job transactions

Revision ID: 20260316_000006
Revises: 20260310_000005
Create Date: 2026-03-16 00:00:06

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20260316_000006"
down_revision = "20260310_000005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "job_transactions",
        sa.Column("is_flagged", sa.Boolean(), nullable=False, server_default=sa.false()),
    )
    op.alter_column("job_transactions", "is_flagged", server_default=None)


def downgrade() -> None:
    op.drop_column("job_transactions", "is_flagged")
