"""refactor job transactions schema

Revision ID: 20260309_000003
Revises: 20260302_000002
Create Date: 2026-03-09 00:00:03

"""

from alembic import op


# revision identifiers, used by Alembic.
revision = "20260309_000003"
down_revision = "20260302_000002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE job_transactions_v2 (
            id VARCHAR(36) NOT NULL PRIMARY KEY,
            job_id VARCHAR(36) NOT NULL,
            page_key VARCHAR(32) NOT NULL,
            page_number INTEGER NOT NULL,
            row_index INTEGER NOT NULL,
            row_number INTEGER NULL,
            date VARCHAR(32) NULL,
            description TEXT NULL,
            debit NUMERIC(18, 2) NULL,
            credit NUMERIC(18, 2) NULL,
            balance NUMERIC(18, 2) NULL,
            row_number_bounds JSONB NULL,
            date_bounds JSONB NULL,
            debit_bounds JSONB NULL,
            credit_bounds JSONB NULL,
            balance_bounds JSONB NULL,
            row_type VARCHAR(32) NOT NULL,
            is_new_row BOOLEAN NOT NULL DEFAULT FALSE,
            is_modified BOOLEAN NOT NULL DEFAULT FALSE,
            is_deleted BOOLEAN NOT NULL DEFAULT FALSE,
            created_at TIMESTAMPTZ NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL
        )
        """
    )
    op.execute(
        """
        INSERT INTO job_transactions_v2 (
            id,
            job_id,
            page_key,
            page_number,
            row_index,
            row_number,
            date,
            description,
            debit,
            credit,
            balance,
            row_number_bounds,
            date_bounds,
            debit_bounds,
            credit_bounds,
            balance_bounds,
            row_type,
            is_new_row,
            is_modified,
            is_deleted,
            created_at,
            updated_at
        )
        SELECT
            legacy.id,
            legacy.job_id,
            CASE
                WHEN legacy.page_key ~ '^page_[0-9]+$' THEN 'page_' || LPAD(SPLIT_PART(legacy.page_key, '_', 2), 3, '0')
                WHEN legacy.page_key ~ '^[0-9]+$' THEN 'page_' || LPAD(legacy.page_key, 3, '0')
                ELSE legacy.page_key
            END AS page_key,
            CASE
                WHEN legacy.page_key ~ '^page_[0-9]+$' THEN CAST(SPLIT_PART(legacy.page_key, '_', 2) AS INTEGER)
                WHEN legacy.page_key ~ '^[0-9]+$' THEN CAST(legacy.page_key AS INTEGER)
                ELSE 0
            END AS page_number,
            ROW_NUMBER() OVER (
                PARTITION BY legacy.job_id
                ORDER BY
                    CASE
                        WHEN legacy.page_key ~ '^page_[0-9]+$' THEN CAST(SPLIT_PART(legacy.page_key, '_', 2) AS INTEGER)
                        WHEN legacy.page_key ~ '^[0-9]+$' THEN CAST(legacy.page_key AS INTEGER)
                        ELSE 0
                    END,
                    legacy.row_index,
                    legacy.id
            ) AS row_index,
            CASE
                WHEN legacy.rownumber IS NOT NULL THEN legacy.rownumber
                WHEN REGEXP_REPLACE(COALESCE(legacy.row_number, ''), '[^0-9]', '', 'g') <> '' THEN
                    CAST(REGEXP_REPLACE(legacy.row_number, '[^0-9]', '', 'g') AS INTEGER)
                ELSE NULL
            END AS row_number,
            legacy.date,
            legacy.description,
            legacy.debit,
            legacy.credit,
            legacy.balance,
            CASE
                WHEN legacy.x1 IS NULL OR legacy.y1 IS NULL OR legacy.x2 IS NULL OR legacy.y2 IS NULL THEN NULL
                ELSE JSONB_BUILD_OBJECT('x1', legacy.x1, 'y1', legacy.y1, 'x2', legacy.x2, 'y2', legacy.y2)
            END AS row_number_bounds,
            CASE
                WHEN legacy.x1 IS NULL OR legacy.y1 IS NULL OR legacy.x2 IS NULL OR legacy.y2 IS NULL THEN NULL
                ELSE JSONB_BUILD_OBJECT('x1', legacy.x1, 'y1', legacy.y1, 'x2', legacy.x2, 'y2', legacy.y2)
            END AS date_bounds,
            CASE
                WHEN legacy.x1 IS NULL OR legacy.y1 IS NULL OR legacy.x2 IS NULL OR legacy.y2 IS NULL THEN NULL
                ELSE JSONB_BUILD_OBJECT('x1', legacy.x1, 'y1', legacy.y1, 'x2', legacy.x2, 'y2', legacy.y2)
            END AS debit_bounds,
            CASE
                WHEN legacy.x1 IS NULL OR legacy.y1 IS NULL OR legacy.x2 IS NULL OR legacy.y2 IS NULL THEN NULL
                ELSE JSONB_BUILD_OBJECT('x1', legacy.x1, 'y1', legacy.y1, 'x2', legacy.x2, 'y2', legacy.y2)
            END AS credit_bounds,
            CASE
                WHEN legacy.x1 IS NULL OR legacy.y1 IS NULL OR legacy.x2 IS NULL OR legacy.y2 IS NULL THEN NULL
                ELSE JSONB_BUILD_OBJECT('x1', legacy.x1, 'y1', legacy.y1, 'x2', legacy.x2, 'y2', legacy.y2)
            END AS balance_bounds,
            legacy.row_type,
            FALSE AS is_new_row,
            COALESCE(legacy.is_manual_edit, FALSE) AS is_modified,
            FALSE AS is_deleted,
            legacy.created_at,
            legacy.updated_at
        FROM job_transactions AS legacy
        """
    )
    op.drop_index("ix_job_transactions_job_page", table_name="job_transactions")
    op.drop_index("ix_job_transactions_job_id", table_name="job_transactions")
    op.drop_table("job_transactions")
    op.rename_table("job_transactions_v2", "job_transactions")
    op.create_index("ix_job_transactions_job_id", "job_transactions", ["job_id"], unique=False)
    op.create_index("ix_job_transactions_job_page", "job_transactions", ["job_id", "page_key"], unique=False)
    op.create_unique_constraint("uq_job_transactions_job_row_index", "job_transactions", ["job_id", "row_index"])


def downgrade() -> None:
    op.execute(
        """
        CREATE TABLE job_transactions_legacy (
            id VARCHAR(36) NOT NULL PRIMARY KEY,
            job_id VARCHAR(36) NOT NULL,
            page_key VARCHAR(32) NOT NULL,
            row_index INTEGER NOT NULL,
            row_id VARCHAR(64) NOT NULL,
            rownumber INTEGER NULL,
            row_number VARCHAR(32) NULL,
            date VARCHAR(32) NULL,
            description TEXT NULL,
            debit NUMERIC(18, 2) NULL,
            credit NUMERIC(18, 2) NULL,
            balance NUMERIC(18, 2) NULL,
            row_type VARCHAR(32) NOT NULL,
            x1 NUMERIC(10, 6) NULL,
            y1 NUMERIC(10, 6) NULL,
            x2 NUMERIC(10, 6) NULL,
            y2 NUMERIC(10, 6) NULL,
            is_manual_edit BOOLEAN NOT NULL DEFAULT FALSE,
            created_at TIMESTAMPTZ NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL
        )
        """
    )
    op.execute(
        """
        INSERT INTO job_transactions_legacy (
            id,
            job_id,
            page_key,
            row_index,
            row_id,
            rownumber,
            row_number,
            date,
            description,
            debit,
            credit,
            balance,
            row_type,
            x1,
            y1,
            x2,
            y2,
            is_manual_edit,
            created_at,
            updated_at
        )
        SELECT
            current.id,
            current.job_id,
            current.page_key,
            ROW_NUMBER() OVER (PARTITION BY current.job_id, current.page_key ORDER BY current.row_index, current.id) AS row_index,
            LPAD(ROW_NUMBER() OVER (PARTITION BY current.job_id, current.page_key ORDER BY current.row_index, current.id)::TEXT, 3, '0') AS row_id,
            current.row_number AS rownumber,
            COALESCE(current.row_number::TEXT, '') AS row_number,
            current.date,
            current.description,
            current.debit,
            current.credit,
            current.balance,
            current.row_type,
            CAST((current.date_bounds ->> 'x1') AS NUMERIC),
            CAST((current.date_bounds ->> 'y1') AS NUMERIC),
            CAST((current.date_bounds ->> 'x2') AS NUMERIC),
            CAST((current.date_bounds ->> 'y2') AS NUMERIC),
            current.is_modified,
            current.created_at,
            current.updated_at
        FROM job_transactions AS current
        """
    )
    op.drop_constraint("uq_job_transactions_job_row_index", "job_transactions", type_="unique")
    op.drop_index("ix_job_transactions_job_page", table_name="job_transactions")
    op.drop_index("ix_job_transactions_job_id", table_name="job_transactions")
    op.drop_table("job_transactions")
    op.rename_table("job_transactions_legacy", "job_transactions")
    op.create_index("ix_job_transactions_job_id", "job_transactions", ["job_id"], unique=False)
    op.create_index("ix_job_transactions_job_page", "job_transactions", ["job_id", "page_key"], unique=False)
    op.create_unique_constraint("uq_job_transactions_job_page_row_index", "job_transactions", ["job_id", "page_key", "row_index"])
