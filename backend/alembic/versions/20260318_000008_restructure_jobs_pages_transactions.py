"""restructure jobs pages and transactions schema

Revision ID: 20260318_000008
Revises: 20260316_000007
Create Date: 2026-03-18 00:00:08

"""

from alembic import op
import sqlalchemy as sa


revision = "20260318_000008"
down_revision = "20260316_000007"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE jobs_v2 (
            id VARCHAR(36) NOT NULL PRIMARY KEY,
            file_name VARCHAR(512) NOT NULL,
            file_size BIGINT NOT NULL,
            job_status VARCHAR(32) NOT NULL,
            started_at TIMESTAMPTZ NULL,
            ended_at TIMESTAMPTZ NULL,
            pages INTEGER NOT NULL DEFAULT 0,
            is_reversed BOOLEAN NOT NULL DEFAULT FALSE
        )
        """
    )
    op.execute(
        """
        INSERT INTO jobs_v2 (
            id,
            file_name,
            file_size,
            job_status,
            started_at,
            ended_at,
            pages,
            is_reversed
        )
        SELECT
            CAST(job_id AS TEXT) AS id,
            COALESCE(file_name, '') AS file_name,
            COALESCE(file_size, 0) AS file_size,
            COALESCE(NULLIF(processing_status, ''), 'queued') AS job_status,
            process_started AS started_at,
            process_end AS ended_at,
            0 AS pages,
            COALESCE(is_reversed, FALSE) AS is_reversed
        FROM jobs
        """
    )
    op.create_index("ix_jobs_job_status", "jobs_v2", ["job_status"], unique=False)

    op.execute(
        """
        CREATE TABLE job_pages_v2 (
            id VARCHAR(36) NOT NULL PRIMARY KEY,
            job_id VARCHAR(36) NOT NULL REFERENCES jobs_v2 (id) ON DELETE CASCADE,
            page_number INTEGER NOT NULL,
            is_digital BOOLEAN NOT NULL DEFAULT FALSE,
            raw_result JSONB NULL,
            notes TEXT NULL
        )
        """
    )
    op.create_index("ix_job_pages_job_id", "job_pages_v2", ["job_id"], unique=False)
    op.create_unique_constraint("uq_job_pages_job_page_number", "job_pages_v2", ["job_id", "page_number"])
    op.execute(
        """
        INSERT INTO job_pages_v2 (
            id,
            job_id,
            page_number,
            is_digital,
            raw_result,
            notes
        )
        SELECT
            md5(legacy.job_id || ':' || legacy.page_number) AS id,
            legacy.job_id,
            legacy.page_number,
            COALESCE(NOT raw.is_ocr, FALSE) AS is_digital,
            NULL AS raw_result,
            NULL AS notes
        FROM (
            SELECT DISTINCT job_id, page_number
            FROM job_transactions
        ) AS legacy
        LEFT JOIN job_results_raw AS raw
            ON raw.job_id = legacy.job_id
        """
    )

    op.execute(
        """
        CREATE TABLE transactions_v2 (
            id VARCHAR(36) NOT NULL PRIMARY KEY,
            job_id VARCHAR(36) NOT NULL REFERENCES jobs_v2 (id) ON DELETE CASCADE,
            page_id VARCHAR(36) NOT NULL REFERENCES job_pages_v2 (id) ON DELETE CASCADE,
            row_index INTEGER NOT NULL,
            row_number INTEGER NULL,
            date VARCHAR(32) NULL,
            description TEXT NULL,
            debit NUMERIC(18, 2) NULL,
            credit NUMERIC(18, 2) NULL,
            balance NUMERIC(18, 2) NULL,
            row_number_bounds JSONB NULL,
            date_bounds JSONB NULL,
            description_bounds JSONB NULL,
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
    op.create_index("ix_transactions_job_id", "transactions_v2", ["job_id"], unique=False)
    op.create_index("ix_transactions_page_id", "transactions_v2", ["page_id"], unique=False)
    op.create_index("ix_transactions_job_page", "transactions_v2", ["job_id", "page_id"], unique=False)
    op.create_unique_constraint("uq_transactions_job_row_index", "transactions_v2", ["job_id", "row_index"])
    op.execute(
        """
        INSERT INTO transactions_v2 (
            id,
            job_id,
            page_id,
            row_index,
            row_number,
            date,
            description,
            debit,
            credit,
            balance,
            row_number_bounds,
            date_bounds,
            description_bounds,
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
            pages.id AS page_id,
            legacy.row_index,
            legacy.row_number,
            legacy.date,
            legacy.description,
            legacy.debit,
            legacy.credit,
            legacy.balance,
            legacy.row_number_bounds,
            legacy.date_bounds,
            COALESCE(legacy.date_bounds, legacy.row_number_bounds) AS description_bounds,
            legacy.debit_bounds,
            legacy.credit_bounds,
            legacy.balance_bounds,
            legacy.row_type,
            legacy.is_new_row,
            legacy.is_modified,
            legacy.is_deleted,
            legacy.created_at,
            legacy.updated_at
        FROM job_transactions AS legacy
        JOIN job_pages_v2 AS pages
            ON pages.job_id = legacy.job_id
           AND pages.page_number = legacy.page_number
        """
    )

    op.execute(
        """
        UPDATE jobs_v2 AS jobs
        SET pages = counts.page_count
        FROM (
            SELECT job_id, COUNT(*)::INTEGER AS page_count
            FROM job_pages_v2
            GROUP BY job_id
        ) AS counts
        WHERE counts.job_id = jobs.id
        """
    )

    op.drop_table("job_transactions")
    op.drop_index("ix_jobs_processing_status", table_name="jobs")
    op.drop_table("jobs")

    op.rename_table("jobs_v2", "jobs")
    op.rename_table("job_pages_v2", "job_pages")
    op.rename_table("transactions_v2", "transactions")


def downgrade() -> None:
    op.execute(
        """
        CREATE TABLE jobs_legacy (
            job_id VARCHAR(36) NOT NULL PRIMARY KEY,
            file_name VARCHAR(512) NOT NULL,
            file_size BIGINT NOT NULL,
            processing_status VARCHAR(32) NOT NULL,
            process_started TIMESTAMPTZ NULL,
            process_end TIMESTAMPTZ NULL,
            is_reversed BOOLEAN NOT NULL DEFAULT FALSE
        )
        """
    )
    op.execute(
        """
        INSERT INTO jobs_legacy (
            job_id,
            file_name,
            file_size,
            processing_status,
            process_started,
            process_end,
            is_reversed
        )
        SELECT
            id,
            file_name,
            file_size,
            job_status,
            started_at,
            ended_at,
            is_reversed
        FROM jobs
        """
    )
    op.create_index("ix_jobs_processing_status", "jobs_legacy", ["processing_status"], unique=False)

    op.execute(
        """
        CREATE TABLE job_transactions_legacy (
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
    op.create_index("ix_job_transactions_job_id", "job_transactions_legacy", ["job_id"], unique=False)
    op.create_index("ix_job_transactions_job_page", "job_transactions_legacy", ["job_id", "page_key"], unique=False)
    op.create_unique_constraint("uq_job_transactions_job_row_index", "job_transactions_legacy", ["job_id", "row_index"])
    op.execute(
        """
        INSERT INTO job_transactions_legacy (
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
            tx.id,
            tx.job_id,
            'page_' || LPAD(CAST(pages.page_number AS TEXT), 3, '0') AS page_key,
            pages.page_number,
            tx.row_index,
            tx.row_number,
            tx.date,
            tx.description,
            tx.debit,
            tx.credit,
            tx.balance,
            tx.row_number_bounds,
            tx.date_bounds,
            tx.debit_bounds,
            tx.credit_bounds,
            tx.balance_bounds,
            tx.row_type,
            tx.is_new_row,
            tx.is_modified,
            tx.is_deleted,
            tx.created_at,
            tx.updated_at
        FROM transactions AS tx
        JOIN job_pages AS pages
            ON pages.id = tx.page_id
        """
    )

    op.drop_table("transactions")
    op.drop_table("job_pages")
    op.drop_index("ix_jobs_job_status", table_name="jobs")
    op.drop_table("jobs")

    op.rename_table("jobs_legacy", "jobs")
    op.rename_table("job_transactions_legacy", "job_transactions")
