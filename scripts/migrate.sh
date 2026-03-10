#!/usr/bin/env sh
set -eu

migration_mode="$(
DB_AUTO_CREATE_SCHEMA=true python - <<'PY'
from __future__ import annotations

import os

from sqlalchemy import create_engine, inspect

DATABASE_URL = os.environ["DATABASE_URL"]
DATA_DIR = os.environ.get("DATA_DIR", "/data")

engine = create_engine(DATABASE_URL, future=True)
try:
    inspector = inspect(engine)
    table_names = set(inspector.get_table_names())
finally:
    engine.dispose()

app_tables = {
    "bank_code_flags",
    "job_results_raw",
    "job_transactions",
    "jobs",
    "submission_pages",
    "transactions",
}

if "alembic_version" in table_names or not table_names.intersection(app_tables):
    print("upgrade")
else:
    from app.jobs.repository import (
        ensure_bank_code_flags_schema,
        ensure_job_results_raw_schema,
        ensure_job_transactions_schema,
        ensure_jobs_schema,
    )

    ensure_jobs_schema(DATA_DIR)
    ensure_job_transactions_schema(DATA_DIR)
    ensure_job_results_raw_schema(DATA_DIR)
    ensure_bank_code_flags_schema(DATA_DIR)
    print("stamp")
PY
)"

if [ "$migration_mode" = "stamp" ]; then
    exec alembic -c backend/alembic.ini stamp head
fi

exec alembic -c backend/alembic.ini upgrade head
