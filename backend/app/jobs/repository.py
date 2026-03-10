"""Persistence helpers for job files plus SQL-backed row and bank-code storage."""

from __future__ import annotations

import datetime as dt
import json
import logging
import os
import tempfile
import threading
import time
import uuid
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Callable, Dict

from sqlalchemy import Text, cast, create_engine, delete, func, inspect, or_, select, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session
from sqlalchemy.exc import OperationalError

from app.infra.db.models import BankCodeFlagRecord, Base, JobRecord, JobResultRawRecord, JobTransactionRecord
from app.settings import load_settings

_DB_ENGINE_CACHE: dict[str, Engine] = {}
_DB_ENGINE_CACHE_GUARD = threading.Lock()
_DB_SCHEMA_READY: set[str] = set()
logger = logging.getLogger(__name__)


class JobsRepository:
    """Manage the filesystem layout and JSON artifacts stored under each job folder."""

    def __init__(self, data_dir: str | Path):
        self.data_dir = Path(data_dir)
        self.jobs_dir = self.data_dir / "jobs"
        self.exports_dir = self.data_dir / "exports"
        self.jobs_dir.mkdir(parents=True, exist_ok=True)
        self.exports_dir.mkdir(parents=True, exist_ok=True)

    def job_dir(self, job_id: str) -> Path:
        return self.jobs_dir / str(job_id)

    def ensure_job_layout(self, job_id: str):
        root = self.job_dir(job_id)
        for part in ("input", "result", "pages", "cleaned", "ocr", "preview"):
            (root / part).mkdir(parents=True, exist_ok=True)
        return root

    def job_exists(self, job_id: str) -> bool:
        return (self.job_dir(job_id) / "input" / "document.pdf").exists()

    def path(self, job_id: str, *parts: str) -> Path:
        return self.job_dir(job_id).joinpath(*parts)

    def write_bytes(self, path: Path, data: bytes):
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "wb") as handle:
            handle.write(data)

    def write_json(self, path: Path, payload: Any):
        """Write JSON atomically by replacing the target file after a temp-file flush."""
        path.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            json.dump(payload, handle, indent=2, default=_json_default)
            tmp = Path(handle.name)
        os.replace(tmp, path)

    def read_json(self, path: Path, default: Any):
        """Read JSON defensively and fall back when the file is missing or malformed."""
        if not path.exists():
            return default
        try:
            with open(path, encoding="utf-8") as handle:
                return json.load(handle)
        except Exception:
            return default

    def read_status(self, job_id: str) -> Dict[str, Any]:
        return self.read_json(self.path(job_id, "status.json"), default={})

    def write_status(self, job_id: str, payload: Dict[str, Any]):
        self.write_json(self.path(job_id, "status.json"), payload)
        try:
            JobStateRepository(self.data_dir).sync_job(
                job_id=str(job_id),
                meta=self.read_json(self.path(job_id, "meta.json"), default={}),
                status=payload,
            )
        except Exception:
            logger.exception("Failed to sync jobs table for %s", job_id)

    def list_png(self, job_id: str, folder: str) -> list[str]:
        target = self.path(job_id, folder)
        if not target.exists():
            return []
        return sorted(item.name for item in target.iterdir() if item.is_file() and item.suffix.lower() == ".png")


def _env_bool(name: str, default: bool) -> bool:
    raw = str(os.getenv(name, str(default))).strip().lower()
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    return default


def _resolve_database_url(data_dir: str | Path) -> str:
    configured = load_settings().database_url
    if configured:
        return configured
    raise RuntimeError(
        "DATABASE_URL is required. Configure Postgres explicitly."
    )


def _db_connect_max_wait_seconds() -> float:
    raw = str(os.getenv("DB_CONNECT_MAX_WAIT_SECONDS") or "45").strip()
    try:
        return max(0.0, float(raw))
    except ValueError:
        return 45.0


def _db_connect_retry_interval_seconds() -> float:
    raw = str(os.getenv("DB_CONNECT_RETRY_INTERVAL_SECONDS") or "2").strip()
    try:
        return max(0.1, float(raw))
    except ValueError:
        return 2.0


def _run_schema_bootstrap(
    data_dir: str | Path,
    operation: Callable[[str, Engine], None],
) -> None:
    """Run schema setup, retrying transient startup failures for PostgreSQL startup races."""
    url = _resolve_database_url(data_dir)
    engine = _get_db_engine(data_dir)

    deadline = time.monotonic() + _db_connect_max_wait_seconds()
    retry_interval = _db_connect_retry_interval_seconds()

    while True:
        try:
            operation(url, engine)
            return
        except OperationalError as exc:
            if time.monotonic() >= deadline:
                raise
            logger.warning(
                "Database bootstrap failed for %s (%s). Retrying in %.1fs until startup timeout.",
                url,
                exc,
                retry_interval,
            )
            with _DB_ENGINE_CACHE_GUARD:
                cached = _DB_ENGINE_CACHE.pop(url, None)
            if cached is not None:
                cached.dispose()
            time.sleep(retry_interval)
            engine = _get_db_engine(data_dir)


def _get_db_engine(data_dir: str | Path) -> Engine:
    """Reuse one SQLAlchemy engine per resolved database URL."""
    url = _resolve_database_url(data_dir)
    with _DB_ENGINE_CACHE_GUARD:
        engine = _DB_ENGINE_CACHE.get(url)
        if engine is not None:
            return engine
        engine = create_engine(url, future=True)
        _DB_ENGINE_CACHE[url] = engine
        return engine


def ensure_job_transactions_schema(data_dir: str | Path) -> None:
    """Create the job-transactions table when schema auto-creation is enabled."""
    if not _env_bool("DB_AUTO_CREATE_SCHEMA", True):
        return
    ensure_jobs_schema(data_dir)

    def _apply(url: str, engine: Engine) -> None:
        key = f"{url}#job_transactions"
        with _DB_ENGINE_CACHE_GUARD:
            if key in _DB_SCHEMA_READY:
                return
        inspector = inspect(engine)
        expected_columns = set(JobTransactionRecord.__table__.c.keys())
        legacy_columns = {
            "row_id",
            "rownumber",
            "x1",
            "y1",
            "x2",
            "y2",
            "is_manual_edit",
        }
        if "job_transactions" not in inspector.get_table_names():
            Base.metadata.create_all(engine, tables=[JobTransactionRecord.__table__])
        else:
            existing_columns = {column.get("name") for column in inspector.get_columns("job_transactions")}
            missing_columns = expected_columns - existing_columns
            if missing_columns:
                if legacy_columns.issubset(existing_columns):
                    _rebuild_legacy_job_transactions_schema(engine)
                else:
                    missing = ", ".join(sorted(missing_columns))
                    raise RuntimeError(
                        "job_transactions schema is incompatible with the current app. "
                        f"Missing columns: {missing}. Run ./scripts/migrate.sh."
                    )
        with _DB_ENGINE_CACHE_GUARD:
            _DB_SCHEMA_READY.add(key)

    _run_schema_bootstrap(data_dir, _apply)


def ensure_job_results_raw_schema(data_dir: str | Path) -> None:
    """Create the job-results-raw table when schema auto-creation is enabled."""
    if not _env_bool("DB_AUTO_CREATE_SCHEMA", True):
        return

    def _apply(url: str, engine: Engine) -> None:
        key = f"{url}#job_results_raw"
        with _DB_ENGINE_CACHE_GUARD:
            if key in _DB_SCHEMA_READY:
                return
        Base.metadata.create_all(engine, tables=[JobResultRawRecord.__table__])
        with _DB_ENGINE_CACHE_GUARD:
            _DB_SCHEMA_READY.add(key)

    _run_schema_bootstrap(data_dir, _apply)


def ensure_bank_code_flags_schema(data_dir: str | Path) -> None:
    """Create or repair the bank-code-flags table used by admin-managed bank mappings."""
    if not _env_bool("DB_AUTO_CREATE_SCHEMA", True):
        return

    def _apply(url: str, engine: Engine) -> None:
        key = f"{url}#bank_code_flags"
        with _DB_ENGINE_CACHE_GUARD:
            if key in _DB_SCHEMA_READY:
                return
        inspector = inspect(engine)
        if "bank_code_flags" in inspector.get_table_names():
            existing_columns = {column.get("name") for column in inspector.get_columns("bank_code_flags")}
            if "particulars" not in existing_columns:
                with engine.begin() as conn:
                    conn.execute(text("DROP TABLE bank_code_flags"))
        Base.metadata.create_all(engine, tables=[BankCodeFlagRecord.__table__])
        with _DB_ENGINE_CACHE_GUARD:
            _DB_SCHEMA_READY.add(key)

    _run_schema_bootstrap(data_dir, _apply)


def ensure_jobs_schema(data_dir: str | Path) -> None:
    """Create or repair the jobs table used for persisted UI-facing job metadata."""
    if not _env_bool("DB_AUTO_CREATE_SCHEMA", True):
        return

    expected_columns = {
        "job_id": 'ALTER TABLE jobs ADD COLUMN job_id VARCHAR(36)',
        "file_name": "ALTER TABLE jobs ADD COLUMN file_name VARCHAR(512) NOT NULL DEFAULT ''",
        "file_size": "ALTER TABLE jobs ADD COLUMN file_size BIGINT NOT NULL DEFAULT 0",
        "processing_status": "ALTER TABLE jobs ADD COLUMN processing_status VARCHAR(32) NOT NULL DEFAULT 'queued'",
        "process_started": "ALTER TABLE jobs ADD COLUMN process_started TIMESTAMPTZ NULL",
        "process_end": "ALTER TABLE jobs ADD COLUMN process_end TIMESTAMPTZ NULL",
        "is_reversed": "ALTER TABLE jobs ADD COLUMN is_reversed BOOLEAN NOT NULL DEFAULT FALSE",
    }

    def _apply(url: str, engine: Engine) -> None:
        key = f"{url}#jobs"
        with _DB_ENGINE_CACHE_GUARD:
            if key in _DB_SCHEMA_READY:
                return
        inspector = inspect(engine)
        if "jobs" not in inspector.get_table_names():
            Base.metadata.create_all(engine, tables=[JobRecord.__table__])
        else:
            existing_columns = {column.get("name") for column in inspector.get_columns("jobs")}
            with engine.begin() as conn:
                for column_name, ddl in expected_columns.items():
                    if column_name not in existing_columns:
                        conn.execute(text(ddl))
                if "job_id" in existing_columns:
                    pk = inspector.get_pk_constraint("jobs") or {}
                    constrained = pk.get("constrained_columns") or []
                    if "job_id" not in constrained:
                        conn.execute(text("ALTER TABLE jobs ADD PRIMARY KEY (job_id)"))
        with _DB_ENGINE_CACHE_GUARD:
            _DB_SCHEMA_READY.add(key)

    _run_schema_bootstrap(data_dir, _apply)


def _rebuild_legacy_job_transactions_schema(engine: Engine) -> None:
    """Rewrite the legacy job_transactions table into the current runtime shape."""
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS job_transactions_v2"))
        conn.execute(
            text(
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
        )
        conn.execute(
            text(
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
        )
        conn.execute(text("DROP INDEX IF EXISTS ix_job_transactions_job_page"))
        conn.execute(text("DROP INDEX IF EXISTS ix_job_transactions_job_id"))
        conn.execute(text("DROP TABLE job_transactions"))
        conn.execute(text("ALTER TABLE job_transactions_v2 RENAME TO job_transactions"))
        conn.execute(text("CREATE INDEX ix_job_transactions_job_id ON job_transactions (job_id)"))
        conn.execute(text("CREATE INDEX ix_job_transactions_job_page ON job_transactions (job_id, page_key)"))
        conn.execute(
            text(
                """
                ALTER TABLE job_transactions
                ADD CONSTRAINT uq_job_transactions_job_row_index UNIQUE (job_id, row_index)
                """
            )
        )


def _to_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return Decimal(str(value))
    text = str(value).strip()
    if not text:
        return None
    cleaned = text.replace(",", "")
    try:
        return Decimal(cleaned)
    except (InvalidOperation, ValueError):
        return None


def _json_default(value: Any) -> Any:
    """Normalize non-standard numeric types before JSON serialization."""
    if isinstance(value, Decimal):
        # Keep JSON numeric values while handling Decimal instances from parser diagnostics.
        return float(value)
    raise TypeError(f"Object of type {value.__class__.__name__} is not JSON serializable")


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, Decimal):
        return float(value)
    text = str(value).strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _prepare_bounds_state(bounds: list[dict[str, Any]] | None) -> dict[str, Any]:
    ordered: list[dict[str, Any]] = []
    by_row_id: dict[str, list[dict[str, Any]]] = {}
    for item in bounds or []:
        if not isinstance(item, dict):
            continue
        payload = dict(item)
        ordered.append(payload)
        row_id = str(payload.get("row_id") or "").strip()
        if row_id:
            by_row_id.setdefault(row_id, []).append(payload)
    return {"ordered": ordered, "by_row_id": by_row_id}


def _consume_bound(state: dict[str, Any], *, row_id: str, row_index: int) -> dict[str, Any]:
    by_row_id = state.get("by_row_id") if isinstance(state.get("by_row_id"), dict) else {}
    if row_id and isinstance(by_row_id.get(row_id), list) and by_row_id[row_id]:
        return dict(by_row_id[row_id].pop(0))
    ordered = state.get("ordered") if isinstance(state.get("ordered"), list) else []
    if 0 <= row_index - 1 < len(ordered):
        item = ordered[row_index - 1]
        if isinstance(item, dict):
            return dict(item)
    return {}


_JOB_TX_BOUND_FIELDS = (
    "row_number_bounds",
    "date_bounds",
    "debit_bounds",
    "credit_bounds",
    "balance_bounds",
)


def _page_sort_value(page_key: str) -> tuple[int, str]:
    text = str(page_key or "").strip()
    if text.isdigit():
        return int(text), text
    if text.startswith("page_") and text[5:].isdigit():
        value = str(int(text[5:]))
        return int(value), value
    return 0, text


def _output_page_key(page_key: str) -> str:
    text = str(page_key or "").strip()
    if text.isdigit():
        return f"page_{int(text):03}"
    if text.startswith("page_") and text[5:].isdigit():
        return f"page_{int(text[5:]):03}"
    return text


def _page_number_from_key(page_key: str) -> int:
    return _page_sort_value(page_key)[0]


def _normalize_bound_payload(value: Any) -> dict[str, float] | None:
    if not isinstance(value, dict):
        return None
    normalized: dict[str, float] = {}
    for key in ("x1", "y1", "x2", "y2"):
        number = _to_float(value.get(key))
        if number is None:
            return None
        normalized[key] = number
    return normalized


def _expand_bound_payload(bound: dict[str, Any]) -> dict[str, dict[str, float] | None]:
    expanded = {field: None for field in _JOB_TX_BOUND_FIELDS}
    legacy_row_bounds = _normalize_bound_payload(bound)
    for field in _JOB_TX_BOUND_FIELDS:
        expanded[field] = _normalize_bound_payload(bound.get(field)) or legacy_row_bounds
    return expanded


def _merge_bound_payloads(*bounds: Any) -> dict[str, float] | None:
    valid = [_normalize_bound_payload(item) for item in bounds]
    valid = [item for item in valid if item is not None]
    if not valid:
        return None
    return {
        "x1": min(item["x1"] for item in valid),
        "y1": min(item["y1"] for item in valid),
        "x2": max(item["x2"] for item in valid),
        "y2": max(item["y2"] for item in valid),
    }


def _row_core_signature(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        _normalize_row_number_value(row.get("row_number"), fallback=row.get("rownumber")),
        str(row.get("date") or ""),
        str(row.get("description") or ""),
        _to_decimal(row.get("debit")),
        _to_decimal(row.get("credit")),
        _to_decimal(row.get("balance")),
        str(row.get("row_type") or "transaction"),
    )


def _normalize_row_number_value(value: Any, fallback: Any = None) -> int | None:
    candidate = value if value is not None else fallback
    if candidate is None:
        return None
    if isinstance(candidate, int):
        return candidate
    text = str(candidate).strip()
    if not text:
        return None
    digits = "".join(ch for ch in text if ch.isdigit())
    if not digits:
        return None
    try:
        return int(digits)
    except ValueError:
        return None


class JobResultsRawRepository:
    """Store one raw extraction payload per job for audit/debug workflows."""

    def __init__(self, data_dir: str | Path):
        self.data_dir = Path(data_dir)
        ensure_jobs_schema(self.data_dir)
        ensure_job_results_raw_schema(self.data_dir)
        self.engine = _get_db_engine(self.data_dir)

    def upsert(
        self,
        *,
        job_id: str,
        is_ocr: bool,
        raw_xml: str | None = None,
        raw_json: dict[str, Any] | list[Any] | None = None,
    ) -> None:
        now = dt.datetime.now(dt.timezone.utc)
        with Session(self.engine) as session:
            record = session.execute(
                select(JobResultRawRecord).where(JobResultRawRecord.job_id == str(job_id)).limit(1)
            ).scalar_one_or_none()
            if record is None:
                record = JobResultRawRecord(
                    id=str(uuid.uuid4()),
                    job_id=str(job_id),
                    is_ocr=bool(is_ocr),
                    raw_xml=raw_xml,
                    raw_json=raw_json,
                    created_at=now,
                    updated_at=now,
                )
                session.add(record)
            else:
                record.is_ocr = bool(is_ocr)
                record.raw_xml = raw_xml
                record.raw_json = raw_json
                record.updated_at = now
            session.commit()

    def get_by_job_id(self, job_id: str) -> dict[str, Any] | None:
        with Session(self.engine) as session:
            record = session.execute(
                select(JobResultRawRecord).where(JobResultRawRecord.job_id == str(job_id)).limit(1)
            ).scalar_one_or_none()
            if record is None:
                return None
            return {
                "id": str(record.id),
                "job_id": str(record.job_id),
                "is_ocr": bool(record.is_ocr),
                "raw_xml": record.raw_xml,
                "raw_json": record.raw_json,
                "created_at": record.created_at.isoformat() if record.created_at else None,
                "updated_at": record.updated_at.isoformat() if record.updated_at else None,
            }

    def clear_all(self) -> int:
        with Session(self.engine) as session:
            count = session.execute(select(func.count()).select_from(JobResultRawRecord)).scalar_one()
            session.execute(delete(JobResultRawRecord))
            session.commit()
            return int(count or 0)


class JobTransactionsRepository:
    """Store normalized parsed rows and bounds in SQL for editing and reporting."""

    def __init__(self, data_dir: str | Path):
        self.data_dir = Path(data_dir)
        ensure_job_transactions_schema(self.data_dir)
        self.engine = _get_db_engine(self.data_dir)

    def has_rows(self, job_id: str) -> bool:
        with Session(self.engine) as session:
            stmt = (
                select(JobTransactionRecord.id)
                .where(
                    JobTransactionRecord.job_id == str(job_id),
                    JobTransactionRecord.is_deleted.is_(False),
                )
                .limit(1)
            )
            return session.execute(stmt).scalar_one_or_none() is not None

    def get_rows_by_job(self, job_id: str) -> Dict[str, list[dict[str, Any]]]:
        records = self._fetch_records(job_id, include_deleted=False)
        rows_by_page, _ = self._records_to_payload(records, include_deleted=False)
        return rows_by_page

    def get_bounds_by_job(self, job_id: str) -> Dict[str, list[dict[str, Any]]]:
        records = self._fetch_records(job_id, include_deleted=False)
        _, bounds_by_page = self._records_to_payload(records, include_deleted=False)
        return bounds_by_page

    def replace_job_rows(
        self,
        job_id: str,
        rows_by_page: Dict[str, list[dict[str, Any]]],
        *,
        bounds_by_page: Dict[str, list[dict[str, Any]]] | None = None,
        is_manual_edit: bool = False,
    ) -> None:
        payloads = self._build_payloads(
            job_id=str(job_id),
            rows_by_page=rows_by_page,
            bounds_by_page=bounds_by_page or {},
            is_manual_edit=is_manual_edit,
        )
        with Session(self.engine) as session:
            self._ensure_job_record(session, str(job_id))
            session.execute(delete(JobTransactionRecord).where(JobTransactionRecord.job_id == str(job_id)))
            session.add_all(JobTransactionRecord(**payload) for payload in payloads)
            session.commit()

    def replace_page_rows(
        self,
        job_id: str,
        page_key: str,
        rows: list[dict[str, Any]],
        *,
        is_manual_edit: bool = True,
    ) -> None:
        output_page_key = _output_page_key(page_key)
        existing_active_rows = self.get_rows_by_job(job_id)
        existing_active_bounds = self.get_bounds_by_job(job_id)
        existing_all_rows, existing_all_bounds = self._records_to_payload(
            self._fetch_records(job_id, include_deleted=True),
            include_deleted=True,
        )

        page_existing_rows = existing_active_rows.get(output_page_key, [])
        page_existing_bounds = existing_active_bounds.get(output_page_key, [])
        existing_by_row_id = {
            str(item.get("row_id") or "").strip(): item for item in page_existing_rows if str(item.get("row_id") or "").strip()
        }
        bounds_by_row_id = {
            str(item.get("row_id") or "").strip(): item for item in page_existing_bounds if str(item.get("row_id") or "").strip()
        }

        next_rows: list[dict[str, Any]] = []
        next_bounds: list[dict[str, Any]] = []
        seen_row_ids: set[str] = set()

        for idx, row in enumerate(rows or [], start=1):
            if not isinstance(row, dict):
                continue
            row_id = str(row.get("row_id") or "").strip() or f"{idx:03}"
            seen_row_ids.add(row_id)
            existing = existing_by_row_id.get(row_id)
            is_new_row = existing is None
            is_modified = bool(is_manual_edit) and (is_new_row or _row_core_signature(existing) != _row_core_signature(row))
            next_rows.append(
                {
                    "row_id": row_id,
                    "row_number": _normalize_row_number_value(row.get("row_number"), fallback=row.get("rownumber")),
                    "date": str(row.get("date") or ""),
                    "description": str(row.get("description") or ""),
                    "debit": row.get("debit"),
                    "credit": row.get("credit"),
                    "balance": row.get("balance"),
                    "row_type": str(row.get("row_type") or "transaction"),
                    "is_new_row": is_new_row,
                    "is_modified": is_modified,
                    "is_deleted": False,
                }
            )
            preserved_bounds = bounds_by_row_id.get(row_id) or {}
            next_bounds.append(dict(preserved_bounds))

        for existing in page_existing_rows:
            row_id = str(existing.get("row_id") or "").strip()
            if not row_id or row_id in seen_row_ids:
                continue
            next_rows.append(
                {
                    "row_id": row_id,
                    "row_number": _normalize_row_number_value(existing.get("row_number"), fallback=existing.get("rownumber")),
                    "date": str(existing.get("date") or ""),
                    "description": str(existing.get("description") or ""),
                    "debit": existing.get("debit"),
                    "credit": existing.get("credit"),
                    "balance": existing.get("balance"),
                    "row_type": str(existing.get("row_type") or "transaction"),
                    "is_new_row": bool(existing.get("is_new_row")),
                    "is_modified": True,
                    "is_deleted": True,
                }
            )
            next_bounds.append(dict(bounds_by_row_id.get(row_id) or {}))

        existing_all_rows[output_page_key] = next_rows
        existing_all_bounds[output_page_key] = next_bounds
        self.replace_job_rows(
            job_id=job_id,
            rows_by_page=existing_all_rows,
            bounds_by_page=existing_all_bounds,
            is_manual_edit=is_manual_edit,
        )

    def clear_all(self) -> int:
        with Session(self.engine) as session:
            count = session.execute(select(func.count()).select_from(JobTransactionRecord)).scalar_one()
            session.execute(delete(JobTransactionRecord))
            session.commit()
            return int(count or 0)

    def list_rows_paginated(
        self,
        *,
        page: int = 1,
        limit: int = 50,
        job_id: str | None = None,
        page_key: str | None = None,
        search: str | None = None,
    ) -> dict[str, Any]:
        safe_page = max(1, int(page))
        safe_limit = max(1, min(50, int(limit)))
        offset = (safe_page - 1) * safe_limit

        filters = []
        job_value = str(job_id or "").strip()
        if job_value:
            filters.append(JobTransactionRecord.job_id == job_value)

        page_value = str(page_key or "").strip()
        if page_value:
            filters.append(JobTransactionRecord.page_key == _output_page_key(page_value))

        search_value = str(search or "").strip().lower()
        if search_value:
            pattern = f"%{search_value}%"
            filters.append(
                or_(
                    func.lower(JobTransactionRecord.job_id).like(pattern),
                    func.lower(JobTransactionRecord.page_key).like(pattern),
                    cast(JobTransactionRecord.row_number, Text).like(pattern),
                    func.lower(JobTransactionRecord.date).like(pattern),
                    func.lower(JobTransactionRecord.description).like(pattern),
                )
            )

        with Session(self.engine) as session:
            count_stmt = select(func.count()).select_from(JobTransactionRecord)
            row_stmt = select(JobTransactionRecord).order_by(
                JobTransactionRecord.updated_at.desc(),
                JobTransactionRecord.job_id.desc(),
                JobTransactionRecord.page_key.asc(),
                JobTransactionRecord.row_index.asc(),
            )
            if filters:
                count_stmt = count_stmt.where(*filters)
                row_stmt = row_stmt.where(*filters)

            total_rows = int(session.execute(count_stmt).scalar_one() or 0)
            records = session.execute(row_stmt.offset(offset).limit(safe_limit)).scalars().all()

        total_pages = max(1, (total_rows + safe_limit - 1) // safe_limit) if total_rows else 1
        rows = [self._serialize_record(record) for record in records]
        return {
            "rows": rows,
            "pagination": {
                "page": safe_page,
                "per_page": safe_limit,
                "total_rows": total_rows,
                "total_pages": total_pages,
                "has_prev": safe_page > 1,
                "has_next": safe_page < total_pages,
            },
            "filters": {
                "job_id": job_value,
                "page_key": page_value,
                "q": search_value,
            },
        }

    def _build_payloads(
        self,
        *,
        job_id: str,
        rows_by_page: Dict[str, list[dict[str, Any]]],
        bounds_by_page: Dict[str, list[dict[str, Any]]],
        is_manual_edit: bool,
    ) -> list[dict[str, Any]]:
        now = dt.datetime.utcnow()
        payloads: list[dict[str, Any]] = []
        global_row_index = 0
        for page_key in sorted((rows_by_page or {}).keys(), key=_page_sort_value):
            rows = rows_by_page.get(page_key) or []
            bounds_state = _prepare_bounds_state(bounds_by_page.get(page_key))
            canonical_page_key = _output_page_key(page_key)
            for row_index, row in enumerate(rows, start=1):
                if not isinstance(row, dict):
                    continue
                row_id = str(row.get("row_id") or "").strip() or f"{row_index:03}"
                bound = _consume_bound(bounds_state, row_id=row_id, row_index=row_index)
                expanded_bounds = _expand_bound_payload(bound)
                global_row_index += 1
                payloads.append(
                    {
                        "id": str(uuid.uuid4()),
                        "job_id": job_id,
                        "page_key": canonical_page_key,
                        "page_number": _page_number_from_key(canonical_page_key),
                        "row_index": int(global_row_index),
                        "row_number": _normalize_row_number_value(row.get("row_number"), fallback=row.get("rownumber")),
                        "date": str(row.get("date") or ""),
                        "description": str(row.get("description") or ""),
                        "debit": _to_decimal(row.get("debit")),
                        "credit": _to_decimal(row.get("credit")),
                        "balance": _to_decimal(row.get("balance")),
                        "row_number_bounds": expanded_bounds["row_number_bounds"],
                        "date_bounds": expanded_bounds["date_bounds"],
                        "debit_bounds": expanded_bounds["debit_bounds"],
                        "credit_bounds": expanded_bounds["credit_bounds"],
                        "balance_bounds": expanded_bounds["balance_bounds"],
                        "row_type": str(row.get("row_type") or "transaction"),
                        "is_new_row": bool(row.get("is_new_row", False)),
                        "is_modified": bool(row.get("is_modified", is_manual_edit)),
                        "is_deleted": bool(row.get("is_deleted", False)),
                        "created_at": now,
                        "updated_at": now,
                    }
                )
        return payloads

    @staticmethod
    def _ensure_job_record(session: Session, job_id: str) -> None:
        record = session.get(JobRecord, str(job_id))
        if record is not None:
            return
        now = dt.datetime.now(dt.timezone.utc)
        session.add(
            JobRecord(
                job_id=str(job_id),
                file_name=f"{job_id}.pdf",
                file_size=0,
                processing_status="done",
                process_started=now,
                process_end=now,
                is_reversed=False,
            )
        )

    def _serialize_record(self, record: JobTransactionRecord) -> dict[str, Any]:
        updated_at = record.updated_at.isoformat() if record.updated_at else None
        merged_bounds = _merge_bound_payloads(
            record.row_number_bounds,
            record.date_bounds,
            record.debit_bounds,
            record.credit_bounds,
            record.balance_bounds,
        )
        return {
            "id": str(record.id),
            "job_id": str(record.job_id),
            "page_key": _output_page_key(record.page_key),
            "page_number": int(record.page_number),
            "row_index": int(record.row_index),
            "row_id": f"{int(record.row_index):03}",
            "rownumber": record.row_number,
            "row_number": str(record.row_number or ""),
            "date": str(record.date or ""),
            "description": str(record.description or ""),
            "debit": _to_float(record.debit),
            "credit": _to_float(record.credit),
            "balance": _to_float(record.balance),
            "row_type": str(record.row_type or "transaction"),
            "bounds": merged_bounds,
            "row_number_bounds": record.row_number_bounds,
            "date_bounds": record.date_bounds,
            "debit_bounds": record.debit_bounds,
            "credit_bounds": record.credit_bounds,
            "balance_bounds": record.balance_bounds,
            "is_new_row": bool(record.is_new_row),
            "is_modified": bool(record.is_modified),
            "is_deleted": bool(record.is_deleted),
            "updated_at": updated_at,
        }

    def _fetch_records(self, job_id: str, *, include_deleted: bool) -> list[JobTransactionRecord]:
        with Session(self.engine) as session:
            stmt = (
                select(JobTransactionRecord)
                .where(JobTransactionRecord.job_id == str(job_id))
                .order_by(
                    JobTransactionRecord.page_number.asc(),
                    JobTransactionRecord.row_index.asc(),
                    JobTransactionRecord.updated_at.asc(),
                )
            )
            if not include_deleted:
                stmt = stmt.where(JobTransactionRecord.is_deleted.is_(False))
            return session.execute(stmt).scalars().all()

    def _records_to_payload(
        self,
        records: list[JobTransactionRecord],
        *,
        include_deleted: bool,
    ) -> tuple[Dict[str, list[dict[str, Any]]], Dict[str, list[dict[str, Any]]]]:
        rows_by_page: Dict[str, list[dict[str, Any]]] = {}
        bounds_by_page: Dict[str, list[dict[str, Any]]] = {}
        page_counters: dict[str, int] = {}
        for record in records:
            if record.is_deleted and not include_deleted:
                continue
            page_key = _output_page_key(record.page_key)
            page_counters[page_key] = page_counters.get(page_key, 0) + 1
            row_id = f"{page_counters[page_key]:03}"
            row_payload = {
                "row_id": row_id,
                "rownumber": record.row_number,
                "row_number": str(record.row_number or ""),
                "date": str(record.date or ""),
                "description": str(record.description or ""),
                "debit": _to_float(record.debit),
                "credit": _to_float(record.credit),
                "balance": _to_float(record.balance),
                "row_type": str(record.row_type or "transaction"),
                "is_new_row": bool(record.is_new_row),
                "is_modified": bool(record.is_modified),
                "is_deleted": bool(record.is_deleted),
            }
            rows_by_page.setdefault(page_key, []).append(row_payload)

            merged_bounds = _merge_bound_payloads(
                record.row_number_bounds,
                record.date_bounds,
                record.debit_bounds,
                record.credit_bounds,
                record.balance_bounds,
            )
            if merged_bounds is None and not any(
                getattr(record, field) is not None for field in _JOB_TX_BOUND_FIELDS
            ):
                continue
            bounds_payload = {
                "row_id": row_id,
                "x1": merged_bounds["x1"] if merged_bounds else None,
                "y1": merged_bounds["y1"] if merged_bounds else None,
                "x2": merged_bounds["x2"] if merged_bounds else None,
                "y2": merged_bounds["y2"] if merged_bounds else None,
                "row_number_bounds": record.row_number_bounds,
                "date_bounds": record.date_bounds,
                "debit_bounds": record.debit_bounds,
                "credit_bounds": record.credit_bounds,
                "balance_bounds": record.balance_bounds,
            }
            bounds_by_page.setdefault(page_key, []).append(bounds_payload)
        return rows_by_page, bounds_by_page


def _parse_iso_datetime(value: Any) -> dt.datetime | None:
    text_value = str(value or "").strip()
    if not text_value:
        return None
    normalized = text_value.replace("Z", "+00:00")
    try:
        return dt.datetime.fromisoformat(normalized)
    except ValueError:
        return None


class JobStateRepository:
    """Persist lightweight job lifecycle metadata for the UI and reload behavior."""

    def __init__(self, data_dir: str | Path):
        self.data_dir = Path(data_dir)
        ensure_jobs_schema(self.data_dir)
        self.engine = _get_db_engine(self.data_dir)

    def sync_job(self, *, job_id: str, meta: dict[str, Any] | None, status: dict[str, Any] | None) -> None:
        payload_meta = dict(meta or {})
        payload_status = dict(status or {})
        file_name = str(payload_meta.get("original_filename") or payload_meta.get("file_name") or "").strip()
        if not file_name:
            return
        raw_file_size = payload_meta.get("file_size")
        try:
            file_size = max(0, int(raw_file_size or 0))
        except (TypeError, ValueError):
            file_size = 0
        processing_status = str(payload_status.get("status") or "queued").strip().lower() or "queued"
        is_reversed = bool(payload_meta.get("is_reversed", False))
        status_updated_at = _parse_iso_datetime(payload_status.get("updated_at"))
        explicit_cancelled_at = _parse_iso_datetime(payload_status.get("cancelled_at"))
        now_utc = dt.datetime.now(dt.timezone.utc)

        with Session(self.engine) as session:
            record = session.get(JobRecord, str(job_id))
            if record is None:
                record = JobRecord(
                    job_id=str(job_id),
                    file_name=file_name,
                    file_size=file_size,
                    processing_status=processing_status,
                    process_started=None,
                    process_end=None,
                    is_reversed=is_reversed,
                )
                session.add(record)
            else:
                record.file_name = file_name
                record.file_size = file_size
                record.processing_status = processing_status
                record.is_reversed = is_reversed

            if record.process_started is None and processing_status in {"queued", "processing", "done", "done_with_warnings", "failed", "cancelled"}:
                record.process_started = status_updated_at or now_utc
            if processing_status in {"done", "done_with_warnings", "failed", "cancelled"}:
                record.process_end = explicit_cancelled_at or status_updated_at or now_utc
            elif processing_status in {"queued", "processing"}:
                record.process_end = None
            session.commit()

    def set_reversed(self, *, job_id: str, is_reversed: bool) -> dict[str, Any]:
        with Session(self.engine) as session:
            record = session.get(JobRecord, str(job_id))
            if record is None:
                raise KeyError(job_id)
            record.is_reversed = bool(is_reversed)
            session.commit()
            return self.serialize(record)

    def get_job(self, job_id: str) -> dict[str, Any] | None:
        with Session(self.engine) as session:
            record = session.get(JobRecord, str(job_id))
            if record is None:
                return None
            return self.serialize(record)

    def clear_all(self) -> int:
        with Session(self.engine) as session:
            count = session.execute(select(func.count()).select_from(JobRecord)).scalar_one()
            session.execute(delete(JobRecord))
            session.commit()
            return int(count or 0)

    @staticmethod
    def serialize(record: JobRecord) -> dict[str, Any]:
        return {
            "job_id": str(record.job_id),
            "file_name": str(record.file_name or ""),
            "file_size": int(record.file_size or 0),
            "processing_status": str(record.processing_status or ""),
            "process_started": record.process_started.isoformat() if record.process_started else None,
            "process_end": record.process_end.isoformat() if record.process_end else None,
            "is_reversed": bool(record.is_reversed),
        }


class BankCodeFlagsRepository:
    """Persist the admin-maintained transaction-code lookup table in SQL."""

    def __init__(self, data_dir: str | Path):
        self.data_dir = Path(data_dir)
        ensure_bank_code_flags_schema(self.data_dir)
        self.engine = _get_db_engine(self.data_dir)

    def count(self) -> int:
        with Session(self.engine) as session:
            value = session.execute(select(func.count()).select_from(BankCodeFlagRecord)).scalar_one()
            return int(value or 0)

    def list_rows(self) -> list[dict[str, Any]]:
        with Session(self.engine) as session:
            stmt = select(BankCodeFlagRecord).order_by(
                BankCodeFlagRecord.bank_id.asc(),
                BankCodeFlagRecord.tx_code.asc(),
                BankCodeFlagRecord.particulars.asc(),
            )
            records = session.execute(stmt).scalars().all()
        return [self._serialize_record(record) for record in records]

    def replace_all(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        payloads = self._build_payloads(rows)
        with Session(self.engine) as session:
            session.execute(delete(BankCodeFlagRecord))
            session.add_all(BankCodeFlagRecord(**payload) for payload in payloads)
            session.commit()
        return self.list_rows()

    def seed_rows(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        existing_rows = self.list_rows()
        if existing_rows:
            has_numeric_bank_ids = all(str(item.get("bank_id") or "").strip().isdigit() for item in existing_rows)
            # Large numeric-ID datasets are treated as auto-seeded data, not manual edits.
            # If they do not exactly match the current workbook seed, replace them so stale
            # partially-migrated rows (for example blank particulars or missing entries)
            # self-heal on the next settings load.
            if rows and has_numeric_bank_ids and len(existing_rows) >= 100:
                existing_signature = {
                    (
                        str(item.get("bank_id") or "").strip().upper(),
                        str(item.get("bank_name") or "").strip().upper(),
                        str(item.get("tx_code") or "").strip().upper(),
                        str(item.get("particulars") or "").strip(),
                    )
                    for item in existing_rows
                    if str(item.get("bank_id") or "").strip()
                    and str(item.get("bank_name") or "").strip()
                    and str(item.get("tx_code") or "").strip()
                }
                seed_signature = {
                    (
                        str(item.get("bank_id") or "").strip().upper(),
                        str(item.get("bank_name") or "").strip().upper(),
                        str(item.get("tx_code") or "").strip().upper(),
                        str(item.get("particulars") or "").strip(),
                    )
                    for item in rows
                    if str(item.get("bank_id") or "").strip()
                    and str(item.get("bank_name") or "").strip()
                    and str(item.get("tx_code") or "").strip()
                }
                if existing_signature != seed_signature:
                    return self.replace_all(rows)
            return existing_rows
        return self.replace_all(rows)

    def _build_payloads(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        now = dt.datetime.utcnow()
        payloads: list[dict[str, Any]] = []
        seen: set[tuple[str, str, str]] = set()
        for item in rows or []:
            if not isinstance(item, dict):
                continue
            bank_id = str(item.get("bank_id") or "").strip().upper()
            bank_name = str(item.get("bank_name") or "").strip().upper()
            tx_code = str(item.get("tx_code") or "").strip().upper()
            particulars = str(item.get("particulars") or "").strip()
            if not bank_id or not bank_name or not tx_code:
                continue
            key = (bank_id, tx_code, particulars)
            if key in seen:
                continue
            seen.add(key)
            payloads.append(
                {
                    "bank_id": bank_id,
                    "bank_name": bank_name,
                    "tx_code": tx_code,
                    "particulars": particulars,
                    "created_at": now,
                    "updated_at": now,
                }
            )
        return payloads

    def _serialize_record(self, record: BankCodeFlagRecord) -> dict[str, Any]:
        return {
            "bank_id": str(record.bank_id or ""),
            "bank_name": str(record.bank_name or ""),
            "tx_code": str(record.tx_code or ""),
            "particulars": str(record.particulars or ""),
        }


__all__ = [
    "BankCodeFlagsRepository",
    "JobResultsRawRepository",
    "JobTransactionsRepository",
    "JobsRepository",
    "ensure_bank_code_flags_schema",
    "ensure_job_results_raw_schema",
    "ensure_job_transactions_schema",
]
