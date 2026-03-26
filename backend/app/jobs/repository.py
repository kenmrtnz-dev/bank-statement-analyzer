"""Persistence helpers for job files plus SQL-backed row and bank-code storage."""

from __future__ import annotations

import datetime as dt
import json
import logging
import os
import re
import tempfile
import threading
import time
import uuid
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Callable, Dict

from sqlalchemy import Text, cast, create_engine, delete, func, inspect, or_, select, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, joinedload
from sqlalchemy.exc import OperationalError

from app.infra.db.models import BankCodeFlagRecord, Base, JobPageRecord, JobRecord, JobResultRawRecord, TransactionRecord
from app.parser.validation import balance_validator
from app.json_utils import json_default, make_json_safe
from app.settings import load_settings

_DB_ENGINE_CACHE: dict[str, Engine] = {}
_DB_ENGINE_CACHE_GUARD = threading.Lock()
_DB_SCHEMA_READY: set[str] = set()
logger = logging.getLogger(__name__)
_JOB_PAGE_TYPES = {"digital", "scanned"}
_JOB_PAGE_PROCESSING_STATUSES = {"pending", "processing", "done", "failed"}
_JOB_ACTIVE_STATUSES = {"queued", "splitting", "processing", "parsing"}
_JOB_TERMINAL_SUCCESS_STATUSES = {"done", "done_with_warnings", "completed"}
_JOB_TERMINAL_STATUSES = _JOB_TERMINAL_SUCCESS_STATUSES | {"failed", "cancelled"}


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
        safe_payload = make_json_safe(payload)
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            json.dump(safe_payload, handle, indent=2, default=json_default, allow_nan=False)
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


def ensure_job_pages_schema(data_dir: str | Path) -> None:
    """Create the job_pages table when schema auto-creation is enabled."""
    if not _env_bool("DB_AUTO_CREATE_SCHEMA", True):
        return
    ensure_jobs_schema(data_dir)

    def _apply(url: str, engine: Engine) -> None:
        key = f"{url}#job_pages"
        with _DB_ENGINE_CACHE_GUARD:
            if key in _DB_SCHEMA_READY:
                return
        inspector = inspect(engine)
        if "job_pages" not in inspector.get_table_names():
            Base.metadata.create_all(engine, tables=[JobPageRecord.__table__])
        else:
            existing_columns = {column.get("name") for column in inspector.get_columns("job_pages")}
            with engine.begin() as conn:
                if "ocr_result" in existing_columns and "raw_result" not in existing_columns:
                    conn.execute(text("ALTER TABLE job_pages RENAME COLUMN ocr_result TO raw_result"))
                    existing_columns.discard("ocr_result")
                    existing_columns.add("raw_result")
                if "page_type" not in existing_columns:
                    conn.execute(text("ALTER TABLE job_pages ADD COLUMN page_type VARCHAR(16) NOT NULL DEFAULT 'digital'"))
                    existing_columns.add("page_type")
                if "raw_text" not in existing_columns:
                    conn.execute(text("ALTER TABLE job_pages ADD COLUMN raw_text TEXT NULL"))
                    existing_columns.add("raw_text")
                if "processing_status" not in existing_columns:
                    conn.execute(
                        text("ALTER TABLE job_pages ADD COLUMN processing_status VARCHAR(32) NOT NULL DEFAULT 'pending'")
                    )
                    existing_columns.add("processing_status")
            if "notes" not in existing_columns:
                with engine.begin() as conn:
                    conn.execute(text("ALTER TABLE job_pages ADD COLUMN notes TEXT NULL"))
            if "raw_result" not in existing_columns:
                with engine.begin() as conn:
                    conn.execute(text("ALTER TABLE job_pages ADD COLUMN raw_result JSONB NULL"))
            with engine.begin() as conn:
                conn.execute(
                    text(
                        """
                        UPDATE job_pages
                        SET page_type = CASE
                            WHEN COALESCE(is_digital, FALSE) THEN 'digital'
                            ELSE 'scanned'
                        END
                        """
                    )
                )
                conn.execute(
                    text(
                        """
                        UPDATE job_pages
                        SET raw_text = NULLIF(raw_result ->> 'text', '')
                        WHERE raw_text IS NULL
                        """
                    )
                )
                conn.execute(
                    text(
                        """
                        UPDATE job_pages
                        SET processing_status = CASE
                            WHEN raw_result IS NOT NULL OR raw_text IS NOT NULL THEN 'done'
                            ELSE COALESCE(NULLIF(processing_status, ''), 'pending')
                        END
                        """
                    )
                )
        with _DB_ENGINE_CACHE_GUARD:
            _DB_SCHEMA_READY.add(key)

    _run_schema_bootstrap(data_dir, _apply)


def ensure_transactions_schema(data_dir: str | Path) -> None:
    """Create the transactions table when schema auto-creation is enabled."""
    if not _env_bool("DB_AUTO_CREATE_SCHEMA", True):
        return
    ensure_jobs_schema(data_dir)
    ensure_job_pages_schema(data_dir)

    def _apply(url: str, engine: Engine) -> None:
        key = f"{url}#transactions"
        with _DB_ENGINE_CACHE_GUARD:
            if key in _DB_SCHEMA_READY:
                return
        Base.metadata.create_all(engine, tables=[TransactionRecord.__table__])
        with _DB_ENGINE_CACHE_GUARD:
            _DB_SCHEMA_READY.add(key)

    _run_schema_bootstrap(data_dir, _apply)


def ensure_job_transactions_schema(data_dir: str | Path) -> None:
    """Backward-compatible alias for the renamed transactions table bootstrap."""
    ensure_transactions_schema(data_dir)


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
        "id": 'ALTER TABLE jobs ADD COLUMN id VARCHAR(36)',
        "file_name": "ALTER TABLE jobs ADD COLUMN file_name VARCHAR(512) NOT NULL DEFAULT ''",
        "file_size": "ALTER TABLE jobs ADD COLUMN file_size BIGINT NOT NULL DEFAULT 0",
        "job_status": "ALTER TABLE jobs ADD COLUMN job_status VARCHAR(32) NOT NULL DEFAULT 'queued'",
        "started_at": "ALTER TABLE jobs ADD COLUMN started_at TIMESTAMPTZ NULL",
        "ended_at": "ALTER TABLE jobs ADD COLUMN ended_at TIMESTAMPTZ NULL",
        "pages": "ALTER TABLE jobs ADD COLUMN pages INTEGER NOT NULL DEFAULT 0",
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
                if "job_id" in existing_columns and "id" in expected_columns:
                    conn.execute(text("UPDATE jobs SET id = COALESCE(id, job_id) WHERE id IS NULL"))
                if "processing_status" in existing_columns and "job_status" in expected_columns:
                    conn.execute(text("UPDATE jobs SET job_status = COALESCE(NULLIF(job_status, ''), processing_status)"))
                if "process_started" in existing_columns and "started_at" in expected_columns:
                    conn.execute(text("UPDATE jobs SET started_at = COALESCE(started_at, process_started)"))
                if "process_end" in existing_columns and "ended_at" in expected_columns:
                    conn.execute(text("UPDATE jobs SET ended_at = COALESCE(ended_at, process_end)"))
                pk = inspector.get_pk_constraint("jobs") or {}
                constrained = pk.get("constrained_columns") or []
                if "id" in existing_columns and "id" not in constrained:
                    conn.execute(text("ALTER TABLE jobs ADD PRIMARY KEY (id)"))
        with _DB_ENGINE_CACHE_GUARD:
            _DB_SCHEMA_READY.add(key)

    _run_schema_bootstrap(data_dir, _apply)


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


def _normalize_job_page_type(value: Any, *, is_digital: Any | None = None) -> str:
    text_value = str(value or "").strip().lower()
    if text_value in _JOB_PAGE_TYPES:
        return text_value
    if is_digital is not None:
        return "digital" if bool(is_digital) else "scanned"
    return "digital"


def _normalize_job_page_processing_status(value: Any, *, fallback: str = "pending") -> str:
    text_value = str(value or "").strip().lower()
    if text_value in _JOB_PAGE_PROCESSING_STATUSES:
        return text_value
    fallback_value = str(fallback or "").strip().lower()
    if fallback_value in _JOB_PAGE_PROCESSING_STATUSES:
        return fallback_value
    return "pending"


def _normalize_job_page_raw_text(value: Any) -> str | None:
    text_value = str(value or "").strip()
    return text_value or None


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
    "description_bounds",
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


def _page_key_from_number(page_number: int) -> str:
    value = max(0, int(page_number or 0))
    return f"page_{value:03}"


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


def _normalize_bank_flag_code(value: Any) -> str:
    return re.sub(r"[^A-Z0-9_-]", "", str(value or "").strip().upper())


def _description_contains_flag_code(description: Any, code: str) -> bool:
    text = str(description or "").upper()
    token = _normalize_bank_flag_code(code)
    if not text or not token:
        return False
    pattern = re.compile(rf"(^|[^A-Z0-9]){re.escape(token)}([^A-Z0-9]|$)")
    return bool(pattern.search(text))


def _compute_is_flagged(description: Any, codes: set[str]) -> bool:
    if not codes:
        return False
    return any(_description_contains_flag_code(description, code) for code in codes)


def _compute_disbalance_fields_by_page(
    rows_by_page: dict[str, list[dict[str, Any]]],
    *,
    is_reversed: bool,
) -> dict[str, list[dict[str, Any]]]:
    prepared_rows_by_page: dict[str, list[dict[str, Any]]] = {}
    ordered_rows: list[dict[str, Any]] = []
    ordered_targets: list[tuple[str, int]] = []

    for page_key, rows in (rows_by_page or {}).items():
        page_rows: list[dict[str, Any]] = []
        for row in rows or []:
            payload = dict(row or {})
            payload["is_disbalanced"] = False
            payload["disbalance_expected_balance"] = None
            payload["disbalance_delta"] = None
            page_rows.append(payload)
        prepared_rows_by_page[page_key] = page_rows

    page_keys = sorted(prepared_rows_by_page.keys(), key=_page_sort_value, reverse=bool(is_reversed))
    for page_key in page_keys:
        page_number = _page_number_from_key(page_key)
        page_rows = prepared_rows_by_page.get(page_key) or []
        row_positions = range(len(page_rows) - 1, -1, -1) if is_reversed else range(len(page_rows))
        for position in row_positions:
            row = page_rows[position]
            ordered_rows.append(
                {
                    "page_number": page_number,
                    "row_index": position + 1,
                    "row_number": _normalize_row_number_value(row.get("row_number"), fallback=row.get("rownumber")),
                    "debit": row.get("debit"),
                    "credit": row.get("credit"),
                    "balance": row.get("balance"),
                }
            )
            ordered_targets.append((page_key, position))

    computed_rows = balance_validator.evaluate_ordered_balance_rows(ordered_rows, balance_logger=logger)
    for (page_key, position), computed in zip(ordered_targets, computed_rows):
        payload = prepared_rows_by_page[page_key][position]
        payload["is_disbalanced"] = bool(computed.get("is_disbalanced", False))
        if computed.get("is_disbalanced"):
            payload["disbalance_expected_balance"] = _to_float(
                computed.get("expected_balance").quantize(balance_validator.BALANCE_TOLERANCE)
            )
            payload["disbalance_delta"] = _to_float(
                computed.get("difference").quantize(balance_validator.BALANCE_TOLERANCE)
            )

    return prepared_rows_by_page


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
        ensure_transactions_schema(self.data_dir)
        ensure_bank_code_flags_schema(self.data_dir)
        self.engine = _get_db_engine(self.data_dir)

    def has_rows(self, job_id: str) -> bool:
        with Session(self.engine) as session:
            stmt = (
                select(TransactionRecord.id)
                .where(
                    TransactionRecord.job_id == str(job_id),
                    TransactionRecord.is_deleted.is_(False),
                )
                .limit(1)
            )
            return session.execute(stmt).scalar_one_or_none() is not None

    def _load_job_reversal_map(self, job_ids: list[str] | set[str] | tuple[str, ...]) -> dict[str, bool]:
        normalized_ids = sorted({str(job_id).strip() for job_id in (job_ids or []) if str(job_id).strip()})
        if not normalized_ids:
            return {}
        with Session(self.engine) as session:
            stmt = select(JobRecord.id, JobRecord.is_reversed).where(JobRecord.id.in_(normalized_ids))
            rows = session.execute(stmt).all()
        reversal_map = {job_id: False for job_id in normalized_ids}
        for job_id, is_reversed in rows:
            reversal_map[str(job_id)] = bool(is_reversed)
        return reversal_map

    def _get_job_is_reversed(self, job_id: str) -> bool:
        return bool(self._load_job_reversal_map([str(job_id)]).get(str(job_id), False))

    def get_rows_by_job(self, job_id: str) -> Dict[str, list[dict[str, Any]]]:
        records = self._fetch_records(job_id, include_deleted=False)
        rows_by_page, _ = self._records_to_payload(
            records,
            include_deleted=False,
            is_reversed=self._get_job_is_reversed(job_id),
        )
        return rows_by_page

    def get_bounds_by_job(self, job_id: str) -> Dict[str, list[dict[str, Any]]]:
        records = self._fetch_records(job_id, include_deleted=False)
        _, bounds_by_page = self._records_to_payload(
            records,
            include_deleted=False,
            is_reversed=self._get_job_is_reversed(job_id),
        )
        return bounds_by_page

    def replace_job_rows(
        self,
        job_id: str,
        rows_by_page: Dict[str, list[dict[str, Any]]],
        *,
        bounds_by_page: Dict[str, list[dict[str, Any]]] | None = None,
        page_metadata_by_page: Dict[str, dict[str, Any]] | None = None,
        is_manual_edit: bool = False,
    ) -> None:
        is_reversed = self._get_job_is_reversed(job_id)
        page_payloads, tx_payloads = self._build_payloads(
            job_id=str(job_id),
            rows_by_page=rows_by_page,
            bounds_by_page=bounds_by_page or {},
            page_metadata_by_page=page_metadata_by_page or {},
            is_manual_edit=is_manual_edit,
            is_reversed=is_reversed,
        )
        with Session(self.engine) as session:
            self._ensure_job_record(session, str(job_id))
            record = session.get(JobRecord, str(job_id))
            session.execute(delete(TransactionRecord).where(TransactionRecord.job_id == str(job_id)))
            session.execute(delete(JobPageRecord).where(JobPageRecord.job_id == str(job_id)))
            session.flush()
            session.add_all(JobPageRecord(**payload) for payload in page_payloads)
            session.add_all(TransactionRecord(**payload) for payload in tx_payloads)
            if record is not None:
                record.pages = len(page_payloads)
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
        is_reversed = self._get_job_is_reversed(job_id)
        existing_active_rows = self.get_rows_by_job(job_id)
        existing_active_bounds = self.get_bounds_by_job(job_id)
        existing_page_metadata = self._load_page_metadata_by_job(job_id)
        existing_all_rows, existing_all_bounds = self._records_to_payload(
            self._fetch_records(job_id, include_deleted=True),
            include_deleted=True,
            is_reversed=is_reversed,
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
            page_metadata_by_page=existing_page_metadata,
            is_manual_edit=is_manual_edit,
        )

    def clear_all(self) -> int:
        with Session(self.engine) as session:
            count = session.execute(select(func.count()).select_from(TransactionRecord)).scalar_one()
            session.execute(delete(TransactionRecord))
            session.execute(delete(JobPageRecord))
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
            filters.append(TransactionRecord.job_id == job_value)

        page_value = str(page_key or "").strip()
        if page_value:
            filters.append(JobPageRecord.page_number == _page_number_from_key(page_value))

        search_value = str(search or "").strip().lower()
        if search_value:
            pattern = f"%{search_value}%"
            filters.append(
                or_(
                    func.lower(TransactionRecord.job_id).like(pattern),
                    cast(JobPageRecord.page_number, Text).like(pattern),
                    cast(TransactionRecord.row_number, Text).like(pattern),
                    func.lower(TransactionRecord.date).like(pattern),
                    func.lower(TransactionRecord.description).like(pattern),
                )
            )

        with Session(self.engine) as session:
            count_stmt = select(func.count()).select_from(TransactionRecord).join(
                JobPageRecord, TransactionRecord.page_id == JobPageRecord.id
            )
            row_stmt = (
                select(TransactionRecord)
                .join(JobPageRecord, TransactionRecord.page_id == JobPageRecord.id)
                .options(joinedload(TransactionRecord.page))
                .order_by(
                    TransactionRecord.updated_at.desc(),
                    TransactionRecord.job_id.desc(),
                    JobPageRecord.page_number.asc(),
                    TransactionRecord.row_index.asc(),
                )
            )
            if filters:
                count_stmt = count_stmt.where(*filters)
                row_stmt = row_stmt.where(*filters)

            total_rows = int(session.execute(count_stmt).scalar_one() or 0)
            records = session.execute(row_stmt.offset(offset).limit(safe_limit)).scalars().all()

        total_pages = max(1, (total_rows + safe_limit - 1) // safe_limit) if total_rows else 1
        extras_by_id = self._compute_record_extras(records)
        rows = [self._serialize_record(record, extras_by_id.get(str(record.id), {})) for record in records]
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
        page_metadata_by_page: Dict[str, dict[str, Any]],
        is_manual_edit: bool,
        is_reversed: bool,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        now = dt.datetime.now(dt.timezone.utc)
        page_payloads: list[dict[str, Any]] = []
        tx_payloads: list[dict[str, Any]] = []
        global_row_index = 0
        canonical_page_metadata: dict[str, dict[str, Any]] = {}
        computed_rows_by_page = _compute_disbalance_fields_by_page(rows_by_page, is_reversed=is_reversed)
        for key, value in (page_metadata_by_page or {}).items():
            canonical_page_metadata[_output_page_key(key)] = dict(value or {})
        existing_page_metadata = self._load_page_metadata_by_job(job_id)

        page_keys = set(canonical_page_metadata)
        page_keys.update(_output_page_key(key) for key in (rows_by_page or {}).keys())
        page_keys.update(existing_page_metadata.keys())

        for page_key in sorted(page_keys, key=_page_sort_value):
            rows = computed_rows_by_page.get(page_key) or []
            bounds_state = _prepare_bounds_state(bounds_by_page.get(page_key))
            metadata = dict(existing_page_metadata.get(page_key) or {})
            supplied_metadata = canonical_page_metadata.get(page_key) or {}
            for meta_key, meta_value in supplied_metadata.items():
                if meta_value is not None or meta_key not in metadata:
                    metadata[meta_key] = meta_value
            page_number = _page_number_from_key(page_key)
            page_id = str(metadata.get("id") or uuid.uuid4())
            page_type = _normalize_job_page_type(metadata.get("page_type"), is_digital=metadata.get("is_digital"))
            raw_result = metadata.get("raw_result") if isinstance(metadata.get("raw_result"), (dict, list)) else None
            raw_text = _normalize_job_page_raw_text(metadata.get("raw_text"))
            supplied_processing_status = str(metadata.get("processing_status") or "").strip().lower()
            processing_status = (
                supplied_processing_status
                if supplied_processing_status in _JOB_PAGE_PROCESSING_STATUSES
                else ("done" if (rows or raw_result or raw_text) else "pending")
            )
            page_payloads.append(
                {
                    "id": page_id,
                    "job_id": str(job_id),
                    "page_number": page_number,
                    "page_type": page_type,
                    "raw_text": raw_text,
                    "processing_status": processing_status,
                    "is_digital": page_type == "digital",
                    "raw_result": raw_result,
                    "notes": str(metadata.get("notes")) if metadata.get("notes") is not None else None,
                }
            )
            for row_index, row in enumerate(rows, start=1):
                if not isinstance(row, dict):
                    continue
                row_id = str(row.get("row_id") or "").strip() or f"{row_index:03}"
                bound = _consume_bound(bounds_state, row_id=row_id, row_index=row_index)
                expanded_bounds = _expand_bound_payload(bound)
                global_row_index += 1
                tx_payloads.append(
                    {
                        "id": str(uuid.uuid4()),
                        "job_id": str(job_id),
                        "page_id": page_id,
                        "row_index": int(global_row_index),
                        "row_number": _normalize_row_number_value(row.get("row_number"), fallback=row.get("rownumber")),
                        "date": str(row.get("date") or ""),
                        "description": str(row.get("description") or ""),
                        "debit": _to_decimal(row.get("debit")),
                        "credit": _to_decimal(row.get("credit")),
                        "balance": _to_decimal(row.get("balance")),
                        "row_number_bounds": expanded_bounds["row_number_bounds"],
                        "date_bounds": expanded_bounds["date_bounds"],
                        "description_bounds": expanded_bounds["description_bounds"],
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
        return page_payloads, tx_payloads

    @staticmethod
    def _ensure_job_record(session: Session, job_id: str) -> None:
        record = session.get(JobRecord, str(job_id))
        if record is not None:
            return
        now = dt.datetime.now(dt.timezone.utc)
        session.add(
            JobRecord(
                id=str(job_id),
                file_name=f"{job_id}.pdf",
                file_size=0,
                job_status="done",
                started_at=now,
                ended_at=now,
                pages=0,
                is_reversed=False,
            )
        )

    def _serialize_record(self, record: TransactionRecord, extras: dict[str, Any]) -> dict[str, Any]:
        updated_at = record.updated_at.isoformat() if record.updated_at else None
        merged_bounds = _merge_bound_payloads(
            record.row_number_bounds,
            record.date_bounds,
            record.description_bounds,
            record.debit_bounds,
            record.credit_bounds,
            record.balance_bounds,
        )
        page_number = int(extras.get("page_number") or (record.page.page_number if record.page else 0))
        return {
            "id": str(record.id),
            "job_id": str(record.job_id),
            "page_key": _page_key_from_number(page_number),
            "page_number": page_number,
            "row_index": int(record.row_index),
            "row_id": str(extras.get("row_id") or f"{int(record.row_index):03}"),
            "rownumber": record.row_number,
            "row_number": str(record.row_number or ""),
            "date": str(record.date or ""),
            "description": str(record.description or ""),
            "debit": _to_float(record.debit),
            "credit": _to_float(record.credit),
            "balance": _to_float(record.balance),
            "row_type": str(record.row_type or "transaction"),
            "is_flagged": bool(extras.get("is_flagged", False)),
            "is_disbalanced": bool(extras.get("is_disbalanced", False)),
            "disbalance_expected_balance": _to_float(extras.get("disbalance_expected_balance")),
            "disbalance_delta": _to_float(extras.get("disbalance_delta")),
            "bounds": merged_bounds,
            "row_number_bounds": record.row_number_bounds,
            "date_bounds": record.date_bounds,
            "description_bounds": record.description_bounds,
            "debit_bounds": record.debit_bounds,
            "credit_bounds": record.credit_bounds,
            "balance_bounds": record.balance_bounds,
            "is_new_row": bool(record.is_new_row),
            "is_modified": bool(record.is_modified),
            "is_deleted": bool(record.is_deleted),
            "updated_at": updated_at,
        }

    def _fetch_records(self, job_id: str, *, include_deleted: bool) -> list[TransactionRecord]:
        with Session(self.engine) as session:
            stmt = (
                select(TransactionRecord)
                .join(JobPageRecord, TransactionRecord.page_id == JobPageRecord.id)
                .options(joinedload(TransactionRecord.page))
                .where(TransactionRecord.job_id == str(job_id))
                .order_by(
                    JobPageRecord.page_number.asc(),
                    TransactionRecord.row_index.asc(),
                    TransactionRecord.updated_at.asc(),
                )
            )
            if not include_deleted:
                stmt = stmt.where(TransactionRecord.is_deleted.is_(False))
            return session.execute(stmt).scalars().all()

    def _records_to_payload(
        self,
        records: list[TransactionRecord],
        *,
        include_deleted: bool,
        is_reversed: bool,
    ) -> tuple[Dict[str, list[dict[str, Any]]], Dict[str, list[dict[str, Any]]]]:
        raw_rows_by_page: Dict[str, list[dict[str, Any]]] = {}
        bounds_by_page: Dict[str, list[dict[str, Any]]] = {}
        page_counters: dict[str, int] = {}
        for record in records:
            if record.is_deleted and not include_deleted:
                continue
            page_number = int(record.page.page_number if record.page else 0)
            page_key = _page_key_from_number(page_number)
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
            raw_rows_by_page.setdefault(page_key, []).append(row_payload)

            merged_bounds = _merge_bound_payloads(
                record.row_number_bounds,
                record.date_bounds,
                record.description_bounds,
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
                "description_bounds": record.description_bounds,
                "debit_bounds": record.debit_bounds,
                "credit_bounds": record.credit_bounds,
                "balance_bounds": record.balance_bounds,
            }
            bounds_by_page.setdefault(page_key, []).append(bounds_payload)
        enriched_rows_by_page = _compute_disbalance_fields_by_page(raw_rows_by_page, is_reversed=is_reversed)
        flag_codes = self._load_flag_codes()
        for rows in enriched_rows_by_page.values():
            for row in rows:
                row["is_flagged"] = _compute_is_flagged(row.get("description"), flag_codes)
        return enriched_rows_by_page, bounds_by_page

    def _load_flag_codes(self) -> set[str]:
        with Session(self.engine) as session:
            stmt = select(BankCodeFlagRecord.tx_code).distinct()
            rows = session.execute(stmt).scalars().all()
        return {_normalize_bank_flag_code(value) for value in rows if _normalize_bank_flag_code(value)}

    @staticmethod
    def _serialize_page_metadata(record: JobPageRecord) -> dict[str, Any]:
        page_type = _normalize_job_page_type(getattr(record, "page_type", None), is_digital=getattr(record, "is_digital", None))
        raw_text = _normalize_job_page_raw_text(getattr(record, "raw_text", None))
        processing_status = _normalize_job_page_processing_status(
            getattr(record, "processing_status", None),
            fallback="done" if (getattr(record, "raw_result", None) is not None or raw_text) else "pending",
        )
        return {
            "id": str(record.id),
            "page_number": int(record.page_number),
            "page_type": page_type,
            "raw_text": raw_text,
            "processing_status": processing_status,
            "is_digital": page_type == "digital",
            "raw_result": record.raw_result,
            "notes": record.notes,
        }

    def _load_page_metadata_by_job(self, job_id: str) -> dict[str, dict[str, Any]]:
        with Session(self.engine) as session:
            stmt = select(JobPageRecord).where(JobPageRecord.job_id == str(job_id)).order_by(JobPageRecord.page_number.asc())
            records = session.execute(stmt).scalars().all()
        return {
            _page_key_from_number(int(record.page_number)): self._serialize_page_metadata(record)
            for record in records
        }

    def get_page_metadata_by_job(self, job_id: str) -> dict[str, dict[str, Any]]:
        return self._load_page_metadata_by_job(job_id)

    def upsert_page_metadata(
        self,
        job_id: str,
        page_number: int,
        *,
        page_type: str | None = None,
        is_digital: bool | None = None,
        raw_text: str | None = None,
        processing_status: str | None = None,
        raw_result: dict[str, Any] | list[Any] | None = None,
        notes: str | None = None,
    ) -> dict[str, Any]:
        page_number = int(page_number)
        if page_number <= 0:
            raise KeyError(page_number)

        page_type_supplied = page_type is not None or is_digital is not None
        normalized_page_type = _normalize_job_page_type(page_type, is_digital=is_digital) if page_type_supplied else None
        normalized_raw_text = _normalize_job_page_raw_text(raw_text) if raw_text is not None else None
        normalized_processing_status = (
            _normalize_job_page_processing_status(processing_status) if processing_status is not None else None
        )
        note_value = None
        if notes is not None:
            note_value = str(notes or "").strip() or None

        with Session(self.engine) as session:
            self._ensure_job_record(session, str(job_id))
            stmt = (
                select(JobPageRecord)
                .where(
                    JobPageRecord.job_id == str(job_id),
                    JobPageRecord.page_number == int(page_number),
                )
                .limit(1)
            )
            record = session.execute(stmt).scalar_one_or_none()
            if record is None:
                record = JobPageRecord(
                    id=str(uuid.uuid4()),
                    job_id=str(job_id),
                    page_number=int(page_number),
                    page_type=normalized_page_type or "digital",
                    raw_text=normalized_raw_text,
                    processing_status=normalized_processing_status or "pending",
                    is_digital=(normalized_page_type or "digital") == "digital",
                    raw_result=raw_result if raw_result is not None else None,
                    notes=note_value,
                )
                session.add(record)
            else:
                if page_type_supplied:
                    record.page_type = normalized_page_type or "digital"
                    record.is_digital = record.page_type == "digital"
                if raw_text is not None:
                    record.raw_text = normalized_raw_text
                if processing_status is not None:
                    record.processing_status = normalized_processing_status or "pending"
                if raw_result is not None:
                    record.raw_result = raw_result
                if notes is not None:
                    record.notes = note_value
                if not page_type_supplied:
                    record.is_digital = _normalize_job_page_type(getattr(record, "page_type", None), is_digital=record.is_digital) == "digital"
            session.commit()
            return self._serialize_page_metadata(record)

    def update_page_intake_fields(
        self,
        job_id: str,
        page_number: int,
        *,
        page_type: str | None = None,
        is_digital: bool | None = None,
        raw_text: str | None = None,
        processing_status: str | None = None,
        raw_result: dict[str, Any] | list[Any] | None = None,
    ) -> dict[str, Any]:
        page_number = int(page_number)
        if page_number <= 0:
            raise KeyError(page_number)

        page_type_supplied = page_type is not None or is_digital is not None
        normalized_page_type = _normalize_job_page_type(page_type, is_digital=is_digital) if page_type_supplied else None
        normalized_raw_text = _normalize_job_page_raw_text(raw_text) if raw_text is not None else None
        normalized_processing_status = (
            _normalize_job_page_processing_status(processing_status) if processing_status is not None else None
        )

        with Session(self.engine) as session:
            stmt = (
                select(JobPageRecord)
                .where(
                    JobPageRecord.job_id == str(job_id),
                    JobPageRecord.page_number == int(page_number),
                )
                .limit(1)
            )
            record = session.execute(stmt).scalar_one_or_none()
            if record is None:
                raise KeyError(f"{job_id}:{page_number}")

            if page_type_supplied:
                record.page_type = normalized_page_type or "digital"
                record.is_digital = record.page_type == "digital"
            if raw_text is not None:
                record.raw_text = normalized_raw_text
            if processing_status is not None:
                record.processing_status = normalized_processing_status or "pending"
            if raw_result is not None:
                record.raw_result = raw_result
            if not page_type_supplied:
                record.is_digital = _normalize_job_page_type(getattr(record, "page_type", None), is_digital=record.is_digital) == "digital"

            session.commit()
            return self._serialize_page_metadata(record)

    def get_page_notes(self, job_id: str, page_key: str) -> str | None:
        page_number = _page_number_from_key(page_key)
        with Session(self.engine) as session:
            stmt = (
                select(JobPageRecord.notes)
                .where(
                    JobPageRecord.job_id == str(job_id),
                    JobPageRecord.page_number == int(page_number),
                )
                .limit(1)
            )
            value = session.execute(stmt).scalar_one_or_none()
        if value is None:
            return None
        text_value = str(value).strip()
        return text_value or None

    def update_page_notes(self, job_id: str, page_key: str, notes: str | None) -> dict[str, Any]:
        page_number = _page_number_from_key(page_key)
        if page_number <= 0:
            raise KeyError(page_key)
        note_value = str(notes or "").strip() or None
        with Session(self.engine) as session:
            self._ensure_job_record(session, str(job_id))
            stmt = (
                select(JobPageRecord)
                .where(
                    JobPageRecord.job_id == str(job_id),
                    JobPageRecord.page_number == int(page_number),
                )
                .limit(1)
            )
            record = session.execute(stmt).scalar_one_or_none()
            if record is None:
                record = JobPageRecord(
                    id=str(uuid.uuid4()),
                    job_id=str(job_id),
                    page_number=int(page_number),
                    page_type="scanned",
                    raw_text=None,
                    processing_status="pending",
                    is_digital=False,
                    raw_result=None,
                    notes=note_value,
                )
                session.add(record)
            else:
                record.notes = note_value
            session.commit()
            return {
                "page": _page_key_from_number(int(page_number)),
                "page_number": int(page_number),
                "notes": record.notes,
            }

    def _compute_record_extras(self, records: list[TransactionRecord]) -> dict[str, dict[str, Any]]:
        extras_by_id: dict[str, dict[str, Any]] = {}
        if not records:
            return extras_by_id
        flag_codes = self._load_flag_codes()
        reversal_map = self._load_job_reversal_map({str(record.job_id) for record in records})

        for job_id in sorted({str(record.job_id) for record in records}):
            full_records = self._fetch_records(job_id, include_deleted=True)
            grouped: dict[str, list[tuple[TransactionRecord, dict[str, Any]]]] = {}
            page_counters: dict[str, int] = {}
            for record in full_records:
                page_number = int(record.page.page_number if record.page else 0)
                page_key = _page_key_from_number(page_number)
                page_counters[page_key] = page_counters.get(page_key, 0) + 1
                row_payload = {
                    "row_id": f"{page_counters[page_key]:03}",
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
                grouped.setdefault(page_key, []).append((record, row_payload))

            computed_rows_by_page = _compute_disbalance_fields_by_page(
                {page_key: [payload for _, payload in items] for page_key, items in grouped.items()},
                is_reversed=reversal_map.get(job_id, False),
            )
            for page_key, items in grouped.items():
                page_number = _page_number_from_key(page_key)
                computed_rows = computed_rows_by_page.get(page_key) or []
                for (record, row_payload), computed in zip(items, computed_rows):
                    extras_by_id[str(record.id)] = {
                        "page_number": page_number,
                        "row_id": row_payload["row_id"],
                        "is_flagged": _compute_is_flagged(computed.get("description"), flag_codes),
                        "is_disbalanced": bool(computed.get("is_disbalanced", False)),
                        "disbalance_expected_balance": computed.get("disbalance_expected_balance"),
                        "disbalance_delta": computed.get("disbalance_delta"),
                    }
        return extras_by_id


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
        job_status = str(payload_status.get("status") or "queued").strip().lower() or "queued"
        raw_pages = payload_status.get("pages_total")
        if raw_pages is None:
            raw_pages = payload_status.get("pages")
        try:
            page_count = max(0, int(raw_pages or 0))
        except (TypeError, ValueError):
            page_count = 0
        is_reversed = bool(payload_meta.get("is_reversed", False))
        status_updated_at = _parse_iso_datetime(payload_status.get("updated_at"))
        explicit_cancelled_at = _parse_iso_datetime(payload_status.get("cancelled_at"))
        now_utc = dt.datetime.now(dt.timezone.utc)

        with Session(self.engine) as session:
            record = session.get(JobRecord, str(job_id))
            if record is None:
                record = JobRecord(
                    id=str(job_id),
                    file_name=file_name,
                    file_size=file_size,
                    job_status=job_status,
                    started_at=None,
                    ended_at=None,
                    pages=page_count,
                    is_reversed=is_reversed,
                )
                session.add(record)
            else:
                record.file_name = file_name
                record.file_size = file_size
                record.job_status = job_status
                record.pages = page_count
                record.is_reversed = is_reversed

            if record.started_at is None and job_status in (_JOB_ACTIVE_STATUSES | _JOB_TERMINAL_STATUSES):
                record.started_at = status_updated_at or now_utc
            if job_status in _JOB_TERMINAL_STATUSES:
                record.ended_at = explicit_cancelled_at or status_updated_at or now_utc
            elif job_status in _JOB_ACTIVE_STATUSES:
                record.ended_at = None
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
            "id": str(record.id),
            "job_id": str(record.id),
            "file_name": str(record.file_name or ""),
            "file_size": int(record.file_size or 0),
            "job_status": str(record.job_status or ""),
            "processing_status": str(record.job_status or ""),
            "started_at": record.started_at.isoformat() if record.started_at else None,
            "ended_at": record.ended_at.isoformat() if record.ended_at else None,
            "process_started": record.started_at.isoformat() if record.started_at else None,
            "process_end": record.ended_at.isoformat() if record.ended_at else None,
            "pages": int(record.pages or 0),
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
    "ensure_job_pages_schema",
    "ensure_job_results_raw_schema",
    "ensure_job_transactions_schema",
    "ensure_jobs_schema",
    "ensure_transactions_schema",
]
