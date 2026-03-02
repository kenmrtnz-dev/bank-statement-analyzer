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

from sqlalchemy import create_engine, delete, func, inspect, or_, select, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session
from sqlalchemy.exc import OperationalError

from app.infra.db.models import BankCodeFlagRecord, Base, JobTransactionRecord

_DB_ENGINE_CACHE: dict[str, Engine] = {}
_DB_ENGINE_CACHE_GUARD = threading.Lock()
_DB_SCHEMA_READY: set[str] = set()
logger = logging.getLogger(__name__)

class JobsRepository:
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
        path.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            json.dump(payload, handle, indent=2)
            tmp = Path(handle.name)
        os.replace(tmp, path)

    def read_json(self, path: Path, default: Any):
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
    configured = str(os.getenv("DATABASE_URL") or "").strip()
    if configured:
        return configured
    db_path = Path(data_dir) / "ocr.db"
    return f"sqlite:///{db_path.resolve()}"


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
    url = _resolve_database_url(data_dir)
    engine = _get_db_engine(data_dir)
    if url.startswith("sqlite"):
        operation(url, engine)
        return

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
    url = _resolve_database_url(data_dir)
    with _DB_ENGINE_CACHE_GUARD:
        engine = _DB_ENGINE_CACHE.get(url)
        if engine is not None:
            return engine
        connect_args = {"check_same_thread": False} if url.startswith("sqlite") else {}
        engine = create_engine(url, future=True, connect_args=connect_args)
        _DB_ENGINE_CACHE[url] = engine
        return engine


def ensure_job_transactions_schema(data_dir: str | Path) -> None:
    if not _env_bool("DB_AUTO_CREATE_SCHEMA", True):
        return

    def _apply(url: str, engine: Engine) -> None:
        with _DB_ENGINE_CACHE_GUARD:
            if url in _DB_SCHEMA_READY:
                return
        Base.metadata.create_all(engine, tables=[JobTransactionRecord.__table__])
        with _DB_ENGINE_CACHE_GUARD:
            _DB_SCHEMA_READY.add(url)

    _run_schema_bootstrap(data_dir, _apply)


def ensure_bank_code_flags_schema(data_dir: str | Path) -> None:
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


class JobTransactionsRepository:
    def __init__(self, data_dir: str | Path):
        self.data_dir = Path(data_dir)
        ensure_job_transactions_schema(self.data_dir)
        self.engine = _get_db_engine(self.data_dir)

    def has_rows(self, job_id: str) -> bool:
        with Session(self.engine) as session:
            stmt = select(JobTransactionRecord.id).where(JobTransactionRecord.job_id == str(job_id)).limit(1)
            return session.execute(stmt).scalar_one_or_none() is not None

    def get_rows_by_job(self, job_id: str) -> Dict[str, list[dict[str, Any]]]:
        with Session(self.engine) as session:
            stmt = (
                select(JobTransactionRecord)
                .where(JobTransactionRecord.job_id == str(job_id))
                .order_by(JobTransactionRecord.page_key.asc(), JobTransactionRecord.row_index.asc())
            )
            records = session.execute(stmt).scalars().all()

        out: Dict[str, list[dict[str, Any]]] = {}
        for record in records:
            page_rows = out.setdefault(str(record.page_key), [])
            page_rows.append(
                {
                    "row_id": str(record.row_id or ""),
                    "rownumber": record.rownumber,
                    "row_number": str(record.row_number or (record.rownumber or "") or ""),
                    "date": str(record.date or ""),
                    "description": str(record.description or ""),
                    "debit": _to_float(record.debit),
                    "credit": _to_float(record.credit),
                    "balance": _to_float(record.balance),
                    "row_type": str(record.row_type or "transaction"),
                }
            )
        return out

    def get_bounds_by_job(self, job_id: str) -> Dict[str, list[dict[str, Any]]]:
        with Session(self.engine) as session:
            stmt = (
                select(JobTransactionRecord)
                .where(JobTransactionRecord.job_id == str(job_id))
                .order_by(JobTransactionRecord.page_key.asc(), JobTransactionRecord.row_index.asc())
            )
            records = session.execute(stmt).scalars().all()

        out: Dict[str, list[dict[str, Any]]] = {}
        for record in records:
            if all(value is None for value in (record.x1, record.y1, record.x2, record.y2)):
                continue
            page_bounds = out.setdefault(str(record.page_key), [])
            page_bounds.append(
                {
                    "row_id": str(record.row_id or ""),
                    "x1": _to_float(record.x1),
                    "y1": _to_float(record.y1),
                    "x2": _to_float(record.x2),
                    "y2": _to_float(record.y2),
                }
            )
        return out

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
        normalized_page = str(page_key)
        existing_bounds = self.get_bounds_by_job(job_id).get(normalized_page, [])
        payloads = self._build_payloads(
            job_id=str(job_id),
            rows_by_page={normalized_page: rows},
            bounds_by_page={normalized_page: existing_bounds},
            is_manual_edit=is_manual_edit,
        )
        with Session(self.engine) as session:
            session.execute(
                delete(JobTransactionRecord).where(
                    JobTransactionRecord.job_id == str(job_id),
                    JobTransactionRecord.page_key == normalized_page,
                )
            )
            session.add_all(JobTransactionRecord(**payload) for payload in payloads)
            session.commit()

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
            filters.append(JobTransactionRecord.page_key == page_value)

        search_value = str(search or "").strip().lower()
        if search_value:
            pattern = f"%{search_value}%"
            filters.append(
                or_(
                    func.lower(JobTransactionRecord.job_id).like(pattern),
                    func.lower(JobTransactionRecord.page_key).like(pattern),
                    func.lower(JobTransactionRecord.row_id).like(pattern),
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
        for page_key in sorted((rows_by_page or {}).keys()):
            rows = rows_by_page.get(page_key) or []
            bounds_state = _prepare_bounds_state(bounds_by_page.get(page_key))
            for row_index, row in enumerate(rows, start=1):
                if not isinstance(row, dict):
                    continue
                row_id = str(row.get("row_id") or "").strip() or f"{row_index:03}"
                bound = _consume_bound(bounds_state, row_id=row_id, row_index=row_index)
                payloads.append(
                    {
                        "id": str(uuid.uuid4()),
                        "job_id": job_id,
                        "page_key": str(page_key),
                        "row_index": int(row_index),
                        "row_id": row_id,
                        "rownumber": row.get("rownumber"),
                        "row_number": str(row.get("row_number") or ""),
                        "date": str(row.get("date") or ""),
                        "description": str(row.get("description") or ""),
                        "debit": _to_decimal(row.get("debit")),
                        "credit": _to_decimal(row.get("credit")),
                        "balance": _to_decimal(row.get("balance")),
                        "row_type": str(row.get("row_type") or "transaction"),
                        "x1": _to_decimal(bound.get("x1")),
                        "y1": _to_decimal(bound.get("y1")),
                        "x2": _to_decimal(bound.get("x2")),
                        "y2": _to_decimal(bound.get("y2")),
                        "is_manual_edit": bool(is_manual_edit),
                        "created_at": now,
                        "updated_at": now,
                    }
                )
        return payloads

    def _serialize_record(self, record: JobTransactionRecord) -> dict[str, Any]:
        updated_at = record.updated_at.isoformat() if record.updated_at else None
        return {
            "id": str(record.id),
            "job_id": str(record.job_id),
            "page_key": str(record.page_key),
            "row_index": int(record.row_index),
            "row_id": str(record.row_id or ""),
            "rownumber": record.rownumber,
            "row_number": str(record.row_number or (record.rownumber or "") or ""),
            "date": str(record.date or ""),
            "description": str(record.description or ""),
            "debit": _to_float(record.debit),
            "credit": _to_float(record.credit),
            "balance": _to_float(record.balance),
            "row_type": str(record.row_type or "transaction"),
            "bounds": {
                "x1": _to_float(record.x1),
                "y1": _to_float(record.y1),
                "x2": _to_float(record.x2),
                "y2": _to_float(record.y2),
            },
            "is_manual_edit": bool(record.is_manual_edit),
            "updated_at": updated_at,
        }


class BankCodeFlagsRepository:
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
    "JobTransactionsRepository",
    "JobsRepository",
    "ensure_bank_code_flags_schema",
    "ensure_job_transactions_schema",
]
