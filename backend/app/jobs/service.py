"""Core job orchestration for uploads, parsing, status tracking, and exports."""

import calendar
import datetime as dt
import fcntl
import io
import json
import logging
import math
import os
import shutil
import threading
import uuid
import zipfile
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, List, Optional
from xml.sax.saxutils import escape as xml_escape

from fastapi import HTTPException
from pdf2image import convert_from_path
from PIL import Image
from pypdf import PdfReader, PdfWriter

from app.bank_profiles import detect_bank_profile
from app.json_utils import make_json_safe
from app.jobs.repository import JobResultsRawRepository, JobStateRepository, JobsRepository, JobTransactionsRepository
from app.ocr import prepare_ocr_pages, process_ocr_page, resolve_parse_mode
from app.ocr.pipeline import (
    _filter_rows_and_bounds,
    _image_size,
    _last_row_balance,
    _last_row_date,
    _ocr_items_to_words,
    _repair_page_flow_columns,
    resolve_ocr_page_path,
)
from app.paths import get_data_dir, get_legacy_volume_storage_dir, get_volume_storage_dir
from app.pdf_text_extract import extract_pdf_layout_pages, layout_page_to_json_payload
from app.services.ocr.router import build_scanned_ocr_router, scanned_render_dpi
from app.services.openai_page_fix import (
    OpenAIPageFixError,
    OpenAIPageFixNotConfiguredError,
    is_openai_page_fix_available,
    repair_page_rows_with_openai,
)
from app.statement_parser import normalize_date, parse_page_with_profile_fallback

DATA_DIR = get_data_dir()
VOLUME_STORAGE_ROOT = get_volume_storage_dir()
LEGACY_VOLUME_STORAGE_ROOT = get_legacy_volume_storage_dir()
logger = logging.getLogger(__name__)
FALLBACK_PREVIEW_DPI = int(os.getenv("FALLBACK_PREVIEW_DPI", "130"))
PREVIEW_MAX_PIXELS = int(os.getenv("PREVIEW_MAX_PIXELS", "6000000"))
_ACTIVE_CELERY_STATES = {"PENDING", "RECEIVED", "STARTED", "RETRY"}
_PAGE_TERMINAL_STATES = {"done", "failed", "cancelled"}
_PAGE_ACTIVE_STATES = {"pending", "processing", "retrying"}
_VOLUME_AUTO_ADVANCE_TERMINAL_STATES = {"completed"}
_JOB_UPDATE_LOCKS: Dict[str, threading.Lock] = {}
_JOB_UPDATE_LOCKS_GUARD = threading.Lock()
_SUPPORTED_PARSE_MODES = {"auto", "text", "ocr", "google_vision", "pdftotext"}
_SUPPORTED_GOOGLE_VISION_PARSERS = {"auto", "sterling_bank_of_asia", "bdo", "generic"}
_SUPPORTED_ROW_TYPES = {"transaction", "balance_only", "opening_balance", "closing_balance"}


def _normalize_export_business_name(raw: str | None) -> str:
    text = str(raw or "").strip().upper()
    if not text:
        return "UNKNOWNBUSINESS"
    normalized = "".join(ch for ch in text if ch.isalnum())
    return normalized or "UNKNOWNBUSINESS"


def _resolve_export_business_name(job_id: str) -> str:
    repo = JobsRepository(DATA_DIR)
    meta = repo.read_json(repo.path(job_id, "meta.json"), default={})
    if not isinstance(meta, dict):
        meta = {}
    candidates = [
        meta.get("source_account_name"),
        meta.get("account_name"),
    ]
    for candidate in candidates:
        normalized = _normalize_export_business_name(str(candidate or "").strip())
        if normalized != "UNKNOWNBUSINESS":
            return normalized
    return "UNKNOWNBUSINESS"


def _build_export_filename(job_id: str, ext: str) -> str:
    ts = dt.datetime.now().strftime("%m%d%Y%H%M")
    business = _resolve_export_business_name(job_id)
    clean_ext = str(ext or "").lower().lstrip(".")
    return f"{ts}-{business}-6MOS-BANKSTATEMENTS.{clean_ext}"


def _utcnow_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _resolve_volume_set_dir(set_name: str) -> Path:
    target_dir = VOLUME_STORAGE_ROOT / set_name
    if target_dir.exists():
        return target_dir

    legacy_dir = LEGACY_VOLUME_STORAGE_ROOT / set_name
    if not legacy_dir.exists() or not legacy_dir.is_dir():
        return target_dir

    target_dir.parent.mkdir(parents=True, exist_ok=True)
    try:
        shutil.move(str(legacy_dir), str(target_dir))
        return target_dir
    except Exception:
        if target_dir.exists():
            return target_dir
        return legacy_dir


def _read_volume_set_meta(set_dir: Path) -> dict[str, Any]:
    payload = {
        "set_name": set_dir.name,
        "uploader_username": "",
        "uploader_role": "",
        "created_at": "",
        "updated_at": "",
        "files": {},
    }
    meta_path = set_dir / ".volume-set.json"
    if not meta_path.exists():
        return payload
    raw = JobsRepository(DATA_DIR).read_json(meta_path, default={})
    if not isinstance(raw, dict):
        return payload
    payload.update(raw)
    files_payload = payload.get("files")
    payload["files"] = files_payload if isinstance(files_payload, dict) else {}
    payload["uploader_username"] = str(payload.get("uploader_username") or payload.get("uploaded_by") or "").strip()
    payload["uploader_role"] = str(payload.get("uploader_role") or "").strip().lower()
    return payload


def _write_volume_set_meta(set_dir: Path, payload: dict[str, Any]) -> None:
    safe_payload = dict(payload or {})
    safe_payload["set_name"] = set_dir.name
    files_payload = safe_payload.get("files")
    safe_payload["files"] = files_payload if isinstance(files_payload, dict) else {}
    JobsRepository(DATA_DIR).write_json(set_dir / ".volume-set.json", safe_payload)


@contextmanager
def _lock_volume_set(set_dir: Path):
    lock_path = set_dir / ".volume-set.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with open(lock_path, "a+", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _visible_volume_set_files(set_dir: Path) -> list[Path]:
    if not set_dir.exists() or not set_dir.is_dir():
        return []
    return sorted(
        [entry for entry in set_dir.iterdir() if entry.is_file() and not entry.name.startswith(".")],
        key=lambda entry: entry.name.lower(),
    )


def _volume_file_last_job_id(file_payload: dict[str, Any]) -> str:
    last_job_id = str(file_payload.get("last_job_id") or "").strip()
    if last_job_id:
        return last_job_id
    job_ids = file_payload.get("job_ids")
    if not isinstance(job_ids, list):
        return ""
    for candidate in reversed(job_ids):
        cleaned = str(candidate or "").strip()
        if cleaned:
            return cleaned
    return ""


def _raw_volume_job_status(repo: JobsRepository, job_id: str) -> str:
    cleaned_job_id = str(job_id or "").strip()
    if not cleaned_job_id:
        return ""
    status_payload = repo.read_status(cleaned_job_id)
    return str((status_payload or {}).get("status") or "").strip().lower()


def _update_job_meta_for_volume(
    *,
    repo: JobsRepository,
    job_id: str,
    set_name: str,
    file_name: str,
    owner_username: str,
    owner_role: str,
    started_by: str,
) -> None:
    existing_meta = repo.read_json(repo.path(job_id, "meta.json"), default={})
    if not isinstance(existing_meta, dict):
        existing_meta = {}
    existing_meta["source_tag"] = "VT"
    existing_meta["source_category"] = "volume_test"
    existing_meta["volume_set_name"] = set_name
    existing_meta["volume_file_name"] = file_name
    if owner_username:
        existing_meta["uploaded_by"] = owner_username
        existing_meta.setdefault("created_by", owner_username)
    if owner_role:
        existing_meta.setdefault("created_role", owner_role)
    if started_by:
        existing_meta["volume_started_by"] = started_by
    repo.write_json(repo.path(job_id, "meta.json"), existing_meta)


def _maybe_start_next_volume_file(job_id: str, *, terminal_status: str) -> dict[str, str] | None:
    normalized_terminal_status = str(terminal_status or "").strip().lower()
    if normalized_terminal_status not in _VOLUME_AUTO_ADVANCE_TERMINAL_STATES:
        return None

    repo = JobsRepository(DATA_DIR)
    meta = repo.read_json(repo.path(job_id, "meta.json"), default={})
    if not isinstance(meta, dict):
        return None
    if str(meta.get("source_category") or "").strip().lower() != "volume_test":
        return None

    set_name = str(meta.get("volume_set_name") or "").strip()
    file_name = Path(str(meta.get("volume_file_name") or "").strip()).name.strip()
    if not set_name or not file_name:
        return None

    set_dir = _resolve_volume_set_dir(set_name)
    if not set_dir.exists() or not set_dir.is_dir():
        return None

    with _lock_volume_set(set_dir):
        set_meta = _read_volume_set_meta(set_dir)
        files_meta = set_meta.get("files")
        if not isinstance(files_meta, dict):
            files_meta = {}
        current_payload = files_meta.get(file_name)
        current_state = dict(current_payload) if isinstance(current_payload, dict) else {}
        if _volume_file_last_job_id(current_state) != str(job_id).strip():
            return None

        has_active_job = False
        next_file_name = ""
        for entry in _visible_volume_set_files(set_dir):
            is_pdf = entry.suffix.lower() == ".pdf"
            file_state = files_meta.get(entry.name)
            payload = dict(file_state) if isinstance(file_state, dict) else {}
            last_job_id = _volume_file_last_job_id(payload)
            raw_status = _raw_volume_job_status(repo, last_job_id)
            if is_pdf and raw_status in {"queued", "splitting", "processing", "parsing"}:
                has_active_job = True
                break
            if not next_file_name and is_pdf and not last_job_id:
                next_file_name = entry.name

        if has_active_job or not next_file_name:
            return None

        next_path = set_dir / next_file_name
        owner_username = str(set_meta.get("uploader_username") or meta.get("uploaded_by") or meta.get("created_by") or "").strip()
        owner_role = str(set_meta.get("uploader_role") or meta.get("created_role") or "").strip().lower()
        started_by = str(meta.get("volume_started_by") or "").strip() or "system"
        create_payload = create_job(
            file_bytes=next_path.read_bytes(),
            filename=next_file_name,
            requested_mode="auto",
            requested_parser="auto",
            auto_start=True,
            created_by=owner_username,
            created_role=owner_role,
        )
        next_job_id = str(create_payload.get("job_id") or "").strip()
        if not next_job_id:
            return None

        _update_job_meta_for_volume(
            repo=repo,
            job_id=next_job_id,
            set_name=set_name,
            file_name=next_file_name,
            owner_username=owner_username,
            owner_role=owner_role,
            started_by=started_by,
        )

        next_file_state = files_meta.get(next_file_name)
        next_payload = dict(next_file_state) if isinstance(next_file_state, dict) else {}
        job_ids = next_payload.get("job_ids")
        job_id_list = [str(item or "").strip() for item in job_ids] if isinstance(job_ids, list) else []
        if next_job_id not in job_id_list:
            job_id_list.append(next_job_id)
        next_payload["job_ids"] = [item for item in job_id_list if item]
        next_payload["last_job_id"] = next_job_id
        next_payload["last_started_at"] = _utcnow_iso()
        next_payload["last_started_by"] = started_by
        next_payload["last_started_for"] = owner_username
        files_meta[next_file_name] = next_payload
        set_meta["files"] = files_meta
        if owner_username and not str(set_meta.get("uploader_username") or "").strip():
            set_meta["uploader_username"] = owner_username
        if owner_role and not str(set_meta.get("uploader_role") or "").strip():
            set_meta["uploader_role"] = owner_role
        set_meta["updated_at"] = _utcnow_iso()
        if not str(set_meta.get("created_at") or "").strip():
            set_meta["created_at"] = set_meta["updated_at"]
        _write_volume_set_meta(set_dir, set_meta)

    logger.info("Auto-started next VT file %s/%s as job %s after %s completed.", set_name, next_file_name, next_job_id, job_id)
    return {"job_id": next_job_id, "file_name": next_file_name}


def normalize_page_name(page: str) -> str:
    """Convert page tokens into the canonical `page_###` name used on disk."""
    value = str(page or "").strip()
    for suffix in (".png", ".pdf"):
        if value.lower().endswith(suffix):
            value = value[: -len(suffix)]
    if not value:
        return ""
    if value.startswith("page_"):
        token = value.replace("page_", "")
        if token.isdigit():
            return f"page_{int(token):03}"
        return value
    if value.isdigit():
        return f"page_{int(value):03}"
    return f"page_{value}"


def _build_page_file_names(page_count: int) -> List[str]:
    total = max(0, int(page_count))
    return [f"page_{idx:03}.png" for idx in range(1, total + 1)]


def _build_page_names(page_count: int) -> List[str]:
    total = max(0, int(page_count))
    return [f"page_{idx:03}" for idx in range(1, total + 1)]


def _split_pages_dir(repo: JobsRepository, job_id: str) -> Path:
    return repo.path(job_id, "split")


def _split_page_pdf_path(repo: JobsRepository, job_id: str, page_name: str) -> Path:
    return _split_pages_dir(repo, job_id) / f"{normalize_page_name(page_name)}.pdf"


def _rendered_page_png_path(repo: JobsRepository, job_id: str, page_name: str) -> Path:
    return repo.path(job_id, "pages", f"{normalize_page_name(page_name)}.png")


def _split_pdf_into_page_pdfs(
    *,
    repo: JobsRepository,
    job_id: str,
    input_pdf: Path,
) -> List[str]:
    split_dir = _split_pages_dir(repo, job_id)
    split_dir.mkdir(parents=True, exist_ok=True)

    reader = PdfReader(str(input_pdf))
    page_names: List[str] = []
    expected_files: set[str] = set()

    for idx, page in enumerate(reader.pages, start=1):
        page_name = f"page_{idx:03}"
        output_path = split_dir / f"{page_name}.pdf"
        writer = PdfWriter()
        writer.add_page(page)
        with open(output_path, "wb") as handle:
            writer.write(handle)
        page_names.append(page_name)
        expected_files.add(output_path.name)

    for entry in split_dir.iterdir():
        if entry.is_file() and entry.suffix.lower() == ".pdf" and entry.name not in expected_files:
            entry.unlink(missing_ok=True)

    return page_names


def _page_number_from_page_file(page_file: str) -> int | None:
    page_name = normalize_page_name(page_file)
    token = page_name.replace("page_", "", 1)
    if not token.isdigit():
        return None
    page_num = int(token)
    if page_num <= 0:
        return None
    return page_num


def _read_pdf_page_count(input_pdf: Path) -> int:
    try:
        reader = PdfReader(str(input_pdf))
    except Exception:
        return 0
    return max(0, len(reader.pages or []))


def _save_png_with_pixel_cap(page: Image.Image, output_path: Path, *, max_pixels: int = PREVIEW_MAX_PIXELS) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    w, h = page.size
    pixels = max(1, w * h)
    cap = max(1, int(max_pixels))
    if pixels > cap:
        scale = math.sqrt(cap / float(pixels))
        page = page.resize(
            (max(1, int(w * scale)), max(1, int(h * scale))),
            resample=Image.Resampling.BILINEAR,
        )
    page.save(output_path, format="PNG")


def _ocr_page_numbers_for_routing(page_files: List[str], page_routing: Dict[str, str]) -> List[int]:
    page_numbers: List[int] = []
    for page_file in page_files:
        page_name = normalize_page_name(str(page_file).replace(".png", ""))
        if str(page_routing.get(page_name) or "ocr").strip().lower() == "text":
            continue
        page_num = _page_number_from_page_file(page_file)
        if page_num is not None:
            page_numbers.append(page_num)
    return page_numbers


def _render_selected_ocr_pages(
    *,
    input_pdf: Path,
    pages_dir: Path,
    page_numbers: List[int],
    dpi: int,
) -> List[str]:
    pages_dir.mkdir(parents=True, exist_ok=True)
    rendered_files: List[str] = []
    for page_num in sorted({int(num) for num in page_numbers if int(num) > 0}):
        filename = f"page_{page_num:03}.png"
        output_path = pages_dir / filename
        if output_path.exists():
            rendered_files.append(filename)
            continue
        pages = convert_from_path(
            str(input_pdf),
            dpi=max(72, int(dpi)),
            fmt="png",
            first_page=page_num,
            last_page=page_num,
        )
        if not pages:
            continue
        _save_png_with_pixel_cap(pages[0], output_path)
        rendered_files.append(filename)
    return rendered_files


def _normalize_requested_mode(requested_mode: str | None, *, field_name: str = "mode") -> str:
    raw = str(requested_mode or "").strip().lower()
    if not raw:
        raw = "auto"
    if raw not in _SUPPORTED_PARSE_MODES:
        raise HTTPException(status_code=400, detail=f"unsupported_{field_name}:{raw}")
    return raw


def _normalize_requested_parser(requested_parser: str | None, *, field_name: str = "parser") -> str:
    raw = str(requested_parser or "").strip().lower()
    if not raw:
        raw = "auto"
    if raw not in _SUPPORTED_GOOGLE_VISION_PARSERS:
        raise HTTPException(status_code=400, detail=f"unsupported_{field_name}:{raw}")
    return raw


def create_job(
    file_bytes: bytes,
    filename: str,
    requested_mode: str = "auto",
    requested_parser: str | None = None,
    auto_start: bool = True,
    *,
    created_by: str | None = None,
    created_role: str | None = None,
) -> Dict:
    """Create a new job, persist the uploaded PDF, and optionally enqueue processing."""
    if not str(filename or "").lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="PDF only")

    job_id = str(uuid.uuid4())
    repo = JobsRepository(DATA_DIR)
    root = repo.ensure_job_layout(job_id)

    input_pdf = root / "input" / "document.pdf"
    repo.write_bytes(input_pdf, file_bytes)
    normalized_requested_mode = _normalize_requested_mode(requested_mode, field_name="requested_mode")
    normalized_requested_parser = _normalize_requested_parser(requested_parser, field_name="requested_parser")
    meta_payload: Dict[str, Any] = {
        "original_filename": filename,
        "file_size": len(file_bytes),
        "requested_mode": normalized_requested_mode,
        "requested_parser": normalized_requested_parser,
        "is_reversed": False,
        "created_at": dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
    }
    owner_username = str(created_by or "").strip()
    if owner_username:
        meta_payload["created_by"] = owner_username
    owner_role = str(created_role or "").strip().lower()
    if owner_role:
        meta_payload["created_role"] = owner_role
    repo.write_json(root / "meta.json", meta_payload)

    parse_mode = normalized_requested_mode
    if parse_mode != "auto":
        parse_mode = resolve_parse_mode(str(input_pdf), normalized_requested_mode)
    # Persist an initial queued status before enqueueing so the UI can render immediately.
    _write_queued_status(repo, job_id, parse_mode=parse_mode)

    started = False
    if auto_start:
        started = _start_job_worker(job_id, parse_mode)

    return {"job_id": job_id, "parse_mode": parse_mode, "started": started}


def start_job(job_id: str, requested_mode: Optional[str] = None, requested_parser: Optional[str] = None) -> Dict:
    """Queue an existing draft job unless it already points to an active worker."""
    repo = JobsRepository(DATA_DIR)
    _require_job(repo, job_id)

    input_pdf = repo.path(job_id, "input", "document.pdf")
    status = repo.read_status(job_id)
    meta = repo.read_json(repo.path(job_id, "meta.json"), default={})
    if not isinstance(meta, dict):
        meta = {}
    base_mode = requested_mode or meta.get("requested_mode") or status.get("parse_mode") or "auto"
    base_mode = _normalize_requested_mode(base_mode, field_name="requested_mode")
    base_parser = requested_parser or meta.get("requested_parser") or "auto"
    base_parser = _normalize_requested_parser(base_parser, field_name="requested_parser")
    parse_mode = base_mode
    if parse_mode != "auto":
        parse_mode = resolve_parse_mode(str(input_pdf), base_mode)
    meta["requested_parser"] = base_parser
    meta["requested_mode"] = base_mode
    repo.write_json(repo.path(job_id, "meta.json"), meta)

    # This prevents duplicate queue entries when the user clicks Start repeatedly.
    if _has_active_task(status):
        return {"job_id": job_id, "parse_mode": parse_mode, "started": False}

    _write_queued_status(repo, job_id, parse_mode=parse_mode)
    started = _start_job_worker(job_id, parse_mode)
    return {"job_id": job_id, "parse_mode": parse_mode, "started": started}


def _parse_iso_datetime(value: Any) -> dt.datetime | None:
    text_value = str(value or "").strip()
    if not text_value:
        return None
    normalized = text_value.replace("Z", "+00:00")
    try:
        parsed = dt.datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def _iso_from_mtime(path: Path | None) -> str:
    if not isinstance(path, Path) or not path.exists():
        return ""
    value = dt.datetime.fromtimestamp(path.stat().st_mtime, tz=dt.timezone.utc)
    return value.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _resolve_job_owner(meta_payload: dict[str, Any]) -> tuple[str, str]:
    if not isinstance(meta_payload, dict):
        return "", ""
    owner = (
        str(meta_payload.get("created_by") or "").strip()
        or str(meta_payload.get("uploaded_by") or "").strip()
        or str(meta_payload.get("source_assigned_user") or "").strip()
    )
    role = str(meta_payload.get("created_role") or "").strip().lower()
    return owner, role


def _build_owned_job_row(job_dir: Path, *, state_repo: JobStateRepository | None = None) -> dict[str, Any]:
    repo = JobsRepository(DATA_DIR)
    job_id = str(job_dir.name)
    meta_path = job_dir / "meta.json"
    status_path = job_dir / "status.json"
    input_path = job_dir / "input" / "document.pdf"
    meta_payload = repo.read_json(meta_path, default={})
    if not isinstance(meta_payload, dict):
        meta_payload = {}
    status_payload = repo.read_json(status_path, default={})
    if not isinstance(status_payload, dict):
        status_payload = {}
    state_payload = state_repo.get_job(job_id) if state_repo is not None else {}
    if not isinstance(state_payload, dict):
        state_payload = {}

    owner_username, owner_role = _resolve_job_owner(meta_payload)
    created_at = (
        str(meta_payload.get("created_at") or "").strip()
        or _iso_from_mtime(meta_path if meta_path.exists() else input_path if input_path.exists() else job_dir)
    )
    updated_at = (
        str(status_payload.get("updated_at") or "").strip()
        or _iso_from_mtime(status_path if status_path.exists() else job_dir)
    )
    raw_size = meta_payload.get("file_size")
    try:
        size_bytes = max(0, int(raw_size or state_payload.get("file_size") or 0))
    except (TypeError, ValueError):
        size_bytes = 0

    status_value = (
        str(status_payload.get("status") or "").strip().lower()
        or str(state_payload.get("job_status") or "").strip().lower()
        or "queued"
    )
    progress_raw = status_payload.get("progress")
    try:
        progress = max(0, min(100, int(progress_raw or 0)))
    except (TypeError, ValueError):
        progress = 0

    return {
        "job_id": job_id,
        "original_filename": str(meta_payload.get("original_filename") or state_payload.get("file_name") or ""),
        "file_name": str(meta_payload.get("original_filename") or state_payload.get("file_name") or ""),
        "size_bytes": size_bytes,
        "owner_username": owner_username,
        "owner_role": owner_role,
        "status": status_value,
        "step": str(status_payload.get("step") or "").strip(),
        "progress": progress,
        "parse_mode": str(status_payload.get("parse_mode") or meta_payload.get("requested_mode") or "auto").strip().lower() or "auto",
        "requested_mode": str(meta_payload.get("requested_mode") or "").strip().lower(),
        "created_at": created_at,
        "updated_at": updated_at,
        "process_started": str(status_payload.get("process_started") or state_payload.get("process_started") or created_at),
        "process_end": str(status_payload.get("process_end") or state_payload.get("process_end") or ""),
        "source_tag": str(meta_payload.get("source_tag") or "").strip().upper(),
        "source_category": str(meta_payload.get("source_category") or "").strip().lower(),
        "volume_set_name": str(meta_payload.get("volume_set_name") or "").strip(),
        "volume_file_name": str(meta_payload.get("volume_file_name") or "").strip(),
        "is_reversed": bool(state_payload.get("is_reversed") or meta_payload.get("is_reversed", False)),
    }


def list_jobs_for_owner(
    owner_username: str,
    *,
    page: int = 1,
    limit: int = 100,
    source_tag: str | None = None,
) -> dict[str, Any]:
    """List persisted jobs that belong to one uploader/evaluator."""
    normalized_owner = str(owner_username or "").strip().lower()
    if not normalized_owner:
        return {
            "rows": [],
            "pagination": {
                "page": 1,
                "per_page": max(1, min(100, int(limit or 100))),
                "total_rows": 0,
                "total_pages": 1,
                "has_prev": False,
                "has_next": False,
            },
            "filters": {"owner": "", "source_tag": str(source_tag or "").strip().upper()},
        }

    jobs_dir = Path(DATA_DIR) / "jobs"
    rows: list[dict[str, Any]] = []
    state_repo = JobStateRepository(DATA_DIR)
    tag_filter = str(source_tag or "").strip().upper()
    if jobs_dir.exists():
        for job_dir in jobs_dir.iterdir():
            if not job_dir.is_dir():
                continue
            row = _build_owned_job_row(job_dir, state_repo=state_repo)
            owner_value = str(row.get("owner_username") or "").strip().lower()
            if owner_value != normalized_owner:
                continue
            if tag_filter and str(row.get("source_tag") or "").strip().upper() != tag_filter:
                continue
            rows.append(row)

    rows.sort(
        key=lambda item: (
            _parse_iso_datetime(item.get("updated_at"))
            or _parse_iso_datetime(item.get("created_at"))
            or dt.datetime.fromtimestamp(0, tz=dt.timezone.utc)
        ),
        reverse=True,
    )

    safe_page = max(1, int(page or 1))
    safe_limit = max(1, min(100, int(limit or 100)))
    total_rows = len(rows)
    total_pages = max(1, (total_rows + safe_limit - 1) // safe_limit) if total_rows else 1
    start = (safe_page - 1) * safe_limit
    end = start + safe_limit
    return {
        "rows": rows[start:end],
        "pagination": {
            "page": safe_page,
            "per_page": safe_limit,
            "total_rows": total_rows,
            "total_pages": total_pages,
            "has_prev": safe_page > 1,
            "has_next": safe_page < total_pages,
        },
        "filters": {"owner": normalized_owner, "source_tag": tag_filter},
    }


def cancel_job(job_id: str) -> Dict[str, Any]:
    """Revoke active Celery work and mark the job plus any page tasks as cancelled."""
    repo = JobsRepository(DATA_DIR)
    _require_job(repo, job_id)

    status = repo.read_status(job_id)
    if not isinstance(status, dict) or not status:
        status = {"status": "queued", "step": "queued", "progress": 0}

    page_status = _load_page_status_map(repo, job_id)
    current_status = str(status.get("status") or "").strip().lower()
    has_active_pages = any(
        str(item.get("status") or "").strip().lower() in _PAGE_ACTIVE_STATES for item in page_status.values()
    )
    if current_status in {"completed", "failed"} and not has_active_pages:
        return {
            "job_id": job_id,
            "cancelled": False,
            "status": current_status or "unknown",
            "revoked_task_ids": [],
        }
    if current_status == "cancelled" and not has_active_pages:
        return {
            "job_id": job_id,
            "cancelled": True,
            "status": "cancelled",
            "revoked_task_ids": [],
        }

    revoked_task_ids: List[str] = []
    # Revoke known task ids first so workers stop mutating state before we write cancellation markers.
    for task_id in _collect_job_task_ids(status_payload=status, page_status=page_status):
        try:
            _revoke_celery_task(task_id)
        except Exception as exc:
            raise HTTPException(status_code=503, detail=f"job_cancel_failed:{_error_message(exc)}")
        revoked_task_ids.append(task_id)

    now_iso = dt.datetime.utcnow().isoformat() + "Z"
    if page_status:
        changed = False
        # OCR jobs track each page independently, so each non-terminal page must be cancelled explicitly.
        for page_name, item in list(page_status.items()):
            page_payload = dict(item or {})
            state = str(page_payload.get("status") or "").strip().lower()
            if state in _PAGE_TERMINAL_STATES:
                continue
            page_payload["status"] = "cancelled"
            page_payload["message"] = "job_cancelled"
            page_payload["updated_at"] = now_iso
            page_payload.pop("retry_in_seconds", None)
            page_payload.pop("wait_seconds", None)
            page_status[page_name] = page_payload
            changed = True
        if changed:
            _write_page_status_map(repo, job_id, page_status)

    parse_mode = str(status.get("parse_mode") or "auto")
    payload: Dict[str, Any] = dict(status)
    payload.update(
        {
            "status": "cancelled",
            "step": "cancelled",
            "progress": _coerce_progress(payload.get("progress"), 0),
            "parse_mode": parse_mode,
            "message": "job_cancelled",
            "cancelled_at": now_iso,
        }
    )
    payload.pop("retry_in_seconds", None)

    if page_status:
        done_pages, failed_pages = _count_page_states(page_status)
        cancelled_pages = sum(
            1 for item in page_status.values() if str(item.get("status") or "").strip().lower() == "cancelled"
        )
        pages_total = int(payload.get("pages_total") or payload.get("pages") or 0)
        if pages_total <= 0:
            manifest = _load_pages_manifest(repo, job_id)
            pages = manifest.get("pages") if isinstance(manifest.get("pages"), list) else []
            pages_total = len(pages) or len(page_status)
        payload.update(
            {
                "pages": pages_total,
                "pages_total": pages_total,
                "pages_done": done_pages,
                "pages_failed": failed_pages,
                "pages_cancelled": cancelled_pages,
                "pages_inflight": 0,
                "failed_pages": _build_failed_pages_payload(page_status),
                "active_task_ids": [],
            }
        )
    else:
        payload.pop("active_task_ids", None)

    repo.write_status(job_id, payload)
    return {
        "job_id": job_id,
        "cancelled": True,
        "status": "cancelled",
        "revoked_task_ids": revoked_task_ids,
    }


def _public_page_status(value: Any) -> str:
    state = str(value or "").strip().lower()
    if state in {"pending", "queued", "retrying"}:
        return "pending"
    if state in {"processing"}:
        return "processing"
    if state in {"done", "failed"}:
        return state
    return state or "pending"


def _build_polling_pages_payload(
    repo: JobsRepository,
    job_id: str,
) -> tuple[List[Dict[str, Any]], int]:
    status_map = _load_page_status_map(repo, job_id)
    manifest = _load_pages_manifest(repo, job_id)
    manifest_pages = manifest.get("pages") if isinstance(manifest.get("pages"), list) else []

    ordered_page_names = [normalize_page_name(item) for item in manifest_pages if normalize_page_name(item)]
    if not ordered_page_names:
        ordered_page_names = sorted(
            status_map.keys(),
            key=lambda value: _page_number_from_page_file(str(value)) or 0,
        )

    pages_payload: List[Dict[str, Any]] = []
    for idx, page_name in enumerate(ordered_page_names, start=1):
        item = dict(status_map.get(page_name) or {})
        page_number = _page_number_from_page_file(page_name) or idx
        pages_payload.append(
            {
                "page": page_number,
                "status": _public_page_status(item.get("status") or "pending"),
            }
        )
    return pages_payload, len(ordered_page_names)


def _resolve_current_page(pages_payload: List[Dict[str, Any]], total_pages: int) -> int:
    for item in pages_payload:
        if str(item.get("status") or "") == "processing":
            return int(item.get("page") or 0)
    for item in pages_payload:
        if str(item.get("status") or "") == "pending":
            return int(item.get("page") or 0)
    return int(total_pages or 0)


def _attach_polling_payload(repo: JobsRepository, job_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        return payload
    pages_payload, total_pages = _build_polling_pages_payload(repo, job_id)
    response = dict(payload)
    response["job_id"] = job_id
    response["total_pages"] = total_pages
    response["current_page"] = _resolve_current_page(pages_payload, total_pages)
    response["pages"] = pages_payload
    status_value = str(response.get("status") or "").strip().lower()
    if status_value in {"done", "done_with_warnings", "completed"}:
        response["status"] = "completed"
        response["step"] = "completed"
    return response


def get_status(job_id: str) -> Dict:
    """Return the latest job status, reconciling stale task state when possible."""
    repo = JobsRepository(DATA_DIR)
    _require_job(repo, job_id)
    status = repo.read_status(job_id)
    job_state_repo = JobStateRepository(DATA_DIR)
    job_state = job_state_repo.get_job(job_id) or {}
    if not job_state:
        meta = repo.read_json(repo.path(job_id, "meta.json"), default={})
        if isinstance(meta, dict):
            job_state_repo.sync_job(job_id=job_id, meta=meta, status=status if isinstance(status, dict) else {})
            job_state = job_state_repo.get_job(job_id) or {}
    if not status:
        return _augment_runtime_features(
            _attach_polling_payload(repo, job_id, {"status": "queued", "step": "queued", "progress": 0, **job_state})
        )

    def _merge_job_state_fields(target: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(target, dict):
            return target
        if job_state.get("process_started") and not target.get("process_started"):
            target["process_started"] = job_state["process_started"]
        if job_state.get("process_end") and not target.get("process_end"):
            target["process_end"] = job_state["process_end"]
        target["is_reversed"] = bool(job_state.get("is_reversed", False))
        return _augment_runtime_features(_attach_polling_payload(repo, job_id, target))

    payload = dict(status)
    parse_mode = str(payload.get("parse_mode") or "auto")
    runtime_status = str(payload.get("status") or "").strip().lower()
    # OCR jobs fan out into page tasks, so the parent job status is synthesized from page state.
    if runtime_status in {"queued", "splitting", "processing", "parsing"}:
        page_status = _load_page_status_map(repo, job_id)
        if page_status:
            changed = False
            for page_name, item in list(page_status.items()):
                state = str(item.get("status") or "").strip().lower()
                if state not in _PAGE_ACTIVE_STATES:
                    continue
                task_id = str(item.get("task_id") or "").strip()
                if not task_id:
                    continue
                task_state = _get_celery_task_state(task_id)
                if task_state in {"SUCCESS", "FAILURE", "REVOKED"}:
                    latest_fragment = _read_page_fragment(repo, job_id, page_name)
                    item = dict(item)
                    if latest_fragment is not None:
                        item["status"] = "done"
                        item["rows_parsed"] = int(
                            (latest_fragment.get("diag") or {}).get("rows_parsed")
                            or len(latest_fragment.get("rows") or [])
                        )
                    else:
                        item["status"] = "failed"
                        item["message"] = f"task_terminated:{task_state.lower()}"
                    item["updated_at"] = dt.datetime.utcnow().isoformat() + "Z"
                    page_status[page_name] = item
                    changed = True
            if changed:
                _write_page_status_map(repo, job_id, page_status)
            payload = _refresh_job_progress(repo, job_id, parse_mode=parse_mode)
            if int(payload.get("pages_total") or 0) > 0 and int(payload.get("pages_inflight") or 0) == 0:
                payload = finalize_job_processing(job_id=job_id, parse_mode=parse_mode)
            payload["progress"] = _coerce_progress(payload.get("progress"), 0)
            return _merge_job_state_fields(payload)

    task_id = str(payload.get("task_id") or "").strip()
    if runtime_status in {"queued", "splitting", "processing", "parsing"} and task_id:
        task_state = _get_celery_task_state(task_id)
        if task_state in {"FAILURE", "REVOKED"}:
            mark_job_failed(
                job_id=job_id,
                parse_mode=parse_mode,
                message=f"task_terminated:{task_state.lower()}",
                step="task_terminated",
                task_id=task_id,
            )
            payload = repo.read_status(job_id)

    payload["progress"] = _coerce_progress(payload.get("progress"), 0)
    return _merge_job_state_fields(payload)


def _augment_runtime_features(payload: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        return payload
    payload["page_ai_fix_enabled"] = is_openai_page_fix_available()
    return payload


def set_job_reversed(job_id: str, is_reversed: bool) -> Dict[str, Any]:
    repo = JobsRepository(DATA_DIR)
    _require_job(repo, job_id)
    meta = repo.read_json(repo.path(job_id, "meta.json"), default={})
    if not isinstance(meta, dict):
        meta = {}
    meta["is_reversed"] = bool(is_reversed)
    repo.write_json(repo.path(job_id, "meta.json"), meta)
    try:
        JobStateRepository(DATA_DIR).set_reversed(job_id=job_id, is_reversed=bool(is_reversed))
    except KeyError:
        JobStateRepository(DATA_DIR).sync_job(job_id=job_id, meta=meta, status=repo.read_status(job_id))
    return {"job_id": job_id, "is_reversed": bool(is_reversed)}


def list_cleaned_pages(job_id: str) -> List[str]:
    repo = JobsRepository(DATA_DIR)
    _require_job(repo, job_id)

    files = repo.list_png(job_id, "cleaned")
    pdf_pages = _list_input_pdf_pages(repo, job_id)
    if files:
        if pdf_pages and set(files) != set(pdf_pages):
            return pdf_pages
        return files

    if pdf_pages:
        return pdf_pages

    parsed = _load_parsed_rows(repo, job_id)
    if parsed:
        return [f"{key}.png" for key in sorted(parsed.keys())]
    return []


def _list_input_pdf_pages(repo: JobsRepository, job_id: str) -> List[str]:
    input_pdf = repo.path(job_id, "input", "document.pdf")
    if not input_pdf.exists():
        return []
    try:
        reader = PdfReader(str(input_pdf))
        total = len(reader.pages or [])
    except Exception:
        return []
    if total <= 0:
        return []
    return [f"page_{idx:03}.png" for idx in range(1, total + 1)]


def get_cleaned_path(job_id: str, filename: str) -> Path:
    repo = JobsRepository(DATA_DIR)
    _require_job(repo, job_id)

    path = repo.path(job_id, "cleaned", filename)
    if path.exists():
        return path

    rendered_path = repo.path(job_id, "pages", filename)
    if rendered_path.exists():
        return rendered_path

    generated = _generate_preview_page_if_missing(repo, job_id, filename, path)
    if generated:
        return path

    raise HTTPException(status_code=404, detail="image_not_found")


def get_preview_path(job_id: str, page: str) -> Path:
    repo = JobsRepository(DATA_DIR)
    _require_job(repo, job_id)

    page_name = normalize_page_name(page)
    if not page_name:
        raise HTTPException(status_code=404, detail="preview_not_found")

    filename = f"{page_name}.png"
    cleaned_path = repo.path(job_id, "cleaned", filename)
    if cleaned_path.exists():
        return cleaned_path

    rendered_path = repo.path(job_id, "pages", filename)
    if rendered_path.exists():
        return rendered_path

    preview_path = repo.path(job_id, "preview", filename)
    if preview_path.exists():
        return preview_path

    generated = _generate_preview_page_if_missing(repo, job_id, filename, preview_path)
    if generated:
        return preview_path

    raise HTTPException(status_code=404, detail="preview_not_found")


def get_ocr_page(job_id: str, page: str) -> List[Dict]:
    repo = JobsRepository(DATA_DIR)
    _require_job(repo, job_id)

    page_name = normalize_page_name(page)
    path = repo.path(job_id, "ocr", f"{page_name}.json")
    if not path.exists():
        raise HTTPException(status_code=404, detail="ocr_not_ready")
    return repo.read_json(path, default=[])


def get_ocr_openai_raw_page(job_id: str, page: str) -> Dict:
    repo = JobsRepository(DATA_DIR)
    _require_job(repo, job_id)

    page_name = normalize_page_name(page)
    path = repo.path(job_id, "ocr", f"{page_name}.openai_raw.json")
    if not path.exists():
        raise HTTPException(status_code=404, detail="openai_ocr_raw_not_ready")
    payload = repo.read_json(path, default={})
    if not isinstance(payload, dict):
        return {}
    return payload


def get_page_bounds(job_id: str, page: str) -> List[Dict]:
    page_name = normalize_page_name(page)
    bounds = get_all_bounds(job_id)
    return bounds.get(page_name, [])


def get_all_bounds(job_id: str) -> Dict[str, List[Dict]]:
    repo = JobsRepository(DATA_DIR)
    _require_job(repo, job_id)

    parsed_repo = JobTransactionsRepository(DATA_DIR)
    if parsed_repo.has_rows(job_id):
        return parsed_repo.get_bounds_by_job(job_id)

    path = repo.path(job_id, "result", "bounds.json")
    if not path.exists():
        raise HTTPException(status_code=404, detail="bounds_not_ready")
    payload = repo.read_json(path, default={})
    return payload if isinstance(payload, dict) else {}


def get_page_rows(job_id: str, page: str) -> List[Dict]:
    page_name = normalize_page_name(page)
    rows = get_all_rows(job_id)
    return rows.get(page_name, [])


def _normalize_page_row_payload_item(row: Dict[str, Any], idx: int) -> Dict[str, Any]:
    row_id = str(row.get("row_id") or "").strip() or f"{idx:03}"
    normalized_rownumber = _normalize_row_number_output(
        row.get("rownumber"),
        fallback=row.get("row_number"),
    )
    row_type = str(row.get("row_type") or "transaction").strip().lower() or "transaction"
    if row_type not in _SUPPORTED_ROW_TYPES:
        row_type = "transaction"
    return {
        "row_id": row_id,
        "rownumber": normalized_rownumber,
        "row_number": _normalize_row_cell(row.get("row_number")) or (str(normalized_rownumber) if normalized_rownumber is not None else ""),
        "date": _normalize_row_date_for_output(row.get("date")),
        "description": _normalize_row_cell(row.get("description")),
        "debit": _normalize_row_amount_output(row.get("debit")),
        "credit": _normalize_row_amount_output(row.get("credit")),
        "balance": _normalize_row_amount_output(row.get("balance")),
        "row_type": row_type,
    }


def _normalize_page_rows_payload(rows: List[Dict]) -> List[Dict]:
    normalized_rows: List[Dict] = []
    for idx, row in enumerate(rows, start=1):
        if not isinstance(row, dict):
            raise HTTPException(status_code=400, detail="invalid_row_item")
        normalized_rows.append(_normalize_page_row_payload_item(row, idx))
    return normalized_rows


def _load_page_ai_fix_raw_payload(repo: JobsRepository, job_id: str, page_name: str) -> tuple[str, Any]:
    parsed_repo = JobTransactionsRepository(DATA_DIR)
    page_metadata_by_page = parsed_repo.get_page_metadata_by_job(job_id)
    page_metadata = page_metadata_by_page.get(page_name) or {}
    raw_result = page_metadata.get("raw_result")
    if isinstance(raw_result, (dict, list)):
        return "page_raw_result", raw_result

    raw_path = _page_raw_result_path(repo, job_id, page_name)
    if raw_path.exists():
        payload = repo.read_json(raw_path, default=None)
        if isinstance(payload, (dict, list)):
            return "page_raw_result_file", payload

    return "none", {}


def get_page_ai_fix(job_id: str, page: str) -> Dict[str, Any]:
    repo = JobsRepository(DATA_DIR)
    _require_job(repo, job_id)

    page_name = normalize_page_name(page)
    if not page_name:
        raise HTTPException(status_code=400, detail="invalid_page")
    if not is_openai_page_fix_available():
        raise HTTPException(status_code=503, detail="page_ai_fix_unavailable")

    parsed_rows = get_page_rows(job_id, page_name)
    raw_source, raw_payload = _load_page_ai_fix_raw_payload(repo, job_id, page_name)
    preview_path = get_preview_path(job_id, page_name)

    try:
        proposal = repair_page_rows_with_openai(
            page_name=page_name,
            parsed_rows=parsed_rows,
            raw_payload=raw_payload,
            raw_source=raw_source,
            image_path=preview_path,
        )
    except OpenAIPageFixNotConfiguredError as exc:
        raise HTTPException(status_code=503, detail=str(exc) or "page_ai_fix_unavailable") from exc
    except OpenAIPageFixError as exc:
        raise HTTPException(status_code=502, detail=str(exc) or "page_ai_fix_failed") from exc

    normalized_rows = _normalize_page_rows_payload(proposal.get("rows") or [])
    summary = proposal.get("summary") if isinstance(proposal.get("summary"), dict) else {}
    return {
        "page": page_name,
        "inputs_used": {
            "has_image": True,
            "raw_source": raw_source,
            "parsed_row_count": len(parsed_rows),
        },
        "proposal": {
            "rows": normalized_rows,
            "summary": {
                "changed": bool(summary.get("changed", False)),
                "issues_found": [str(item).strip() for item in summary.get("issues_found", [])]
                if isinstance(summary.get("issues_found"), list)
                else [],
                "rationale": str(summary.get("rationale") or "").strip(),
            },
        },
    }


def get_all_rows(job_id: str) -> Dict[str, List[Dict]]:
    repo = JobsRepository(DATA_DIR)
    _require_job(repo, job_id)
    return _load_parsed_rows(repo, job_id, required=True)


def update_page_rows(job_id: str, page: str, rows: List[Dict]) -> Dict:
    """Replace one page's parsed rows after an evaluator edits them in the UI."""
    repo = JobsRepository(DATA_DIR)
    _require_job(repo, job_id)

    page_name = normalize_page_name(page)
    if not page_name:
        raise HTTPException(status_code=400, detail="invalid_page")
    if not isinstance(rows, list):
        raise HTTPException(status_code=400, detail="invalid_rows_payload")

    normalized_rows = _normalize_page_rows_payload(rows)

    lock = _get_job_update_lock(job_id)
    with lock:
        parsed_repo = JobTransactionsRepository(DATA_DIR)
        parsed_repo.replace_page_rows(job_id=job_id, page_key=page_name, rows=normalized_rows, is_manual_edit=True)
        rows_by_page = _load_parsed_rows(repo, job_id, required=True)
        repo.write_json(repo.path(job_id, "result", "parsed_rows.json"), rows_by_page)
        repo.write_json(repo.path(job_id, "result", "bounds.json"), parsed_repo.get_bounds_by_job(job_id))
        summary = compute_summary(_flatten_rows(rows_by_page))
        repo.write_json(repo.path(job_id, "result", "summary.json"), summary)

    return {"page": page_name, "rows": rows_by_page.get(page_name, []), "summary": summary}


def get_page_notes(job_id: str, page: str) -> Dict[str, Any]:
    repo = JobsRepository(DATA_DIR)
    _require_job(repo, job_id)

    page_name = normalize_page_name(page)
    if not page_name:
        raise HTTPException(status_code=400, detail="invalid_page")

    parsed_repo = JobTransactionsRepository(DATA_DIR)
    notes = parsed_repo.get_page_notes(job_id=job_id, page_key=page_name)
    return {"page": page_name, "notes": notes}


def update_page_notes(job_id: str, page: str, notes: str | None) -> Dict[str, Any]:
    repo = JobsRepository(DATA_DIR)
    _require_job(repo, job_id)

    page_name = normalize_page_name(page)
    if not page_name:
        raise HTTPException(status_code=400, detail="invalid_page")

    lock = _get_job_update_lock(job_id)
    with lock:
        parsed_repo = JobTransactionsRepository(DATA_DIR)
        try:
            payload = parsed_repo.update_page_notes(job_id=job_id, page_key=page_name, notes=notes)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="page_not_found") from exc
    return payload


def _get_job_update_lock(job_id: str) -> threading.Lock:
    key = str(job_id or "").strip()
    with _JOB_UPDATE_LOCKS_GUARD:
        lock = _JOB_UPDATE_LOCKS.get(key)
        if lock is None:
            lock = threading.Lock()
            _JOB_UPDATE_LOCKS[key] = lock
        return lock


def get_summary(job_id: str) -> Dict:
    """Load or recompute the cached summary block for a processed job."""
    repo = JobsRepository(DATA_DIR)
    _require_job(repo, job_id)

    summary_path = repo.path(job_id, "result", "summary.json")
    if summary_path.exists():
        cached = repo.read_json(summary_path, default={})
        if isinstance(cached, dict) and not _summary_needs_refresh(cached):
            return cached

    rows_by_page = _load_parsed_rows(repo, job_id, required=True)
    rows = _flatten_rows(rows_by_page)
    summary = compute_summary(rows)
    repo.write_json(summary_path, summary)
    return summary


def get_parse_diagnostics(job_id: str) -> Dict:
    """Return parser diagnostics captured while building a job's outputs."""
    repo = JobsRepository(DATA_DIR)
    _require_job(repo, job_id)

    path = repo.path(job_id, "result", "parse_diagnostics.json")
    if not path.exists():
        raise HTTPException(status_code=404, detail="parse_diagnostics_not_ready")
    payload = repo.read_json(path, default={})
    return make_json_safe(payload) if isinstance(payload, dict) else {}


def export_pdf(job_id: str) -> tuple[bytes, str]:
    """Build the lightweight PDF summary export for a processed job."""
    rows = _flatten_rows(get_all_rows(job_id))
    summary = get_summary(job_id)
    pdf_bytes = _build_minimal_report_pdf(job_id, summary, rows)
    return pdf_bytes, _build_export_filename(job_id, "pdf")


def export_excel(job_id: str) -> tuple[bytes, str]:
    """Flatten parsed rows into the XLSX export used by downloads and CRM uploads."""
    rows = _flatten_rows(get_all_rows(job_id))
    headers = ["page", "row_id", "date", "description", "debit", "credit", "balance"]
    matrix: List[List[str]] = [headers]
    for row in rows:
        matrix.append(
            [
                str(row.get("page") or ""),
                str(row.get("row_id") or ""),
                str(row.get("date") or ""),
                str(row.get("description") or ""),
                str(row.get("debit") or ""),
                str(row.get("credit") or ""),
                str(row.get("balance") or ""),
            ]
        )

    workbook_bytes = _build_minimal_xlsx(matrix)
    return workbook_bytes, _build_export_filename(job_id, "xlsx")


def _start_job_worker(job_id: str, parse_mode: str) -> bool:
    repo = JobsRepository(DATA_DIR)
    status = repo.read_status(job_id)
    if _has_active_task(status):
        return False

    try:
        task_id = _enqueue_job(job_id=job_id, parse_mode=parse_mode)
    except Exception as exc:
        mark_job_failed(
            job_id=job_id,
            parse_mode=parse_mode,
            message=f"queue_dispatch_failed: {_error_message(exc)}",
            step="queue_failed",
        )
        return False

    latest_status = repo.read_status(job_id)
    latest_state = str((latest_status or {}).get("status") or "").strip().lower()
    if latest_state in {"completed", "failed"}:
        if task_id and not latest_status.get("task_id"):
            latest_payload = dict(latest_status)
            latest_payload["task_id"] = task_id
            repo.write_status(job_id, latest_payload)
    else:
        _write_queued_status(repo, job_id, parse_mode=parse_mode, task_id=task_id)
    return True


def process_job(job_id: str, parse_mode: str, task_id: Optional[str] = None) -> Dict[str, Any]:
    """Split the PDF into page PDFs, then enqueue independent per-page processing."""
    started_at = dt.datetime.now(dt.timezone.utc)
    repo = JobsRepository(DATA_DIR)
    _require_job(repo, job_id)
    root = repo.job_dir(job_id)

    def report(status: str, step: str, progress: int):
        payload = {
            "status": status,
            "step": step,
            "progress": _coerce_progress(progress, 0),
            "parse_mode": parse_mode,
        }
        if task_id:
            payload["task_id"] = task_id
        repo.write_status(job_id, payload)

    report("splitting", "splitting", 1)
    input_pdf = repo.path(job_id, "input", "document.pdf")
    result_dir = repo.path(job_id, "result")
    fragments_dir = result_dir / "page_fragments"
    fragments_dir.mkdir(parents=True, exist_ok=True)

    page_names = _split_pdf_into_page_pdfs(repo=repo, job_id=job_id, input_pdf=input_pdf)
    if not page_names:
        raise RuntimeError("no_pages_split")
    _write_pages_manifest(repo, job_id, page_names)
    logger.info(
        "Job %s split %s pages into individual PDFs in %sms before enqueue.",
        job_id,
        len(page_names),
        int((dt.datetime.now(dt.timezone.utc) - started_at).total_seconds() * 1000),
    )

    page_status = _load_page_status_map(repo, job_id)
    pending_pages: List[str] = []
    active_task_ids: List[str] = []
    now_iso = dt.datetime.utcnow().isoformat() + "Z"
    for idx, page_name in enumerate(page_names, start=1):
        normalized_page_name = normalize_page_name(page_name)
        page_payload = dict(page_status.get(page_name) or {})
        existing_fragment = _page_fragment_path(repo, job_id, normalized_page_name)
        if existing_fragment.exists():
            # Completed page fragments survive retries, so they can be reused without re-running OCR.
            page_payload["status"] = "done"
            page_payload["updated_at"] = now_iso
        state = str(page_payload.get("status") or "").strip().lower()
        if state == "done":
            page_status[normalized_page_name] = page_payload
            continue
        if state in _PAGE_ACTIVE_STATES and _is_celery_task_active(str(page_payload.get("task_id") or "")):
            page_status[normalized_page_name] = page_payload
            task_ref = str(page_payload.get("task_id") or "").strip()
            if task_ref:
                active_task_ids.append(task_ref)
            continue
        pending_pages.append(normalized_page_name)
        page_status[normalized_page_name] = {
            "status": "pending",
            "page_index": idx,
            "page_count": len(page_names),
            "retry_attempt": int(page_payload.get("retry_attempt") or 0),
            "step": "pending",
            "updated_at": now_iso,
        }

    _write_page_status_map(repo, job_id, page_status)

    report("processing", "processing", 5)
    enqueue_started = dt.datetime.now(dt.timezone.utc)
    for page_name in pending_pages:
        page_payload = page_status.get(page_name) or {}
        page_index = int(page_payload.get("page_index") or 1)
        task_id_page = _enqueue_page_job(
            job_id=job_id,
            parse_mode=parse_mode,
            page_name=page_name,
            page_index=page_index,
            page_count=len(page_names),
        )
        page_payload["task_id"] = task_id_page
        page_payload["status"] = "pending"
        page_payload["step"] = "pending"
        page_payload["updated_at"] = dt.datetime.utcnow().isoformat() + "Z"
        page_status[page_name] = page_payload
        active_task_ids.append(task_id_page)
    _write_page_status_map(repo, job_id, page_status)
    logger.info(
        "Job %s queued %s pending page tasks in %sms (%s total pages).",
        job_id,
        len(pending_pages),
        int((dt.datetime.now(dt.timezone.utc) - enqueue_started).total_seconds() * 1000),
        len(page_names),
    )

    done_pages, failed_pages = _count_page_states(page_status)
    inflight_pages = max(0, len(page_names) - done_pages - failed_pages)
    progress = _compute_page_progress(total=len(page_names), done=done_pages, failed=failed_pages)
    queued_payload: Dict[str, Any] = {
        "status": "processing",
        "step": "processing",
        "progress": progress,
        "parse_mode": parse_mode,
        "pages_total": len(page_names),
        "pages_done": done_pages,
        "pages_failed": failed_pages,
        "pages_inflight": inflight_pages,
        "failed_pages": _build_failed_pages_payload(page_status),
        "active_task_ids": active_task_ids,
    }
    if task_id:
        queued_payload["task_id"] = task_id
    repo.write_status(job_id, queued_payload)
    return queued_payload


def _page_raw_result_path(repo: JobsRepository, job_id: str, page_name: str) -> Path:
    return repo.path(job_id, "ocr", f"{normalize_page_name(page_name)}.raw.json")


def _has_required_ocr_page_images(
    *,
    page_files: List[str],
    page_routing: Dict[str, str],
    pages_dir: Path,
    cleaned_dir: Path,
) -> bool:
    for page_file in page_files:
        page_name = normalize_page_name(str(page_file).replace(".png", ""))
        if str(page_routing.get(page_name) or "ocr").strip().lower() == "text":
            continue
        page_path = resolve_ocr_page_path(page_file=page_file, pages_dir=pages_dir, cleaned_dir=cleaned_dir)
        if not page_path.exists():
            return False
    return True


def _prepare_page_routing_inputs(
    *,
    repo: JobsRepository,
    job_id: str,
    input_pdf: Path,
    page_files: List[str],
    requested_mode: str,
) -> Dict[str, str]:
    page_names = [normalize_page_name(str(page_file).replace(".png", "")) for page_file in page_files]
    routing = {page_name: "ocr" for page_name in page_names if page_name}
    if requested_mode not in {"auto", "text", "pdftotext"}:
        for page_name in page_names:
            raw_path = _page_raw_result_path(repo, job_id, page_name)
            if raw_path.exists():
                existing = repo.read_json(raw_path, default=None)
                if isinstance(existing, dict) and str(existing.get("source_type") or "").strip().lower() == "text":
                    raw_path.unlink(missing_ok=True)
        return routing

    try:
        layout_pages = extract_pdf_layout_pages(str(input_pdf))
    except Exception:
        layout_pages = []

    for idx, page_name in enumerate(page_names, start=1):
        page_layout = layout_pages[idx - 1] if idx - 1 < len(layout_pages) else {}
        raw_payload = layout_page_to_json_payload(page_layout if isinstance(page_layout, dict) else {}, page_number=idx)
        raw_path = _page_raw_result_path(repo, job_id, page_name)
        if not raw_payload.get("is_digital"):
            if raw_path.exists():
                existing = repo.read_json(raw_path, default=None)
                if isinstance(existing, dict) and str(existing.get("source_type") or "").strip().lower() == "text":
                    raw_path.unlink(missing_ok=True)
            continue
        repo.write_json(raw_path, raw_payload)
        routing[page_name] = "text"
    return routing


def _parse_text_page_raw_result(
    raw_payload: Dict[str, Any],
    *,
    header_hint: Dict[str, Any] | None = None,
    last_date_hint: str | None = None,
    previous_balance_hint=None,
) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, Any]]:
    words = raw_payload.get("words") if isinstance(raw_payload.get("words"), list) else []
    page_w = float(raw_payload.get("width") or 1.0)
    page_h = float(raw_payload.get("height") or 1.0)
    text = str(raw_payload.get("text") or "")
    profile = detect_bank_profile(text)
    page_rows, page_bounds, parser_diag = parse_page_with_profile_fallback(
        words,
        page_w,
        page_h,
        profile,
        header_hint=header_hint,
        last_date_hint=last_date_hint,
    )
    filtered_rows, filtered_bounds = _filter_rows_and_bounds(page_rows, page_bounds, profile)
    filtered_rows = _repair_page_flow_columns(filtered_rows, previous_balance_hint=previous_balance_hint)
    diag = {
        "source_type": "text",
        "ocr_backend": "pdftotext",
        "bank_profile": profile.name,
        "rows_parsed": len(filtered_rows),
        "profile_detected": parser_diag.get("profile_detected", profile.name),
        "profile_selected": parser_diag.get("profile_selected", profile.name),
        "fallback_applied": bool(parser_diag.get("fallback_applied", False)),
        "header_detected": bool(parser_diag.get("header_detected", False)),
        "header_hint_used": bool(parser_diag.get("header_hint_used", False)),
    }
    if isinstance(parser_diag.get("header_anchors"), dict):
        diag["header_anchors"] = parser_diag["header_anchors"]
    return filtered_rows, filtered_bounds, diag


def _extract_text_page_raw_result(*, page_pdf_path: Path, page_number: int) -> Dict[str, Any]:
    try:
        layout_pages = extract_pdf_layout_pages(str(page_pdf_path))
    except Exception:
        layout_pages = []
    page_layout = layout_pages[0] if layout_pages else {}
    if not isinstance(page_layout, dict):
        page_layout = {}
    return layout_page_to_json_payload(page_layout, page_number=page_number)


def _extract_page_raw_text(raw_payload: Dict[str, Any] | None) -> str | None:
    if not isinstance(raw_payload, dict):
        return None
    text = str(raw_payload.get("text") or "").strip()
    return text or None


def _render_split_pdf_page_to_png(*, page_pdf_path: Path, output_path: Path, dpi: int | None = None) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_path.exists():
        return output_path
    pages = convert_from_path(
        str(page_pdf_path),
        dpi=max(72, int(dpi or scanned_render_dpi())),
        fmt="png",
        first_page=1,
        last_page=1,
    )
    if not pages:
        raise RuntimeError(f"split_page_render_failed:{page_pdf_path.name}")
    _save_png_with_pixel_cap(pages[0], output_path)
    return output_path


def process_job_page(
    job_id: str,
    parse_mode: str,
    page_name: str,
    page_index: int,
    page_count: int,
    task_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Process one page using embedded text when available, else OCR the page image."""
    repo = JobsRepository(DATA_DIR)
    _require_job(repo, job_id)
    pages_dir = repo.path(job_id, "pages")
    cleaned_dir = repo.path(job_id, "cleaned")
    ocr_dir = repo.path(job_id, "ocr")

    page_name = normalize_page_name(page_name)
    page_file = f"{page_name}.png"
    page_number = _page_number_from_page_file(page_name) or int(page_index)
    page_pdf_path = _split_page_pdf_path(repo, job_id, page_name)
    if not page_pdf_path.exists():
        raise RuntimeError(f"split_page_missing:{page_name}")

    _update_page_runtime_status(
        repo=repo,
        job_id=job_id,
        page_name=page_name,
        page_index=page_index,
        page_count=page_count,
        status="processing",
        step="detecting_page_type",
        task_id=task_id,
    )
    _refresh_job_progress(repo, job_id, parse_mode=parse_mode, active_task_id=task_id)

    def _heartbeat(wait_seconds: float):
        # Long OCR waits still emit status so the processing UI can show that work is alive.
        with _edit_page_status_map(repo, job_id) as current:
            target = dict(current.get(page_name) or {})
            if str(target.get("status") or "").strip().lower() == "done":
                return
            target["status"] = "processing"
            target["step"] = "rate_limit_wait"
            target["wait_seconds"] = round(float(wait_seconds), 3)
            target["updated_at"] = dt.datetime.utcnow().isoformat() + "Z"
            current[page_name] = target

    raw_result = _extract_text_page_raw_result(page_pdf_path=page_pdf_path, page_number=page_number)
    page_type = "digital" if bool(raw_result.get("is_digital")) else "scanned"
    persisted_raw_result: Dict[str, Any] | None = None

    def _persist_raw_and_mark_parsing(raw_payload: Dict[str, Any], *, resolved_page_type: str) -> None:
        nonlocal persisted_raw_result
        persisted_raw_result = dict(raw_payload or {})
        repo.write_json(_page_raw_result_path(repo, job_id, page_name), persisted_raw_result)
        _upsert_page_intake_record(
            job_id=job_id,
            page_name=page_name,
            page_type=resolved_page_type,
            raw_text=_extract_page_raw_text(raw_payload),
            processing_status="processing",
            raw_result=raw_payload,
        )
        _update_page_runtime_status(
            repo=repo,
            job_id=job_id,
            page_name=page_name,
            page_index=page_index,
            page_count=page_count,
            status="processing",
            step="parsing",
            task_id=task_id,
            page_type=resolved_page_type,
        )
        _refresh_job_progress(repo, job_id, parse_mode=parse_mode, active_task_id=task_id)

    if page_type == "digital":
        _persist_raw_and_mark_parsing(raw_result, resolved_page_type="digital")
        page_rows, page_bounds, page_diag = _parse_text_page_raw_result(raw_result)
    else:
        _render_split_pdf_page_to_png(page_pdf_path=page_pdf_path, output_path=_rendered_page_png_path(repo, job_id, page_name))
        page_name, page_rows, page_bounds, page_diag = process_ocr_page(
            page_file=page_file,
            pages_dir=pages_dir,
            cleaned_dir=cleaned_dir,
            ocr_dir=ocr_dir,
            rate_limit_heartbeat=_heartbeat,
            raw_result_callback=lambda payload: _persist_raw_and_mark_parsing(payload, resolved_page_type="scanned"),
        )
        if persisted_raw_result is None:
            raw_result_path = _page_raw_result_path(repo, job_id, page_name)
            fallback_payload = repo.read_json(raw_result_path, default=None) if raw_result_path.exists() else None
            if isinstance(fallback_payload, dict):
                _persist_raw_and_mark_parsing(fallback_payload, resolved_page_type="scanned")
    page_diag = dict(page_diag or {})
    page_diag["page_type"] = page_type
    _write_page_fragment(repo, job_id, page_name, page_rows=page_rows, page_bounds=page_bounds, page_diag=page_diag)
    if persisted_raw_result is not None:
        _upsert_page_intake_record(
            job_id=job_id,
            page_name=page_name,
            page_type=page_type,
            raw_text=_extract_page_raw_text(persisted_raw_result),
            processing_status="done",
            raw_result=persisted_raw_result,
        )

    _update_page_runtime_status(
        repo=repo,
        job_id=job_id,
        page_name=page_name,
        page_index=page_index,
        page_count=page_count,
        status="done",
        step="done",
        task_id=task_id,
        page_type=page_type,
        rows_parsed=int(page_diag.get("rows_parsed") or len(page_rows)),
    )
    payload = _load_page_status_map(repo, job_id).get(page_name) or {}

    merged = _refresh_job_progress(repo, job_id, parse_mode=parse_mode, active_task_id=task_id)
    if merged.get("pages_total", 0) > 0 and int(merged.get("pages_inflight") or 0) == 0:
        try:
            _enqueue_finalize_job(job_id=job_id, parse_mode=parse_mode)
        except Exception:
            pass
    return payload


def mark_page_retrying(
    job_id: str,
    parse_mode: str,
    page_name: str,
    retry_attempt: int,
    retry_max_attempts: int,
    retry_in_seconds: int,
    message: str = "",
    task_id: Optional[str] = None,
) -> None:
    """Persist retry metadata for a page task before Celery requeues it."""
    repo = JobsRepository(DATA_DIR)
    page_name = normalize_page_name(page_name)
    _upsert_page_intake_record(job_id=job_id, page_name=page_name, processing_status="pending")
    with _edit_page_status_map(repo, job_id) as page_status:
        payload = dict(page_status.get(page_name) or {})
        payload["status"] = "retrying"
        payload["retry_attempt"] = max(0, int(retry_attempt))
        payload["retry_max_attempts"] = max(0, int(retry_max_attempts))
        payload["retry_in_seconds"] = max(0, int(retry_in_seconds))
        if message:
            payload["message"] = _error_message(message)
        payload["updated_at"] = dt.datetime.utcnow().isoformat() + "Z"
        if task_id:
            payload["task_id"] = task_id
        page_status[page_name] = payload
    _refresh_job_progress(repo, job_id, parse_mode=parse_mode, active_task_id=task_id)


def mark_page_failed(
    job_id: str,
    parse_mode: str,
    page_name: str,
    message: str,
    task_id: Optional[str] = None,
    retry_attempt: int = 0,
    retry_max_attempts: int = 0,
) -> None:
    """Persist a terminal page failure and finalize when no pages remain in flight."""
    repo = JobsRepository(DATA_DIR)
    page_name = normalize_page_name(page_name)
    _upsert_page_intake_record(job_id=job_id, page_name=page_name, processing_status="failed")
    with _edit_page_status_map(repo, job_id) as page_status:
        payload = dict(page_status.get(page_name) or {})
        payload["status"] = "failed"
        payload["message"] = _error_message(message)
        payload["retry_attempt"] = max(0, int(retry_attempt))
        payload["retry_max_attempts"] = max(0, int(retry_max_attempts))
        payload["updated_at"] = dt.datetime.utcnow().isoformat() + "Z"
        if task_id:
            payload["task_id"] = task_id
        page_status[page_name] = payload
    merged = _refresh_job_progress(repo, job_id, parse_mode=parse_mode, active_task_id=task_id)
    if merged.get("pages_total", 0) > 0 and int(merged.get("pages_inflight") or 0) == 0:
        try:
            _enqueue_finalize_job(job_id=job_id, parse_mode=parse_mode)
        except Exception:
            pass


def finalize_job_processing(job_id: str, parse_mode: str, task_id: Optional[str] = None) -> Dict[str, Any]:
    """Merge page fragments, compute summary outputs, and mark the OCR job complete."""
    repo = JobsRepository(DATA_DIR)
    _require_job(repo, job_id)

    page_status = _load_page_status_map(repo, job_id)
    manifest = _load_pages_manifest(repo, job_id)
    page_files = manifest.get("pages") if isinstance(manifest.get("pages"), list) else []
    if not page_files:
        raise RuntimeError("pages_manifest_missing")

    parsed_output: Dict[str, List[Dict]] = {}
    bounds_output: Dict[str, List[Dict]] = {}
    diagnostics: Dict[str, Dict] = {
        "job": {
            "source_type": "unknown",
            "ocr_backend": None,
            "parse_mode": parse_mode,
        },
        "pages": {},
    }

    success_pages = 0
    failed_list: List[Dict[str, str]] = []
    for page_file in page_files:
        page_name = str(page_file).replace(".png", "")
        fragment = _read_page_fragment(repo, job_id, page_name)
        state = str((page_status.get(page_name) or {}).get("status") or "").strip().lower()
        if fragment is not None:
            parsed_output[page_name] = fragment.get("rows") or []
            bounds_output[page_name] = fragment.get("bounds") or []
            diagnostics["pages"][page_name] = fragment.get("diag") or {"rows_parsed": len(parsed_output[page_name])}
            success_pages += 1
            continue
        # Missing fragments are recorded as failures so partial OCR runs can still complete with warnings.
        error = str((page_status.get(page_name) or {}).get("message") or "page_processing_failed")
        if state != "failed":
            error = "page_not_completed"
        failed_list.append({"page": page_name, "error": error})

    parsed_output, bounds_output, diagnostics["pages"] = _rebuild_ocr_outputs_from_saved_artifacts(
        repo=repo,
        job_id=job_id,
        page_files=page_files,
        parsed_output=parsed_output,
        bounds_output=bounds_output,
        page_diagnostics=diagnostics["pages"],
    )
    page_source_types = {
        str((payload or {}).get("source_type") or "").strip().lower()
        for payload in diagnostics["pages"].values()
        if isinstance(payload, dict)
    }
    page_source_types.discard("")
    if len(page_source_types) > 1:
        diagnostics["job"]["source_type"] = "mixed"
    elif page_source_types:
        diagnostics["job"]["source_type"] = next(iter(page_source_types))
    else:
        diagnostics["job"]["source_type"] = "unknown"

    ocr_backends = {
        str((payload or {}).get("ocr_backend") or "").strip().lower()
        for payload in diagnostics["pages"].values()
        if isinstance(payload, dict) and str((payload or {}).get("source_type") or "").strip().lower() == "ocr"
    }
    ocr_backends.discard("")
    if ocr_backends:
        diagnostics["job"]["ocr_backend"] = ",".join(sorted(ocr_backends))

    result_dir = repo.path(job_id, "result")
    parsed_output = _normalize_rows_by_page_for_output(parsed_output)
    has_ocr_pages = any(
        str((payload or {}).get("source_type") or "").strip().lower() == "ocr"
        for payload in diagnostics["pages"].values()
        if isinstance(payload, dict)
    )
    _persist_job_raw_result(repo, job_id, is_ocr=has_ocr_pages, raw_json=_collect_ocr_raw_payload(repo, job_id, page_files))
    _persist_parsed_rows(repo, job_id, parsed_output, bounds_by_page=bounds_output, is_manual_edit=False)
    repo.write_json(result_dir / "parsed_rows.json", parsed_output)
    repo.write_json(result_dir / "bounds.json", bounds_output)
    repo.write_json(result_dir / "parse_diagnostics.json", diagnostics)

    rows = _flatten_rows(_load_parsed_rows(repo, job_id, required=True))
    summary = compute_summary(rows)
    repo.write_json(result_dir / "summary.json", summary)

    if success_pages == 0:
        status_value = "failed"
        step_value = "failed"
        completion_outcome = "failed"
    elif failed_list:
        status_value = "completed"
        step_value = "completed"
        completion_outcome = "done_with_warnings"
    else:
        status_value = "completed"
        step_value = "completed"
        completion_outcome = "done"

    final_payload: Dict[str, Any] = {
        "status": status_value,
        "step": step_value,
        "completion_outcome": completion_outcome,
        "progress": 100,
        "parse_mode": parse_mode,
        "pages": len(page_files),
        "pages_total": len(page_files),
        "pages_done": success_pages,
        "pages_failed": len(failed_list),
        "pages_inflight": 0,
        "failed_pages": failed_list,
    }
    if task_id:
        final_payload["task_id"] = task_id
    repo.write_status(job_id, final_payload)
    auto_started = _maybe_start_next_volume_file(job_id, terminal_status=status_value)
    if auto_started:
        final_payload["volume_next_job_id"] = auto_started["job_id"]
        final_payload["volume_next_file_name"] = auto_started["file_name"]
        repo.write_status(job_id, final_payload)
    return final_payload


def _rebuild_ocr_outputs_from_saved_artifacts(
    *,
    repo: JobsRepository,
    job_id: str,
    page_files: List[str],
    parsed_output: Dict[str, List[Dict]],
    bounds_output: Dict[str, List[Dict]],
    page_diagnostics: Dict[str, Dict[str, Any]],
) -> tuple[Dict[str, List[Dict]], Dict[str, List[Dict]], Dict[str, Dict[str, Any]]]:
    pages_dir = repo.path(job_id, "pages")
    cleaned_dir = repo.path(job_id, "cleaned")
    ocr_dir = repo.path(job_id, "ocr")

    rebuilt_rows = dict(parsed_output)
    rebuilt_bounds = dict(bounds_output)
    rebuilt_diags = {str(key): dict(value or {}) for key, value in page_diagnostics.items()}

    header_hint: Dict[str, Any] | None = None
    last_date_hint: str = ""
    last_balance_hint = None

    for page_file in page_files:
        page_name = str(page_file).replace(".png", "")
        raw_result_path = _page_raw_result_path(repo, job_id, page_name)
        raw_result = repo.read_json(raw_result_path, default=None) if raw_result_path.exists() else None
        ocr_path = ocr_dir / f"{page_name}.json"

        text_raw = raw_result if isinstance(raw_result, dict) and str(raw_result.get("source_type") or "").strip().lower() == "text" else None
        ocr_raw = raw_result if isinstance(raw_result, dict) and str(raw_result.get("source_type") or "").strip().lower() == "ocr" else None
        if text_raw:
            filtered_rows, filtered_bounds, page_diag = _parse_text_page_raw_result(
                text_raw,
                header_hint=header_hint,
                last_date_hint=last_date_hint or None,
                previous_balance_hint=last_balance_hint,
            )
            rebuilt_rows[page_name] = filtered_rows
            rebuilt_bounds[page_name] = filtered_bounds
            rebuilt_diags[page_name] = page_diag
        else:
            ocr_items = None
            if isinstance(ocr_raw, dict):
                payload_items = ocr_raw.get("ocr_items")
                if isinstance(payload_items, list):
                    ocr_items = payload_items
            if ocr_items is None:
                ocr_items = repo.read_json(ocr_path, default=None) if ocr_path.exists() else None
            page_path = resolve_ocr_page_path(page_file=page_file, pages_dir=pages_dir, cleaned_dir=cleaned_dir)
            can_reparse = isinstance(ocr_items, list) and ocr_items and page_path.exists()
            if can_reparse:
                page_h, page_w = _image_size(page_path)
                text = " ".join((item.get("text") or "") for item in ocr_items if isinstance(item, dict))
                profile = detect_bank_profile(text)
                page_rows, page_bounds, parser_diag = parse_page_with_profile_fallback(
                    _ocr_items_to_words(ocr_items),
                    page_w,
                    page_h,
                    profile,
                    header_hint=header_hint,
                    last_date_hint=last_date_hint or None,
                )
                filtered_rows, filtered_bounds = _filter_rows_and_bounds(page_rows, page_bounds, profile)
                filtered_rows = _repair_page_flow_columns(filtered_rows, previous_balance_hint=last_balance_hint)
                rebuilt_rows[page_name] = filtered_rows
                rebuilt_bounds[page_name] = filtered_bounds

                page_diag = {
                    "source_type": "ocr",
                    "ocr_backend": str((ocr_raw or {}).get("provider") or "google_vision"),
                    "bank_profile": profile.name,
                    "rows_parsed": len(filtered_rows),
                    "profile_detected": parser_diag.get("profile_detected", profile.name),
                    "profile_selected": parser_diag.get("profile_selected", profile.name),
                    "fallback_applied": bool(parser_diag.get("fallback_applied", False)),
                    "header_detected": bool(parser_diag.get("header_detected", False)),
                    "header_hint_used": bool(parser_diag.get("header_hint_used", False)),
                }
                if isinstance(parser_diag.get("header_anchors"), dict):
                    page_diag["header_anchors"] = parser_diag["header_anchors"]
                rebuilt_diags[page_name] = page_diag
            else:
                existing_diag = dict(rebuilt_diags.get(page_name) or {})
                existing_rows = rebuilt_rows.get(page_name) or []
                if existing_diag:
                    rebuilt_diags[page_name] = existing_diag

        current_diag = rebuilt_diags.get(page_name) or {}
        anchors = current_diag.get("header_anchors")
        if isinstance(anchors, dict) and anchors:
            header_hint = dict(anchors)
        page_last_date = _last_row_date(rebuilt_rows.get(page_name) or [])
        if page_last_date:
            last_date_hint = page_last_date
        page_last_balance = _last_row_balance(rebuilt_rows.get(page_name) or [])
        if page_last_balance is not None:
            last_balance_hint = page_last_balance

    return rebuilt_rows, rebuilt_bounds, rebuilt_diags


def _collect_ocr_raw_payload(repo: JobsRepository, job_id: str, page_files: list[str]) -> dict[str, Any]:
    payload: dict[str, Any] = {"pages": {}}
    for page_file in page_files:
        page_name = normalize_page_name(str(page_file).replace(".png", ""))
        page_payload: dict[str, Any] = {}

        raw_path = _page_raw_result_path(repo, job_id, page_name)
        if raw_path.exists():
            page_payload["raw_result"] = repo.read_json(raw_path, default={})

        ocr_path = repo.path(job_id, "ocr", f"{page_name}.json")
        if ocr_path.exists():
            page_payload["ocr_items"] = repo.read_json(ocr_path, default=[])

        openai_path = repo.path(job_id, "ocr", f"{page_name}.openai_raw.json")
        if openai_path.exists():
            page_payload["openai_raw"] = repo.read_json(openai_path, default={})

        google_path = repo.path(job_id, "ocr", f"{page_name}.google_vision_raw.json")
        if google_path.exists():
            page_payload["google_vision_raw"] = repo.read_json(google_path, default={})

        if page_payload:
            payload["pages"][page_name] = page_payload

    return payload


def _persist_job_raw_result(
    repo: JobsRepository,
    job_id: str,
    *,
    is_ocr: bool,
    raw_xml: str | None = None,
    raw_json: dict[str, Any] | list[Any] | None = None,
) -> None:
    if raw_xml is None and raw_json is None:
        return
    try:
        JobResultsRawRepository(DATA_DIR).upsert(
            job_id=str(job_id),
            is_ocr=bool(is_ocr),
            raw_xml=raw_xml,
            raw_json=raw_json,
        )
    except Exception:
        # Raw-source persistence is non-critical and should not fail the parsing job.
        pass


def mark_job_retrying(
    job_id: str,
    parse_mode: str,
    retry_attempt: int,
    retry_max_attempts: int,
    retry_in_seconds: int,
    message: str = "",
    task_id: Optional[str] = None,
) -> None:
    """Persist retry metadata for the single-task text parsing path."""
    repo = JobsRepository(DATA_DIR)
    payload: Dict[str, Any] = {
        "status": "queued",
        "step": "retrying",
        "progress": 0,
        "parse_mode": parse_mode,
        "retry_attempt": max(0, int(retry_attempt)),
        "retry_max_attempts": max(0, int(retry_max_attempts)),
        "retry_in_seconds": max(0, int(retry_in_seconds)),
    }
    text = _error_message(message)
    if text:
        payload["message"] = text
    if task_id:
        payload["task_id"] = task_id
    repo.write_status(job_id, payload)


def mark_job_failed(
    job_id: str,
    parse_mode: str,
    message: str,
    step: str = "failed",
    task_id: Optional[str] = None,
    retry_attempt: int = 0,
    retry_max_attempts: int = 0,
) -> None:
    """Persist a terminal failure for the single-task text parsing path."""
    repo = JobsRepository(DATA_DIR)
    payload: Dict[str, Any] = {
        "status": "failed",
        "step": str(step or "failed"),
        "progress": 100,
        "parse_mode": parse_mode,
        "message": _error_message(message),
        "retry_attempt": max(0, int(retry_attempt)),
        "retry_max_attempts": max(0, int(retry_max_attempts)),
    }
    if task_id:
        payload["task_id"] = task_id
    repo.write_status(job_id, payload)


def _enqueue_job(job_id: str, parse_mode: str) -> str:
    from app.worker.tasks import process_job_task

    async_result = process_job_task.apply_async(kwargs={"job_id": job_id, "parse_mode": parse_mode})
    task_id = str(async_result.id or "").strip()
    if not task_id:
        raise RuntimeError("task_id_missing")
    return task_id


def _enqueue_page_job(job_id: str, parse_mode: str, page_name: str, page_index: int, page_count: int) -> str:
    from app.worker.tasks import process_page_task

    async_result = process_page_task.apply_async(
        kwargs={
            "job_id": job_id,
            "parse_mode": parse_mode,
            "page_name": page_name,
            "page_index": int(page_index),
            "page_count": int(page_count),
        }
    )
    task_id = str(async_result.id or "").strip()
    if not task_id:
        raise RuntimeError("page_task_id_missing")
    return task_id


def _enqueue_finalize_job(job_id: str, parse_mode: str) -> str:
    from app.worker.tasks import finalize_job_task

    async_result = finalize_job_task.apply_async(kwargs={"job_id": job_id, "parse_mode": parse_mode})
    task_id = str(async_result.id or "").strip()
    if not task_id:
        raise RuntimeError("finalize_task_id_missing")
    return task_id


def _collect_job_task_ids(status_payload: Dict[str, Any], page_status: Dict[str, Dict[str, Any]]) -> List[str]:
    seen: set[str] = set()
    out: List[str] = []

    def _add(value: Any) -> None:
        task_ref = str(value or "").strip()
        if not task_ref or task_ref in seen:
            return
        seen.add(task_ref)
        out.append(task_ref)

    _add((status_payload or {}).get("task_id"))
    active_refs = (status_payload or {}).get("active_task_ids")
    if isinstance(active_refs, list):
        for item in active_refs:
            _add(item)
    for page_name in sorted(page_status.keys()):
        page_payload = page_status.get(page_name) or {}
        state = str(page_payload.get("status") or "").strip().lower()
        if state in _PAGE_TERMINAL_STATES:
            continue
        _add(page_payload.get("task_id"))
    return out


def _revoke_celery_task(task_id: str) -> None:
    task_ref = str(task_id or "").strip()
    if not task_ref:
        return
    from app.worker.celery_app import celery

    celery.control.revoke(task_ref, terminate=True)


def _pages_manifest_path(repo: JobsRepository, job_id: str) -> Path:
    return repo.path(job_id, "result", "pages_manifest.json")


def _page_status_path(repo: JobsRepository, job_id: str) -> Path:
    return repo.path(job_id, "result", "page_status.json")


def _page_status_lock_path(repo: JobsRepository, job_id: str) -> Path:
    return repo.path(job_id, "result", "page_status.lock")


def _page_fragment_path(repo: JobsRepository, job_id: str, page_name: str) -> Path:
    return repo.path(job_id, "result", "page_fragments", f"{normalize_page_name(page_name)}.json")


def _coerce_page_status_map(data: Any) -> Dict[str, Dict[str, Any]]:
    if not isinstance(data, dict):
        return {}
    out: Dict[str, Dict[str, Any]] = {}
    for key, value in data.items():
        if isinstance(value, dict):
            out[str(key)] = dict(value)
    return out


def _load_pages_manifest(repo: JobsRepository, job_id: str) -> Dict[str, Any]:
    path = _pages_manifest_path(repo, job_id)
    data = repo.read_json(path, default={})
    return data if isinstance(data, dict) else {}


def get_pages_status(job_id: str) -> Dict[str, Dict[str, Any]]:
    """Expose the page-level OCR state map used by the processing UI."""
    repo = JobsRepository(DATA_DIR)
    _require_job(repo, job_id)
    status_map = _load_page_status_map(repo, job_id)
    manifest = _load_pages_manifest(repo, job_id)
    pages = manifest.get("pages") if isinstance(manifest.get("pages"), list) else []
    if not pages:
        return status_map
    out: Dict[str, Dict[str, Any]] = {}
    for idx, page_file in enumerate(pages, start=1):
        page_name = normalize_page_name(page_file)
        payload = dict(status_map.get(page_name) or {})
        payload.setdefault("page_index", idx)
        payload.setdefault("page_count", len(pages))
        payload["status"] = _public_page_status(payload.get("status") or "pending")
        out[page_name] = payload
    return out


def _upsert_page_intake_record(
    *,
    job_id: str,
    page_name: str,
    page_type: str | None = None,
    raw_text: str | None = None,
    processing_status: str | None = None,
    raw_result: Dict[str, Any] | List[Any] | None = None,
) -> Dict[str, Any]:
    page_number = _page_number_from_page_file(page_name)
    if page_number is None:
        raise KeyError(page_name)
    repo = JobTransactionsRepository(DATA_DIR)
    if page_type is None and raw_text is None and raw_result is None:
        try:
            return repo.update_page_intake_fields(
                job_id=job_id,
                page_number=page_number,
                processing_status=processing_status,
            )
        except KeyError:
            return {}
    return repo.upsert_page_metadata(
        job_id=job_id,
        page_number=page_number,
        page_type=page_type,
        raw_text=raw_text,
        processing_status=processing_status,
        raw_result=raw_result,
    )


def _update_page_runtime_status(
    *,
    repo: JobsRepository,
    job_id: str,
    page_name: str,
    page_index: int,
    page_count: int,
    status: str,
    step: str,
    task_id: Optional[str] = None,
    page_type: str | None = None,
    rows_parsed: int | None = None,
) -> None:
    with _edit_page_status_map(repo, job_id) as page_status:
        payload = dict(page_status.get(page_name) or {})
        payload["status"] = status
        payload["step"] = step
        payload["page_index"] = int(page_index)
        payload["page_count"] = int(page_count)
        payload["updated_at"] = dt.datetime.utcnow().isoformat() + "Z"
        if page_type:
            payload["page_type"] = page_type
        if rows_parsed is not None:
            payload["rows_parsed"] = int(rows_parsed)
        if task_id:
            payload["task_id"] = task_id
        page_status[page_name] = payload


def _write_pages_manifest(repo: JobsRepository, job_id: str, page_files: List[str]) -> None:
    payload = {
        "pages": list(page_files),
        "updated_at": dt.datetime.utcnow().isoformat() + "Z",
    }
    repo.write_json(_pages_manifest_path(repo, job_id), payload)


def _load_page_status_map(repo: JobsRepository, job_id: str) -> Dict[str, Dict[str, Any]]:
    path = _page_status_path(repo, job_id)
    return _coerce_page_status_map(repo.read_json(path, default={}))


@contextmanager
def _edit_page_status_map(repo: JobsRepository, job_id: str):
    """Serialize read-modify-write updates so parallel page workers cannot drop each other's state."""
    lock_path = _page_status_lock_path(repo, job_id)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with open(lock_path, "a+", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        payload = _load_page_status_map(repo, job_id)
        try:
            yield payload
        except Exception:
            raise
        else:
            repo.write_json(_page_status_path(repo, job_id), payload)
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _write_page_status_map(repo: JobsRepository, job_id: str, payload: Dict[str, Dict[str, Any]]) -> None:
    repo.write_json(_page_status_path(repo, job_id), payload)


def _write_page_fragment(
    repo: JobsRepository,
    job_id: str,
    page_name: str,
    *,
    page_rows: List[Dict],
    page_bounds: List[Dict],
    page_diag: Dict[str, Any],
) -> None:
    payload = {
        "page": normalize_page_name(page_name),
        "rows": page_rows,
        "bounds": page_bounds,
        "diag": page_diag,
        "updated_at": dt.datetime.utcnow().isoformat() + "Z",
    }
    repo.write_json(_page_fragment_path(repo, job_id, page_name), payload)


def _read_page_fragment(repo: JobsRepository, job_id: str, page_name: str) -> Optional[Dict[str, Any]]:
    path = _page_fragment_path(repo, job_id, page_name)
    if not path.exists():
        return None
    data = repo.read_json(path, default={})
    if not isinstance(data, dict):
        return None
    return data


def _count_page_states(page_status: Dict[str, Dict[str, Any]]) -> tuple[int, int]:
    done = 0
    failed = 0
    for payload in page_status.values():
        state = str(payload.get("status") or "").strip().lower()
        if state == "done":
            done += 1
        elif state == "failed":
            failed += 1
    return done, failed


def _compute_page_progress(total: int, done: int, failed: int) -> int:
    """Estimate OCR progress without reaching 100% until final aggregation succeeds."""
    if total <= 0:
        return 0
    completed = max(0, min(total, int(done) + int(failed)))
    return max(0, min(99, int((completed / float(total)) * 99)))


def _build_failed_pages_payload(page_status: Dict[str, Dict[str, Any]]) -> List[Dict[str, str]]:
    failed_rows: List[Dict[str, str]] = []
    for page_name, payload in sorted(page_status.items()):
        state = str(payload.get("status") or "").strip().lower()
        if state != "failed":
            continue
        failed_rows.append(
            {
                "page": str(page_name),
                "error": _error_message(payload.get("message") or "page_processing_failed"),
            }
        )
    return failed_rows


def _refresh_job_progress(
    repo: JobsRepository,
    job_id: str,
    *,
    parse_mode: str,
    active_task_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Rebuild the parent OCR job status from the latest per-page task states."""
    status = repo.read_status(job_id)
    page_status = _load_page_status_map(repo, job_id)
    manifest = _load_pages_manifest(repo, job_id)
    total = len(manifest.get("pages") or [])
    done, failed = _count_page_states(page_status)
    inflight = max(0, total - done - failed)
    parsing_started = any(
        str((item or {}).get("step") or "").strip().lower() == "parsing"
        or str((item or {}).get("status") or "").strip().lower() == "done"
        for item in page_status.values()
    )
    active_task_ids: List[str] = []
    for item in page_status.values():
        state = str(item.get("status") or "").strip().lower()
        if state not in _PAGE_ACTIVE_STATES:
            continue
        task_ref = str(item.get("task_id") or "").strip()
        if task_ref:
            active_task_ids.append(task_ref)
    runtime_status = "parsing" if parsing_started else "processing"
    payload: Dict[str, Any] = dict(status or {})
    payload.update(
        {
            "status": runtime_status,
            "step": runtime_status,
            "parse_mode": parse_mode,
            "progress": _compute_page_progress(total=total, done=done, failed=failed),
            "pages_total": total,
            "pages_done": done,
            "pages_failed": failed,
            "pages_inflight": inflight,
            "failed_pages": _build_failed_pages_payload(page_status),
            "active_task_ids": active_task_ids,
        }
    )
    if active_task_id:
        payload["task_id"] = active_task_id
    repo.write_status(job_id, payload)
    return payload


def _is_celery_task_active(task_id: str) -> bool:
    state = _get_celery_task_state(task_id)
    return state in _ACTIVE_CELERY_STATES


def _get_celery_task_state(task_id: str) -> str:
    if not str(task_id or "").strip():
        return ""

    try:
        from app.worker.celery_app import celery

        state = str(celery.AsyncResult(task_id).state or "").upper()
    except Exception:
        # If broker/result backend probing fails, avoid double-enqueueing.
        return "UNKNOWN"
    return state


def _has_active_task(status_payload: Dict[str, Any]) -> bool:
    status = str((status_payload or {}).get("status") or "").strip().lower()
    if status not in {"queued", "splitting", "processing", "parsing"}:
        return False
    multi = (status_payload or {}).get("active_task_ids")
    if isinstance(multi, list):
        for task_ref in multi:
            task_id = str(task_ref or "").strip()
            if task_id and _is_celery_task_active(task_id):
                return True
    task_id = str((status_payload or {}).get("task_id") or "").strip()
    if not task_id:
        return False
    return _is_celery_task_active(task_id)


def _write_queued_status(repo: JobsRepository, job_id: str, parse_mode: str, task_id: Optional[str] = None) -> None:
    payload: Dict[str, Any] = {
        "status": "queued",
        "step": "queued",
        "progress": 0,
        "parse_mode": parse_mode,
    }
    if task_id:
        payload["task_id"] = str(task_id)
    repo.write_status(job_id, payload)


def _error_message(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return "unknown_error"
    return text[:1200]


def _require_job(repo: JobsRepository, job_id: str):
    if not repo.job_exists(job_id):
        raise HTTPException(status_code=404, detail="job_not_found")


def _persist_parsed_rows(
    repo: JobsRepository,
    job_id: str,
    rows_by_page: Dict[str, List[Dict]],
    *,
    bounds_by_page: Dict[str, List[Dict]] | None = None,
    is_manual_edit: bool,
) -> None:
    parsed_repo = JobTransactionsRepository(DATA_DIR)
    existing_page_metadata = parsed_repo.get_page_metadata_by_job(job_id)
    page_metadata_by_page = _build_page_metadata_by_page(repo, job_id, rows_by_page)
    for page_name, metadata in page_metadata_by_page.items():
        existing = existing_page_metadata.get(page_name) or {}
        if metadata.get("page_type") is None and existing.get("page_type") is not None:
            metadata["page_type"] = existing.get("page_type")
        if metadata.get("raw_text") is None and existing.get("raw_text") is not None:
            metadata["raw_text"] = existing.get("raw_text")
        existing_processing_status = str(existing.get("processing_status") or "").strip().lower()
        if existing_processing_status == "failed":
            metadata["processing_status"] = "failed"
        elif metadata.get("processing_status") is None and existing.get("processing_status") is not None:
            metadata["processing_status"] = existing.get("processing_status")
        if metadata.get("raw_result") is None and existing.get("raw_result") is not None:
            metadata["raw_result"] = existing.get("raw_result")
        if metadata.get("notes") is None and existing.get("notes") is not None:
            metadata["notes"] = existing.get("notes")
    parsed_repo.replace_job_rows(
        job_id=job_id,
        rows_by_page=rows_by_page,
        bounds_by_page=bounds_by_page or {},
        page_metadata_by_page=page_metadata_by_page,
        is_manual_edit=is_manual_edit,
    )


def _build_page_metadata_by_page(
    repo: JobsRepository,
    job_id: str,
    rows_by_page: Dict[str, List[Dict]],
) -> Dict[str, Dict[str, Any]]:
    manifest = _load_pages_manifest(repo, job_id)
    manifest_pages = manifest.get("pages") if isinstance(manifest.get("pages"), list) else []
    page_names = {normalize_page_name(name) for name in manifest_pages if normalize_page_name(name)}
    page_names.update(normalize_page_name(name) for name in (rows_by_page or {}).keys() if normalize_page_name(name))

    metadata: Dict[str, Dict[str, Any]] = {}
    for page_name in sorted(page_names):
        raw_result = None
        for suffix in (".raw.json", ".openai_raw.json", ".google_vision_raw.json", ".json"):
            path = repo.path(job_id, "ocr", f"{page_name}{suffix}")
            if not path.exists():
                continue
            payload = repo.read_json(path, default=None)
            if isinstance(payload, (dict, list)):
                raw_result = payload
                break
        page_number = int(page_name.split("_", 1)[1]) if page_name.startswith("page_") and page_name.split("_", 1)[1].isdigit() else 0
        is_digital = False
        if isinstance(raw_result, dict):
            source_type = str(raw_result.get("source_type") or "").strip().lower()
            if source_type == "text":
                is_digital = True
        raw_text = _extract_page_raw_text(raw_result)
        processing_status = "done" if (rows_by_page.get(page_name) or raw_result or raw_text) else None
        page_type = "digital" if is_digital else "scanned"
        metadata[page_name] = {
            "page_number": page_number,
            "page_type": page_type,
            "raw_text": raw_text,
            "processing_status": processing_status,
            "is_digital": is_digital,
            "raw_result": raw_result,
            "notes": None,
        }
    return metadata


def _load_parsed_rows(repo: JobsRepository, job_id: str, required: bool = False) -> Dict[str, List[Dict]]:
    parsed_repo = JobTransactionsRepository(DATA_DIR)
    if parsed_repo.has_rows(job_id):
        data = parsed_repo.get_rows_by_job(job_id)
        normalized = _normalize_rows_by_page_for_output(data)
        normalized = _backfill_ocr_row_numbers_from_openai_raw(repo, job_id, normalized)
        return normalized

    path = repo.path(job_id, "result", "parsed_rows.json")
    if not path.exists():
        if required:
            raise HTTPException(status_code=404, detail="parsed_rows_not_ready")
        return {}
    data = repo.read_json(path, default={})
    if not isinstance(data, dict):
        return {}
    normalized = _normalize_rows_by_page_for_output(data)
    bounds_path = repo.path(job_id, "result", "bounds.json")
    bounds_payload = repo.read_json(bounds_path, default={}) if bounds_path.exists() else {}
    if isinstance(bounds_payload, dict):
        _persist_parsed_rows(repo, job_id, normalized, bounds_by_page=bounds_payload, is_manual_edit=False)
    normalized = _backfill_ocr_row_numbers_from_openai_raw(repo, job_id, normalized)
    return normalized


def _flatten_rows(rows_by_page: Dict[str, List[Dict]]) -> List[Dict]:
    merged: List[Dict] = []
    for page in sorted(rows_by_page.keys()):
        rows = rows_by_page.get(page) or []
        for row in rows:
            payload = dict(row)
            payload["page"] = page
            merged.append(payload)
    return merged


def _summary_needs_refresh(summary: Dict[str, Any]) -> bool:
    if not isinstance(summary, dict):
        return True
    if summary.get("summary_version") != 2:
        return True
    if "monthly_credit_average" not in summary or "monthly_disposable_income" not in summary:
        return True
    monthly = summary.get("monthly")
    if monthly is None:
        return False
    if not isinstance(monthly, list):
        return True
    for row in monthly:
        if not isinstance(row, dict):
            return True
        if "debit_count" not in row or "credit_count" not in row:
            return True
    return False


def _coerce_progress(value, default: int = 0) -> int:
    try:
        return max(0, min(100, int(value)))
    except Exception:
        return default


def _normalize_row_cell(value) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _normalize_row_date_for_output(value) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    iso_value = normalize_date(raw, ["mdy", "dmy", "ymd"])
    if iso_value:
        try:
            parsed = dt.datetime.strptime(iso_value, "%Y-%m-%d").date()
            return parsed.strftime("%m/%d/%Y")
        except Exception:
            pass
    for fmt in ("%m/%d/%Y", "%d/%m/%Y", "%m-%d-%Y", "%d-%m-%Y", "%Y-%m-%d"):
        try:
            parsed = dt.datetime.strptime(raw, fmt).date()
            parsed = _coerce_statement_century(parsed)
            return parsed.strftime("%m/%d/%Y")
        except Exception:
            continue
    return raw


def _normalize_row_number_output(value, fallback=None) -> int | None:
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
    except Exception:
        return None


def _normalize_row_amount_output(value) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text:
        return None
    cleaned = "".join(ch for ch in text if ch.isdigit() or ch in ".-")
    if cleaned in {"", "-", ".", "-."}:
        return None
    try:
        return float(cleaned)
    except Exception:
        return None


def _normalize_rows_by_page_for_output(rows_by_page: Dict[str, List[Dict]]) -> Dict[str, List[Dict]]:
    out: Dict[str, List[Dict]] = {}
    for page_name, rows in (rows_by_page or {}).items():
        page_rows: List[Dict] = []
        for idx, row in enumerate(rows or [], start=1):
            if not isinstance(row, dict):
                continue
            row_id = str(row.get("row_id") or "").strip() or f"{idx:03}"
            parsed_rownumber = _normalize_row_number_output(
                row.get("rownumber"),
                fallback=row.get("row_number"),
            )
            page_rows.append(
                {
                    "row_id": row_id,
                    "rownumber": parsed_rownumber,
                    "row_number": str(parsed_rownumber or ""),
                    "date": _normalize_row_date_for_output(row.get("date")),
                    "description": _normalize_row_cell(row.get("description")),
                    "debit": _normalize_row_amount_output(row.get("debit")),
                    "credit": _normalize_row_amount_output(row.get("credit")),
                    "balance": _normalize_row_amount_output(row.get("balance")),
                    "row_type": _normalize_row_cell(row.get("row_type")) or "transaction",
                    "is_flagged": bool(row.get("is_flagged", False)),
                    "is_disbalanced": bool(row.get("is_disbalanced", False)),
                    "disbalance_expected_balance": _normalize_row_amount_output(row.get("disbalance_expected_balance")),
                    "disbalance_delta": _normalize_row_amount_output(row.get("disbalance_delta")),
                }
            )
        out[str(page_name)] = page_rows
    return out


def _backfill_ocr_row_numbers_from_openai_raw(
    repo: JobsRepository,
    job_id: str,
    rows_by_page: Dict[str, List[Dict]],
) -> Dict[str, List[Dict]]:
    status = repo.read_status(job_id)
    parse_mode = str(status.get("parse_mode") or "").strip().lower()
    if parse_mode != "ocr":
        return rows_by_page

    out: Dict[str, List[Dict]] = {}
    changed = False
    for page_name, rows in (rows_by_page or {}).items():
        page_rows = [dict(r) for r in (rows or [])]
        raw_numbers = _extract_openai_raw_rownumbers(repo, job_id, page_name)
        if raw_numbers:
            for idx, row in enumerate(page_rows):
                current = _normalize_row_number_output(row.get("rownumber"), fallback=row.get("row_number"))
                if current is not None:
                    continue
                if idx >= len(raw_numbers):
                    continue
                rn = raw_numbers[idx]
                if rn is None:
                    continue
                row["rownumber"] = rn
                row["row_number"] = str(rn)
                changed = True
        out[page_name] = page_rows

    if changed:
        # Persist repaired values so subsequent reads are stable.
        repo.write_json(repo.path(job_id, "result", "parsed_rows.json"), out)
    return out


def _extract_openai_raw_rownumbers(repo: JobsRepository, job_id: str, page_name: str) -> List[int | None]:
    path = repo.path(job_id, "ocr", f"{normalize_page_name(page_name)}.openai_raw.json")
    if not path.exists():
        return []
    payload = repo.read_json(path, default={})
    if not isinstance(payload, dict):
        return []
    response = payload.get("response")
    if not isinstance(response, dict):
        return []
    choices = response.get("choices")
    if not isinstance(choices, list) or not choices:
        return []
    message = (choices[0] or {}).get("message")
    if not isinstance(message, dict):
        return []
    content = message.get("content")
    if isinstance(content, list):
        text = "".join(item.get("text", "") if isinstance(item, dict) else str(item) for item in content)
    else:
        text = str(content or "")
    text = text.strip()
    if not text:
        return []
    try:
        parsed = json.loads(text)
    except Exception:
        return []
    rows = parsed.get("rows") if isinstance(parsed, dict) else None
    if not isinstance(rows, list):
        return []
    out: List[int | None] = []
    for item in rows:
        if not isinstance(item, dict):
            out.append(None)
            continue
        out.append(_normalize_row_number_output(item.get("rownumber"), fallback=item.get("row_number")))
    return out


def _generate_preview_page_if_missing(
    repo: JobsRepository,
    job_id: str,
    filename: str,
    output_path: Path,
    dpi: int = FALLBACK_PREVIEW_DPI,
    max_pixels: int = PREVIEW_MAX_PIXELS,
) -> bool:
    if not filename.endswith(".png") or not filename.startswith("page_"):
        return False

    token = filename.replace(".png", "").replace("page_", "")
    try:
        page_num = int(token)
    except Exception:
        return False
    if page_num <= 0:
        return False

    input_pdf = repo.path(job_id, "input", "document.pdf")
    if not input_pdf.exists():
        return False

    output_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        pages = convert_from_path(
            str(input_pdf),
            dpi=max(72, int(dpi)),
            fmt="png",
            first_page=page_num,
            last_page=page_num,
        )
        if not pages:
            return False
        page = pages[0]
        w, h = page.size
        pixels = max(1, w * h)
        cap = max(1, int(max_pixels))
        if pixels > cap:
            scale = math.sqrt(cap / float(pixels))
            page = page.resize(
                (max(1, int(w * scale)), max(1, int(h * scale))),
                resample=Image.Resampling.BILINEAR,
            )
        page.save(output_path, format="PNG")
        return output_path.exists()
    except Exception:
        return False


def _to_float(value) -> Optional[float]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    cleaned = "".join(ch for ch in text if ch.isdigit() or ch in ".-")
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except Exception:
        return None


def _parse_date(value: str) -> Optional[dt.date]:
    raw = str(value or "").strip()
    if not raw:
        return None
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y", "%m-%d-%Y", "%d-%m-%Y"):
        try:
            parsed = dt.datetime.strptime(raw, fmt).date()
            return _coerce_statement_century(parsed)
        except Exception:
            continue
    return None


def _coerce_statement_century(value: dt.date) -> dt.date:
    year = int(value.year)
    now_limit = dt.date.today().year + 1
    if 1900 <= year < 2000 and (year + 100) <= now_limit:
        try:
            return value.replace(year=year + 100)
        except ValueError:
            return value
    return value


def compute_summary(rows: List[Dict]) -> Dict:
    """Aggregate parsed rows into the summary metrics shown in the UI and exports."""
    tx_count = 0
    debit_count = 0
    credit_count = 0
    total_debit = 0.0
    total_credit = 0.0
    monthly: Dict[str, Dict] = {}

    normalized = []
    for row in rows:
        row_type = str(row.get("row_type") or "transaction").strip().lower() or "transaction"
        is_transaction = row_type == "transaction"
        date = _parse_date(row.get("date"))
        debit = _to_float(row.get("debit"))
        credit = _to_float(row.get("credit"))
        balance = _to_float(row.get("balance"))
        if is_transaction:
            tx_count += 1
            if debit is not None and abs(debit) > 0:
                debit_count += 1
                total_debit += abs(debit)
            if credit is not None and abs(credit) > 0:
                credit_count += 1
                total_credit += abs(credit)
        normalized.append((date, debit, credit, balance, is_transaction))

    ending_balance = None
    for _, _, _, bal, _ in reversed(normalized):
        if bal is not None:
            ending_balance = bal
            break

    for date, debit, credit, balance, is_transaction in normalized:
        if not date:
            continue
        key = date.strftime("%Y-%m")
        days_in_month = calendar.monthrange(date.year, date.month)[1]
        bucket = monthly.setdefault(
            key,
            {
                "month": key,
                "debit": 0.0,
                "credit": 0.0,
                "debit_count": 0,
                "credit_count": 0,
                "balance_sum": 0.0,
                "balance_count": 0,
            },
        )
        if is_transaction and debit is not None:
            bucket["debit"] += abs(debit)
            if abs(debit) > 0:
                bucket["debit_count"] += 1
        if is_transaction and credit is not None:
            bucket["credit"] += abs(credit)
            if abs(credit) > 0:
                bucket["credit_count"] += 1
        if balance is not None:
            bucket["balance_sum"] += balance
            bucket["balance_count"] += 1

    monthly_rows = []
    for key in sorted(monthly.keys()):
        item = monthly[key]
        balance_sum = _to_float(item.get("balance_sum")) or 0.0
        balance_count = int(item.get("balance_count") or 0)
        monthly_adb = (balance_sum / balance_count) if balance_count > 0 else 0.0
        monthly_rows.append(
            {
                "month": key,
                "debit": round(item["debit"], 2),
                "credit": round(item["credit"], 2),
                "debit_count": int(item["debit_count"]),
                "credit_count": int(item["credit_count"]),
                "avg_debit": round((item["debit"] / item["debit_count"]), 2) if item["debit_count"] else 0.0,
                "avg_credit": round((item["credit"] / item["credit_count"]), 2) if item["credit_count"] else 0.0,
                "adb": round(monthly_adb, 2),
            }
        )

    monthly_credit_average = (total_credit / len(monthly_rows)) if monthly_rows else 0.0
    monthly_disposable_income = monthly_credit_average * 0.30
    adb = (
        sum(_to_float(item.get("adb")) or 0.0 for item in monthly_rows) / len(monthly_rows)
        if monthly_rows
        else None
    )

    return {
        "summary_version": 2,
        "total_transactions": tx_count,
        "debit_transactions": debit_count,
        "credit_transactions": credit_count,
        "total_debit": round(total_debit, 2),
        "total_credit": round(total_credit, 2),
        "monthly_credit_average": round(monthly_credit_average, 2),
        "monthly_disposable_income": round(monthly_disposable_income, 2),
        "ending_balance": round(ending_balance, 2) if ending_balance is not None else None,
        "adb": round(adb, 2) if adb is not None else None,
        "monthly": monthly_rows,
    }


def _pdf_escape(text: str) -> str:
    return text.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")


def _build_minimal_report_pdf(job_id: str, summary: Dict, rows: List[Dict]) -> bytes:
    lines = [
        "Bank Statement Summary",
        f"Job: {job_id}",
        "",
        f"Total Transactions: {summary.get('total_transactions')}",
        f"Debit Transactions: {summary.get('debit_transactions')}",
        f"Credit Transactions: {summary.get('credit_transactions')}",
        f"Total Debit: {summary.get('total_debit')}",
        f"Total Credit: {summary.get('total_credit')}",
        f"Monthly Credit Average: {summary.get('monthly_credit_average')}",
        f"Monthly Disposable Income: {summary.get('monthly_disposable_income')}",
        f"ADB: {summary.get('adb')}",
        "",
        "Top Transactions:",
    ]
    for row in rows[:25]:
        lines.append(
            f"{row.get('date') or '-'} | {row.get('description') or '-'} | "
            f"D:{row.get('debit')} C:{row.get('credit')} B:{row.get('balance')}"
        )

    content = ["BT", "/F1 11 Tf", "40 790 Td", "14 TL"]
    for idx, line in enumerate(lines):
        safe = _pdf_escape(str(line))
        if idx == 0:
            content.append(f"({safe}) Tj")
        else:
            content.append(f"T* ({safe}) Tj")
    content.append("ET")
    stream = "\n".join(content).encode("latin-1", errors="replace")

    objects = []
    objects.append(b"<< /Type /Catalog /Pages 2 0 R >>")
    objects.append(b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>")
    objects.append(
        (
            b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 595 842] "
            b"/Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 R >>"
        )
    )
    objects.append(b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>")
    objects.append(f"<< /Length {len(stream)} >>\nstream\n".encode("ascii") + stream + b"\nendstream")

    output = bytearray(b"%PDF-1.4\n")
    offsets = [0]
    for idx, obj in enumerate(objects, start=1):
        offsets.append(len(output))
        output.extend(f"{idx} 0 obj\n".encode("ascii"))
        output.extend(obj)
        output.extend(b"\nendobj\n")

    xref_pos = len(output)
    output.extend(f"xref\n0 {len(objects) + 1}\n".encode("ascii"))
    output.extend(b"0000000000 65535 f \n")
    for off in offsets[1:]:
        output.extend(f"{off:010d} 00000 n \n".encode("ascii"))
    output.extend(f"trailer << /Size {len(objects) + 1} /Root 1 0 R >>\nstartxref\n{xref_pos}\n%%EOF\n".encode("ascii"))
    return bytes(output)


def _build_minimal_xlsx(rows: List[List[str]]) -> bytes:
    def _col_ref(idx: int) -> str:
        value = idx + 1
        letters = []
        while value > 0:
            value, rem = divmod(value - 1, 26)
            letters.append(chr(65 + rem))
        return "".join(reversed(letters))

    sheet_rows: List[str] = []
    for r_idx, row in enumerate(rows, start=1):
        cells: List[str] = []
        for c_idx, raw in enumerate(row):
            ref = f"{_col_ref(c_idx)}{r_idx}"
            text = xml_escape(str(raw or ""))
            cells.append(f'<c r="{ref}" t="inlineStr"><is><t>{text}</t></is></c>')
        sheet_rows.append(f'<row r="{r_idx}">{"".join(cells)}</row>')

    sheet_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        '<sheetData>'
        f'{"".join(sheet_rows)}'
        "</sheetData>"
        "</worksheet>"
    )

    content_types_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
        '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
        '<Default Extension="xml" ContentType="application/xml"/>'
        '<Override PartName="/xl/workbook.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>'
        '<Override PartName="/xl/worksheets/sheet1.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
        '<Override PartName="/xl/styles.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>'
        "</Types>"
    )

    root_rels_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" '
        'Target="xl/workbook.xml"/>'
        "</Relationships>"
    )

    workbook_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
        'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
        '<sheets><sheet name="Transactions" sheetId="1" r:id="rId1"/></sheets>'
        "</workbook>"
    )

    workbook_rels_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" '
        'Target="worksheets/sheet1.xml"/>'
        '<Relationship Id="rId2" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" '
        'Target="styles.xml"/>'
        "</Relationships>"
    )

    styles_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        '<fonts count="1"><font><sz val="11"/><name val="Calibri"/></font></fonts>'
        '<fills count="1"><fill><patternFill patternType="none"/></fill></fills>'
        '<borders count="1"><border/></borders>'
        '<cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs>'
        '<cellXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/></cellXfs>'
        '<cellStyles count="1"><cellStyle name="Normal" xfId="0" builtinId="0"/></cellStyles>'
        "</styleSheet>"
    )

    output = io.BytesIO()
    with zipfile.ZipFile(output, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("[Content_Types].xml", content_types_xml)
        zf.writestr("_rels/.rels", root_rels_xml)
        zf.writestr("xl/workbook.xml", workbook_xml)
        zf.writestr("xl/_rels/workbook.xml.rels", workbook_rels_xml)
        zf.writestr("xl/styles.xml", styles_xml)
        zf.writestr("xl/worksheets/sheet1.xml", sheet_xml)
    return output.getvalue()


__all__ = [
    "compute_summary",
    "create_job",
    "export_excel",
    "export_pdf",
    "get_all_bounds",
    "get_all_rows",
    "get_cleaned_path",
    "get_ocr_page",
    "get_ocr_openai_raw_page",
    "get_page_bounds",
    "get_page_ai_fix",
    "get_page_rows",
    "get_pages_status",
    "get_preview_path",
    "get_status",
    "get_summary",
    "list_cleaned_pages",
    "mark_job_failed",
    "mark_job_retrying",
    "mark_page_failed",
    "mark_page_retrying",
    "normalize_page_name",
    "process_job",
    "process_job_page",
    "finalize_job_processing",
    "start_job",
    "update_page_rows",
]
