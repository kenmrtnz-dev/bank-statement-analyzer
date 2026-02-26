from __future__ import annotations

import csv
import datetime as dt
import io
import json
import math
import os
import time
import uuid
import zipfile
from pathlib import Path
from typing import Any, Dict, List, Optional
from xml.sax.saxutils import escape as xml_escape

from fastapi import HTTPException
from pdf2image import convert_from_path
from PIL import Image

from app.modules.jobs.repository import JobsRepository
from app.modules.ocr import prepare_ocr_pages, process_ocr_page, resolve_parse_mode, run_pipeline
from app.statement_parser import normalize_date

DATA_DIR = os.getenv("DATA_DIR", "./data")
FALLBACK_PREVIEW_DPI = int(os.getenv("FALLBACK_PREVIEW_DPI", "130"))
PREVIEW_MAX_PIXELS = int(os.getenv("PREVIEW_MAX_PIXELS", "6000000"))
_ACTIVE_CELERY_STATES = {"PENDING", "RECEIVED", "STARTED", "RETRY"}
_PAGE_TERMINAL_STATES = {"done", "failed"}
_PAGE_ACTIVE_STATES = {"queued", "processing", "retrying"}


def normalize_page_name(page: str) -> str:
    value = str(page or "").strip().replace(".png", "")
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


def create_job(file_bytes: bytes, filename: str, requested_mode: str = "auto", auto_start: bool = True) -> Dict:
    if not str(filename or "").lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="PDF only")

    job_id = str(uuid.uuid4())
    repo = JobsRepository(DATA_DIR)
    root = repo.ensure_job_layout(job_id)

    input_pdf = root / "input" / "document.pdf"
    repo.write_bytes(input_pdf, file_bytes)
    repo.write_json(root / "meta.json", {"original_filename": filename, "requested_mode": requested_mode})

    parse_mode = resolve_parse_mode(str(input_pdf), requested_mode)
    _write_queued_status(repo, job_id, parse_mode=parse_mode)

    started = False
    if auto_start:
        started = _start_job_worker(job_id, parse_mode)

    return {"job_id": job_id, "parse_mode": parse_mode, "started": started}


def start_job(job_id: str, requested_mode: Optional[str] = None) -> Dict:
    repo = JobsRepository(DATA_DIR)
    _require_job(repo, job_id)

    input_pdf = repo.path(job_id, "input", "document.pdf")
    status = repo.read_status(job_id)
    base_mode = requested_mode or status.get("parse_mode") or "auto"
    parse_mode = resolve_parse_mode(str(input_pdf), base_mode)

    if _has_active_task(status):
        return {"job_id": job_id, "parse_mode": parse_mode, "started": False}

    _write_queued_status(repo, job_id, parse_mode=parse_mode)
    started = _start_job_worker(job_id, parse_mode)
    return {"job_id": job_id, "parse_mode": parse_mode, "started": started}


def get_status(job_id: str) -> Dict:
    repo = JobsRepository(DATA_DIR)
    _require_job(repo, job_id)
    status = repo.read_status(job_id)
    if not status:
        return {"status": "queued", "step": "queued", "progress": 0}

    payload = dict(status)
    parse_mode = str(payload.get("parse_mode") or "auto")
    runtime_status = str(payload.get("status") or "").strip().lower()
    if parse_mode == "ocr" and runtime_status in {"queued", "processing"}:
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
                if task_state in {"FAILURE", "REVOKED"}:
                    item = dict(item)
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
        return payload

    task_id = str(payload.get("task_id") or "").strip()
    if runtime_status in {"queued", "processing"} and task_id:
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
    return payload


def list_cleaned_pages(job_id: str) -> List[str]:
    repo = JobsRepository(DATA_DIR)
    _require_job(repo, job_id)

    files = repo.list_png(job_id, "cleaned")
    if files:
        return files

    parsed = _load_parsed_rows(repo, job_id)
    if parsed:
        return [f"{key}.png" for key in sorted(parsed.keys())]
    return []


def get_cleaned_path(job_id: str, filename: str) -> Path:
    repo = JobsRepository(DATA_DIR)
    _require_job(repo, job_id)

    path = repo.path(job_id, "cleaned", filename)
    if path.exists():
        return path

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

    path = repo.path(job_id, "result", "bounds.json")
    if not path.exists():
        raise HTTPException(status_code=404, detail="bounds_not_ready")
    return repo.read_json(path, default={})


def get_page_rows(job_id: str, page: str) -> List[Dict]:
    page_name = normalize_page_name(page)
    rows = get_all_rows(job_id)
    return rows.get(page_name, [])


def get_all_rows(job_id: str) -> Dict[str, List[Dict]]:
    repo = JobsRepository(DATA_DIR)
    _require_job(repo, job_id)
    return _load_parsed_rows(repo, job_id, required=True)


def update_page_rows(job_id: str, page: str, rows: List[Dict]) -> Dict:
    repo = JobsRepository(DATA_DIR)
    _require_job(repo, job_id)

    page_name = normalize_page_name(page)
    if not page_name:
        raise HTTPException(status_code=400, detail="invalid_page")
    if not isinstance(rows, list):
        raise HTTPException(status_code=400, detail="invalid_rows_payload")

    normalized_rows: List[Dict] = []
    for idx, row in enumerate(rows, start=1):
        if not isinstance(row, dict):
            raise HTTPException(status_code=400, detail="invalid_row_item")
        row_id = str(row.get("row_id") or "").strip() or f"{idx:03}"
        normalized_rows.append(
            {
                "row_id": row_id,
                "rownumber": _normalize_row_number_output(
                    row.get("rownumber"),
                    fallback=row.get("row_number"),
                ),
                "row_number": _normalize_row_cell(row.get("row_number")),
                "date": _normalize_row_date_for_output(row.get("date")),
                "description": _normalize_row_cell(row.get("description")),
                "debit": _normalize_row_amount_output(row.get("debit")),
                "credit": _normalize_row_amount_output(row.get("credit")),
                "balance": _normalize_row_amount_output(row.get("balance")),
            }
        )

    rows_by_page = _load_parsed_rows(repo, job_id, required=True)
    rows_by_page[page_name] = normalized_rows

    repo.write_json(repo.path(job_id, "result", "parsed_rows.json"), rows_by_page)
    summary = compute_summary(_flatten_rows(rows_by_page))
    repo.write_json(repo.path(job_id, "result", "summary.json"), summary)

    return {"page": page_name, "rows": normalized_rows, "summary": summary}


def get_summary(job_id: str) -> Dict:
    repo = JobsRepository(DATA_DIR)
    _require_job(repo, job_id)

    summary_path = repo.path(job_id, "result", "summary.json")
    if summary_path.exists():
        return repo.read_json(summary_path, default={})

    rows_by_page = _load_parsed_rows(repo, job_id, required=True)
    rows = _flatten_rows(rows_by_page)
    summary = compute_summary(rows)
    repo.write_json(summary_path, summary)
    return summary


def export_pdf(job_id: str) -> tuple[bytes, str]:
    rows = _flatten_rows(get_all_rows(job_id))
    summary = get_summary(job_id)
    pdf_bytes = _build_minimal_report_pdf(job_id, summary, rows)
    return pdf_bytes, f"{job_id}-summary.pdf"


def export_csv(job_id: str) -> tuple[bytes, str]:
    rows = _flatten_rows(get_all_rows(job_id))
    out = io.StringIO()
    writer = csv.DictWriter(
        out,
        fieldnames=["page", "row_id", "date", "description", "debit", "credit", "balance"],
    )
    writer.writeheader()
    for row in rows:
        writer.writerow(
            {
                "page": row.get("page"),
                "row_id": row.get("row_id"),
                "date": row.get("date"),
                "description": row.get("description"),
                "debit": row.get("debit"),
                "credit": row.get("credit"),
                "balance": row.get("balance"),
            }
        )
    return out.getvalue().encode("utf-8"), f"{job_id}-rows.csv"


def export_excel(job_id: str) -> tuple[bytes, str]:
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
    return workbook_bytes, f"{job_id}-rows.xlsx"


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
    if latest_state in {"done", "failed"}:
        if task_id and not latest_status.get("task_id"):
            latest_payload = dict(latest_status)
            latest_payload["task_id"] = task_id
            repo.write_status(job_id, latest_payload)
    else:
        _write_queued_status(repo, job_id, parse_mode=parse_mode, task_id=task_id)
    return True


def process_job(job_id: str, parse_mode: str, task_id: Optional[str] = None) -> Dict[str, Any]:
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

    report("processing", "initializing", 1)
    selected_mode = str(parse_mode or "auto").strip().lower()
    if selected_mode != "ocr":
        result = run_pipeline(root, parse_mode, report=report)
        rows = _flatten_rows(result.get("parsed_rows") or {})
        summary = compute_summary(rows)
        repo.write_json(repo.path(job_id, "result", "summary.json"), summary)

        done_payload = {
            "status": "done",
            "step": "completed",
            "progress": 100,
            "parse_mode": result.get("parse_mode", parse_mode),
            "pages": int(result.get("pages") or len(result.get("parsed_rows") or {})),
        }
        if task_id:
            done_payload["task_id"] = task_id
        repo.write_status(job_id, done_payload)
        return done_payload

    input_pdf = repo.path(job_id, "input", "document.pdf")
    pages_dir = repo.path(job_id, "pages")
    cleaned_dir = repo.path(job_id, "cleaned")
    ocr_dir = repo.path(job_id, "ocr")
    result_dir = repo.path(job_id, "result")
    fragments_dir = result_dir / "page_fragments"
    fragments_dir.mkdir(parents=True, exist_ok=True)

    manifest = _load_pages_manifest(repo, job_id)
    page_files = manifest.get("pages") if isinstance(manifest.get("pages"), list) else []
    if not page_files:
        page_files = prepare_ocr_pages(input_pdf=input_pdf, pages_dir=pages_dir, cleaned_dir=cleaned_dir, report=report)
        _write_pages_manifest(repo, job_id, page_files)

    page_status = _load_page_status_map(repo, job_id)
    pending_pages: List[str] = []
    active_task_ids: List[str] = []
    now_iso = dt.datetime.utcnow().isoformat() + "Z"
    for idx, page_file in enumerate(page_files, start=1):
        page_name = str(page_file).replace(".png", "")
        page_payload = dict(page_status.get(page_name) or {})
        existing_fragment = _page_fragment_path(repo, job_id, page_name)
        if existing_fragment.exists():
            page_payload["status"] = "done"
            page_payload["updated_at"] = now_iso
        state = str(page_payload.get("status") or "").strip().lower()
        if state == "done":
            page_status[page_name] = page_payload
            continue
        if state in _PAGE_ACTIVE_STATES and _is_celery_task_active(str(page_payload.get("task_id") or "")):
            page_status[page_name] = page_payload
            task_ref = str(page_payload.get("task_id") or "").strip()
            if task_ref:
                active_task_ids.append(task_ref)
            continue
        pending_pages.append(page_name)
        page_status[page_name] = {
            "status": "queued",
            "page_index": idx,
            "page_count": len(page_files),
            "retry_attempt": int(page_payload.get("retry_attempt") or 0),
            "updated_at": now_iso,
        }

    _write_page_status_map(repo, job_id, page_status)

    for page_name in pending_pages:
        page_payload = page_status.get(page_name) or {}
        page_index = int(page_payload.get("page_index") or 1)
        task_id_page = _enqueue_page_job(job_id=job_id, parse_mode=parse_mode, page_name=page_name, page_index=page_index, page_count=len(page_files))
        page_payload["task_id"] = task_id_page
        page_payload["status"] = "queued"
        page_payload["updated_at"] = dt.datetime.utcnow().isoformat() + "Z"
        page_status[page_name] = page_payload
        active_task_ids.append(task_id_page)
    _write_page_status_map(repo, job_id, page_status)

    done_pages, failed_pages = _count_page_states(page_status)
    inflight_pages = max(0, len(page_files) - done_pages - failed_pages)
    progress = _compute_page_progress(total=len(page_files), done=done_pages, failed=failed_pages)
    queued_payload: Dict[str, Any] = {
        "status": "processing",
        "step": "ocr_parsing",
        "progress": progress,
        "parse_mode": parse_mode,
        "pages_total": len(page_files),
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


def process_job_page(
    job_id: str,
    parse_mode: str,
    page_name: str,
    page_index: int,
    page_count: int,
    task_id: Optional[str] = None,
) -> Dict[str, Any]:
    repo = JobsRepository(DATA_DIR)
    _require_job(repo, job_id)
    cleaned_dir = repo.path(job_id, "cleaned")
    ocr_dir = repo.path(job_id, "ocr")

    page_file = f"{normalize_page_name(page_name)}.png"
    page_name = page_file.replace(".png", "")
    if not (cleaned_dir / page_file).exists():
        raise RuntimeError(f"cleaned_page_missing:{page_file}")

    page_status = _load_page_status_map(repo, job_id)
    payload = dict(page_status.get(page_name) or {})
    payload["status"] = "processing"
    payload["page_index"] = int(page_index)
    payload["page_count"] = int(page_count)
    payload["updated_at"] = dt.datetime.utcnow().isoformat() + "Z"
    if task_id:
        payload["task_id"] = task_id
    page_status[page_name] = payload
    _write_page_status_map(repo, job_id, page_status)
    _refresh_job_progress(repo, job_id, parse_mode=parse_mode, active_task_id=task_id)

    def _heartbeat(wait_seconds: float):
        current = _load_page_status_map(repo, job_id)
        target = dict(current.get(page_name) or {})
        if str(target.get("status") or "").strip().lower() == "done":
            return
        target["status"] = "processing"
        target["step"] = "rate_limit_wait"
        target["wait_seconds"] = round(float(wait_seconds), 3)
        target["updated_at"] = dt.datetime.utcnow().isoformat() + "Z"
        current[page_name] = target
        _write_page_status_map(repo, job_id, current)

    page_name, page_rows, page_bounds, page_diag = process_ocr_page(
        page_file=page_file,
        cleaned_dir=cleaned_dir,
        ocr_dir=ocr_dir,
        rate_limit_heartbeat=_heartbeat,
    )
    _write_page_fragment(repo, job_id, page_name, page_rows=page_rows, page_bounds=page_bounds, page_diag=page_diag)

    page_status = _load_page_status_map(repo, job_id)
    payload = dict(page_status.get(page_name) or {})
    payload["status"] = "done"
    payload["rows_parsed"] = int(page_diag.get("rows_parsed") or len(page_rows))
    payload["updated_at"] = dt.datetime.utcnow().isoformat() + "Z"
    page_status[page_name] = payload
    _write_page_status_map(repo, job_id, page_status)

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
    repo = JobsRepository(DATA_DIR)
    page_name = normalize_page_name(page_name)
    page_status = _load_page_status_map(repo, job_id)
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
    _write_page_status_map(repo, job_id, page_status)
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
    repo = JobsRepository(DATA_DIR)
    page_name = normalize_page_name(page_name)
    page_status = _load_page_status_map(repo, job_id)
    payload = dict(page_status.get(page_name) or {})
    payload["status"] = "failed"
    payload["message"] = _error_message(message)
    payload["retry_attempt"] = max(0, int(retry_attempt))
    payload["retry_max_attempts"] = max(0, int(retry_max_attempts))
    payload["updated_at"] = dt.datetime.utcnow().isoformat() + "Z"
    if task_id:
        payload["task_id"] = task_id
    page_status[page_name] = payload
    _write_page_status_map(repo, job_id, page_status)
    merged = _refresh_job_progress(repo, job_id, parse_mode=parse_mode, active_task_id=task_id)
    if merged.get("pages_total", 0) > 0 and int(merged.get("pages_inflight") or 0) == 0:
        try:
            _enqueue_finalize_job(job_id=job_id, parse_mode=parse_mode)
        except Exception:
            pass


def finalize_job_processing(job_id: str, parse_mode: str, task_id: Optional[str] = None) -> Dict[str, Any]:
    repo = JobsRepository(DATA_DIR)
    _require_job(repo, job_id)

    page_status = _load_page_status_map(repo, job_id)
    manifest = _load_pages_manifest(repo, job_id)
    page_files = manifest.get("pages") if isinstance(manifest.get("pages"), list) else []
    if not page_files:
        raise RuntimeError("pages_manifest_missing")

    parsed_output: Dict[str, List[Dict]] = {}
    bounds_output: Dict[str, List[Dict]] = {}
    diagnostics: Dict[str, Dict] = {"job": {"source_type": "ocr", "ocr_backend": "openai_vision", "parse_mode": parse_mode}, "pages": {}}

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
        error = str((page_status.get(page_name) or {}).get("message") or "page_processing_failed")
        if state != "failed":
            error = "page_not_completed"
        failed_list.append({"page": page_name, "error": error})

    result_dir = repo.path(job_id, "result")
    parsed_output = _normalize_rows_by_page_for_output(parsed_output)
    repo.write_json(result_dir / "parsed_rows.json", parsed_output)
    repo.write_json(result_dir / "bounds.json", bounds_output)
    repo.write_json(result_dir / "parse_diagnostics.json", diagnostics)

    rows = _flatten_rows(parsed_output)
    summary = compute_summary(rows)
    repo.write_json(result_dir / "summary.json", summary)

    if success_pages == 0:
        status_value = "failed"
        step_value = "failed"
    elif failed_list:
        status_value = "done_with_warnings"
        step_value = "completed_with_warnings"
    else:
        status_value = "done"
        step_value = "completed"

    final_payload: Dict[str, Any] = {
        "status": status_value,
        "step": step_value,
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
    return final_payload


def mark_job_retrying(
    job_id: str,
    parse_mode: str,
    retry_attempt: int,
    retry_max_attempts: int,
    retry_in_seconds: int,
    message: str = "",
    task_id: Optional[str] = None,
) -> None:
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


def _pages_manifest_path(repo: JobsRepository, job_id: str) -> Path:
    return repo.path(job_id, "result", "pages_manifest.json")


def _page_status_path(repo: JobsRepository, job_id: str) -> Path:
    return repo.path(job_id, "result", "page_status.json")


def _page_fragment_path(repo: JobsRepository, job_id: str, page_name: str) -> Path:
    return repo.path(job_id, "result", "page_fragments", f"{normalize_page_name(page_name)}.json")


def _load_pages_manifest(repo: JobsRepository, job_id: str) -> Dict[str, Any]:
    path = _pages_manifest_path(repo, job_id)
    data = repo.read_json(path, default={})
    return data if isinstance(data, dict) else {}


def get_pages_status(job_id: str) -> Dict[str, Dict[str, Any]]:
    repo = JobsRepository(DATA_DIR)
    _require_job(repo, job_id)
    status_map = _load_page_status_map(repo, job_id)
    manifest = _load_pages_manifest(repo, job_id)
    pages = manifest.get("pages") if isinstance(manifest.get("pages"), list) else []
    if not pages:
        return status_map
    out: Dict[str, Dict[str, Any]] = {}
    for idx, page_file in enumerate(pages, start=1):
        page_name = str(page_file).replace(".png", "")
        payload = dict(status_map.get(page_name) or {})
        payload.setdefault("page_index", idx)
        payload.setdefault("page_count", len(pages))
        out[page_name] = payload
    return out


def _write_pages_manifest(repo: JobsRepository, job_id: str, page_files: List[str]) -> None:
    payload = {
        "pages": list(page_files),
        "updated_at": dt.datetime.utcnow().isoformat() + "Z",
    }
    repo.write_json(_pages_manifest_path(repo, job_id), payload)


def _load_page_status_map(repo: JobsRepository, job_id: str) -> Dict[str, Dict[str, Any]]:
    path = _page_status_path(repo, job_id)
    data = repo.read_json(path, default={})
    if not isinstance(data, dict):
        return {}
    out: Dict[str, Dict[str, Any]] = {}
    for key, value in data.items():
        if isinstance(value, dict):
            out[str(key)] = dict(value)
    return out


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
    status = repo.read_status(job_id)
    page_status = _load_page_status_map(repo, job_id)
    manifest = _load_pages_manifest(repo, job_id)
    total = len(manifest.get("pages") or [])
    done, failed = _count_page_states(page_status)
    inflight = max(0, total - done - failed)
    active_task_ids: List[str] = []
    for item in page_status.values():
        state = str(item.get("status") or "").strip().lower()
        if state not in _PAGE_ACTIVE_STATES:
            continue
        task_ref = str(item.get("task_id") or "").strip()
        if task_ref:
            active_task_ids.append(task_ref)
    payload: Dict[str, Any] = dict(status or {})
    payload.update(
        {
            "status": "processing",
            "step": "ocr_parsing",
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
    if status not in {"queued", "processing"}:
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


def _load_parsed_rows(repo: JobsRepository, job_id: str, required: bool = False) -> Dict[str, List[Dict]]:
    path = repo.path(job_id, "result", "parsed_rows.json")
    if not path.exists():
        if required:
            raise HTTPException(status_code=404, detail="parsed_rows_not_ready")
        return {}
    data = repo.read_json(path, default={})
    if not isinstance(data, dict):
        return {}
    normalized = _normalize_rows_by_page_for_output(data)
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
            return dt.datetime.strptime(raw, fmt).date()
        except Exception:
            continue
    return None


def compute_summary(rows: List[Dict]) -> Dict:
    tx_count = len(rows)
    debit_count = 0
    credit_count = 0
    total_debit = 0.0
    total_credit = 0.0
    daily_balances: Dict[dt.date, float] = {}
    monthly: Dict[str, Dict] = {}

    normalized = []
    for row in rows:
        date = _parse_date(row.get("date"))
        debit = _to_float(row.get("debit"))
        credit = _to_float(row.get("credit"))
        balance = _to_float(row.get("balance"))
        if debit is not None and abs(debit) > 0:
            debit_count += 1
            total_debit += abs(debit)
        if credit is not None and abs(credit) > 0:
            credit_count += 1
            total_credit += abs(credit)
        if date and balance is not None:
            daily_balances[date] = balance
        normalized.append((date, debit, credit, balance))

    ending_balance = None
    for _, _, _, bal in reversed(normalized):
        if bal is not None:
            ending_balance = bal
            break

    sorted_days = sorted(daily_balances.items(), key=lambda item: item[0])
    adb = None
    if sorted_days:
        weighted = 0.0
        total_days = 0
        for idx, (day, bal) in enumerate(sorted_days):
            next_day = sorted_days[idx + 1][0] if idx < len(sorted_days) - 1 else day + dt.timedelta(days=1)
            span = max(1, (next_day - day).days)
            weighted += bal * span
            total_days += span
        if total_days > 0:
            adb = weighted / total_days

    for date, debit, credit, balance in normalized:
        if not date:
            continue
        key = date.strftime("%Y-%m")
        bucket = monthly.setdefault(
            key,
            {
                "month": key,
                "debit": 0.0,
                "credit": 0.0,
                "debit_count": 0,
                "credit_count": 0,
                "balance_weighted": 0.0,
                "days": 0,
            },
        )
        if debit is not None:
            bucket["debit"] += abs(debit)
            if abs(debit) > 0:
                bucket["debit_count"] += 1
        if credit is not None:
            bucket["credit"] += abs(credit)
            if abs(credit) > 0:
                bucket["credit_count"] += 1
        if balance is not None:
            bucket["balance_weighted"] += balance
            bucket["days"] += 1

    monthly_rows = []
    for key in sorted(monthly.keys()):
        item = monthly[key]
        monthly_rows.append(
            {
                "month": key,
                "debit": round(item["debit"], 2),
                "credit": round(item["credit"], 2),
                "avg_debit": round((item["debit"] / item["debit_count"]), 2) if item["debit_count"] else 0.0,
                "avg_credit": round((item["credit"] / item["credit_count"]), 2) if item["credit_count"] else 0.0,
                "adb": round((item["balance_weighted"] / item["days"]), 2) if item["days"] else 0.0,
            }
        )

    return {
        "total_transactions": tx_count,
        "debit_transactions": debit_count,
        "credit_transactions": credit_count,
        "total_debit": round(total_debit, 2),
        "total_credit": round(total_credit, 2),
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
        f"Ending Balance: {summary.get('ending_balance')}",
        f"ADB: {summary.get('adb')}",
        "",
        "Top Transactions:",
    ]
    for row in rows[:25]:
        lines.append(
            f"{row.get('date') or '-'} | {row.get('description') or '-'} | D:{row.get('debit')} C:{row.get('credit')} B:{row.get('balance')}"
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
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 595 842] /Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 R >>"
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
    "export_csv",
    "export_excel",
    "export_pdf",
    "get_all_bounds",
    "get_all_rows",
    "get_cleaned_path",
    "get_ocr_page",
    "get_ocr_openai_raw_page",
    "get_page_bounds",
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
