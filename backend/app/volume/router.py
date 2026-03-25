from __future__ import annotations

import datetime as dt
import io
import json
import re
import zipfile
from pathlib import Path

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile
from fastapi.responses import HTMLResponse, Response

from app.auth.deps import require_admin, require_evaluator_or_admin
from app.jobs.repository import JobsRepository
from app.jobs.service import create_job, get_status
from app.paths import get_data_dir, get_project_root

router = APIRouter(dependencies=[Depends(require_evaluator_or_admin)])

STATIC_DIR = Path(__file__).resolve().parents[1] / "static"
STORAGE_ROOT = get_project_root() / "storage"
_SET_NAME_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9 ._-]{0,127}$")
_SET_META_FILENAME = ".volume-set.json"
_VOLUME_ACTIVE_STATES = {"queued", "processing"}
_VOLUME_DONE_STATES = {"done", "done_with_warnings"}
_VOLUME_RETRYABLE_STATES = {"failed", "cancelled"}


def _utcnow_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _sanitize_set_name(value: str) -> str:
    cleaned = str(value or "").strip()
    if not cleaned:
        raise HTTPException(status_code=400, detail="set_name_required")
    if cleaned in {".", ".."}:
        raise HTTPException(status_code=400, detail="invalid_set_name")
    if "/" in cleaned or "\\" in cleaned or "\x00" in cleaned:
        raise HTTPException(status_code=400, detail="invalid_set_name")
    if not _SET_NAME_PATTERN.fullmatch(cleaned):
        raise HTTPException(status_code=400, detail="invalid_set_name")
    return cleaned


def _safe_filename(value: str) -> str:
    name = Path(str(value or "").strip()).name.strip()
    if not name or name in {".", ".."}:
        raise HTTPException(status_code=400, detail="invalid_filename")
    return name


def _next_available_path(target_dir: Path, filename: str) -> Path:
    candidate = target_dir / filename
    if not candidate.exists():
        return candidate

    stem = candidate.stem
    suffix = candidate.suffix
    index = 2
    while True:
        next_candidate = target_dir / f"{stem}_{index}{suffix}"
        if not next_candidate.exists():
            return next_candidate
        index += 1


def _visible_set_files(target_dir: Path) -> list[Path]:
    if not target_dir.exists() or not target_dir.is_dir():
        return []
    return sorted(
        [
            entry
            for entry in target_dir.iterdir()
            if entry.is_file() and not entry.name.startswith(".")
        ],
        key=lambda entry: entry.name.lower(),
    )


def _set_meta_path(target_dir: Path) -> Path:
    return target_dir / _SET_META_FILENAME


def _is_volume_set_dir(target_dir: Path) -> bool:
    if not target_dir.exists() or not target_dir.is_dir():
        return False
    if _set_meta_path(target_dir).exists():
        return True
    return any(entry.suffix.lower() == ".pdf" for entry in _visible_set_files(target_dir))


def _read_set_meta(target_dir: Path) -> dict[str, object]:
    default_payload: dict[str, object] = {
        "set_name": target_dir.name,
        "uploader_username": "",
        "uploader_role": "",
        "created_at": "",
        "updated_at": "",
        "files": {},
    }
    meta_path = _set_meta_path(target_dir)
    if not meta_path.exists():
        return default_payload
    try:
        payload = json.loads(meta_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return default_payload
    if not isinstance(payload, dict):
        return default_payload

    out = dict(default_payload)
    out.update(payload)
    files_payload = out.get("files")
    out["files"] = files_payload if isinstance(files_payload, dict) else {}
    uploader_username = str(out.get("uploader_username") or out.get("uploaded_by") or "").strip()
    uploader_role = str(out.get("uploader_role") or "").strip().lower()
    out["uploader_username"] = uploader_username
    out["uploader_role"] = uploader_role
    return out


def _write_set_meta(target_dir: Path, payload: dict[str, object]) -> None:
    safe_payload = dict(payload or {})
    safe_payload["set_name"] = target_dir.name
    files_payload = safe_payload.get("files")
    safe_payload["files"] = files_payload if isinstance(files_payload, dict) else {}
    meta_path = _set_meta_path(target_dir)
    meta_path.parent.mkdir(parents=True, exist_ok=True)
    meta_path.write_text(json.dumps(safe_payload, indent=2), encoding="utf-8")


def _volume_status_from_job(status_value: str | None, *, is_pdf: bool, has_job: bool) -> str:
    normalized = str(status_value or "").strip().lower()
    if not is_pdf:
        return "unsupported"
    if not has_job:
        return "pending"
    return normalized or "queued"


def _load_job_status(job_id: str | None) -> dict[str, object]:
    cleaned_job_id = str(job_id or "").strip()
    if not cleaned_job_id:
        return {}
    try:
        payload = get_status(cleaned_job_id)
    except HTTPException:
        return {}
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _build_volume_file_rows(target_dir: Path, meta_payload: dict[str, object]) -> list[dict[str, object]]:
    files_meta = meta_payload.get("files")
    files_state = files_meta if isinstance(files_meta, dict) else {}
    raw_rows: list[dict[str, object]] = []
    for file_path in _visible_set_files(target_dir):
        stat = file_path.stat()
        entry = files_state.get(file_path.name)
        file_state = dict(entry) if isinstance(entry, dict) else {}
        last_job_id = str(file_state.get("last_job_id") or "").strip()
        if not last_job_id:
            job_ids = file_state.get("job_ids")
            if isinstance(job_ids, list):
                for candidate in reversed(job_ids):
                    cleaned = str(candidate or "").strip()
                    if cleaned:
                        last_job_id = cleaned
                        break
        is_pdf = file_path.suffix.lower() == ".pdf"
        job_payload = _load_job_status(last_job_id)
        job_status = str(job_payload.get("status") or "").strip().lower()
        volume_status = _volume_status_from_job(job_status, is_pdf=is_pdf, has_job=bool(last_job_id))
        try:
            progress = max(0, min(100, int(job_payload.get("progress") or 0)))
        except (TypeError, ValueError):
            progress = 0
        raw_rows.append(
            {
                "file_name": file_path.name,
                "size_bytes": stat.st_size,
                "updated_at": stat.st_mtime,
                "is_pdf": is_pdf,
                "job_id": last_job_id,
                "job_status": job_status,
                "volume_status": volume_status,
                "job_step": str(job_payload.get("step") or "").strip(),
                "progress": progress,
                "parse_mode": str(job_payload.get("parse_mode") or "").strip(),
                "last_started_at": str(file_state.get("last_started_at") or "").strip(),
                "last_started_by": str(file_state.get("last_started_by") or "").strip(),
                "last_started_for": str(file_state.get("last_started_for") or "").strip(),
            }
        )

    has_active_job = any(str(row.get("volume_status") or "") in _VOLUME_ACTIVE_STATES for row in raw_rows)
    rows: list[dict[str, object]] = []
    for row in raw_rows:
        status_value = str(row.get("volume_status") or "")
        can_start = bool(row.get("is_pdf")) and status_value in {"pending", *_VOLUME_RETRYABLE_STATES} and not has_active_job
        rows.append(
            {
                **row,
                "can_start": can_start,
                "can_open_job": bool(row.get("job_id")),
                "is_completed": status_value in _VOLUME_DONE_STATES,
                "is_active": status_value in _VOLUME_ACTIVE_STATES,
            }
        )
    return rows


def _build_volume_set_payload(target_dir: Path) -> dict[str, object]:
    meta_payload = _read_set_meta(target_dir)
    file_rows = _build_volume_file_rows(target_dir, meta_payload)
    visible_files = _visible_set_files(target_dir)
    updated_at = max((entry.stat().st_mtime for entry in visible_files), default=target_dir.stat().st_mtime)
    total_size = sum(entry.stat().st_size for entry in visible_files)

    pending_count = 0
    active_count = 0
    completed_count = 0
    failed_count = 0
    unsupported_count = 0
    next_file_name = ""
    for row in file_rows:
        status_value = str(row.get("volume_status") or "")
        if not next_file_name and bool(row.get("can_start")):
            next_file_name = str(row.get("file_name") or "")
        if status_value == "pending":
            pending_count += 1
        elif status_value in _VOLUME_ACTIVE_STATES:
            active_count += 1
        elif status_value in _VOLUME_DONE_STATES:
            completed_count += 1
        elif status_value in _VOLUME_RETRYABLE_STATES:
            failed_count += 1
        elif status_value == "unsupported":
            unsupported_count += 1

    return {
        "set_name": target_dir.name,
        "file_count": len(file_rows),
        "total_size": total_size,
        "updated_at": updated_at,
        "uploader_username": str(meta_payload.get("uploader_username") or "").strip(),
        "uploader_role": str(meta_payload.get("uploader_role") or "").strip().lower(),
        "created_at": str(meta_payload.get("created_at") or "").strip(),
        "pending_count": pending_count,
        "active_count": active_count,
        "completed_count": completed_count,
        "failed_count": failed_count,
        "unsupported_count": unsupported_count,
        "has_active_job": active_count > 0,
        "next_file_name": next_file_name,
        "files": file_rows,
    }


def _list_volume_sets() -> list[dict[str, object]]:
    if not STORAGE_ROOT.exists():
        return []

    rows: list[dict[str, object]] = []
    for directory in STORAGE_ROOT.iterdir():
        if not _is_volume_set_dir(directory):
            continue
        rows.append(_build_volume_set_payload(directory))

    rows.sort(key=lambda row: (float(row["updated_at"]), str(row["set_name"]).lower()), reverse=True)
    return rows


def _get_volume_set_payload(set_name: str) -> dict[str, object]:
    normalized_set_name = _sanitize_set_name(set_name)
    target_dir = STORAGE_ROOT / normalized_set_name
    if not _is_volume_set_dir(target_dir):
        raise HTTPException(status_code=404, detail="set_not_found")
    return _build_volume_set_payload(target_dir)


def _update_job_meta_for_volume(
    *,
    job_id: str,
    set_name: str,
    file_name: str,
    owner_username: str,
    owner_role: str,
    started_by: str,
) -> None:
    repo = JobsRepository(get_data_dir())
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


def _start_volume_file(*, set_name: str, file_name: str, admin_user: dict[str, object]) -> dict[str, object]:
    normalized_set_name = _sanitize_set_name(set_name)
    normalized_file_name = _safe_filename(file_name)
    target_dir = STORAGE_ROOT / normalized_set_name
    if not target_dir.exists() or not target_dir.is_dir():
        raise HTTPException(status_code=404, detail="set_not_found")

    detail = _build_volume_set_payload(target_dir)
    selected_file = next(
        (row for row in detail.get("files", []) if str(row.get("file_name") or "") == normalized_file_name),
        None,
    )
    if not isinstance(selected_file, dict):
        raise HTTPException(status_code=404, detail="volume_file_not_found")
    if not bool(selected_file.get("is_pdf")):
        raise HTTPException(status_code=400, detail="volume_file_pdf_required")
    if str(selected_file.get("volume_status") or "") in _VOLUME_DONE_STATES:
        raise HTTPException(status_code=409, detail="volume_file_already_processed")
    if detail.get("has_active_job") and str(selected_file.get("volume_status") or "") not in _VOLUME_ACTIVE_STATES:
        raise HTTPException(status_code=409, detail="volume_set_has_active_job")
    if str(selected_file.get("volume_status") or "") in _VOLUME_ACTIVE_STATES:
        raise HTTPException(status_code=409, detail="volume_file_already_processing")

    file_path = target_dir / normalized_file_name
    if not file_path.exists() or not file_path.is_file():
        raise HTTPException(status_code=404, detail="volume_file_not_found")

    meta_payload = _read_set_meta(target_dir)
    owner_username = str(meta_payload.get("uploader_username") or admin_user.get("username") or "").strip()
    owner_role = str(meta_payload.get("uploader_role") or admin_user.get("role") or "").strip().lower()

    payload = create_job(
        file_bytes=file_path.read_bytes(),
        filename=normalized_file_name,
        requested_mode="auto",
        requested_parser="auto",
        auto_start=True,
        created_by=owner_username,
        created_role=owner_role,
    )
    job_id = str(payload.get("job_id") or "").strip()
    if not job_id:
        raise HTTPException(status_code=500, detail="volume_job_create_failed")

    _update_job_meta_for_volume(
        job_id=job_id,
        set_name=normalized_set_name,
        file_name=normalized_file_name,
        owner_username=owner_username,
        owner_role=owner_role,
        started_by=str(admin_user.get("username") or "").strip(),
    )

    files_meta = meta_payload.get("files")
    if not isinstance(files_meta, dict):
        files_meta = {}
    file_state = files_meta.get(normalized_file_name)
    file_payload = dict(file_state) if isinstance(file_state, dict) else {}
    job_ids = file_payload.get("job_ids")
    job_id_list = [str(item or "").strip() for item in job_ids] if isinstance(job_ids, list) else []
    if job_id not in job_id_list:
        job_id_list.append(job_id)
    file_payload["job_ids"] = [item for item in job_id_list if item]
    file_payload["last_job_id"] = job_id
    file_payload["last_started_at"] = _utcnow_iso()
    file_payload["last_started_by"] = str(admin_user.get("username") or "").strip()
    file_payload["last_started_for"] = owner_username
    files_meta[normalized_file_name] = file_payload
    meta_payload["files"] = files_meta
    if owner_username and not str(meta_payload.get("uploader_username") or "").strip():
        meta_payload["uploader_username"] = owner_username
    if owner_role and not str(meta_payload.get("uploader_role") or "").strip():
        meta_payload["uploader_role"] = owner_role
    meta_payload["updated_at"] = _utcnow_iso()
    if not str(meta_payload.get("created_at") or "").strip():
        meta_payload["created_at"] = meta_payload["updated_at"]
    _write_set_meta(target_dir, meta_payload)

    return {
        "ok": True,
        "job_id": job_id,
        "set_name": normalized_set_name,
        "file_name": normalized_file_name,
        "owner_username": owner_username,
        "owner_role": owner_role,
        "started": bool(payload.get("started")),
        "item": _build_volume_set_payload(target_dir),
    }


@router.get("/volume")
def volume_page():
    template = (STATIC_DIR / "volume.html").read_text(encoding="utf-8")
    initial_sets = json.dumps(_list_volume_sets()).replace("</", "<\\/")
    html = template.replace("__VOLUME_INITIAL_SETS__", initial_sets, 1)
    response = HTMLResponse(html)
    response.headers["Cache-Control"] = "no-store"
    return response


@router.get("/volume/sets")
def list_volume_sets():
    return {"ok": True, "items": _list_volume_sets()}


@router.get("/volume/sets/{set_name}/download")
def download_volume_set(set_name: str):
    normalized_set_name = _sanitize_set_name(set_name)
    target_dir = STORAGE_ROOT / normalized_set_name
    if not target_dir.exists() or not target_dir.is_dir():
        raise HTTPException(status_code=404, detail="set_not_found")

    files = _visible_set_files(target_dir)
    if not files:
        raise HTTPException(status_code=404, detail="set_has_no_files")

    output = io.BytesIO()
    with zipfile.ZipFile(output, mode="w", compression=zipfile.ZIP_DEFLATED) as archive:
        for file_path in sorted(files, key=lambda entry: entry.name.lower()):
            archive.write(file_path, arcname=f"{normalized_set_name}/{file_path.name}")

    filename = f"{normalized_set_name}.zip"
    return Response(
        content=output.getvalue(),
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.post("/volume/upload")
async def upload_volume_files(
    set_name: str = Form(...),
    files: list[UploadFile] = File(...),
    user=Depends(require_evaluator_or_admin),
):
    normalized_set_name = _sanitize_set_name(set_name)
    if not files:
        raise HTTPException(status_code=400, detail="files_required")

    target_dir = STORAGE_ROOT / normalized_set_name
    target_dir.mkdir(parents=True, exist_ok=True)
    meta_payload = _read_set_meta(target_dir)
    if not str(meta_payload.get("created_at") or "").strip():
        meta_payload["created_at"] = _utcnow_iso()
    if not str(meta_payload.get("uploader_username") or "").strip():
        meta_payload["uploader_username"] = str(user.get("username") or "").strip()
    if not str(meta_payload.get("uploader_role") or "").strip():
        meta_payload["uploader_role"] = str(user.get("role") or "").strip().lower()
    files_meta = meta_payload.get("files")
    if not isinstance(files_meta, dict):
        files_meta = {}

    saved_files: list[str] = []
    for upload in files:
        filename = _safe_filename(upload.filename or "")
        destination = _next_available_path(target_dir, filename)
        destination.write_bytes(await upload.read())
        saved_files.append(destination.name)
        files_meta[destination.name] = {
            **(files_meta.get(destination.name) if isinstance(files_meta.get(destination.name), dict) else {}),
            "added_at": _utcnow_iso(),
            "original_filename": filename,
        }

    meta_payload["files"] = files_meta
    meta_payload["updated_at"] = _utcnow_iso()
    _write_set_meta(target_dir, meta_payload)

    return {
        "ok": True,
        "set_name": normalized_set_name,
        "saved_count": len(saved_files),
        "saved_dir": str(target_dir),
        "files": saved_files,
        "uploader_username": str(meta_payload.get("uploader_username") or "").strip(),
        "uploader_role": str(meta_payload.get("uploader_role") or "").strip().lower(),
    }


@router.get("/admin/volume-sets", dependencies=[Depends(require_admin)])
def list_admin_volume_sets():
    return {"ok": True, "items": _list_volume_sets()}


@router.get("/admin/volume-sets/{set_name}", dependencies=[Depends(require_admin)])
def get_admin_volume_set(set_name: str):
    return {"ok": True, "item": _get_volume_set_payload(set_name)}


@router.post("/admin/volume-sets/{set_name}/start-next", dependencies=[Depends(require_admin)])
def start_next_admin_volume_file(set_name: str, user=Depends(require_admin)):
    detail = _get_volume_set_payload(set_name)
    next_file_name = str(detail.get("next_file_name") or "").strip()
    if not next_file_name:
        if detail.get("has_active_job"):
            raise HTTPException(status_code=409, detail="volume_set_has_active_job")
        raise HTTPException(status_code=409, detail="volume_set_has_no_pending_files")
    return _start_volume_file(set_name=set_name, file_name=next_file_name, admin_user=user)


@router.post("/admin/volume-sets/{set_name}/files/{file_name}/start", dependencies=[Depends(require_admin)])
def start_admin_volume_file(set_name: str, file_name: str, user=Depends(require_admin)):
    return _start_volume_file(set_name=set_name, file_name=file_name, admin_user=user)


__all__ = ["router", "STORAGE_ROOT"]
