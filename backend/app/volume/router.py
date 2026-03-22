from __future__ import annotations

import io
import json
import re
import zipfile
from pathlib import Path

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile
from fastapi.responses import HTMLResponse, Response

from app.auth.deps import require_evaluator_or_admin
from app.paths import get_project_root

router = APIRouter(dependencies=[Depends(require_evaluator_or_admin)])

STATIC_DIR = Path(__file__).resolve().parents[1] / "static"
STORAGE_ROOT = get_project_root() / "storage"
_SET_NAME_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9 ._-]{0,127}$")


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


def _list_volume_sets() -> list[dict[str, object]]:
    if not STORAGE_ROOT.exists():
        return []

    rows: list[dict[str, object]] = []
    for directory in STORAGE_ROOT.iterdir():
        if not directory.is_dir():
            continue
        files = [entry for entry in directory.iterdir() if entry.is_file()]
        updated_at = max((entry.stat().st_mtime for entry in files), default=directory.stat().st_mtime)
        total_size = sum(entry.stat().st_size for entry in files)
        rows.append(
            {
                "set_name": directory.name,
                "file_count": len(files),
                "total_size": total_size,
                "updated_at": updated_at,
            }
        )

    rows.sort(key=lambda row: (float(row["updated_at"]), str(row["set_name"]).lower()), reverse=True)
    return rows


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

    files = [entry for entry in target_dir.iterdir() if entry.is_file()]
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
):
    normalized_set_name = _sanitize_set_name(set_name)
    if not files:
        raise HTTPException(status_code=400, detail="files_required")

    target_dir = STORAGE_ROOT / normalized_set_name
    target_dir.mkdir(parents=True, exist_ok=True)

    saved_files: list[str] = []
    for upload in files:
        filename = _safe_filename(upload.filename or "")
        destination = _next_available_path(target_dir, filename)
        destination.write_bytes(await upload.read())
        saved_files.append(destination.name)

    return {
        "ok": True,
        "set_name": normalized_set_name,
        "saved_count": len(saved_files),
        "saved_dir": str(target_dir),
        "files": saved_files,
    }


__all__ = ["router", "STORAGE_ROOT"]
