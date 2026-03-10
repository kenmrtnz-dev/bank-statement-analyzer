"""HTTP routes for job creation, processing, review, and export flows."""

from __future__ import annotations

import uuid
from typing import Any

from fastapi import APIRouter, Depends, File, Form, Query, UploadFile
from fastapi.responses import FileResponse, Response

from app.auth.deps import require_evaluator, require_evaluator_or_admin
from app.jobs.schemas import JobCancelResponse, JobCreateResponse, JobStartResponse
from app.jobs.service import (
    cancel_job,
    create_job,
    export_excel,
    export_pdf,
    get_all_bounds,
    get_all_rows,
    get_cleaned_path,
    get_ocr_page,
    get_ocr_openai_raw_page,
    get_parse_diagnostics,
    get_page_bounds,
    get_page_rows,
    get_pages_status,
    get_preview_path,
    get_status,
    get_summary,
    list_cleaned_pages,
    set_job_reversed,
    start_job,
    update_page_rows,
)

# Keep the router thin: auth lives in dependencies and all business logic stays in the service layer.
router = APIRouter(dependencies=[Depends(require_evaluator_or_admin)])


@router.post("/jobs", response_model=JobCreateResponse)
async def create_job_endpoint(
    file: UploadFile = File(...),
    mode: str = Form("auto"),
    parser: str = Form("auto"),
    auto_start: bool = Form(True),
    user=Depends(require_evaluator),
):
    payload = create_job(
        file_bytes=await file.read(),
        filename=file.filename,
        requested_mode=mode,
        requested_parser=parser,
        auto_start=auto_start,
        created_by=str(user.get("username") or "").strip(),
        created_role=str(user.get("role") or "").strip().lower(),
    )
    return JobCreateResponse(**payload)


@router.post("/jobs/draft", response_model=JobCreateResponse)
async def create_job_draft_endpoint(
    file: UploadFile = File(...),
    mode: str = Form("auto"),
    parser: str = Form("auto"),
    user=Depends(require_evaluator),
):
    payload = create_job(
        file_bytes=await file.read(),
        filename=file.filename,
        requested_mode=mode,
        requested_parser=parser,
        auto_start=False,
        created_by=str(user.get("username") or "").strip(),
        created_role=str(user.get("role") or "").strip().lower(),
    )
    return JobCreateResponse(**payload)


@router.post("/jobs/{job_id}/start", response_model=JobStartResponse)
def start_job_endpoint(
    job_id: uuid.UUID,
    mode: str | None = Query(default=None),
    parser: str | None = Query(default=None),
):
    payload = start_job(str(job_id), requested_mode=mode, requested_parser=parser)
    return JobStartResponse(**payload)


@router.post("/jobs/{job_id}/reverse-order")
def set_job_reverse_order_endpoint(job_id: uuid.UUID, payload: dict[str, bool]):
    return set_job_reversed(str(job_id), bool(payload.get("is_reversed", False)))


@router.post("/jobs/{job_id}/cancel", response_model=JobCancelResponse)
def cancel_job_endpoint(job_id: uuid.UUID):
    payload = cancel_job(str(job_id))
    return JobCancelResponse(**payload)


@router.delete("/jobs/{job_id}", response_model=JobCancelResponse)
def delete_job_endpoint(job_id: uuid.UUID):
    payload = cancel_job(str(job_id))
    return JobCancelResponse(**payload)


@router.get("/jobs/{job_id}")
def get_job_status_endpoint(job_id: uuid.UUID):
    return get_status(str(job_id))


@router.get("/jobs/{job_id}/pages/status")
def get_job_pages_status_endpoint(job_id: uuid.UUID):
    return get_pages_status(str(job_id))


@router.get("/jobs/{job_id}/cleaned")
def list_cleaned_endpoint(job_id: uuid.UUID):
    return {"pages": list_cleaned_pages(str(job_id))}


@router.get("/jobs/{job_id}/cleaned/{filename}")
def get_cleaned_endpoint(job_id: uuid.UUID, filename: str):
    return FileResponse(get_cleaned_path(str(job_id), filename), media_type="image/png")


@router.get("/jobs/{job_id}/preview/{page}")
def get_preview_endpoint(job_id: uuid.UUID, page: str):
    return FileResponse(get_preview_path(str(job_id), page), media_type="image/png")


@router.get("/jobs/{job_id}/ocr/{page}")
def get_ocr_endpoint(job_id: uuid.UUID, page: str):
    return get_ocr_page(str(job_id), page)


@router.get("/jobs/{job_id}/ocr/{page}/openai-raw")
def get_ocr_openai_raw_endpoint(job_id: uuid.UUID, page: str):
    return get_ocr_openai_raw_page(str(job_id), page)


@router.get("/jobs/{job_id}/rows/{page}/bounds")
def get_row_bounds_endpoint(job_id: uuid.UUID, page: str):
    return get_page_bounds(str(job_id), page)


@router.get("/jobs/{job_id}/bounds")
def get_all_bounds_endpoint(job_id: uuid.UUID):
    return get_all_bounds(str(job_id))


@router.get("/jobs/{job_id}/parsed/{page}")
def get_parsed_page_endpoint(job_id: uuid.UUID, page: str):
    return get_page_rows(str(job_id), page)


@router.put("/jobs/{job_id}/parsed/{page}")
def update_parsed_page_endpoint(job_id: uuid.UUID, page: str, rows: list[dict[str, Any]]):
    return update_page_rows(str(job_id), page, rows)


@router.get("/jobs/{job_id}/parsed")
def get_parsed_all_endpoint(job_id: uuid.UUID):
    return get_all_rows(str(job_id))


@router.get("/jobs/{job_id}/summary")
def get_summary_endpoint(job_id: uuid.UUID):
    return get_summary(str(job_id))


@router.get("/jobs/{job_id}/parse-diagnostics")
def get_parse_diagnostics_endpoint(job_id: uuid.UUID):
    return get_parse_diagnostics(str(job_id))


@router.get("/jobs/{job_id}/export/pdf")
def export_pdf_endpoint(job_id: uuid.UUID):
    data, filename = export_pdf(str(job_id))
    return Response(
        content=data,
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.get("/jobs/{job_id}/export/excel")
def export_excel_endpoint(job_id: uuid.UUID):
    data, filename = export_excel(str(job_id))
    return Response(
        content=data,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


__all__ = ["router"]
