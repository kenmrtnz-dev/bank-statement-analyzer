from __future__ import annotations

from fastapi import APIRouter, Depends, Query

from app.modules.auth.deps import require_evaluator_or_admin
from app.modules.crm.service import (
    create_job_from_attachment,
    download_bank_statement_attachment,
    export_job_excel_to_crm_lead,
    list_bank_statement_attachments,
)

router = APIRouter(prefix="/crm", dependencies=[Depends(require_evaluator_or_admin)])


@router.get("/attachments")
def list_attachments_endpoint(
    limit: int = Query(default=25, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    probe: str | None = Query(default=None),
    q: str | None = Query(default=None),
):
    return list_bank_statement_attachments(limit=limit, offset=offset, probe=probe, q=q)


@router.get("/attachments/{attachment_id}/file")
def download_attachment_endpoint(attachment_id: str):
    return download_bank_statement_attachment(attachment_id)


@router.post("/attachments/{attachment_id}/begin-process")
def begin_process_from_attachment_endpoint(attachment_id: str):
    return create_job_from_attachment(attachment_id=attachment_id, requested_mode="auto")


@router.post("/jobs/{job_id}/export-excel")
def export_job_excel_to_crm_endpoint(job_id: str, lead_id: str | None = None):
    return export_job_excel_to_crm_lead(job_id=job_id, lead_id=lead_id)


__all__ = ["router"]
