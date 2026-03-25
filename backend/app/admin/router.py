from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Query
from fastapi.responses import Response
from pydantic import BaseModel, Field

from app.admin.service import (
    clear_jobs_and_exports,
    export_admin_job_excel,
    export_admin_job_pdf,
    get_admin_job_result,
    get_ui_settings,
    list_admin_jobs,
    list_job_transactions,
    set_bank_code_flags,
    set_upload_testing_enabled,
)
from app.auth.deps import get_current_user, require_admin
from app.auth.service import create_evaluator_account, delete_user_account, list_users, update_user_account

router = APIRouter(prefix="/admin", tags=["admin"], dependencies=[Depends(require_admin)])


class CreateEvaluatorPayload(BaseModel):
    username: str
    password: str


class UpdateUserPayload(BaseModel):
    username: str | None = None
    password: str | None = None
    role: str | None = None


class UploadTestingTogglePayload(BaseModel):
    enabled: bool


class BankCodeFlagRowPayload(BaseModel):
    bank_id: str = Field(min_length=1)
    bank_name: str = Field(min_length=1)
    tx_code: str = Field(min_length=1)
    particulars: str = ""


class BankCodeFlagsPayload(BaseModel):
    rows: list[BankCodeFlagRowPayload] = Field(default_factory=list)


@router.post("/evaluators")
def create_evaluator(payload: CreateEvaluatorPayload):
    create_evaluator_account(payload.username, payload.password)
    return {"ok": True, "username": payload.username, "role": "evaluator"}


@router.get("/users")
def get_users(current_user=Depends(get_current_user)):
    rows = list_users(acting_username=str(current_user.get("username") or "").strip())
    return {"ok": True, "rows": rows, "count": len(rows)}


@router.patch("/users/{username}")
def update_user(username: str, payload: UpdateUserPayload, current_user=Depends(get_current_user)):
    updated = update_user_account(
        username,
        acting_username=str(current_user.get("username") or "").strip(),
        next_username=payload.username,
        next_password=payload.password,
        next_role=payload.role,
    )
    return {"ok": True, "user": updated}


@router.delete("/users/{username}")
def delete_user(username: str, current_user=Depends(get_current_user)):
    delete_user_account(username, acting_username=str(current_user.get("username") or "").strip())
    return {"ok": True}


@router.post("/clear-store")
def clear_store():
    result = clear_jobs_and_exports()
    return {"ok": True, **result}


@router.get("/job-transactions")
def get_job_transactions(
    page: int = Query(default=1, ge=1),
    limit: int = Query(default=50, ge=1, le=50),
    job_id: str | None = Query(default=None),
    page_key: str | None = Query(default=None),
    q: str | None = Query(default=None),
):
    payload = list_job_transactions(page=page, limit=limit, job_id=job_id, page_key=page_key, search=q)
    return {"ok": True, **payload}


@router.get("/jobs")
def get_jobs(
    page: int = Query(default=1, ge=1),
    limit: int = Query(default=20, ge=1, le=100),
    job_id: str | None = Query(default=None),
    status: str | None = Query(default=None),
    owner: str | None = Query(default=None),
    q: str | None = Query(default=None),
):
    payload = list_admin_jobs(page=page, limit=limit, job_id=job_id, status=status, owner=owner, search=q)
    return {"ok": True, **payload}


@router.get("/jobs/{job_id}/result")
def get_job_result(job_id: str, limit: int = Query(default=50, ge=1, le=50)):
    payload = get_admin_job_result(job_id, row_limit=limit)
    return {"ok": True, **payload}


@router.get("/jobs/{job_id}/export/pdf")
def export_job_pdf(job_id: str):
    data, filename = export_admin_job_pdf(job_id)
    return Response(
        content=data,
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.get("/jobs/{job_id}/export/excel")
def export_job_excel(job_id: str):
    data, filename = export_admin_job_excel(job_id)
    return Response(
        content=data,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.get("/settings")
def get_settings():
    return {"ok": True, **get_ui_settings()}


@router.post("/settings/upload-testing")
def set_upload_testing(payload: UploadTestingTogglePayload):
    updated = set_upload_testing_enabled(payload.enabled)
    return {"ok": True, **updated}


@router.post("/settings/bank-code-flags")
def update_bank_code_flags(payload: BankCodeFlagsPayload):
    rows: list[dict[str, Any]] = [
        {
            "bank_id": row.bank_id,
            "bank_name": row.bank_name,
            "tx_code": row.tx_code,
            "particulars": row.particulars,
        }
        for row in payload.rows
    ]
    updated = set_bank_code_flags(rows)
    return {"ok": True, **updated}
