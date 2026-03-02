from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, Field

from app.modules.admin.service import (
    clear_jobs_and_exports,
    get_ui_settings,
    list_job_transactions,
    set_bank_code_flags,
    set_upload_testing_enabled,
)
from app.modules.auth.deps import require_admin
from app.modules.auth.service import create_evaluator_account

router = APIRouter(prefix="/admin", tags=["admin"], dependencies=[Depends(require_admin)])


class CreateEvaluatorPayload(BaseModel):
    username: str
    password: str


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
