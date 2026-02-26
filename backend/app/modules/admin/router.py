from __future__ import annotations

from pydantic import BaseModel
from fastapi import APIRouter, Depends

from app.modules.admin.service import (
    clear_jobs_and_exports,
    get_ui_settings,
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


@router.post("/evaluators")
def create_evaluator(payload: CreateEvaluatorPayload):
    create_evaluator_account(payload.username, payload.password)
    return {"ok": True, "username": payload.username, "role": "evaluator"}


@router.post("/clear-store")
def clear_store():
    result = clear_jobs_and_exports()
    return {"ok": True, **result}


@router.get("/settings")
def get_settings():
    return {"ok": True, **get_ui_settings()}


@router.post("/settings/upload-testing")
def set_upload_testing(payload: UploadTestingTogglePayload):
    updated = set_upload_testing_enabled(payload.enabled)
    return {"ok": True, **updated}
