from pydantic import BaseModel


class JobCreateResponse(BaseModel):
    job_id: str
    parse_mode: str
    started: bool


class JobStartResponse(BaseModel):
    job_id: str
    parse_mode: str
    started: bool


class JobCancelResponse(BaseModel):
    job_id: str
    cancelled: bool
    status: str
    revoked_task_ids: list[str]


__all__ = ["JobCreateResponse", "JobStartResponse", "JobCancelResponse"]
