from pydantic import BaseModel


class JobCreateResponse(BaseModel):
    job_id: str
    parse_mode: str
    started: bool


class JobStartResponse(BaseModel):
    job_id: str
    parse_mode: str
    started: bool


__all__ = ["JobCreateResponse", "JobStartResponse"]
