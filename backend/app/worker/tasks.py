from __future__ import annotations

import os
import random

from app.modules.jobs.service import (
    finalize_job_processing,
    mark_job_failed,
    mark_job_retrying,
    mark_page_failed,
    mark_page_retrying,
    process_job,
    process_job_page,
)
from app.worker.celery_app import celery

_RETRYABLE_OSERROR_ERRNOS = {
    54,  # ECONNRESET (macOS)
    60,  # ETIMEDOUT (macOS)
    61,  # ECONNREFUSED (macOS)
    104,  # ECONNRESET (linux)
    110,  # ETIMEDOUT (linux)
    111,  # ECONNREFUSED (linux)
}
_RETRYABLE_MESSAGE_SNIPPETS = (
    "timed out",
    "timeout",
    "temporarily unavailable",
    "connection reset",
    "connection refused",
    "connection aborted",
    "rate limit",
    "too many requests",
    "service unavailable",
)


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name, str(default))
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


def _max_retries() -> int:
    return max(0, _env_int("CELERY_TASK_MAX_RETRIES", 3))


def _retry_delay_seconds(retry_attempt: int) -> int:
    base = max(1, _env_int("CELERY_TASK_RETRY_BACKOFF_SECONDS", 15))
    cap = max(base, _env_int("CELERY_TASK_RETRY_BACKOFF_MAX_SECONDS", 300))
    jitter = max(0, _env_int("CELERY_TASK_RETRY_JITTER_SECONDS", 3))

    exponent = max(0, int(retry_attempt) - 1)
    delay = min(cap, base * (2**exponent))
    if jitter:
        delay += random.randint(0, jitter)
    return max(1, delay)


def _is_retryable_exception(exc: Exception) -> bool:
    if isinstance(exc, (TimeoutError, ConnectionError)):
        return True

    if isinstance(exc, OSError):
        err_no = getattr(exc, "errno", None)
        if err_no in _RETRYABLE_OSERROR_ERRNOS:
            return True

    text = str(exc).strip().lower()
    if text and any(snippet in text for snippet in _RETRYABLE_MESSAGE_SNIPPETS):
        return True

    return False


@celery.task(
    bind=True,
    name="jobs.process_job",
    queue=os.getenv("CELERY_TASK_DEFAULT_QUEUE", "jobs"),
    acks_late=True,
    reject_on_worker_lost=True,
)
def process_job_task(self, job_id: str, parse_mode: str) -> dict:
    task_id = str(self.request.id or "").strip() or None
    retries_so_far = int(self.request.retries or 0)
    max_retries = _max_retries()

    try:
        return process_job(job_id=job_id, parse_mode=parse_mode, task_id=task_id)
    except Exception as exc:
        if _is_retryable_exception(exc) and retries_so_far < max_retries:
            next_attempt = retries_so_far + 1
            countdown = _retry_delay_seconds(next_attempt)
            mark_job_retrying(
                job_id=job_id,
                parse_mode=parse_mode,
                retry_attempt=next_attempt,
                retry_max_attempts=max_retries,
                retry_in_seconds=countdown,
                message=str(exc),
                task_id=task_id,
            )
            raise self.retry(exc=exc, countdown=countdown, max_retries=max_retries)

        mark_job_failed(
            job_id=job_id,
            parse_mode=parse_mode,
            message=str(exc),
            task_id=task_id,
            retry_attempt=retries_so_far,
            retry_max_attempts=max_retries,
        )
        raise


@celery.task(
    bind=True,
    name="jobs.process_page",
    queue=os.getenv("CELERY_TASK_DEFAULT_QUEUE", "jobs"),
    acks_late=True,
    reject_on_worker_lost=True,
)
def process_page_task(self, job_id: str, parse_mode: str, page_name: str, page_index: int, page_count: int) -> dict:
    task_id = str(self.request.id or "").strip() or None
    retries_so_far = int(self.request.retries or 0)
    max_retries = _max_retries()

    try:
        return process_job_page(
            job_id=job_id,
            parse_mode=parse_mode,
            page_name=page_name,
            page_index=int(page_index),
            page_count=int(page_count),
            task_id=task_id,
        )
    except Exception as exc:
        if _is_retryable_exception(exc) and retries_so_far < max_retries:
            next_attempt = retries_so_far + 1
            countdown = _retry_delay_seconds(next_attempt)
            mark_page_retrying(
                job_id=job_id,
                parse_mode=parse_mode,
                page_name=page_name,
                retry_attempt=next_attempt,
                retry_max_attempts=max_retries,
                retry_in_seconds=countdown,
                message=str(exc),
                task_id=task_id,
            )
            raise self.retry(exc=exc, countdown=countdown, max_retries=max_retries)

        mark_page_failed(
            job_id=job_id,
            parse_mode=parse_mode,
            page_name=page_name,
            message=str(exc),
            task_id=task_id,
            retry_attempt=retries_so_far,
            retry_max_attempts=max_retries,
        )
        raise


@celery.task(
    bind=True,
    name="jobs.finalize_job",
    queue=os.getenv("CELERY_TASK_DEFAULT_QUEUE", "jobs"),
    acks_late=True,
    reject_on_worker_lost=True,
)
def finalize_job_task(self, job_id: str, parse_mode: str) -> dict:
    task_id = str(self.request.id or "").strip() or None
    return finalize_job_processing(job_id=job_id, parse_mode=parse_mode, task_id=task_id)


__all__ = ["process_job_task", "process_page_task", "finalize_job_task"]
