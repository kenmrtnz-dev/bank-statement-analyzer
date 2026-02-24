from __future__ import annotations

import os

from celery import Celery


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name, str(default))
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


def _env_bool(name: str, default: bool) -> bool:
    raw = str(os.getenv(name, str(default))).strip().lower()
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    return default


REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
CELERY_BROKER_URL = os.getenv("CELERY_BROKER_URL", REDIS_URL)
CELERY_RESULT_BACKEND = os.getenv("CELERY_RESULT_BACKEND", CELERY_BROKER_URL)
CELERY_TASK_DEFAULT_QUEUE = os.getenv("CELERY_TASK_DEFAULT_QUEUE", "jobs")

celery = Celery(
    "bank_statement_analyzer",
    broker=CELERY_BROKER_URL,
    backend=CELERY_RESULT_BACKEND,
    include=["app.worker.tasks"],
)

celery.conf.update(
    task_default_queue=CELERY_TASK_DEFAULT_QUEUE,
    task_track_started=True,
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    worker_prefetch_multiplier=max(1, _env_int("CELERY_WORKER_PREFETCH_MULTIPLIER", 1)),
    broker_connection_retry_on_startup=True,
    broker_transport_options={
        "visibility_timeout": max(60, _env_int("CELERY_VISIBILITY_TIMEOUT_SECONDS", 3600)),
    },
    task_soft_time_limit=max(30, _env_int("CELERY_TASK_SOFT_TIME_LIMIT", 7200)),
    task_time_limit=max(30, _env_int("CELERY_TASK_TIME_LIMIT", 7500)),
    result_expires=max(300, _env_int("CELERY_RESULT_EXPIRES_SECONDS", 86400)),
    task_always_eager=_env_bool("CELERY_TASK_ALWAYS_EAGER", False),
    task_eager_propagates=_env_bool("CELERY_TASK_EAGER_PROPAGATES", True),
)

__all__ = ["celery"]
