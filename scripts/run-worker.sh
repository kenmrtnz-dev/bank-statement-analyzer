#!/usr/bin/env sh
set -eu

exec celery -A app.worker.celery_app.celery worker \
  --loglevel=INFO \
  --pool=prefork \
  --queues="${CELERY_TASK_DEFAULT_QUEUE:-jobs}" \
  --prefetch-multiplier="${CELERY_WORKER_PREFETCH_MULTIPLIER:-1}" \
  --concurrency="${CELERY_CONCURRENCY:-4}" \
  --max-tasks-per-child="${CELERY_MAX_TASKS_PER_CHILD:-25}"
