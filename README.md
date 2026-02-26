# Bank Statement Analyzer

Minimal bank-statement workflow:
- Upload PDF
- Auto-select parser (`text` when PDF has text layer, otherwise `ocr`)
- Extract transaction rows + row bounds
- View parsed output + summary
- Export PDF summary or CSV rows

## Kept Features
- File upload
- PDF text parsing
- OCR parsing for scanned docs (separate OCR module)
- Row bounds endpoint for table-row click highlighting
- Exports (PDF + CSV)
- Summary metrics

## Auth and Roles
- Simple login with cookie session (`admin`, `evaluator`)
- `admin`: can clear stored jobs/files and create evaluator accounts
- `evaluator`: can upload/process/review/export jobs

## Architecture (Current)
```text
backend/
  app/
    main.py
    api/
      routers/
        ui.py
    modules/
      auth/
        router.py
        service.py
        deps.py
      admin/
        router.py
        service.py
      jobs/
        router.py
        service.py
        repository.py
        schemas.py
      ocr/
        pipeline.py
        image_tools.py
      worker/
        celery_app.py
        tasks.py
    bank_profiles.py
    statement_parser.py
    pdf_text_extract.py
    ocr_engine.py
    image_cleaner.py
```

## API
- `POST /jobs` (multipart: `file`, optional `mode=auto|text|ocr`, optional `auto_start=true|false`)
- `POST /jobs/draft` (upload only, no processing)
- `POST /jobs/{job_id}/start`
- `GET /jobs/{job_id}`
- `GET /jobs/{job_id}/parsed`
- `GET /jobs/{job_id}/parsed/{page}`
- `GET /jobs/{job_id}/rows/{page}/bounds`
- `GET /jobs/{job_id}/bounds`
- `GET /jobs/{job_id}/preview/{page}`
- `GET /jobs/{job_id}/ocr/{page}`
- `GET /jobs/{job_id}/summary`
- `GET /jobs/{job_id}/export/pdf`
- `GET /jobs/{job_id}/export/excel` (XLSX payload)
- `GET /crm/attachments` (evaluator/admin: load Lead + `cBankStatementsIds` from EspoCRM and probe attachment files)
  - Supports `limit`, `offset`, and `probe=lazy|eager` query params for scalable pagination and optional probing.
- `GET /crm/attachments/{attachment_id}/file` (evaluator/admin: proxy-download EspoCRM attachment)
- `POST /crm/attachments/{attachment_id}/begin-process` (evaluator/admin: download CRM PDF to server and start processing job)
- `GET /health`
- `GET /login`
- `POST /auth/login`
- `POST /auth/logout`
- `GET /auth/me`
- `POST /admin/evaluators` (admin only)
- `POST /admin/clear-store` (admin only)

Default admin credentials (override via env):
- `ADMIN_USERNAME=admin`
- `ADMIN_PASSWORD=admin123`

## Run
From `/Users/kenito/Desktop/bank-statement-analyzer/backend`:

```bash
uvicorn app.main:app --reload
```

Ensure Redis is running (for example: `docker compose up redis` from repo root), then in another shell run a Celery worker:

```bash
celery -A app.worker.celery_app.celery worker \
  --loglevel=INFO \
  --pool=prefork \
  --concurrency=${CELERY_CONCURRENCY:-4} \
  --max-tasks-per-child=${CELERY_MAX_TASKS_PER_CHILD:-25}
```

Open:
- [http://localhost:8000](http://localhost:8000)
- [http://localhost:8000/health](http://localhost:8000/health)

## Notes
- Jobs are queued to Redis and executed by Celery workers.
- Retry/backoff knobs are configurable via `CELERY_TASK_MAX_RETRIES`, `CELERY_TASK_RETRY_BACKOFF_SECONDS`, and related `CELERY_TASK_*` env vars.
- OCR backend for scanned documents is OpenAI Vision OCR.
- Data is stored in `${DATA_DIR:-./data}/jobs/<job_id>/...`.
- Scanned PDFs route to OpenAI Vision OCR:
  - `OPENAI_API_KEY`
  - `OPENAI_OCR_USE_STRUCTURED_ROWS=true` (use OpenAI to return row fields + bounds directly)
  - `MAX_OPENAI_PAGES_PER_DOC=50`
  - `OPENAI_TIMEOUT_SECONDS=60`
- EspoCRM evaluator file table requires:
  - `ESPOCRM_API_KEY`
  - Optional: `ESPOCRM_BASE_URL` (defaults to `https://staging-crm.discoverycsc.com/api/v1`)
  - Optional performance knobs:
    - `CRM_ATTACHMENT_PROBE_MODE=lazy|eager`
    - `CRM_ATTACHMENT_CACHE_TTL_SECONDS`
    - `CRM_ATTACHMENT_PROBE_CONCURRENCY`
