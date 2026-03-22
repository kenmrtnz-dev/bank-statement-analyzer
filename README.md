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

## Architecture
```text
frontend/
  e2e/
storage/
backend/
  app/
    main.py
    admin/
      router.py
      service.py
    auth/
      router.py
      service.py
      deps.py
    crm/
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
    parser/
      pipeline.py
      extractors/google_vision.py
      templates/*.json
    api/
      routers/
        ui.py
    worker/
      celery_app.py
      tasks.py
    bank_profiles.py
    statement_parser.py
    pdf_text_extract.py
    image_cleaner.py
scripts/
  migrate.sh
  run-api.sh
  run-worker.sh
```

`backend/app/static/*` is the production UI served by FastAPI. `frontend/` is Playwright E2E tooling only.

## API
- `POST /jobs` (multipart: `file`, optional `mode=auto|text|ocr|pdftotext|google_vision`, optional `auto_start=true|false`)
- `POST /jobs/draft` (upload only, no processing)
- `POST /jobs/{job_id}/start`
- `POST /jobs/{job_id}/cancel` (cancel queued/in-flight processing, keep stored job files)
- `DELETE /jobs/{job_id}` (same as cancel)
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

Default admin credentials when `SEED_DEFAULT_USERS=true`:
- `ADMIN_USERNAME=admin`
- `ADMIN_PASSWORD=admin123`

## Repo-Root Run
Create a virtualenv from repo root, install the package, then use the provided scripts:

```bash
python -m venv .venv
. .venv/bin/activate
pip install .[dev]
```

Apply schema migrations from repo root:

```bash
./scripts/migrate.sh
```

Start the API from repo root:

```bash
./scripts/run-api.sh
```

In another shell, start the Celery worker:

```bash
./scripts/run-worker.sh
```

Open:
- [http://localhost:8000](http://localhost:8000)
- [http://localhost:8000/health](http://localhost:8000/health)

For containerized single-machine runs:

```bash
docker compose up --build
```

The compose stack now runs a one-shot `migrate` service before `api` and `worker` start, so schema upgrades happen as part of deploy startup instead of during image build.

For local hot reload on top of the production-like compose file:

```bash
docker compose -f docker-compose.yml -f docker-compose.dev.yml up --build
```

## Deployment Notes
- Jobs are queued to Redis and executed by Celery workers.
- API and worker must share the same `DATA_DIR`.
- Production is single-machine/local-disk oriented. Separate API and worker machines without shared storage are out of scope.
- `DATABASE_URL` is required for the API, worker, and Alembic.
- In production, set `APP_ENV=prod`, `DB_AUTO_CREATE_SCHEMA=false`, `SEED_DEFAULT_USERS=false`, and a real `JWT_SECRET`.
- Retry/backoff knobs are configurable via `CELERY_TASK_MAX_RETRIES`, `CELERY_TASK_RETRY_BACKOFF_SECONDS`, and related `CELERY_TASK_*` env vars.
- `mode=pdftotext` forces the modern text-layer pipeline.
- `mode=google_vision` forces the modern OCR pipeline.
- Data is stored in `${DATA_DIR:-<repo>/storage}/jobs/<job_id>/...`.
- Scanned PDFs route to Google Vision OCR:
  - `GOOGLE_VISION_API_KEY` (or `GOOGLE_APPLICATION_CREDENTIALS`)
  - `GOOGLE_VISION_BATCH_SIZE=5`
  - `GOOGLE_VISION_PDF_DPI=120`
- EspoCRM evaluator file table requires:
  - `ESPOCRM_API_KEY`
  - Optional: `ESPOCRM_BASE_URL` (defaults to `https://staging-crm.discoverycsc.com/api/v1`)
  - Optional performance knobs:
    - `CRM_ATTACHMENT_PROBE_MODE=lazy|eager`
    - `CRM_ATTACHMENT_CACHE_TTL_SECONDS`
    - `CRM_ATTACHMENT_PROBE_CONCURRENCY`
