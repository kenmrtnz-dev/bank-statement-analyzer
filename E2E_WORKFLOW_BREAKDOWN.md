# Bank Statement Analyzer - Full End-to-End Workflow Breakdown

This document describes the complete processing workflow in the current codebase under `/backend/app`, including:

- every major function execution path from UI to backend to worker
- variables/state keys used at each stage
- endpoints called (internal + external)
- mode-specific parser/OCR behavior (`auto`, `text`, `ocr`, `pdftotext`, `google_vision`)
- storage artifacts (files/DB rows) written during processing

## 1. Runtime Architecture (Current App)

## 1.1 Core Components

- **FastAPI app**: `backend/app/main.py`
- **Frontend UI** (same process): `backend/app/static/index.html`, `backend/app/static/ui.js`
- **Job API router**: `backend/app/jobs/router.py`
- **Job orchestration service**: `backend/app/jobs/service.py`
- **Filesystem + SQL persistence**: `backend/app/jobs/repository.py`
- **OCR pipeline**: `backend/app/ocr/pipeline.py`
- **OCR engine wrapper**: `backend/app/services/ocr/router.py`
- **OpenAI Vision client**: `backend/app/services/ocr/openai_vision.py`
- **Legacy v1 parser pipeline**: `backend/app/parser/pipeline.py`
- **Google Vision extractor for legacy pipeline**: `backend/app/parser/extractors/google_vision.py`
- **CRM integration**: `backend/app/crm/router.py`, `backend/app/crm/service.py`
- **Background queue**: Celery (`backend/app/worker/celery_app.py`, `backend/app/worker/tasks.py`)

## 1.2 App Startup Sequence

1. FastAPI app is created in `create_app()`.
2. `lifespan()` runs `_bootstrap_dirs()` before serving requests.
3. `_bootstrap_dirs()` creates data dirs under `DATA_DIR` and ensures DB schema (`job_transactions`) exists.
4. Routers are mounted:
- UI router
- auth router
- jobs router
- CRM router
- admin router

### Key startup variables

- `DATA_DIR` (`app.paths.get_data_dir()`)
- `DEFAULT_DATA_DIR = PROJECT_ROOT / "storage"`
- optional env override: `DATA_DIR`

## 2. Frontend Boot and Session Flow

Primary file: `backend/app/static/ui.js`

## 2.1 UI Initialization Order

At script load (`(() => { ... })()`):

1. Initializes constants:
- `STORAGE_KEY = 'bsa_uploaded_jobs_v1'`
- `MODE_STORAGE_KEY = 'bsa_process_mode_v1'`
- `SUPPORTED_PROCESS_MODES = {auto,text,ocr,pdftotext,google_vision}`

2. Captures DOM refs in `els`.
3. Creates global state object `state` with keys including:
- job tracking: `jobId`, `currentParseMode`, `pollTimer`
- parsed data: `pages`, `parsedByPage`, `boundsByPage`, `openaiRawByPage`
- CRM: `crmAttachments`, `crmProcessByAttachment`, `crmLeadByJobId`, `crmUploadedByJobId`, `crmStatusTimer`
- summary/editor: `summaryRaw`, `summaryIncludedMonths`, `selectedRowId`, `pageSaveTimers`

4. Calls startup functions:
- `renderUploadedRows()`
- `initRequestedProcessMode()` (loads parse mode from localStorage)
- `syncRoute(...)`
- `initAuth()`
- `reconcileStoredJobsStatuses()` (best-effort after auth)

## 2.2 Auth/API calls on boot

`initAuth()` does:

1. `GET /auth/me`
- sets `state.authRole`

2. `GET /ui/settings`
- sets `state.uploadTestingEnabled`
- sets `state.bankCodeFlags`

3. If role is evaluator/admin, loads CRM list:
- `GET /crm/attachments?...`

4. Starts CRM status poller when in uploads view.

### Auth backend functions

- `/auth/me` -> `auth.router.me` -> dependency `get_current_user`
- `/ui/settings` -> `auth.router.ui_settings` -> `admin.service.get_ui_settings`

## 3. Processing Mode Selection

## 3.1 UI Mode Variable Flow

- Dropdown DOM: `#mode` (from `index.html`)
- UI reads mode via `getRequestedProcessMode()`
- UI normalizes with `normalizeRequestedProcessMode()`
- persisted in localStorage key `bsa_process_mode_v1`

## 3.2 Accepted Modes

- `auto`
- `text`
- `ocr`
- `pdftotext`
- `google_vision`

## 3.3 Backend Normalization/Resolution

- `app.ocr.pipeline.normalize_parse_mode(mode)` validates mode
- `app.ocr.pipeline.resolve_parse_mode(input_pdf, requested_mode)` delegates to `services/ocr/router.py`
- `resolve_document_parse_mode(...)` behavior:
- if requested is explicit (`text|ocr|google_vision|pdftotext`) -> use it
- else detect profile via PDF text density and return `text` or `ocr`

## 4. E2E Flow A: Direct Upload -> Auto Start

This is the main evaluator flow from the upload card.

## 4.1 Frontend upload execution

Triggered by form submit or file drop.

Functions and order:

1. `createJob(e)`
2. `uploadSelectedFile(file)`
3. `uploadWithProgress(file, mode, autoStart=true)`

HTTP call:

- `POST /jobs` (multipart/form-data)
- form fields:
- `file`
- `mode`
- `auto_start`

UI variables updated after success:

- `state.uploadedJobs` via `upsertUploadedJob(...)`
- row fields: `jobId`, `fileName`, `sizeBytes`, `status`, `step`, `progress`, `parseMode`

If `payload.started === true`, UI calls `setActiveJob(payload.job_id, true)`.

## 4.2 Backend create-job route chain

Endpoint:

- `POST /jobs` -> `jobs.router.create_job_endpoint`

Function chain:

1. `create_job_endpoint(...)`
2. `jobs.service.create_job(...)`

`create_job(...)` exact behavior:

1. Validates `.pdf` extension
2. Generates `job_id = uuid4()`
3. Creates job folder layout (`JobsRepository.ensure_job_layout`):
- `input/`
- `result/`
- `pages/`
- `cleaned/`
- `ocr/`
- `preview/`
4. Writes uploaded PDF to `input/document.pdf`
5. Writes `meta.json` with:
- `original_filename`
- `requested_mode`
- `created_at`
- optional `created_by`
- optional `created_role`
6. Resolves parse mode via `resolve_parse_mode(...)`
7. Writes initial `status.json` (`queued`) through `_write_queued_status`
8. If `auto_start`, dispatches worker `_start_job_worker(job_id, parse_mode)`

Returned payload:

- `{job_id, parse_mode, started}`

## 4.3 Queue dispatch details

`_start_job_worker(job_id, parse_mode)`:

1. checks active task via `_has_active_task(status)`
2. enqueues Celery task via `_enqueue_job(...)`
3. writes/updates `status.json` with `task_id`

Celery task queued:

- name: `jobs.process_job`
- function: `worker.tasks.process_job_task`

## 5. E2E Flow B: Start Existing Draft / Retry

Frontend trigger:

- `startJob(jobId)`

Calls:

1. `setActiveJob(id, true)`
2. `POST /jobs/{id}/start?mode={selectedMode}`

Backend route:

- `jobs.router.start_job_endpoint`

Service logic (`jobs.service.start_job`):

1. ensures job exists
2. loads `status.json`
3. computes `base_mode = requested_mode or status.parse_mode or "auto"`
4. resolves parse mode from input PDF
5. prevents duplicate enqueue if active task exists
6. writes queued status
7. calls `_start_job_worker`
8. returns `{job_id, parse_mode, started}`

## 6. E2E Flow C: Polling, Completion, and Result Hydration

## 6.1 Polling loop

Frontend:

- `startPolling()` sets 2s interval
- `pollStatus()` -> `GET /jobs/{jobId}`

Backend endpoint:

- `jobs.router.get_job_status_endpoint` -> `jobs.service.get_status(job_id)`

`get_status` important behavior:

- returns current status payload
- if OCR mode and job still active:
- reconciles page task states
- marks terminated tasks failed
- refreshes aggregate progress from page map
- may auto-call `finalize_job_processing(...)` when no pages in flight
- for single-task modes, checks parent task state and can mark job failed if task is `FAILURE/REVOKED`

## 6.2 Terminal status handling in UI

`pollStatus()` status mapping:

- `done` -> `completed`
- `done_with_warnings` -> `needs_review`

When completed/needs_review:

- stop poll timer
- call `loadResultData()`

When failed/cancelled:

- stop polling

## 6.3 Result load sequence

`loadResultData()` calls in parallel:

- `GET /jobs/{jobId}/cleaned`
- `GET /jobs/{jobId}/summary`
- `GET /jobs/{jobId}/parse-diagnostics` (best-effort)

Then per page:

- `GET /jobs/{jobId}/parsed/{page}`
- `GET /jobs/{jobId}/rows/{page}/bounds`
- preview image URL: `GET /jobs/{jobId}/preview/{page}`

Optional debug JSON:

- `GET /jobs/{jobId}/ocr/{page}/openai-raw`

## 7. E2E Flow D: Manual Row Editing and Autosave

Frontend editing flow (`Parsed Rows` table):

1. user edits cell
2. `queuePageRowsSave(page)` debounces
3. `persistPageRows(page)` sends:
- `PUT /jobs/{jobId}/parsed/{page}`
- body: array of row objects (`row_id`, `date`, `description`, `debit`, `credit`, `balance`)

Backend endpoint chain:

- `jobs.router.update_parsed_page_endpoint`
- `jobs.service.update_page_rows(...)`

`update_page_rows` behavior:

1. validates page + rows payload
2. normalizes each row field (`row_id`, numeric/date sanitization)
3. acquires per-job lock `_get_job_update_lock(job_id)`
4. replaces page rows in SQL (`JobTransactionsRepository.replace_page_rows`)
5. reloads all rows and writes back:
- `result/parsed_rows.json`
- `result/bounds.json`
- recomputed `result/summary.json`
6. returns updated page rows + summary

## 8. E2E Flow E: Cancel Job

Frontend:

- `cancelJob(jobId)` -> `POST /jobs/{id}/cancel`

Backend:

- `jobs.router.cancel_job_endpoint`
- `jobs.service.cancel_job(job_id)`

Cancel logic:

1. reads parent `status.json` + `page_status.json`
2. collects task IDs from parent and active page entries
3. revokes each via `celery.control.revoke(task_id, terminate=True)`
4. marks non-terminal pages as `cancelled`
5. writes parent status `cancelled` with progress/page counters
6. returns `{job_id, cancelled, status, revoked_task_ids}`

## 9. Mode-Specific Processing Internals

## 9.1 Branch point: `jobs.service.process_job(job_id, parse_mode, task_id)`

Top-level behavior:

1. writes progress heartbeat via local `report(status, step, progress)` closure
2. normalizes `selected_mode`
3. branches:
- `google_vision` or `pdftotext` -> **legacy v1 parser pipeline**
- non-`ocr` (usually `text`) -> `ocr.run_pipeline(...)` single-task
- `ocr` -> fan-out page task model

## 9.2 Text mode path (`selected_mode != "ocr"` and not legacy)

Functions executed:

1. `ocr.pipeline.run_pipeline(root, parse_mode, report)`
2. `ocr.pipeline._run_text_pipeline(...)`
3. `extract_pdf_layout_pages(...)` (`pdftotext -bbox-layout`)
4. per page:
- `detect_bank_profile(text)`
- `parse_page_with_profile_fallback(words, page_w, page_h, profile, header_hint)`
- `_filter_rows_and_bounds(...)`
5. write intermediate empty OCR page json (`ocr/page_###.json = []`)
6. pipeline writes:
- `result/parsed_rows.json`
- `result/bounds.json`
- `result/parse_diagnostics.json`
7. service persists rows to SQL via `_persist_parsed_rows`
8. computes summary `compute_summary(...)` -> `result/summary.json`
9. writes final status: `done/completed/progress=100`

### Text-mode page diagnostics keys

- `source_type`
- `bank_profile`
- `rows_parsed`
- `profile_detected`
- `profile_selected`
- `fallback_applied`
- `header_detected`
- `header_hint_used`
- `fallback_mode`

## 9.3 OCR mode path (`selected_mode == "ocr"`)

This mode is asynchronous and page-fanout.

### Parent task stage (`process_job`)

1. Build dirs and fragment dir: `result/page_fragments/`
2. Load `result/pages_manifest.json`
3. If no manifest, generate pages:
- `prepare_ocr_pages(input_pdf, pages_dir, cleaned_dir, report)`
- `_render_pdf_pages(...)` using pdf2image
- `clean_page(...)` image preprocessing
4. write manifest via `_write_pages_manifest`
5. load existing `page_status.json`
6. for each page:
- if fragment exists -> mark done
- if active task alive -> keep inflight
- else mark queued and add to `pending_pages`
7. enqueue one Celery page task per pending page using `_enqueue_page_job`
8. write parent status with counters:
- `pages_total`, `pages_done`, `pages_failed`, `pages_inflight`
- `failed_pages`
- `active_task_ids`
- progress computed by `_compute_page_progress` (caps at 99 until finalize)

### Per-page worker stage (`process_job_page`)

Celery task: `jobs.process_page` -> `worker.tasks.process_page_task` -> `jobs.service.process_job_page`

Flow:

1. mark page `processing` in `page_status.json`
2. define heartbeat callback for rate-limit waits
3. call `ocr.pipeline.process_ocr_page(...)`

`process_ocr_page` internal branches:

1. if structured rows enabled and engine=openai:
- `openai_client.extract_structured_rows(page_path, rate_limit_heartbeat)`
- writes `ocr/page_###.openai_raw.json`
- normalizes rows/bounds via `_normalize_structured_ai_rows`
- returns immediately if rows valid

2. fallback path:
- `ocr_router.ocr_page(page_path)` -> OpenAI token OCR
- writes `ocr/page_###.json`
- optionally writes `ocr/page_###.openai_raw.json`
- converts tokens to words `_ocr_items_to_words`
- profile detect + `parse_page_with_profile_fallback`
- `_filter_rows_and_bounds`

Back in service:

4. write page fragment file: `result/page_fragments/page_###.json`
5. mark page `done` with `rows_parsed`
6. refresh parent progress
7. if no inflight pages, enqueue finalize task (`jobs.finalize_job`)

### Finalize stage (`finalize_job_processing`)

Celery task: `jobs.finalize_job` -> `worker.tasks.finalize_job_task` -> `jobs.service.finalize_job_processing`

Flow:

1. load manifest page list
2. for each page:
- read fragment if exists -> merge rows/bounds/diag
- else add failure entry
3. normalize merged rows
4. persist rows to SQL (`_persist_parsed_rows`)
5. write results:
- `result/parsed_rows.json`
- `result/bounds.json`
- `result/parse_diagnostics.json`
- `result/summary.json`
6. choose terminal status:
- no successful pages -> `failed`
- some failures -> `done_with_warnings`
- all succeeded -> `done`
7. write final `status.json` with `progress=100`

## 9.4 Legacy mode path (`google_vision`, `pdftotext`)

Triggered in `jobs.service.process_job` when mode is one of those two.

Flow:

1. call `run_legacy_parser_document(input_pdf, ocr_engine=selected_mode)`
2. inside `parser.pipeline.process_document(...)`:

- if `pdftotext`:
- `pdf_text_extractor.extract_text(path)`

- if `google_vision`:
- `google_vision.extract_text_with_details(path)`
- may call Vision REST API (API key mode) or Vision client (service account mode)

- if `auto` (legacy not used in this branch from jobs.service):
- checks `has_embedded_text` then picks pdftotext or google vision

3. parser stages executed in order:
- `bank_detector.detect_bank(text)`
- `template_loader.load_template(bank)`
- `row_detector.detect_rows(text)`
- `column_detector.detect_columns(template)`
- `table_builder.build_table(rows, column_map)`
- `description_merger.merge_descriptions(raw_table)`
- `transaction_extractor.extract_transactions(merged_rows)`
- `normalizer.normalize_transactions(extracted_rows)`
- `balance_validator.validate_balances(normalized_rows)`
- `_build_summary(normalized_rows)`

4. jobs service maps legacy transactions into UI row shape (`page_001` only)
5. writes optional raw OCR payload:
- `ocr/page_001.google_vision_raw.json` (when available)
6. persists parsed rows/bounds, writes diagnostics + summary
7. writes final status `done`

### Legacy result diagnostics fields

`diagnostics.job` includes:

- `parse_mode`
- `source_type = legacy_parser`
- `parser_strategy = v1`
- `ocr_engine_requested`
- `ocr_source`
- `bank`
- optional `validation`
- optional `legacy_summary`
- optional `ocr_raw_available`, `ocr_raw_page_count`

## 10. Celery Retry + Failure Bookkeeping

Task wrappers in `worker/tasks.py`:

- `process_job_task`
- `process_page_task`
- `finalize_job_task`

Retry behavior:

1. detect retryable errors via `_is_retryable_exception`
2. compute backoff via `_retry_delay_seconds`
3. if retrying:
- job task -> `mark_job_retrying(...)`
- page task -> `mark_page_retrying(...)`
4. after retry budget exhausted:
- job task -> `mark_job_failed(...)`
- page task -> `mark_page_failed(...)`

Page failure still allows finalize to complete with warnings/partial outputs.

## 11. CRM Processing and Export E2E

## 11.1 CRM attachment listing

Frontend call:

- `GET /crm/attachments?limit&offset&probe&q`

Backend route:

- `crm.router.list_attachments_endpoint` -> `crm.service.list_bank_statement_attachments`

Service behavior:

1. resolves CRM settings/env
2. fetches Lead + Account pages from EspoCRM APIs
3. extracts `cBankStatementsIds`
4. builds attachment rows
5. optional metadata probe (lazy/eager)
6. overlays backend process status from local jobs (`_load_attachment_process_index`)

## 11.2 Begin processing from CRM attachment

Frontend call:

- `POST /crm/attachments/{attachment_id}/begin-process?mode=...`

Backend chain:

- `crm.router.begin_process_from_attachment_endpoint`
- `crm.service.create_job_from_attachment(...)`

Service behavior:

1. downloads attachment PDF from EspoCRM
2. resolves filename
3. tries to find owning Lead/Account
4. calls `jobs.service.create_job(..., auto_start=True, requested_mode=mode)`
5. enriches job `meta.json` with source fields:
- `source_attachment_id`
- `source_attachment_filename`
- `source_record_id`
- `source_record_type`
- `source_entity_name`
- `source_account_name`
- `source_lead_id` (if source entity is Lead)

## 11.3 Export completed job workbook back to CRM

Frontend call:

- `POST /crm/jobs/{job_id}/export-excel?lead_id=...` (optional query)

Backend chain:

- `crm.router.export_job_excel_to_crm_endpoint`
- `crm.service.export_job_excel_to_crm_lead(...)`

Service behavior:

1. validates `job_id` and resolves target lead id
2. builds workbook via `jobs.service.export_excel(job_id)`
3. constructs filename via `_build_crm_export_basename(account_name)`
4. uploads file as base64 data URL to EspoCRM `POST /Attachment`
5. updates Lead record via `PUT /Lead/{lead_id}`
6. marks local meta:
- `crm_export_uploaded=true`
- `crm_export_uploaded_at`
- `crm_export_attachment_id`
- `crm_export_lead_id`
- `crm_export_filename`

UI then marks corresponding CRM process state as `uploaded`.

## 12. Endpoint Matrix (Processing-Relevant)

## 12.1 UI/Auth bootstrap

- `GET /` -> serve app or redirect
- `GET /uploads` -> serve app
- `GET /processing` -> serve app
- `GET /login` -> login page
- `POST /auth/login`
- `POST /auth/logout`
- `GET /auth/me`
- `GET /ui/settings`

## 12.2 Job lifecycle endpoints

- `POST /jobs` -> create and optionally auto-start
- `POST /jobs/draft` -> create without start
- `POST /jobs/{job_id}/start`
- `POST /jobs/{job_id}/cancel`
- `DELETE /jobs/{job_id}` (same cancel behavior)
- `GET /jobs/{job_id}` status
- `GET /jobs/{job_id}/pages/status`

## 12.3 Job result/data endpoints

- `GET /jobs/{job_id}/cleaned`
- `GET /jobs/{job_id}/cleaned/{filename}`
- `GET /jobs/{job_id}/preview/{page}`
- `GET /jobs/{job_id}/ocr/{page}`
- `GET /jobs/{job_id}/ocr/{page}/openai-raw`
- `GET /jobs/{job_id}/rows/{page}/bounds`
- `GET /jobs/{job_id}/bounds`
- `GET /jobs/{job_id}/parsed/{page}`
- `PUT /jobs/{job_id}/parsed/{page}`
- `GET /jobs/{job_id}/parsed`
- `GET /jobs/{job_id}/summary`
- `GET /jobs/{job_id}/parse-diagnostics`

## 12.4 Export endpoints

- `GET /jobs/{job_id}/export/pdf`
- `GET /jobs/{job_id}/export/excel`
- `POST /crm/jobs/{job_id}/export-excel`

## 12.5 CRM ingestion endpoints

- `GET /crm/attachments`
- `GET /crm/attachments/{attachment_id}/file`
- `POST /crm/attachments/{attachment_id}/begin-process`

## 13. External Endpoints Called by This App

## 13.1 OpenAI OCR

Called by `OpenAIVisionOCR`:

- `POST {OPENAI_BASE_URL}/chat/completions`

Used for:

- plain OCR text (`_call_openai`)
- structured token OCR (`_call_openai_structured`)
- structured row OCR (`_call_openai_structured_rows`)

## 13.2 Google Vision

Legacy extractor supports two modes:

1. API key REST mode:
- `POST https://vision.googleapis.com/v1/images:annotate?key={GOOGLE_VISION_API_KEY}`

2. Service-account client mode:
- `vision.ImageAnnotatorClient().document_text_detection(...)`

## 13.3 EspoCRM

Base URL env: `ESPOCRM_BASE_URL`.

Used endpoints:

- `GET {base}/Lead`
- `GET {base}/Account`
- `GET {base}/Attachment/file/{id}` (fallback `Attachments` entity too)
- `POST {base}/Attachment`
- `PUT {base}/Lead/{id}`

## 14. Job Artifacts and Data Layout

Per job directory: `DATA_DIR/jobs/{job_id}`

Always expected:

- `input/document.pdf`
- `meta.json`
- `status.json`

Often produced:

- `result/parsed_rows.json`
- `result/bounds.json`
- `result/summary.json`
- `result/parse_diagnostics.json`

OCR mode specific:

- `pages/page_###.png`
- `cleaned/page_###.png`
- `result/pages_manifest.json`
- `result/page_status.json`
- `result/page_fragments/page_###.json`
- `ocr/page_###.json`
- `ocr/page_###.openai_raw.json`

Legacy google vision mode may also write:

- `ocr/page_001.google_vision_raw.json`

Optional on-demand generated preview:

- `preview/page_###.png`

## 14.1 SQL persistence

Table: `job_transactions`

Primary row fields written from parser outputs:

- identity: `id`, `job_id`, `page_key`, `row_index`, `row_id`
- row values: `rownumber`, `row_number`, `date`, `description`, `debit`, `credit`, `balance`, `row_type`
- bounds: `x1`, `y1`, `x2`, `y2`
- edit flag/timestamps: `is_manual_edit`, `created_at`, `updated_at`

Table: `bank_code_flags` (UI/admin flagging lookup)

## 15. Status Payload Schemas (Effective)

## 15.1 `status.json` common keys

Common keys used across modes:

- `status`
- `step`
- `progress`
- `parse_mode`
- optional `task_id`
- optional `message`

Retry/cancel keys when applicable:

- `retry_attempt`
- `retry_max_attempts`
- `retry_in_seconds`
- `cancelled_at`

OCR aggregate keys:

- `pages_total`
- `pages_done`
- `pages_failed`
- `pages_inflight`
- `failed_pages` (list of `{page,error}`)
- `active_task_ids`

## 15.2 `page_status.json` keys (OCR mode)

Per `page_###` entry may include:

- `status` (`queued|processing|retrying|done|failed|cancelled`)
- `task_id`
- `page_index`
- `page_count`
- `rows_parsed`
- `step`
- `message`
- `retry_attempt`
- `retry_max_attempts`
- `retry_in_seconds`
- `wait_seconds` (rate-limit heartbeat)
- `updated_at`

## 16. Environment Variable Map

## 16.1 Core paths/data

- `DATA_DIR`: storage root override
- `DATABASE_URL`: SQL URL override (`ocr.db` sqlite fallback)
- `DB_AUTO_CREATE_SCHEMA`
- `DB_CONNECT_MAX_WAIT_SECONDS`
- `DB_CONNECT_RETRY_INTERVAL_SECONDS`

## 16.2 Celery/Redis

- `REDIS_URL`
- `CELERY_BROKER_URL`
- `CELERY_RESULT_BACKEND`
- `CELERY_TASK_DEFAULT_QUEUE`
- `CELERY_WORKER_PREFETCH_MULTIPLIER`
- `CELERY_VISIBILITY_TIMEOUT_SECONDS`
- `CELERY_TASK_SOFT_TIME_LIMIT`
- `CELERY_TASK_TIME_LIMIT`
- `CELERY_RESULT_EXPIRES_SECONDS`
- `CELERY_TASK_ALWAYS_EAGER`
- `CELERY_TASK_EAGER_PROPAGATES`
- `CELERY_TASK_MAX_RETRIES`
- `CELERY_TASK_RETRY_BACKOFF_SECONDS`
- `CELERY_TASK_RETRY_BACKOFF_MAX_SECONDS`
- `CELERY_TASK_RETRY_JITTER_SECONDS`

## 16.3 OCR/OpenAI

- `OPENAI_API_KEY`
- `OPENAI_OCR_MODEL`
- `OPENAI_TIMEOUT_SECONDS`
- `OPENAI_BASE_URL`
- `OPENAI_OCR_CACHE_DIR`
- `OPENAI_OCR_MAX_TOKENS`
- `OPENAI_OCR_USE_STRUCTURED_ROWS`
- `OPENAI_OCR_PAGE_BATCH_SIZE`
- `OPENAI_OCR_RPM_LIMIT`
- `OPENAI_OCR_RATE_WINDOW_SECONDS`
- `OPENAI_OCR_RATE_WAIT_TIMEOUT_SECONDS`
- `OPENAI_OCR_RATE_KEY`
- `SCANNED_RENDER_DPI`
- `OCR_ROW_FILTER_LENIENT`
- `PREVIEW_MAX_PIXELS`
- `FALLBACK_PREVIEW_DPI`

## 16.4 Google Vision (legacy pipeline)

- `GOOGLE_VISION_API_KEY`
- `GOOGLE_VISION_BATCH_SIZE`
- `GOOGLE_VISION_PDF_DPI`

## 16.5 CRM

- `ESPOCRM_BASE_URL`
- `ESPOCRM_API_KEY`
- `CRM_ATTACHMENT_PROBE_MODE`
- `CRM_ATTACHMENT_CACHE_TTL_SECONDS`
- `CRM_ATTACHMENT_PROBE_CONCURRENCY`
- `CRM_ATTACHMENT_FILENAME_PROBE_CONCURRENCY`

## 16.6 Auth/admin misc

- `ADMIN_USERNAME`
- `ADMIN_PASSWORD`
- `BANK_CODE_SEED_XLSX`
- `BANK_PROFILES_CONFIG`

## 17. Function Call Graphs (Condensed)

## 17.1 Upload + process

1. `ui.js:createJob` -> `uploadSelectedFile` -> `uploadWithProgress`
2. `POST /jobs` -> `jobs.router.create_job_endpoint`
3. `jobs.service.create_job`
4. `_start_job_worker` -> `_enqueue_job`
5. `worker.tasks.process_job_task`
6. `jobs.service.process_job`
7. branch to mode pipeline (`run_pipeline` / legacy parser / OCR fanout)

## 17.2 OCR fanout

1. `process_job` parent queues page tasks
2. `worker.tasks.process_page_task` (one per page)
3. `jobs.service.process_job_page`
4. `ocr.pipeline.process_ocr_page`
5. write fragment + update page status
6. enqueue finalize when inflight=0
7. `worker.tasks.finalize_job_task`
8. `jobs.service.finalize_job_processing`

## 17.3 CRM attachment process

1. UI click `Begin Process` -> `POST /crm/attachments/{id}/begin-process?mode=...`
2. `crm.service.create_job_from_attachment`
3. downloads CRM file + `jobs.service.create_job(auto_start=True)`
4. normal worker flow proceeds with selected mode

## 18. Important Behavioral Notes

1. `auto` mode currently resolves only to `text` or `ocr` (not to `google_vision` / `pdftotext`).
2. Legacy modes (`google_vision`, `pdftotext`) are single-task and currently collapsed into `page_001` output in `jobs.service.process_job`.
3. OCR job progress is intentionally capped below 100 until finalize succeeds (`_compute_page_progress` max 99).
4. UI status normalizes backend states:
- `done` -> `completed`
- `done_with_warnings` -> `needs_review`
5. Manual row edits persist to SQL first, then regenerate JSON artifacts and summary.
6. OpenAI OCR has on-disk cache + Redis-backed rate limiter; if Redis unavailable, rate limiting is skipped (best effort).


## 19. Function-to-Variable Map (High-Value Execution Paths)

This section maps the most important runtime variables per function so you can trace state changes linearly.

## 19.1 Frontend (`ui.js`)

| Function | Key Variables Read | Key Variables Written | Endpoint(s) |
|---|---|---|---|
| `initRequestedProcessMode()` | `MODE_STORAGE_KEY`, `els.mode`, `SUPPORTED_PROCESS_MODES` | `els.mode.value`, localStorage mode value | none |
| `uploadWithProgress(file, mode, autoStart)` | `file`, `mode`, `autoStart` | upload progress UI via `setUploadProgress` | `POST /jobs` |
| `uploadSelectedFile(file)` | `file`, `getRequestedProcessMode()` | `state.uploadedJobs`, progress UI | `POST /jobs` |
| `setActiveJob(jobId, switchToProcessing)` | `jobId`, `state.crmLeadByJobId` | `state.jobId`, `state.currentCrmLeadId`, resets `state.parsedByPage/state.boundsByPage/state.openaiRawByPage` | `GET /jobs/{id}` |
| `startJob(jobId)` | `jobId`, mode dropdown | updates upload row status | `POST /jobs/{id}/start` |
| `cancelJob(jobId)` | `jobId`, cached uploaded row | updates status row and active header | `POST /jobs/{id}/cancel`, then `GET /jobs/{id}` |
| `pollStatus()` | `state.jobId` | `state.isCompleted`, `state.currentParseMode`, upload row status | `GET /jobs/{id}` |
| `loadResultData()` | `state.jobId` | `state.pages`, `state.currentPage`, parsed/bounds caches, summary render | `GET /jobs/{id}/cleaned`, `GET /summary`, `GET /parse-diagnostics` |
| `loadCurrentPageData()` | `state.jobId`, `state.currentPage` | `state.parsedByPage[page]`, `state.boundsByPage[page]` | `GET /jobs/{id}/parsed/{page}`, `GET /jobs/{id}/rows/{page}/bounds` |
| `ensureCurrentPageOpenaiRawLoaded()` | `state.jobId`, `state.currentPage` | `state.openaiRawByPage[page]` | `GET /jobs/{id}/ocr/{page}/openai-raw` |
| `persistPageRows(page)` | `state.parsedByPage[page]`, `state.jobId` | row cache normalization + summary rerender | `PUT /jobs/{id}/parsed/{page}` |
| `loadCrmAttachments(reset)` | `state.crmLimit`, `state.crmOffset`, `state.crmProbeMode`, `state.crmSearch` | `state.crmAttachments`, paging flags, `state.crmProcessByAttachment` | `GET /crm/attachments` |
| CRM begin-process click handler | `attachmentId`, selected mode | `state.crmProcessByAttachment[attachmentId]`, `state.crmLeadByJobId` | `POST /crm/attachments/{id}/begin-process?mode=...` |
| `exportToCrm()` | `state.jobId`, `state.crmLeadByJobId`, `state.currentCrmLeadId` | `state.crmUploadedByJobId` via `markCrmExportUploadedByJob` | `POST /crm/jobs/{id}/export-excel` |

## 19.2 Job Service (`jobs/service.py`)

| Function | Inputs / Locals | Persistent Writes |
|---|---|---|
| `create_job(file_bytes, filename, requested_mode, auto_start, created_by, created_role)` | `job_id`, `root`, `input_pdf`, `meta_payload`, `parse_mode`, `started` | `input/document.pdf`, `meta.json`, `status.json` |
| `start_job(job_id, requested_mode)` | `status`, `input_pdf`, `base_mode`, `parse_mode`, `started` | `status.json` |
| `_start_job_worker(job_id, parse_mode)` | `status`, `task_id`, `latest_status` | `status.json` (queued + `task_id`) |
| `process_job(job_id, parse_mode, task_id)` | `selected_mode`, `report(...)`, mode-branch outputs | status + result artifacts + SQL rows |
| OCR parent branch in `process_job` | `page_files`, `page_status`, `pending_pages`, `active_task_ids`, counters | `result/pages_manifest.json`, `result/page_status.json`, `status.json` |
| `process_job_page(job_id, parse_mode, page_name, page_index, page_count, task_id)` | `page_file`, `payload`, `_heartbeat`, `page_rows`, `page_bounds`, `page_diag` | `result/page_fragments/*.json`, `result/page_status.json`, `status.json` |
| `finalize_job_processing(job_id, parse_mode, task_id)` | `page_files`, `parsed_output`, `bounds_output`, `diagnostics`, `failed_list`, `success_pages` | `result/parsed_rows.json`, `result/bounds.json`, `result/summary.json`, `result/parse_diagnostics.json`, `status.json` |
| `cancel_job(job_id)` | `status`, `page_status`, `revoked_task_ids`, `payload` | `result/page_status.json`, `status.json` |
| `get_status(job_id)` | `payload`, `parse_mode`, `runtime_status`, `page_status` | may rewrite `result/page_status.json` and `status.json` |
| `update_page_rows(job_id, page, rows)` | `normalized_rows`, per-job `lock`, `summary` | SQL rows, `result/parsed_rows.json`, `result/bounds.json`, `result/summary.json` |

## 19.3 Worker Tasks (`worker/tasks.py`)

| Task Function | Retry Variables | Service Function Called |
|---|---|---|
| `process_job_task(self, job_id, parse_mode)` | `retries_so_far`, `max_retries`, `countdown`, `task_id` | `process_job(...)` |
| `process_page_task(self, job_id, parse_mode, page_name, page_index, page_count)` | `retries_so_far`, `max_retries`, `countdown`, `task_id` | `process_job_page(...)` |
| `finalize_job_task(self, job_id, parse_mode)` | `task_id` | `finalize_job_processing(...)` |

## 19.4 OCR Pipeline + OpenAI Client Variables

| Function | Key Variables | Meaning |
|---|---|---|
| `ocr.pipeline.run_pipeline(job_dir, parse_mode, report)` | `selected_mode`, `parsed_output`, `bounds_output`, `diagnostics` | top-level text/OCR execution + result writes |
| `prepare_ocr_pages(...)` | `page_files`, `src`, `dst`, `cleaned` | renders PDF pages + cleaned PNGs |
| `process_ocr_page(...)` | `page_name`, `page_path`, `page_h/page_w`, `ocr_router`, `ai_rows/ai_bounds`, `ocr_items` | per-page OCR + parse fallback |
| `OpenAIVisionOCR.extract_structured_rows(...)` | `configs`, `page_w/page_h`, `last_error` | structured-row OCR with retry across image configs |
| `OpenAIVisionOCR._call_openai_structured_rows(...)` | `payload`, `headers`, `body`, `choices`, `content`, `rows` | actual OpenAI request/response handling |
| `OpenAIVisionOCR._wait_for_rate_limit(...)` | `limit`, `window_ms`, `deadline`, Redis script result (`ok`, `wait_ms`) | distributed request throttling |

## 19.5 CRM Service Variables

| Function | Key Variables | Purpose |
|---|---|---|
| `list_bank_statement_attachments(...)` | `limit`, `offset`, `probe_mode`, `search_query`, `process_index`, `rows` | list CRM attachments + overlay local process state |
| `create_job_from_attachment(attachment_id, requested_mode)` | `cleaned_attachment_id`, `source_name`, `owner`, `payload`, `job_id` | create processing job from CRM file and stamp source metadata |
| `export_job_excel_to_crm_lead(job_id, lead_id)` | `resolved_lead_id`, `workbook_bytes`, `safe_filename`, `attachment_id`, `meta` | upload export workbook back to CRM Lead |

