import json
import os
from decimal import Decimal
from pathlib import Path

from app.jobs import service as jobs_service
from sqlalchemy import create_engine, text


def _fetch_job_transactions(job_id: str):
    engine = create_engine(str(os.environ["DATABASE_URL"]), future=True)
    try:
        with engine.connect() as conn:
            return conn.execute(
                text(
                    "SELECT description, credit, (date_bounds ->> 'x1')::numeric FROM job_transactions "
                    "WHERE job_id = :job_id ORDER BY row_index"
                ),
                {"job_id": job_id},
            ).fetchall()
    finally:
        engine.dispose()


def _fetch_updated_job_transactions(job_id: str):
    engine = create_engine(str(os.environ["DATABASE_URL"]), future=True)
    try:
        with engine.connect() as conn:
            return conn.execute(
                text(
                    "SELECT description, debit, credit, balance, (date_bounds ->> 'x1')::numeric FROM job_transactions "
                    "WHERE job_id = :job_id ORDER BY row_index"
                ),
                {"job_id": job_id},
            ).fetchall()
    finally:
        engine.dispose()


def test_health(client):
    res = client.get("/health")
    assert res.status_code == 200
    assert res.json().get("ok") is True



def test_create_job_rejects_non_pdf(client):
    res = client.post(
        "/jobs",
        files={"file": ("notes.txt", b"hello", "text/plain")},
        data={"mode": "auto", "auto_start": "false"},
    )
    assert res.status_code == 400
    assert res.json().get("detail") == "PDF only"


def test_create_job_rejects_unsupported_mode(client):
    res = client.post(
        "/jobs",
        files={"file": ("statement.pdf", b"%PDF-1.4\n%%EOF", "application/pdf")},
        data={"mode": "google vision", "auto_start": "false"},
    )
    assert res.status_code == 400
    assert "unsupported_requested_mode" in str(res.json().get("detail") or "")



def test_job_flow_with_mocked_pipeline(client, monkeypatch):
    def _run_inline_enqueue(job_id: str, parse_mode: str):
        jobs_service.process_job(job_id=job_id, parse_mode=parse_mode, task_id="inline-task-1")
        return "inline-task-1"

    def _fake_resolve_mode(_pdf_path, _requested):
        return "text"

    def _fake_pipeline(job_dir, parse_mode, report):
        root = Path(job_dir)
        result_dir = root / "result"
        ocr_dir = root / "ocr"
        result_dir.mkdir(parents=True, exist_ok=True)
        ocr_dir.mkdir(parents=True, exist_ok=True)

        parsed = {
            "page_001": [
                {
                    "row_id": "001",
                    "date": "2026-02-01",
                    "description": "Deposit",
                    "debit": None,
                    "credit": "1000.00",
                    "balance": "1000.00",
                }
            ]
        }
        bounds = {
            "page_001": [
                {
                    "row_id": "001",
                    "x1": 0.1,
                    "y1": 0.2,
                    "x2": 0.9,
                    "y2": 0.25,
                }
            ]
        }
        diagnostics = {"job": {"parse_mode": parse_mode}, "pages": {"page_001": {"rows_parsed": 1}}}

        with open(result_dir / "parsed_rows.json", "w", encoding="utf-8") as handle:
            json.dump(parsed, handle)
        with open(result_dir / "bounds.json", "w", encoding="utf-8") as handle:
            json.dump(bounds, handle)
        with open(result_dir / "parse_diagnostics.json", "w", encoding="utf-8") as handle:
            json.dump(diagnostics, handle)
        with open(ocr_dir / "page_001.json", "w", encoding="utf-8") as handle:
            json.dump([], handle)

        report("processing", "mocked", 80)
        return {
            "parse_mode": parse_mode,
            "pages": 1,
            "parsed_rows": parsed,
            "bounds": bounds,
            "diagnostics": diagnostics,
        }

    monkeypatch.setattr(jobs_service, "_enqueue_job", _run_inline_enqueue)
    monkeypatch.setattr(jobs_service, "resolve_parse_mode", _fake_resolve_mode)
    monkeypatch.setattr(jobs_service, "run_pipeline", _fake_pipeline)

    create = client.post(
        "/jobs",
        files={"file": ("statement.pdf", b"%PDF-1.4\n%%EOF", "application/pdf")},
        data={"mode": "auto", "auto_start": "false"},
    )
    assert create.status_code == 200

    payload = create.json()
    job_id = payload["job_id"]

    started = client.post(f"/jobs/{job_id}/start")
    assert started.status_code == 200
    assert started.json().get("started") is True

    status = client.get(f"/jobs/{job_id}")
    assert status.status_code == 200
    assert status.json().get("status") == "done"

    pages_status = client.get(f"/jobs/{job_id}/pages/status")
    assert pages_status.status_code == 200
    assert isinstance(pages_status.json(), dict)

    parsed = client.get(f"/jobs/{job_id}/parsed")
    assert parsed.status_code == 200
    assert parsed.json()["page_001"][0]["description"] == "Deposit"

    stored = _fetch_job_transactions(job_id)
    assert stored == [("Deposit", Decimal("1000.00"), Decimal("0.100000"))]

    updated_rows = [
        {
            "row_id": "001",
            "date": "10/10/1925",
            "description": "Edited Deposit",
            "debit": "50.00",
            "credit": "",
            "balance": "950.00",
        }
    ]
    update = client.put(f"/jobs/{job_id}/parsed/page_001", json=updated_rows)
    assert update.status_code == 200
    assert update.json()["rows"][0]["description"] == "Edited Deposit"
    assert update.json()["rows"][0]["date"] == "10/10/2025"
    assert update.json()["summary"]["debit_transactions"] == 1
    assert update.json()["summary"]["total_debit"] == 50.0

    parsed_after_update = client.get(f"/jobs/{job_id}/parsed")
    assert parsed_after_update.status_code == 200
    assert parsed_after_update.json()["page_001"][0]["description"] == "Edited Deposit"

    stored_after_update = _fetch_updated_job_transactions(job_id)
    assert stored_after_update == [("Edited Deposit", Decimal("50.00"), None, Decimal("950.00"), Decimal("0.100000"))]

    bounds = client.get(f"/jobs/{job_id}/rows/page_001/bounds")
    assert bounds.status_code == 200
    assert bounds.json()[0]["row_id"] == "001"

    summary = client.get(f"/jobs/{job_id}/summary")
    assert summary.status_code == 200
    assert summary.json().get("total_transactions") == 1

    diagnostics = client.get(f"/jobs/{job_id}/parse-diagnostics")
    assert diagnostics.status_code == 200
    assert diagnostics.json().get("pages", {}).get("page_001", {}).get("rows_parsed") == 1

    export_pdf = client.get(f"/jobs/{job_id}/export/pdf")
    assert export_pdf.status_code == 200
    assert export_pdf.headers.get("content-type", "").startswith("application/pdf")

    export_excel = client.get(f"/jobs/{job_id}/export/excel")
    assert export_excel.status_code == 200
    assert "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" in export_excel.headers.get(
        "content-type", ""
    )


def test_job_flow_with_google_vision_uses_modern_pipeline(client, monkeypatch):
    def _run_inline_enqueue(job_id: str, parse_mode: str):
        jobs_service.process_job(job_id=job_id, parse_mode=parse_mode, task_id="inline-task-google")
        return "inline-task-google"

    def _prepare_pages(*, input_pdf, pages_dir, cleaned_dir, report):
        pages_dir.mkdir(parents=True, exist_ok=True)
        cleaned_dir.mkdir(parents=True, exist_ok=True)
        (pages_dir / "page_001.png").write_bytes(b"png")
        (cleaned_dir / "page_001.png").write_bytes(b"png")
        return ["page_001.png"]

    def _enqueue_page_job(job_id: str, parse_mode: str, page_name: str, page_index: int, page_count: int) -> str:
        task_id = f"task-{page_name}"
        jobs_service.process_job_page(
            job_id=job_id,
            parse_mode=parse_mode,
            page_name=page_name,
            page_index=page_index,
            page_count=page_count,
            task_id=task_id,
        )
        return task_id

    def _enqueue_finalize_job(job_id: str, parse_mode: str) -> str:
        jobs_service.finalize_job_processing(job_id=job_id, parse_mode=parse_mode, task_id="finalize-task")
        return "finalize-task"

    monkeypatch.setattr(jobs_service, "_enqueue_job", _run_inline_enqueue)
    monkeypatch.setattr(jobs_service, "resolve_parse_mode", lambda *_args, **_kwargs: "google_vision")
    monkeypatch.setattr(
        jobs_service,
        "prepare_ocr_pages",
        _prepare_pages,
    )
    monkeypatch.setattr(
        jobs_service,
        "process_ocr_page",
        lambda **_kwargs: (
            "page_001",
            [
                {
                    "row_id": "001",
                    "row_number": "1",
                    "date": "03/01/2026",
                    "description": "Modern Deposit",
                    "debit": None,
                    "credit": "1500.25",
                    "balance": "1500.25",
                    "row_type": "transaction",
                }
            ],
            [],
            {"source_type": "ocr", "rows_parsed": 1, "ocr_backend": "google_vision"},
        ),
    )
    monkeypatch.setattr(
        jobs_service,
        "_enqueue_page_job",
        _enqueue_page_job,
    )
    monkeypatch.setattr(
        jobs_service,
        "_enqueue_finalize_job",
        _enqueue_finalize_job,
    )

    create = client.post(
        "/jobs",
        files={"file": ("statement.pdf", b"%PDF-1.4\n%%EOF", "application/pdf")},
        data={"mode": "google_vision", "auto_start": "false"},
    )
    assert create.status_code == 200
    job_id = create.json()["job_id"]

    started = client.post(f"/jobs/{job_id}/start?mode=google_vision")
    assert started.status_code == 200
    assert started.json()["parse_mode"] == "google_vision"
    assert started.json()["started"] is True

    jobs_service.finalize_job_processing(job_id=job_id, parse_mode="google_vision", task_id="finalize-task-manual")

    status = client.get(f"/jobs/{job_id}")
    assert status.status_code == 200
    status_payload = status.json()
    assert status_payload["status"] == "done"
    assert status_payload["parse_mode"] == "google_vision"

    parsed = client.get(f"/jobs/{job_id}/parsed")
    assert parsed.status_code == 200
    assert parsed.json()["page_001"][0]["description"] == "Modern Deposit"

    diagnostics = client.get(f"/jobs/{job_id}/parse-diagnostics")
    assert diagnostics.status_code == 200
    payload = diagnostics.json()
    assert payload["job"]["source_type"] == "ocr"
    assert payload["pages"]["page_001"]["rows_parsed"] == 1


def test_reparse_google_vision_endpoint_uses_processing_parser_choice(client, monkeypatch):
    monkeypatch.setattr(jobs_service, "resolve_parse_mode", lambda *_args, **_kwargs: "google_vision")
    monkeypatch.setattr(
        jobs_service,
        "parse_google_vision_raw_payload",
        lambda raw_payload, parser_profile="auto", detected_bank="generic": (
            [
                {
                    "row_number": 1,
                    "page_number": 1,
                    "date": "2026-03-01",
                    "description": f"parsed-with-{parser_profile}",
                    "debit": None,
                    "credit": "100.00",
                    "balance": "100.00",
                }
            ],
            parser_profile,
        ),
    )

    create = client.post(
        "/jobs",
        files={"file": ("statement.pdf", b"%PDF-1.4\n%%EOF", "application/pdf")},
        data={"mode": "google_vision", "auto_start": "false"},
    )
    assert create.status_code == 200
    job_id = create.json()["job_id"]

    repo = jobs_service.JobsRepository(jobs_service.DATA_DIR)
    repo.write_json(
        repo.path(job_id, "ocr", "page_001.google_vision_raw.json"),
        {"provider": "google_vision", "page_count": 1, "pages": []},
    )
    repo.write_json(
        repo.path(job_id, "result", "parse_diagnostics.json"),
        {"job": {"bank": "bdo", "ocr_source": "google_vision"}, "pages": {}},
    )

    res = client.post(f"/jobs/{job_id}/reparse-google-vision?parser=sterling_bank_of_asia")
    assert res.status_code == 200
    payload = res.json()
    assert payload["status"] == "done"
    assert payload["parse_mode"] == "google_vision"
    assert payload["parser_profile_used"] == "sterling_bank_of_asia"

    parsed = client.get(f"/jobs/{job_id}/parsed")
    assert parsed.status_code == 200
    assert parsed.json()["page_001"][0]["description"] == "parsed-with-sterling_bank_of_asia"


def test_reparse_google_vision_keeps_rows_split_per_page(client, monkeypatch):
    monkeypatch.setattr(jobs_service, "resolve_parse_mode", lambda *_args, **_kwargs: "google_vision")
    monkeypatch.setattr(
        jobs_service,
        "parse_google_vision_raw_payload",
        lambda raw_payload, parser_profile="auto", detected_bank="generic": (
            [
                {
                    "row_number": 1,
                    "page_number": 1,
                    "date": "2026-03-01",
                    "description": "page-1-row",
                    "debit": None,
                    "credit": "100.00",
                    "balance": "100.00",
                },
                {
                    "row_number": 2,
                    "page_number": 2,
                    "date": "2026-03-02",
                    "description": "page-2-row",
                    "debit": "20.00",
                    "credit": None,
                    "balance": "80.00",
                },
            ],
            parser_profile,
        ),
    )

    create = client.post(
        "/jobs",
        files={"file": ("statement.pdf", b"%PDF-1.4\n%%EOF", "application/pdf")},
        data={"mode": "google_vision", "auto_start": "false"},
    )
    assert create.status_code == 200
    job_id = create.json()["job_id"]

    repo = jobs_service.JobsRepository(jobs_service.DATA_DIR)
    repo.write_json(
        repo.path(job_id, "ocr", "page_001.google_vision_raw.json"),
        {"provider": "google_vision", "page_count": 2, "pages": []},
    )
    repo.write_json(
        repo.path(job_id, "result", "parse_diagnostics.json"),
        {"job": {"bank": "generic", "ocr_source": "google_vision"}, "pages": {}},
    )

    res = client.post(f"/jobs/{job_id}/reparse-google-vision?parser=generic")
    assert res.status_code == 200
    assert res.json()["pages"] == 2

    parsed = client.get(f"/jobs/{job_id}/parsed")
    assert parsed.status_code == 200
    payload = parsed.json()
    assert payload["page_001"][0]["description"] == "page-1-row"
    assert payload["page_002"][0]["description"] == "page-2-row"

    cleaned = client.get(f"/jobs/{job_id}/cleaned")
    assert cleaned.status_code == 200
    assert cleaned.json()["pages"] == ["page_001.png", "page_002.png"]


def test_cancel_job_endpoint_marks_draft_cancelled(client, monkeypatch):
    monkeypatch.setattr(jobs_service, "resolve_parse_mode", lambda *_args, **_kwargs: "text")

    create = client.post(
        "/jobs",
        files={"file": ("statement.pdf", b"%PDF-1.4\n%%EOF", "application/pdf")},
        data={"mode": "auto", "auto_start": "false"},
    )
    assert create.status_code == 200

    job_id = create.json()["job_id"]

    cancel = client.post(f"/jobs/{job_id}/cancel")
    assert cancel.status_code == 200
    assert cancel.json() == {
        "job_id": job_id,
        "cancelled": True,
        "status": "cancelled",
        "revoked_task_ids": [],
    }

    status = client.get(f"/jobs/{job_id}")
    assert status.status_code == 200
    payload = status.json()
    assert payload["status"] == "cancelled"
    assert payload["step"] == "cancelled"
    assert payload["message"] == "job_cancelled"


def test_delete_job_endpoint_revokes_active_tasks(client, monkeypatch):
    monkeypatch.setattr(jobs_service, "resolve_parse_mode", lambda *_args, **_kwargs: "ocr")

    create = client.post(
        "/jobs",
        files={"file": ("statement.pdf", b"%PDF-1.4\n%%EOF", "application/pdf")},
        data={"mode": "ocr", "auto_start": "false"},
    )
    assert create.status_code == 200

    job_id = create.json()["job_id"]
    repo = jobs_service.JobsRepository(jobs_service.DATA_DIR)
    repo.write_status(
        job_id,
        {
            "status": "processing",
            "step": "ocr_parsing",
            "progress": 42,
            "parse_mode": "ocr",
            "task_id": "root-task",
            "active_task_ids": ["page-task-1", "page-task-2"],
            "pages_total": 3,
            "pages_done": 1,
            "pages_failed": 0,
            "pages_inflight": 2,
        },
    )
    repo.write_json(
        repo.path(job_id, "result", "page_status.json"),
        {
            "page_001": {"status": "done", "task_id": "done-task", "page_index": 1, "page_count": 3},
            "page_002": {"status": "processing", "task_id": "page-task-1", "page_index": 2, "page_count": 3},
            "page_003": {"status": "queued", "task_id": "page-task-2", "page_index": 3, "page_count": 3},
        },
    )

    revoked: list[str] = []

    def _fake_revoke(task_id: str) -> None:
        revoked.append(task_id)

    monkeypatch.setattr(jobs_service, "_revoke_celery_task", _fake_revoke)

    cancel = client.delete(f"/jobs/{job_id}")
    assert cancel.status_code == 200
    assert cancel.json() == {
        "job_id": job_id,
        "cancelled": True,
        "status": "cancelled",
        "revoked_task_ids": ["root-task", "page-task-1", "page-task-2"],
    }
    assert revoked == ["root-task", "page-task-1", "page-task-2"]

    status = client.get(f"/jobs/{job_id}")
    assert status.status_code == 200
    payload = status.json()
    assert payload["status"] == "cancelled"
    assert payload["step"] == "cancelled"
    assert payload["pages_total"] == 3
    assert payload["pages_done"] == 1
    assert payload["pages_failed"] == 0
    assert payload["pages_cancelled"] == 2
    assert payload["pages_inflight"] == 0
    assert payload["active_task_ids"] == []

    pages_status = client.get(f"/jobs/{job_id}/pages/status")
    assert pages_status.status_code == 200
    pages_payload = pages_status.json()
    assert pages_payload["page_001"]["status"] == "done"
    assert pages_payload["page_002"]["status"] == "cancelled"
    assert pages_payload["page_003"]["status"] == "cancelled"
