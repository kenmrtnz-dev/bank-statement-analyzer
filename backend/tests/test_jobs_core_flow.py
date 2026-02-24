import json
from pathlib import Path

from app.modules.jobs import service as jobs_service



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

    parsed = client.get(f"/jobs/{job_id}/parsed")
    assert parsed.status_code == 200
    assert parsed.json()["page_001"][0]["description"] == "Deposit"

    updated_rows = [
        {
            "row_id": "001",
            "date": "02/01/2026",
            "description": "Edited Deposit",
            "debit": "50.00",
            "credit": "",
            "balance": "950.00",
        }
    ]
    update = client.put(f"/jobs/{job_id}/parsed/page_001", json=updated_rows)
    assert update.status_code == 200
    assert update.json()["rows"][0]["description"] == "Edited Deposit"
    assert update.json()["summary"]["debit_transactions"] == 1
    assert update.json()["summary"]["total_debit"] == 50.0

    parsed_after_update = client.get(f"/jobs/{job_id}/parsed")
    assert parsed_after_update.status_code == 200
    assert parsed_after_update.json()["page_001"][0]["description"] == "Edited Deposit"

    bounds = client.get(f"/jobs/{job_id}/rows/page_001/bounds")
    assert bounds.status_code == 200
    assert bounds.json()[0]["row_id"] == "001"

    summary = client.get(f"/jobs/{job_id}/summary")
    assert summary.status_code == 200
    assert summary.json().get("total_transactions") == 1

    export_pdf = client.get(f"/jobs/{job_id}/export/pdf")
    assert export_pdf.status_code == 200
    assert export_pdf.headers.get("content-type", "").startswith("application/pdf")

    export_excel = client.get(f"/jobs/{job_id}/export/excel")
    assert export_excel.status_code == 200
    assert "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" in export_excel.headers.get(
        "content-type", ""
    )
