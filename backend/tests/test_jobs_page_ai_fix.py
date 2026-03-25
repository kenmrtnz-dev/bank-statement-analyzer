import base64
import uuid

import pytest
from fastapi import HTTPException

from app.jobs.repository import JobsRepository
from app.jobs import service as jobs_service
from app.services.openai_page_fix import OpenAIPageFixError


_ONE_PIXEL_PNG = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAusB9WnSle8AAAAASUVORK5CYII="
)


def _seed_job(tmp_path, job_id: str | None = None) -> str:
    if not job_id:
        job_id = str(uuid.uuid4())
    repo = JobsRepository(tmp_path)
    repo.ensure_job_layout(job_id)
    repo.write_bytes(repo.path(job_id, "input", "document.pdf"), b"%PDF-1.4\n%%EOF\n")
    repo.write_bytes(repo.path(job_id, "cleaned", "page_001.png"), _ONE_PIXEL_PNG)
    repo.write_json(
        repo.path(job_id, "ocr", "page_001.raw.json"),
        {
            "provider": "google_vision",
            "source_type": "ocr",
            "page": "page_001",
            "text": "TRANSFER AB1 RECEIVED 100.00",
            "ocr_items": [{"text": "TRANSFER"}, {"text": "AB1"}, {"text": "RECEIVED"}, {"text": "100.00"}],
        },
    )
    repo.write_json(
        repo.path(job_id, "result", "parsed_rows.json"),
        {
            "page_001": [
                {
                    "row_id": "001",
                    "date": "2026-02-01",
                    "description": "Tranfer recived",
                    "debit": "",
                    "credit": "100.00",
                    "balance": "100.00",
                    "row_type": "opening_balance",
                }
            ]
        },
    )
    repo.write_json(repo.path(job_id, "result", "bounds.json"), {"page_001": []})
    return job_id


def test_get_page_ai_fix_uses_canonical_raw_result(tmp_path, monkeypatch):
    job_id = _seed_job(tmp_path)
    monkeypatch.setattr(jobs_service, "DATA_DIR", str(tmp_path))
    monkeypatch.setattr(jobs_service, "is_openai_page_fix_available", lambda: True)
    monkeypatch.setattr(
        jobs_service,
        "get_page_rows",
        lambda _job_id, _page: [
            {
                "row_id": "001",
                "date": "2026-02-01",
                "description": "Tranfer recived",
                "debit": "",
                "credit": "100.00",
                "balance": "100.00",
                "row_type": "opening_balance",
            }
        ],
    )
    monkeypatch.setattr(
        jobs_service,
        "_load_page_ai_fix_raw_payload",
        lambda _repo, _job_id, _page_name: (
            "page_raw_result",
            {"provider": "google_vision", "source_type": "ocr", "ocr_items": [{"text": "Transfer"}]},
        ),
    )

    seen = {}

    def _fake_repair(**kwargs):
        seen.update(kwargs)
        return {
            "rows": [
                {
                    "row_id": "001",
                    "rownumber": None,
                    "row_number": "",
                    "date": "02/01/2026",
                    "description": "Beginning balance",
                    "debit": "",
                    "credit": "",
                    "balance": "100.00",
                    "row_type": "opening_balance",
                }
            ],
            "summary": {
                "changed": True,
                "issues_found": ["description_typo"],
                "rationale": "Corrected the obvious OCR typo in the description.",
            },
        }

    monkeypatch.setattr(jobs_service, "repair_page_rows_with_openai", _fake_repair)

    payload = jobs_service.get_page_ai_fix(job_id, "page_001")

    assert payload["page"] == "page_001"
    assert payload["inputs_used"] == {
        "has_image": True,
        "raw_source": "page_raw_result",
        "parsed_row_count": 1,
    }
    assert payload["proposal"]["rows"][0]["description"] == "Beginning balance"
    assert payload["proposal"]["rows"][0]["row_type"] == "opening_balance"
    assert seen["raw_source"] == "page_raw_result"
    assert seen["raw_payload"] == {"provider": "google_vision", "source_type": "ocr", "ocr_items": [{"text": "Transfer"}]}


def test_get_page_ai_fix_uses_canonical_page_raw_result(tmp_path, monkeypatch):
    job_id = _seed_job(tmp_path)
    monkeypatch.setattr(jobs_service, "DATA_DIR", str(tmp_path))
    monkeypatch.setattr(jobs_service, "is_openai_page_fix_available", lambda: True)
    monkeypatch.setattr(
        jobs_service,
        "get_page_rows",
        lambda _job_id, _page: [
            {
                "row_id": "001",
                "date": "2026-02-01",
                "description": "Tranfer recived",
                "debit": "",
                "credit": "100.00",
                "balance": "100.00",
                "row_type": "opening_balance",
            }
        ],
    )
    monkeypatch.setattr(
        jobs_service,
        "_load_page_ai_fix_raw_payload",
        lambda _repo, _job_id, _page_name: (
            "page_raw_result",
            {"source_type": "text", "provider": "pdftotext", "text": "TRANSFER AB1 RECEIVED 100.00"},
        ),
    )

    seen = {}

    def _fake_repair(**kwargs):
        seen.update(kwargs)
        return {
            "rows": kwargs["parsed_rows"],
            "summary": {"changed": False, "issues_found": [], "rationale": "No confident improvement found."},
        }

    monkeypatch.setattr(jobs_service, "repair_page_rows_with_openai", _fake_repair)

    payload = jobs_service.get_page_ai_fix(job_id, "page_001")

    assert payload["inputs_used"]["raw_source"] == "page_raw_result"
    assert seen["raw_source"] == "page_raw_result"
    assert seen["raw_payload"] == {"source_type": "text", "provider": "pdftotext", "text": "TRANSFER AB1 RECEIVED 100.00"}
    assert payload["proposal"]["rows"][0]["row_type"] == "opening_balance"


def test_get_page_ai_fix_returns_503_when_disabled(tmp_path, monkeypatch):
    job_id = _seed_job(tmp_path)
    monkeypatch.setattr(jobs_service, "DATA_DIR", str(tmp_path))
    monkeypatch.setattr(jobs_service, "is_openai_page_fix_available", lambda: False)

    with pytest.raises(HTTPException) as exc:
        jobs_service.get_page_ai_fix(job_id, "page_001")

    assert exc.value.status_code == 503
    assert exc.value.detail == "page_ai_fix_unavailable"


def test_get_page_ai_fix_returns_502_for_invalid_model_output(tmp_path, monkeypatch):
    job_id = _seed_job(tmp_path)
    monkeypatch.setattr(jobs_service, "DATA_DIR", str(tmp_path))
    monkeypatch.setattr(jobs_service, "is_openai_page_fix_available", lambda: True)
    monkeypatch.setattr(
        jobs_service,
        "get_page_rows",
        lambda _job_id, _page: [
            {
                "row_id": "001",
                "date": "2026-02-01",
                "description": "Tranfer recived",
                "debit": "",
                "credit": "100.00",
                "balance": "100.00",
                "row_type": "opening_balance",
            }
        ],
    )
    monkeypatch.setattr(
        jobs_service,
        "_load_page_ai_fix_raw_payload",
        lambda _repo, _job_id, _page_name: ("page_raw_result", {"provider": "google_vision", "source_type": "ocr"}),
    )
    monkeypatch.setattr(
        jobs_service,
        "repair_page_rows_with_openai",
        lambda **_kwargs: (_ for _ in ()).throw(OpenAIPageFixError("page_ai_fix_invalid_json")),
    )

    with pytest.raises(HTTPException) as exc:
        jobs_service.get_page_ai_fix(job_id, "page_001")

    assert exc.value.status_code == 502
    assert exc.value.detail == "page_ai_fix_invalid_json"


def test_page_ai_fix_api_does_not_persist_until_apply(client, app_with_temp_data, monkeypatch):
    _app, tmp_path = app_with_temp_data
    job_id = _seed_job(tmp_path)
    monkeypatch.setattr(jobs_service, "is_openai_page_fix_available", lambda: True)
    monkeypatch.setattr(
        jobs_service,
        "repair_page_rows_with_openai",
        lambda **_kwargs: {
            "rows": [
                {
                    "row_id": "001",
                    "rownumber": None,
                    "row_number": "",
                    "date": "02/01/2026",
                    "description": "Beginning balance",
                    "debit": "",
                    "credit": "",
                    "balance": "100.00",
                    "row_type": "opening_balance",
                }
            ],
            "summary": {
                "changed": True,
                "issues_found": ["description_typo"],
                "rationale": "Fixed the OCR typo before apply.",
            },
        },
    )

    before = client.get(f"/jobs/{job_id}/parsed/page_001")
    assert before.status_code == 200
    assert before.json()[0]["description"] == "Tranfer recived"
    assert before.json()[0]["row_type"] == "opening_balance"

    proposal = client.post(f"/jobs/{job_id}/pages/page_001/ai-fix")
    assert proposal.status_code == 200
    proposal_rows = proposal.json()["proposal"]["rows"]
    assert proposal_rows[0]["description"] == "Beginning balance"
    assert proposal_rows[0]["row_type"] == "opening_balance"

    after_proposal = client.get(f"/jobs/{job_id}/parsed/page_001")
    assert after_proposal.status_code == 200
    assert after_proposal.json()[0]["description"] == "Tranfer recived"
    assert after_proposal.json()[0]["row_type"] == "opening_balance"

    applied = client.put(f"/jobs/{job_id}/parsed/page_001", json=proposal_rows)
    assert applied.status_code == 200
    assert applied.json()["rows"][0]["description"] == "Beginning balance"
    assert applied.json()["rows"][0]["row_type"] == "opening_balance"

    after_apply = client.get(f"/jobs/{job_id}/parsed/page_001")
    assert after_apply.status_code == 200
    assert after_apply.json()[0]["description"] == "Beginning balance"
    assert after_apply.json()[0]["row_type"] == "opening_balance"
