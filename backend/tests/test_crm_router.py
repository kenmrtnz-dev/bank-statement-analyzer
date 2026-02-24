from fastapi.responses import Response

import importlib

crm_router_module = importlib.import_module("app.modules.crm.router")


def test_crm_attachments_list_endpoint(client, monkeypatch):
    payload = {
        "items": [
            {
                "lead_id": "lead-1",
                "account_name": "Borrower A",
                "attachment_id": "att-1",
                "filename": "statement.pdf",
                "content_type": "application/pdf",
                "size_bytes": 1234,
                "status": "available",
                "error": "",
                "download_url": "/crm/attachments/att-1/file",
            }
        ],
        "lead_count": 1,
        "attachment_count": 1,
    }

    captured = {}

    def _fake_list(limit=25, offset=0, probe=None):
        captured["limit"] = limit
        captured["offset"] = offset
        captured["probe"] = probe
        return payload

    monkeypatch.setattr(crm_router_module, "list_bank_statement_attachments", _fake_list)

    res = client.get("/crm/attachments?limit=10&offset=20&probe=eager")
    assert res.status_code == 200
    body = res.json()
    assert body["lead_count"] == 1
    assert body["attachment_count"] == 1
    assert body["items"][0]["attachment_id"] == "att-1"
    assert captured == {"limit": 10, "offset": 20, "probe": "eager"}


def test_crm_attachment_download_endpoint(client, monkeypatch):
    def _fake_download(_attachment_id: str):
        return Response(
            content=b"fake-pdf-data",
            media_type="application/pdf",
            headers={"Content-Disposition": 'attachment; filename="statement.pdf"'},
        )

    monkeypatch.setattr(crm_router_module, "download_bank_statement_attachment", _fake_download)

    res = client.get("/crm/attachments/att-1/file")
    assert res.status_code == 200
    assert res.content == b"fake-pdf-data"
    assert "application/pdf" in res.headers.get("content-type", "")
    assert "statement.pdf" in res.headers.get("content-disposition", "")


def test_crm_attachment_begin_process_endpoint(client, monkeypatch):
    monkeypatch.setattr(
        crm_router_module,
        "create_job_from_attachment",
        lambda attachment_id, requested_mode="auto": {
            "job_id": "job-123",
            "parse_mode": requested_mode,
            "started": True,
            "attachment_id": attachment_id,
            "source_filename": "statement.pdf",
        },
    )

    res = client.post("/crm/attachments/att-2/begin-process")
    assert res.status_code == 200
    body = res.json()
    assert body["job_id"] == "job-123"
    assert body["attachment_id"] == "att-2"
    assert body["started"] is True
