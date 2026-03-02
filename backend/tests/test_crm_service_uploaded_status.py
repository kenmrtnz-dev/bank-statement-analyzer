import json
from pathlib import Path

from app.modules.crm import service as crm_service


def test_load_attachment_process_index_marks_uploaded_from_meta(monkeypatch, tmp_path: Path):
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    job_dir = tmp_path / "jobs" / "job-001"
    job_dir.mkdir(parents=True, exist_ok=True)

    (job_dir / "meta.json").write_text(
        json.dumps(
            {
                "source_attachment_id": "att-123",
                "crm_export_uploaded": True,
                "crm_export_attachment_id": "att-result-1",
            }
        ),
        encoding="utf-8",
    )
    (job_dir / "status.json").write_text(
        json.dumps({"status": "done", "step": "completed", "progress": 100}),
        encoding="utf-8",
    )

    index = crm_service._load_attachment_process_index()
    assert "att-123" in index
    item = index["att-123"]
    assert item["process_job_id"] == "job-001"
    assert item["process_status"] == "uploaded"
    assert item["process_step"] == "uploaded"
    assert item["process_progress"] == 100

