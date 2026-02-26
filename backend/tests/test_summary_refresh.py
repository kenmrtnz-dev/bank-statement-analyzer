import json
from pathlib import Path

from app.modules.jobs.service import get_summary


def test_get_summary_recomputes_when_cached_summary_is_missing_new_fields(monkeypatch, tmp_path: Path):
    job_id = "00000000-0000-0000-0000-000000000999"
    jobs_root = tmp_path / "jobs" / job_id
    (jobs_root / "input").mkdir(parents=True, exist_ok=True)
    (jobs_root / "result").mkdir(parents=True, exist_ok=True)
    (jobs_root / "input" / "document.pdf").write_bytes(b"%PDF-1.4\n%%EOF")

    parsed_rows = {
        "page_001": [
            {"row_id": "001", "date": "01/01/2026", "description": "A", "debit": None, "credit": 600, "balance": 600}
        ]
    }
    (jobs_root / "result" / "parsed_rows.json").write_text(json.dumps(parsed_rows), encoding="utf-8")

    stale_summary = {
        "total_transactions": 1,
        "debit_transactions": 0,
        "credit_transactions": 1,
        "total_debit": 0.0,
        "total_credit": 600.0,
        "ending_balance": 600.0,
        "adb": 600.0,
        "monthly": [{"month": "2026-01", "debit": 0.0, "credit": 600.0, "avg_debit": 0.0, "avg_credit": 600.0, "adb": 600.0}],
    }
    (jobs_root / "result" / "summary.json").write_text(json.dumps(stale_summary), encoding="utf-8")

    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    from app.modules.jobs import service as jobs_service

    monkeypatch.setattr(jobs_service, "DATA_DIR", str(tmp_path))

    refreshed = get_summary(job_id)
    assert "total_credit_monthly_average" in refreshed
    assert refreshed["total_credit_monthly_average"] == 30.0
    assert refreshed["monthly"][0]["credit_count"] == 1
    assert refreshed["monthly"][0]["debit_count"] == 0
