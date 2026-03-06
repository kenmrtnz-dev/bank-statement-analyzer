from pathlib import Path

from app.ocr import pipeline as ocr_pipeline
from app.ocr.pipeline import _normalize_structured_ai_rows, _normalize_structured_row_date


def test_normalize_structured_row_date_to_mdy():
    assert _normalize_structured_row_date("2026-02-23") == "02/23/2026"
    assert _normalize_structured_row_date("23/02/2026") == "02/23/2026"
    assert _normalize_structured_row_date("2-3-2026") == "02/03/2026"
    assert _normalize_structured_row_date("10/10/1925") == "10/10/2025"


def test_normalize_structured_ai_rows_keeps_row_number_and_formats_date():
    rows, bounds = _normalize_structured_ai_rows(
        structured_rows=[
            {
                "rownumber": 15,
                "date": "2026-02-24",
                "description": "ATM Withdrawal",
                "debit": 100.0,
                "credit": None,
                "balance": 900.0,
                "bounds": {"x1": 10, "y1": 20, "x2": 200, "y2": 40},
            },
            {
                "date": "02/25/2026",
                "description": "No row number present",
                "debit": None,
                "credit": 500.0,
                "balance": 1400.0,
                "bounds": {"x1": 10, "y1": 50, "x2": 200, "y2": 70},
            },
        ],
        page_width=1000,
        page_height=2000,
    )

    assert len(rows) == 2
    assert len(bounds) == 2

    assert rows[0]["rownumber"] == 15
    assert rows[0]["row_number"] == "15"
    assert rows[0]["date"] == "02/24/2026"
    assert rows[0]["debit"] == 100.0
    assert rows[0]["credit"] is None

    assert rows[1]["rownumber"] is None
    assert rows[1]["row_number"] == ""
    assert rows[1]["date"] == "02/25/2026"
    assert rows[1]["credit"] == 500.0


def test_normalize_structured_ai_rows_keeps_row_number_empty_when_missing():
    rows, _ = _normalize_structured_ai_rows(
        structured_rows=[
            {
                "date": "10-10-2025",
                "description": "CK I 1320695",
                "debit": 100.0,
                "credit": None,
                "balance": 200.0,
                "bounds": {"x1": 10, "y1": 20, "x2": 200, "y2": 40},
            }
        ],
        page_width=1000,
        page_height=2000,
    )
    assert rows[0]["rownumber"] is None
    assert rows[0]["row_number"] == ""


def test_run_pipeline_text_fallback_uses_google_vision(monkeypatch, tmp_path: Path):
    job_dir = tmp_path / "job"
    (job_dir / "input").mkdir(parents=True, exist_ok=True)
    (job_dir / "input" / "document.pdf").write_bytes(b"%PDF-1.4\n%%EOF")

    monkeypatch.setattr(
        ocr_pipeline,
        "_run_text_pipeline",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("text_failed")),
    )
    monkeypatch.setattr(
        ocr_pipeline,
        "_run_google_vision_legacy_pipeline",
        lambda **_kwargs: (
            {"page_001": [{"row_id": "001", "date": "01/01/2026", "description": "x", "debit": 0, "credit": 1, "balance": 1}]},
            {"page_001": []},
            {"job": {"ocr_backend": "google_vision"}, "pages": {"page_001": {"rows_parsed": 1}}},
        ),
    )

    result = ocr_pipeline.run_pipeline(job_dir, "text", report=lambda *_args, **_kwargs: None)
    assert result["parse_mode"] == "google_vision"
    assert "page_001" in result["parsed_rows"]
