from pathlib import Path

from app.bank_profiles import PROFILES
from app.ocr import pipeline as ocr_pipeline
from app.ocr.pipeline import _filter_rows_and_bounds, _normalize_structured_ai_rows, _normalize_structured_row_date


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


def test_normalize_structured_ai_rows_reuses_last_date_for_missing_transaction_date():
    rows, _ = _normalize_structured_ai_rows(
        structured_rows=[
            {
                "date": "10-10-2025",
                "description": "CK I 1320695",
                "debit": 100.0,
                "credit": None,
                "balance": 200.0,
                "bounds": {"x1": 10, "y1": 20, "x2": 200, "y2": 40},
            },
            {
                "date": "",
                "description": "SERVICE FEE",
                "debit": 25.0,
                "credit": None,
                "balance": 175.0,
                "bounds": {"x1": 10, "y1": 50, "x2": 200, "y2": 70},
            },
        ],
        page_width=1000,
        page_height=2000,
    )

    assert rows[0]["date"] == "10/10/2025"
    assert rows[1]["date"] == "10/10/2025"
    assert rows[1]["debit"] == 25.0


def test_normalize_structured_ai_rows_reuses_last_date_hint_from_previous_page():
    rows, _ = _normalize_structured_ai_rows(
        structured_rows=[
            {
                "date": "",
                "description": "SERVICE FEE",
                "debit": 25.0,
                "credit": None,
                "balance": 175.0,
                "bounds": {"x1": 10, "y1": 50, "x2": 200, "y2": 70},
            },
        ],
        page_width=1000,
        page_height=2000,
        last_date_hint="10/10/2025",
    )

    assert rows[0]["date"] == "10/10/2025"


def test_normalize_structured_ai_rows_preserves_zero_and_credit_values():
    rows, _ = _normalize_structured_ai_rows(
        structured_rows=[
            {
                "date": "2026-02-24",
                "description": "Deposit",
                "debit": 0.0,
                "credit": 500.0,
                "balance": 1500.0,
                "bounds": {"x1": 10, "y1": 20, "x2": 200, "y2": 40},
            }
        ],
        page_width=1000,
        page_height=2000,
    )

    assert rows[0]["debit"] == 0.0
    assert rows[0]["credit"] == 500.0


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


def _ocr_item(text: str, x1: int, y1: int, x2: int, y2: int) -> dict:
    return {"text": text, "bbox": [(x1, y1), (x2, y1), (x2, y2), (x1, y2)]}


def test_run_ocr_pipeline_reuses_header_hint_for_headerless_continuation_page(monkeypatch, tmp_path: Path):
    job_dir = tmp_path / "job"
    pages_dir = job_dir / "pages"
    cleaned_dir = job_dir / "cleaned"
    ocr_dir = job_dir / "ocr"
    for directory in (pages_dir, cleaned_dir, ocr_dir):
        directory.mkdir(parents=True, exist_ok=True)

    page_files = ["page_001.png", "page_002.png"]
    for page_file in page_files:
        (cleaned_dir / page_file).write_bytes(b"png")

    page_1_items = [
        _ocr_item("Date", 40, 20, 80, 30),
        _ocr_item("Description", 180, 20, 270, 30),
        _ocr_item("Debit", 470, 20, 520, 30),
        _ocr_item("Credit", 560, 20, 620, 30),
        _ocr_item("Balance", 680, 20, 750, 30),
        _ocr_item("08/27/2025", 40, 60, 114, 70),
        _ocr_item("PAYMENT", 180, 60, 250, 70),
        _ocr_item("1000.00", 560, 60, 618, 70),
        _ocr_item("9000.00", 690, 60, 748, 70),
    ]
    page_2_items = [
        _ocr_item("TRANSFER", 180, 60, 258, 70),
        _ocr_item("500.00", 560, 60, 612, 70),
        _ocr_item("9500.00", 690, 60, 748, 70),
    ]

    class _FakeRouter:
        engine_name = "fake_ocr"
        openai_client = None

        def ocr_page(self, page_path):
            name = Path(page_path).name
            return page_1_items if name == "page_001.png" else page_2_items

    monkeypatch.setattr(ocr_pipeline, "prepare_ocr_pages", lambda **_kwargs: page_files)
    monkeypatch.setattr(ocr_pipeline, "build_scanned_ocr_router", lambda page_count=1: _FakeRouter())
    monkeypatch.setattr(ocr_pipeline, "_image_size", lambda _path: (1200, 900))

    parsed, _bounds, diagnostics = ocr_pipeline._run_ocr_pipeline(
        input_pdf=job_dir / "input.pdf",
        pages_dir=pages_dir,
        cleaned_dir=cleaned_dir,
        ocr_dir=ocr_dir,
        report=lambda *_args, **_kwargs: None,
    )

    assert diagnostics["pages"]["page_002"]["header_hint_used"] is True
    assert parsed["page_002"][0]["credit"] == "500.00"
    assert parsed["page_002"][0]["debit"] is None


def test_filter_rows_drops_balance_forwarded_and_beginning_balance_rows():
    rows = [
        {
            "row_id": "001",
            "date": "08/27/2025",
            "description": "Beggining Balance",
            "debit": None,
            "credit": None,
            "balance": 9000.0,
        },
        {
            "row_id": "002",
            "date": "08/28/2025",
            "description": "Balance forwarded",
            "debit": None,
            "credit": None,
            "balance": 9000.0,
        },
        {
            "row_id": "003",
            "date": "08/28/2025",
            "description": "TRANSFER",
            "debit": 500.0,
            "credit": None,
            "balance": 8500.0,
        },
    ]
    bounds = [
        {"row_id": "001", "x1": 0.1, "y1": 0.1, "x2": 0.2, "y2": 0.2},
        {"row_id": "002", "x1": 0.1, "y1": 0.2, "x2": 0.2, "y2": 0.3},
        {"row_id": "003", "x1": 0.1, "y1": 0.3, "x2": 0.2, "y2": 0.4},
    ]

    filtered_rows, filtered_bounds = _filter_rows_and_bounds(rows, bounds, PROFILES["GENERIC"])

    assert filtered_rows == [
        {
            "row_id": "001",
            "date": "08/28/2025",
            "description": "TRANSFER",
            "debit": 500.0,
            "credit": None,
            "balance": 8500.0,
            "row_type": "transaction",
        }
    ]
    assert filtered_bounds == [{"row_id": "001", "x1": 0.1, "y1": 0.3, "x2": 0.2, "y2": 0.4}]
