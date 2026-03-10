from pathlib import Path

from app.bank_profiles import PROFILES
from app.ocr import pipeline as ocr_pipeline
from app.ocr.pipeline import _dedupe_document_rows, _dedupe_page_overlap, _repair_page_flow_columns


def test_dedupe_page_overlap_drops_repeated_leading_rows_and_renumbers_bounds():
    previous_rows = [
        {"row_id": "001", "date": "2025-11-10", "description": "A", "debit": "10.00", "credit": None, "balance": "100.00", "row_type": "transaction"},
        {"row_id": "002", "date": "2025-11-10", "description": "B", "debit": "20.00", "credit": None, "balance": "80.00", "row_type": "transaction"},
    ]
    current_rows = [
        {"row_id": "001", "date": "2025-11-10", "description": "A", "debit": "10.00", "credit": None, "balance": "100.00", "row_type": "transaction"},
        {"row_id": "002", "date": "2025-11-10", "description": "B", "debit": "20.00", "credit": None, "balance": "80.00", "row_type": "transaction"},
        {"row_id": "003", "date": "2025-11-11", "description": "C", "debit": None, "credit": "50.00", "balance": "130.00", "row_type": "transaction"},
    ]
    current_bounds = [
        {"row_id": "001", "x1": 0.1, "y1": 0.1, "x2": 0.2, "y2": 0.2},
        {"row_id": "002", "x1": 0.1, "y1": 0.2, "x2": 0.2, "y2": 0.3},
        {"row_id": "003", "x1": 0.1, "y1": 0.3, "x2": 0.2, "y2": 0.4},
    ]

    rows, bounds = _dedupe_page_overlap(previous_rows, current_rows, current_bounds)

    assert rows == [
        {
            "row_id": "001",
            "date": "2025-11-11",
            "description": "C",
            "debit": None,
            "credit": "50.00",
            "balance": "130.00",
            "row_type": "transaction",
        }
    ]
    assert bounds == [{"row_id": "001", "x1": 0.1, "y1": 0.3, "x2": 0.2, "y2": 0.4}]


def test_dedupe_document_rows_drops_exact_rows_seen_on_prior_pages():
    seen = {
        ("2026-01-27", "OTHER BANKS 85606849 471373471373 9 IBTD 000000000", "", "1300.00", "7033252.41", "transaction"),
    }
    current_rows = [
        {"row_id": "001", "date": "2026-01-27", "description": "OTHER BANKS 85606849 471373471373 9 IBTD 000000000", "debit": None, "credit": "1300.00", "balance": "7033252.41", "row_type": "transaction"},
        {"row_id": "002", "date": "2026-01-27", "description": "RETAIL BANKING INTERNET FT DBFT CA-CA 026027070042 DIGIFI POB 000000000", "debit": None, "credit": "38400.00", "balance": "7071652.41", "row_type": "transaction"},
    ]
    current_bounds = [
        {"row_id": "001", "x1": 0.1, "y1": 0.1, "x2": 0.2, "y2": 0.2},
        {"row_id": "002", "x1": 0.1, "y1": 0.2, "x2": 0.2, "y2": 0.3},
    ]

    rows, bounds = _dedupe_document_rows(seen, current_rows, current_bounds)

    assert rows == [
        {
            "row_id": "001",
            "date": "2026-01-27",
            "description": "RETAIL BANKING INTERNET FT DBFT CA-CA 026027070042 DIGIFI POB 000000000",
            "debit": None,
            "credit": "38400.00",
            "balance": "7071652.41",
            "row_type": "transaction",
        }
    ]
    assert bounds == [{"row_id": "001", "x1": 0.1, "y1": 0.2, "x2": 0.2, "y2": 0.3}]


def _word(text: str, x: float, y: float, w: float = 48.0, h: float = 10.0) -> dict:
    return {
        "text": text,
        "x1": x,
        "y1": y,
        "x2": x + w,
        "y2": y + h,
    }


def test_run_text_pipeline_reuses_last_date_from_previous_page(monkeypatch, tmp_path: Path):
    page_one_words = [
        _word("Date", 40, 20),
        _word("Description", 180, 20, 90),
        _word("Debit", 470, 20),
        _word("Credit", 560, 20),
        _word("Balance", 680, 20, 70),
        _word("08/27/2025", 40, 60, 74),
        _word("PAYMENT", 180, 60, 70),
        _word("1000.00", 470, 60, 58),
        _word("9000.00", 690, 60, 58),
    ]
    page_two_words = [
        _word("Date", 40, 20),
        _word("Description", 180, 20, 90),
        _word("Debit", 470, 20),
        _word("Credit", 560, 20),
        _word("Balance", 680, 20, 70),
        _word("TRANSFER", 180, 60, 74),
        _word("500.00", 470, 60, 52),
        _word("8500.00", 690, 60, 58),
    ]

    monkeypatch.setattr(
        ocr_pipeline,
        "extract_pdf_layout_pages",
        lambda _path: [
            {"width": 900.0, "height": 1200.0, "words": page_one_words, "text": "Date Description Debit Credit Balance 08/27/2025 PAYMENT 1000.00 9000.00"},
            {"width": 900.0, "height": 1200.0, "words": page_two_words, "text": "Date Description Debit Credit Balance TRANSFER 500.00 8500.00"},
        ],
    )

    parsed, _bounds, diagnostics = ocr_pipeline._run_text_pipeline(
        input_pdf=tmp_path / "sample.pdf",
        ocr_dir=tmp_path / "ocr",
        report=lambda *_args, **_kwargs: None,
    )

    assert diagnostics["pages"]["page_002"]["rows_parsed"] == 1
    assert parsed["page_002"][0]["date"] == "2025-08-27"
    assert parsed["page_002"][0]["description"] == "TRANSFER"
    assert parsed["page_002"][0]["debit"] == "500.00"


def test_run_text_pipeline_reuses_last_header_hint_across_profile_change(monkeypatch, tmp_path: Path):
    page_one_words = [
        _word("Book", 158, 20, 52),
        _word("Date", 216, 20, 54),
        _word("Reference", 572, 20, 260),
        _word("Descript", 1050, 20, 270),
        _word("Value", 1450, 20, 62),
        _word("Date", 1518, 20, 112),
        _word("Cheque", 1988, 20, 90),
        _word("Number", 2086, 20, 76),
        _word("Debit", 2228, 20, 112),
        _word("Credit", 2642, 20, 130),
        _word("Closing", 3056, 20, 150),
        _word("Balance", 3215, 20, 191),
        _word("02", 158, 60, 34),
        _word("MAY", 202, 60, 62),
        _word("24", 272, 60, 32),
        _word("TT2412333BRM\\F", 572, 60, 380),
        _word("Cash", 1075, 60, 90),
        _word("Deposit", 1172, 60, 116),
        _word("02", 1518, 60, 34),
        _word("MAY", 1562, 60, 62),
        _word("24", 1632, 60, 32),
        _word("14,000.00", 2670, 60, 144),
        _word("952,374.45", 3140, 60, 164),
    ]
    page_two_words = [
        _word("14", 158, 60, 34),
        _word("MAY", 202, 60, 62),
        _word("24", 272, 60, 32),
        _word("FT24135P4QYB\\E", 572, 60, 386),
        _word("InstaPay", 1075, 60, 140),
        _word("from", 1224, 60, 62),
        _word("other", 1294, 60, 74),
        _word("14", 1518, 60, 34),
        _word("MAY", 1562, 60, 62),
        _word("24", 1632, 60, 32),
        _word("200.00", 2678, 60, 112),
        _word("107,668.45", 3142, 60, 170),
    ]

    def _detect_profile(text: str):
        if "Book Date Reference" in text:
            return PROFILES["EWB"]
        return PROFILES["GENERIC"]

    monkeypatch.setattr(ocr_pipeline, "detect_bank_profile", _detect_profile)
    monkeypatch.setattr(
        ocr_pipeline,
        "extract_pdf_layout_pages",
        lambda _path: [
            {
                "width": 3600.0,
                "height": 4800.0,
                "words": page_one_words,
                "text": "Book Date Reference Descript Value Date Cheque Number Debit Credit Closing Balance",
            },
            {
                "width": 3600.0,
                "height": 4800.0,
                "words": page_two_words,
                "text": "Continuation page without repeated header",
            },
        ],
    )

    parsed, _bounds, diagnostics = ocr_pipeline._run_text_pipeline(
        input_pdf=tmp_path / "sample.pdf",
        ocr_dir=tmp_path / "ocr",
        report=lambda *_args, **_kwargs: None,
    )

    assert diagnostics["pages"]["page_001"]["rows_parsed"] == 1
    assert diagnostics["pages"]["page_002"]["header_hint_used"] is True
    assert diagnostics["pages"]["page_002"]["fallback_mode"] == "header_hint_reuse"
    assert parsed["page_002"][0]["date"] == "2024-05-14"
    assert parsed["page_002"][0]["debit"] is None
    assert parsed["page_002"][0]["credit"] == "200.00"
    assert parsed["page_002"][0]["balance"] == "107668.45"


def test_repair_page_flow_columns_keeps_descending_debits_as_debits():
    rows = [
        {
            "row_id": "001",
            "date": "2024-10-23",
            "description": "S44939015 ATM WITHDRAWAL",
            "debit": "40000.00",
            "credit": None,
            "balance": "1801415.34",
            "row_type": "transaction",
        },
        {
            "row_id": "002",
            "date": "2024-10-23",
            "description": "S44937699 ATM WITHDRAWAL",
            "debit": "40000.00",
            "credit": None,
            "balance": "1841415.34",
            "row_type": "transaction",
        },
        {
            "row_id": "003",
            "date": "2024-10-23",
            "description": "UB388021 SERVICE FEE",
            "debit": "30.00",
            "credit": None,
            "balance": "1881415.34",
            "row_type": "transaction",
        },
        {
            "row_id": "004",
            "date": "2024-10-23",
            "description": "UB388008 SERVICE FEE",
            "debit": "30.00",
            "credit": None,
            "balance": "1881445.34",
            "row_type": "transaction",
        },
    ]

    repaired = _repair_page_flow_columns(rows)

    assert repaired[1]["debit"] == "40000.00"
    assert repaired[1]["credit"] is None
    assert repaired[3]["debit"] == "30.00"
    assert repaired[3]["credit"] is None


def test_repair_page_flow_columns_still_swaps_wrong_side_on_ascending_pages():
    rows = [
        {
            "row_id": "001",
            "date": "2026-01-01",
            "description": "DEPOSIT",
            "debit": None,
            "credit": "200.00",
            "balance": "1200.00",
            "row_type": "transaction",
        },
        {
            "row_id": "002",
            "date": "2026-01-01",
            "description": "TRANSFER IN",
            "debit": "50.00",
            "credit": None,
            "balance": "1250.00",
            "row_type": "transaction",
        },
        {
            "row_id": "003",
            "date": "2026-01-01",
            "description": "ATM WITHDRAWAL",
            "debit": "20.00",
            "credit": None,
            "balance": "1230.00",
            "row_type": "transaction",
        },
    ]

    repaired = _repair_page_flow_columns(rows, previous_balance_hint="1000.00")

    assert repaired[1]["debit"] is None
    assert repaired[1]["credit"] == "50.00"
    assert repaired[2]["debit"] == "20.00"
    assert repaired[2]["credit"] is None


def test_repair_page_flow_columns_drops_zero_side_from_dual_amount_rows():
    rows = [
        {
            "row_id": "001",
            "date": "2026-02-01",
            "description": "Deposit",
            "debit": "0.00",
            "credit": "500.00",
            "balance": "1500.00",
            "row_type": "transaction",
        }
    ]

    repaired = _repair_page_flow_columns(rows, previous_balance_hint="1000.00")

    assert repaired[0]["debit"] is None
    assert repaired[0]["credit"] == "500.00"


def test_repair_page_flow_columns_reduces_dual_amount_rows_to_the_matching_side():
    rows = [
        {
            "row_id": "001",
            "date": "2026-02-01",
            "description": "Deposit",
            "debit": None,
            "credit": "500.00",
            "balance": "1500.00",
            "row_type": "transaction",
        },
        {
            "row_id": "002",
            "date": "2026-02-02",
            "description": "ATM WITHDRAWAL",
            "debit": "50.00",
            "credit": "20.00",
            "balance": "1450.00",
            "row_type": "transaction",
        },
    ]

    repaired = _repair_page_flow_columns(rows, previous_balance_hint="1000.00")

    assert repaired[1]["debit"] == "50.00"
    assert repaired[1]["credit"] is None


def test_run_text_pipeline_keeps_descending_unionbank_debits(monkeypatch, tmp_path: Path):
    page_rows = [
        {
            "row_id": "001",
            "date": "2024-10-23",
            "description": "S44939015 ATM WITHDRAWAL",
            "debit": "40000.00",
            "credit": None,
            "balance": "1801415.34",
        },
        {
            "row_id": "002",
            "date": "2024-10-23",
            "description": "S44937699 ATM WITHDRAWAL",
            "debit": "40000.00",
            "credit": None,
            "balance": "1841415.34",
        },
        {
            "row_id": "003",
            "date": "2024-10-23",
            "description": "UB388021 SERVICE FEE",
            "debit": "30.00",
            "credit": None,
            "balance": "1881415.34",
        },
        {
            "row_id": "004",
            "date": "2024-10-23",
            "description": "UB388008 SERVICE FEE",
            "debit": "30.00",
            "credit": None,
            "balance": "1881445.34",
        },
    ]

    monkeypatch.setattr(
        ocr_pipeline,
        "extract_pdf_layout_pages",
        lambda _path: [{"width": 900.0, "height": 1200.0, "words": [], "text": "unionbank statement"}],
    )
    monkeypatch.setattr(ocr_pipeline, "detect_bank_profile", lambda _text: PROFILES["UNIONBANK"])
    monkeypatch.setattr(
        ocr_pipeline,
        "parse_page_with_profile_fallback",
        lambda *_args, **_kwargs: (
            page_rows,
            [],
            {
                "profile_detected": "UNIONBANK",
                "profile_selected": "UNIONBANK",
                "header_detected": True,
                "header_hint_used": False,
                "fallback_applied": False,
                "fallback_mode": None,
            },
        ),
    )

    parsed, _bounds, diagnostics = ocr_pipeline._run_text_pipeline(
        input_pdf=tmp_path / "sample.pdf",
        ocr_dir=tmp_path / "ocr",
        report=lambda *_args, **_kwargs: None,
    )

    assert diagnostics["pages"]["page_001"]["rows_parsed"] == 4
    assert parsed["page_001"][1]["debit"] == "40000.00"
    assert parsed["page_001"][1]["credit"] is None
    assert parsed["page_001"][3]["debit"] == "30.00"
    assert parsed["page_001"][3]["credit"] is None


def test_repair_page_flow_columns_keeps_debit_biased_rows_on_mixed_direction_pages():
    rows = [
        {
            "row_id": "001",
            "date": "2024-10-03",
            "description": "UB612962 ONLINE INSTAPAY-SEND",
            "debit": "16550.00",
            "credit": None,
            "balance": "2716847.72",
            "row_type": "transaction",
        },
        {
            "row_id": "002",
            "date": "2024-10-03",
            "description": "UB436102 SERVICE FEE",
            "debit": "30.00",
            "credit": None,
            "balance": "2733397.72",
            "row_type": "transaction",
        },
        {
            "row_id": "003",
            "date": "2024-10-03",
            "description": "UB435743 SERVICE FEE",
            "debit": "30.00",
            "credit": None,
            "balance": "2733427.72",
            "row_type": "transaction",
        },
        {
            "row_id": "004",
            "date": "2024-10-03",
            "description": "UB199704 Reversal of UB199695",
            "debit": None,
            "credit": "3000.00",
            "balance": "2748457.72",
            "row_type": "transaction",
        },
        {
            "row_id": "005",
            "date": "2024-10-03",
            "description": "UB199695 ONLINE INSTAPAY-SEND",
            "debit": "3000.00",
            "credit": None,
            "balance": "2745457.72",
            "row_type": "transaction",
        },
    ]

    repaired = _repair_page_flow_columns(rows)

    assert repaired[2]["debit"] == "30.00"
    assert repaired[2]["credit"] is None
    assert repaired[3]["debit"] is None
    assert repaired[3]["credit"] == "3000.00"
