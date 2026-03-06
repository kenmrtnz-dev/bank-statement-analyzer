from pathlib import Path

from app.parser import pipeline
from app.parser.simple_column_parser import parse_transactions_from_pdftotext_text


def test_pdftotext_layered_parser_three_passes():
    text = """
STATEMENT 2025
Date Description Debit Credit Balance
01/05 ATM Withdrawal 500.00 9500.00
via Terminal
01/06 Deposit 11500.00
SERVICE CHARGE
PAGE TOTAL 11500.00
"""
    rows = parse_transactions_from_pdftotext_text(text)
    assert len(rows) == 2
    assert rows[0]["date"] == "2025-01-05"
    assert rows[0]["description"] == "ATM Withdrawal via Terminal"
    assert rows[0]["debit"] == 500.0
    assert rows[0]["credit"] == 0.0
    assert rows[0]["balance"] == 9500.0
    assert rows[1]["date"] == "2025-01-06"
    assert rows[1]["credit"] == 2000.0
    assert rows[1]["balance"] == 11500.0


def test_process_document_pdftotext_uses_layered_parser_for_generic(monkeypatch, tmp_path: Path):
    sample_pdf = tmp_path / "sample.pdf"
    sample_pdf.write_text("dummy", encoding="utf-8")

    monkeypatch.setattr(
        pipeline.pdf_text_extractor,
        "extract_text",
        lambda _path: "STATEMENT 2025\n01/05 ATM Withdrawal 500.00 9500.00\n01/06 Deposit 11500.00\n",
    )
    monkeypatch.setattr(pipeline.bank_detector, "detect_bank", lambda _text: "generic")

    payload = pipeline.process_document(str(sample_pdf), ocr_engine="pdftotext", parser_profile="generic")

    assert payload["ocr_source"] == "pdftotext"
    assert payload["parser_profile_used"] == "generic"
    assert payload["parser_strategy"] == "pdftotext_layered_parser"
    assert len(payload["transactions"]) == 2
