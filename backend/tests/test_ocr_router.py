from pathlib import Path

import pytest

from app.services.ocr import router
from app.services.ocr.openai_vision import plain_text_to_ocr_items


class _FakePage:
    def __init__(self, text: str):
        self._text = text

    def extract_text(self):
        return self._text


class _FakeReader:
    def __init__(self, _path: str, pages: list[_FakePage]):
        self.pages = pages


def test_resolve_document_parse_mode_prefers_text_when_avg_chars_high(monkeypatch, tmp_path: Path):
    pdf = tmp_path / "sample.pdf"
    pdf.write_bytes(b"%PDF-1.4\n%%EOF")

    def _reader(_path: str):
        return _FakeReader(_path, [_FakePage("A" * 400), _FakePage("B" * 360)])

    monkeypatch.setattr(router, "PdfReader", _reader)
    assert router.resolve_document_parse_mode(str(pdf), "auto") == "text"


def test_resolve_document_parse_mode_uses_ocr_when_text_is_low(monkeypatch, tmp_path: Path):
    pdf = tmp_path / "scan.pdf"
    pdf.write_bytes(b"%PDF-1.4\n%%EOF")

    def _reader(_path: str):
        return _FakeReader(_path, [_FakePage("tiny"), _FakePage("")])

    monkeypatch.setattr(router, "PdfReader", _reader)
    assert router.resolve_document_parse_mode(str(pdf), "auto") == "ocr"


def test_openai_selected_even_when_page_count_exceeds_previous_limit(monkeypatch):
    monkeypatch.setenv("ENABLE_OPENAI_OCR", "true")

    class _DummyOpenAIClient:
        pass

    monkeypatch.setattr(router.OpenAIVisionOCR, "from_env", staticmethod(lambda: _DummyOpenAIClient()))
    selected = router.build_scanned_ocr_router(page_count=75, fallback_backend="easyocr")
    assert selected.engine_name == "openai_vision"
    assert selected.openai_client is not None


def test_plain_text_to_ocr_items_shapes_tokens():
    items = plain_text_to_ocr_items("01/01/2026 Deposit 100.00", page_width=1000, page_height=1000)
    assert items
    assert items[0]["text"] == "01/01/2026"
    assert len(items[0]["bbox"]) == 4
