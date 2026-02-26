from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List

from pypdf import PdfReader

from app.services.ocr.openai_vision import OpenAIVisionOCR

DEFAULT_DIGITAL_TEXT_THRESHOLD = 300


def _env_int(name: str, default: int) -> int:
    try:
        return int(str(os.getenv(name, default)).strip())
    except Exception:
        return default


@dataclass(frozen=True)
class DocumentTextProfile:
    page_count: int
    total_chars: int
    average_chars_per_page: float
    is_digital: bool


@dataclass
class ScannedOCRRouter:
    engine_name: str
    openai_client: OpenAIVisionOCR

    def ocr_page(self, image_path: str | Path) -> List[Dict]:
        return self.openai_client.extract_ocr_items(image_path)


def detect_document_text_profile(
    input_pdf: str | Path,
    chars_threshold: int = DEFAULT_DIGITAL_TEXT_THRESHOLD,
) -> DocumentTextProfile:
    pdf_path = Path(input_pdf)
    try:
        reader = PdfReader(str(pdf_path))
    except Exception:
        return DocumentTextProfile(page_count=0, total_chars=0, average_chars_per_page=0.0, is_digital=False)

    page_count = len(reader.pages or [])
    if page_count <= 0:
        return DocumentTextProfile(page_count=0, total_chars=0, average_chars_per_page=0.0, is_digital=False)

    total_chars = 0
    for page in reader.pages:
        try:
            text = page.extract_text() or ""
        except Exception:
            text = ""
        total_chars += len(text.strip())

    average = float(total_chars) / float(max(page_count, 1))
    threshold = max(1, int(chars_threshold))
    is_digital = average > float(threshold)
    return DocumentTextProfile(
        page_count=page_count,
        total_chars=total_chars,
        average_chars_per_page=average,
        is_digital=is_digital,
    )


def resolve_document_parse_mode(input_pdf: str | Path, requested_mode: str | None) -> str:
    mode = str(requested_mode or "").strip().lower()
    if mode in {"text", "ocr"}:
        return mode

    profile = detect_document_text_profile(input_pdf, chars_threshold=DEFAULT_DIGITAL_TEXT_THRESHOLD)
    if profile.is_digital:
        return "text"
    return "ocr"


def scanned_render_dpi() -> int:
    raw = _env_int("SCANNED_RENDER_DPI", 180)
    return max(150, min(200, raw))


def build_scanned_ocr_router(page_count: int) -> ScannedOCRRouter:
    openai_client = OpenAIVisionOCR.from_env()
    return ScannedOCRRouter(
        engine_name="openai_vision",
        openai_client=openai_client,
    )
