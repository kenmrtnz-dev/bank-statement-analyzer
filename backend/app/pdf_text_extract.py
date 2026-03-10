import re
import subprocess
import xml.etree.ElementTree as ET
from typing import Dict, List


def extract_pdf_layout_xml(pdf_path: str) -> str:
    """Return raw pdftotext bbox-layout XML for a text-layer PDF."""
    raw_output = subprocess.check_output(
        ["pdftotext", "-bbox-layout", pdf_path, "-"],
        text=True,
        stderr=subprocess.STDOUT,
    )
    return _sanitize_bbox_layout_xml(raw_output)


def _sanitize_bbox_layout_xml(raw_output: str) -> str:
    """Strip pdftotext warnings that can leak into stdout ahead of the XHTML payload."""
    text = str(raw_output or "")
    if not text:
        return text

    text = re.sub(r"Syntax Error \([^)]*\):[^\n]*(?:\n|$)", "", text)
    text = re.sub(r"Command Line Error:[^\n]*(?:\n|$)", "", text)

    start_candidates = [
        marker for marker in ("<!DOCTYPE", "<?xml", "<html", "<doc") if marker in text
    ]
    if start_candidates:
        start = min(text.index(marker) for marker in start_candidates)
        if start > 0:
            text = text[start:]

    end_candidates = [
        marker for marker in ("</html>", "</doc>") if marker in text
    ]
    if end_candidates:
        end = max(text.rfind(marker) + len(marker) for marker in end_candidates)
        text = text[:end]

    return text.strip()


def extract_pdf_layout_pages(pdf_path: str) -> List[Dict]:
    """
    Extract per-page word layout from a text-layer PDF using pdftotext -bbox-layout.
    Returns: [{"width", "height", "words", "text"}, ...]
    """
    xml_text = extract_pdf_layout_xml(pdf_path)

    root = ET.fromstring(xml_text)
    pages: List[Dict] = []

    for page in root.findall(".//{*}page"):
        width = float(page.attrib.get("width", "1"))
        height = float(page.attrib.get("height", "1"))

        words = []
        text_parts = []

        for word in page.findall(".//{*}word"):
            text = (word.text or "").strip()
            if not text:
                continue

            x1 = float(word.attrib.get("xMin", "0"))
            y1 = float(word.attrib.get("yMin", "0"))
            x2 = float(word.attrib.get("xMax", str(width)))
            y2 = float(word.attrib.get("yMax", "0"))

            words.append({
                "text": text,
                "x1": x1,
                "y1": y1,
                "x2": x2,
                "y2": y2,
            })
            text_parts.append(text)

        pages.append({
            "width": width,
            "height": height,
            "words": words,
            "text": " ".join(text_parts),
        })

    return pages
