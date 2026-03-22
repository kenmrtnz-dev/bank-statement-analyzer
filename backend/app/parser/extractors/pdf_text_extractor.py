"""PDF text extraction for digital statements."""

import logging
from pathlib import Path
import subprocess

logger = logging.getLogger(__name__)


def _run_pdftotext(file_path: str | Path) -> str:
    """Run pdftotext and return its stdout output."""
    try:
        result = subprocess.run(
            ["pdftotext", "-layout", str(file_path), "-"],
            capture_output=True,
            text=True,
            check=False,
        )
    except FileNotFoundError:
        logger.warning("pdftotext is not installed or not available in PATH.")
        return ""
    # TODO: Add richer stderr handling for malformed or encrypted PDFs.
    return result.stdout or ""


def has_embedded_text(file_path: str | Path) -> bool:
    """Return True when the PDF appears to contain embedded text."""
    return bool(_run_pdftotext(file_path).strip())


def extract_text(file_path: str | Path) -> str:
    """Extract text from a digital PDF using pdftotext."""
    return _run_pdftotext(file_path)
