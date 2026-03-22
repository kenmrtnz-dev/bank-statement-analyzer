"""Row detection helpers."""


def detect_rows(text: str) -> list[str]:
    """Split extracted text into non-empty candidate rows."""
    return [line.rstrip() for line in text.splitlines() if line.strip()]
