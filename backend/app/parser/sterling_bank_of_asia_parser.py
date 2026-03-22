"""Sterling Bank of Asia OCR parser based on header-aligned column bounds."""

from __future__ import annotations

from collections import defaultdict
from statistics import median
import re
from typing import Any

COLUMN_ORDER = ["row_number", "date", "credit", "debit", "balance", "description"]
HEADER_KEYWORDS = {
    "row_number": {"ln"},
    "date": {"date"},
    "description": {"tc"},
    "debit": {"withrawal", "withdrawal"},
    "credit": {"credit", "deposit"},
    "balance": {"bal", "balance"},
}
MONTH_MAP = {
    "JAN": 1,
    "FEB": 2,
    "MAR": 3,
    "APR": 4,
    "MAY": 5,
    "JUN": 6,
    "JUL": 7,
    "AUG": 8,
    "SEP": 9,
    "OCT": 10,
    "NOV": 11,
    "DEC": 12,
}
DATE_ROW_RE = re.compile(r"(?P<row>\d{1,4})\)\s*(?P<date>\d{1,2}[A-Z]{3}\d{4})", re.IGNORECASE)
AMOUNT_RE = re.compile(r"\d[\d,]*\.\d{2}")


def _normalize_token(text: str) -> str:
    """Normalize OCR token text for robust keyword matching."""
    return re.sub(r"[^a-z0-9]+", "", (text or "").lower())


def _vertices(node: dict[str, Any]) -> list[dict[str, Any]]:
    """Return OCR vertices for both camel and snake case payloads."""
    poly = node.get("boundingPoly") or node.get("bounding_poly") or {}
    return poly.get("vertices") or []


def _rect_from_vertices(vertices: list[dict[str, Any]]) -> dict[str, float] | None:
    """Return geometric bounds derived from a Vision polygon."""
    if not vertices:
        return None
    xs = [float(point.get("x", 0) or 0) for point in vertices]
    ys = [float(point.get("y", 0) or 0) for point in vertices]
    x1, x2 = min(xs), max(xs)
    y1, y2 = min(ys), max(ys)
    return {
        "x1": x1,
        "x2": x2,
        "y1": y1,
        "y2": y2,
        "cx": (x1 + x2) / 2,
        "cy": (y1 + y2) / 2,
        "height": max(y2 - y1, 1),
    }


def _read_word_text(word: dict[str, Any]) -> str:
    """Read a full word text value from Vision symbols."""
    symbols = word.get("symbols") or []
    return "".join(str(symbol.get("text") or "") for symbol in symbols).strip()


def _extract_tokens(page_payload: dict[str, Any], page_number: int) -> list[dict[str, Any]]:
    """Extract positional OCR tokens from one page response."""
    annotations = page_payload.get("textAnnotations") or page_payload.get("text_annotations") or []
    tokens: list[dict[str, Any]] = []

    if len(annotations) > 1:
        for item in annotations[1:]:
            text = str(item.get("description") or "").strip()
            rect = _rect_from_vertices(_vertices(item))
            if not text or rect is None:
                continue
            tokens.append(
                {
                    "page_number": page_number,
                    "text": text,
                    "norm": _normalize_token(text),
                    **rect,
                }
            )
        return tokens

    full_text = page_payload.get("fullTextAnnotation") or page_payload.get("full_text_annotation") or {}
    pages = full_text.get("pages") or []
    for page in pages:
        for block in page.get("blocks") or []:
            for paragraph in block.get("paragraphs") or []:
                for word in paragraph.get("words") or []:
                    text = _read_word_text(word)
                    rect = _rect_from_vertices(_vertices(word))
                    if not text or rect is None:
                        continue
                    tokens.append(
                        {
                            "page_number": page_number,
                            "text": text,
                            "norm": _normalize_token(text),
                            **rect,
                        }
                    )
    return tokens


def _find_headers(tokens: list[dict[str, Any]]) -> tuple[dict[str, dict[str, Any]], float]:
    """Find best header tokens in the upper part of the page."""
    if not tokens:
        return {}, 0.0

    min_y = min(token["cy"] for token in tokens)
    max_y = max(token["cy"] for token in tokens)
    header_cutoff = min_y + ((max_y - min_y) * 0.35)
    header_area = [token for token in tokens if token["cy"] <= header_cutoff]

    found: dict[str, dict[str, Any]] = {}
    for column, keywords in HEADER_KEYWORDS.items():
        matches = [token for token in header_area if token["norm"] in keywords]
        if matches:
            found[column] = sorted(matches, key=lambda token: (token["cy"], token["cx"]))[0]

    header_y = median(item["cy"] for item in found.values()) if found else 0.0
    return found, header_y


def _build_bounds(headers: dict[str, dict[str, Any]], page_width: float) -> dict[str, tuple[float, float]]:
    """Build full-page x bounds from detected header centers."""
    ordered = sorted(
        ((column, token["cx"]) for column, token in headers.items() if column in COLUMN_ORDER),
        key=lambda item: item[1],
    )
    if len(ordered) < 4:
        return {}

    bounds: dict[str, tuple[float, float]] = {}
    for index, (column, center_x) in enumerate(ordered):
        left = 0.0 if index == 0 else (ordered[index - 1][1] + center_x) / 2
        right = page_width if index == len(ordered) - 1 else (center_x + ordered[index + 1][1]) / 2
        bounds[column] = (left, right)
    return bounds


def _parse_ln_date(value: str) -> tuple[int | None, str | None]:
    """Parse `1)01JAN2025` into `(1, 01/01/2025)`."""
    compact = re.sub(r"\s+", "", (value or "").upper())
    match = DATE_ROW_RE.search(compact)
    if not match:
        return None, None

    row_number = int(match.group("row"))
    date_token = match.group("date")
    date_match = re.match(r"(?P<day>\d{1,2})(?P<month>[A-Z]{3})(?P<year>\d{4})", date_token)
    if not date_match:
        return row_number, None

    day = int(date_match.group("day"))
    month = MONTH_MAP.get(date_match.group("month"))
    year = int(date_match.group("year"))
    if month is None:
        return row_number, None
    return row_number, f"{month:02d}/{day:02d}/{year:04d}"


def _extract_amount(value: str, prefer_last: bool = False) -> str:
    """Extract numeric amount text and drop masking characters."""
    cleaned = (value or "").replace("*", " ")
    matches = AMOUNT_RE.findall(cleaned)
    if not matches:
        matches = AMOUNT_RE.findall(cleaned.replace(" ", ""))
    if not matches:
        return ""
    return matches[-1] if prefer_last else matches[0]


def _is_valid_row(
    parsed_row_number: int | None,
    parsed_date: str | None,
    debit: str,
    credit: str,
    balance: str,
) -> bool:
    """Apply strict row validation to filter OCR artifacts."""
    has_row_number = parsed_row_number is not None
    has_date = bool(parsed_date)
    has_debit_or_credit = bool(debit or credit)
    has_balance = bool(balance)

    # Keep only high-confidence transaction rows:
    # LN + DATE + (DEBIT or CREDIT) + BALANCE.
    return has_row_number and has_date and has_debit_or_credit and has_balance


def _row_tolerance(tokens: list[dict[str, Any]]) -> float:
    """Compute y tolerance used for grouping nearby tokens into rows."""
    heights = [token["height"] for token in tokens if token.get("height")]
    if not heights:
        return 10.0
    return max(8.0, min(24.0, float(median(heights) * 0.8)))


def _build_rows_from_page(
    tokens: list[dict[str, Any]],
    bounds: dict[str, tuple[float, float]],
    header_y: float,
    next_row_seed: int,
    page_number: int,
) -> list[dict[str, Any]]:
    """Assign tokens to header bounds and return parsed rows."""
    if not bounds:
        return []

    tolerance = _row_tolerance(tokens)
    content_tokens = [token for token in tokens if token["cy"] > header_y + tolerance]
    assigned: list[dict[str, Any]] = []
    for token in content_tokens:
        for column, (left, right) in bounds.items():
            if left <= token["cx"] <= right:
                assigned.append({**token, "column": column})
                break

    if not assigned:
        return []

    assigned.sort(key=lambda token: (token["cy"], token["cx"]))
    grouped: list[dict[str, Any]] = []
    for token in assigned:
        if not grouped or abs(token["cy"] - grouped[-1]["anchor_y"]) > tolerance:
            grouped.append({"anchor_y": token["cy"], "items": [token]})
        else:
            grouped[-1]["items"].append(token)
            grouped[-1]["anchor_y"] = (grouped[-1]["anchor_y"] + token["cy"]) / 2

    rows: list[dict[str, Any]] = []
    next_row = next_row_seed
    used_row_numbers: set[int] = set()
    for group in grouped:
        by_column: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for token in group["items"]:
            by_column[token["column"]].append(token)

        date_raw = " ".join(item["text"] for item in sorted(by_column["date"], key=lambda token: token["cx"]))
        ln_raw = " ".join(
            item["text"] for item in sorted(by_column["row_number"], key=lambda token: token["cx"])
        )
        parsed_row_number, parsed_date = _parse_ln_date(f"{ln_raw} {date_raw}")

        debit_raw = " ".join(item["text"] for item in sorted(by_column["debit"], key=lambda token: token["cx"]))
        credit_raw = " ".join(item["text"] for item in sorted(by_column["credit"], key=lambda token: token["cx"]))
        balance_raw = " ".join(item["text"] for item in sorted(by_column["balance"], key=lambda token: token["cx"]))
        description = " ".join(
            item["text"] for item in sorted(by_column["description"], key=lambda token: token["cx"])
        ).strip()

        debit = _extract_amount(debit_raw, prefer_last=False)
        credit = _extract_amount(credit_raw, prefer_last=False)
        balance = _extract_amount(balance_raw, prefer_last=True)

        if not _is_valid_row(parsed_row_number, parsed_date, debit, credit, balance):
            continue

        row_number = parsed_row_number
        if row_number is None or row_number in used_row_numbers:
            row_number = next_row
        used_row_numbers.add(row_number)
        next_row = max(next_row, row_number + 1)

        rows.append(
            {
                "row_number": row_number,
                "page_number": int(page_number),
                "date": parsed_date or "",
                "description": description,
                "debit": debit,
                "credit": credit,
                "balance": balance,
            }
        )

    return rows


def parse_transactions_from_ocr_raw(raw_payload: dict[str, Any] | None) -> list[dict[str, Any]]:
    """Parse Sterling rows from Google Vision raw OCR payload."""
    if not raw_payload:
        return []

    pages = raw_payload.get("pages") or []
    all_rows: list[dict[str, Any]] = []
    next_row = 1

    for page in pages:
        response = page.get("response") or {}
        page_number = int(page.get("page_number") or 0)
        tokens = _extract_tokens(response, page_number)
        if not tokens:
            continue

        headers, header_y = _find_headers(tokens)
        bounds = _build_bounds(headers, max(token["x2"] for token in tokens))
        page_rows = _build_rows_from_page(tokens, bounds, header_y, next_row, page_number)
        if not page_rows:
            continue
        all_rows.extend(page_rows)
        next_row = all_rows[-1]["row_number"] + 1

    return all_rows
