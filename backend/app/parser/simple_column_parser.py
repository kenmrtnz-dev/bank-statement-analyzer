"""Generic Google Vision OCR parser using a 3-pass layered strategy.

Pass 1: Layout reconstruction (rows with date/description/numbers).
Pass 2: Ledger reconstruction (debit/credit/balance via balance math).
Pass 3: Semantic cleanup (date normalization, row filtering, description merge).
"""

from __future__ import annotations

from collections import Counter
import datetime as dt
import re
from statistics import median
from typing import Any

_DATE_WITH_YEAR = re.compile(r"^(?P<a>\d{1,4})[/-](?P<b>\d{1,2})[/-](?P<c>\d{1,4})$")
_DATE_NO_YEAR = re.compile(r"^(?P<m>\d{1,2})[/-](?P<d>\d{1,2})$")
_DATE_TEXTUAL = re.compile(
    r"^(?:(?P<mon1>[A-Za-z]{3,9})[\s-]+(?P<day1>\d{1,2})(?:[\s,/-]+(?P<year1>\d{2,4}))?"
    r"|(?P<day2>\d{1,2})[\s-]+(?P<mon2>[A-Za-z]{3,9})(?:[\s,/-]+(?P<year2>\d{2,4}))?)$",
    re.IGNORECASE,
)
_DATE_TEXTUAL_EMBEDDED = re.compile(
    r"\b(?:"
    r"(?P<mon1>[A-Za-z]{3,9})[\s-]+(?P<day1>\d{1,2})(?:[\s,/-]+(?P<year1>\d{2,4}))?"
    r"|(?P<day2>\d{1,2})[\s-]+(?P<mon2>[A-Za-z]{3,9})(?:[\s,/-]+(?P<year2>\d{2,4}))?"
    r")\b",
    re.IGNORECASE,
)
_YEAR_PATTERN = re.compile(r"(19|20)\d{2}")
_AMOUNT_PATTERN = re.compile(r"[-+]?[\d,]+(?:\.\d{1,2})?")
_LINE_DATE_PATTERN = re.compile(r"\b\d{1,4}[/-]\d{1,2}(?:[/-]\d{1,4})?\b")
_NON_TXN_DESCRIPTION_MARKERS = {
    "balance forward",
    "service charge",
    "page total",
    "carried forward",
    "brought forward",
}
_MONTH_MAP = {
    "jan": 1,
    "january": 1,
    "feb": 2,
    "february": 2,
    "mar": 3,
    "march": 3,
    "apr": 4,
    "april": 4,
    "may": 5,
    "jun": 6,
    "june": 6,
    "jul": 7,
    "july": 7,
    "aug": 8,
    "august": 8,
    "sep": 9,
    "sept": 9,
    "september": 9,
    "oct": 10,
    "october": 10,
    "nov": 11,
    "november": 11,
    "dec": 12,
    "december": 12,
}


def _vertices(node: dict[str, Any]) -> list[dict[str, Any]]:
    poly = node.get("boundingPoly") or node.get("bounding_poly") or {}
    return poly.get("vertices") or []


def _rect_from_vertices(vertices: list[dict[str, Any]]) -> dict[str, float] | None:
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
        "width": max(x2 - x1, 1),
        "height": max(y2 - y1, 1),
    }


def _extract_tokens_from_text_annotations(page: dict[str, Any], page_number: int) -> list[dict[str, Any]]:
    annotations = page.get("textAnnotations") or page.get("text_annotations") or []
    if len(annotations) <= 1:
        return []

    tokens: list[dict[str, Any]] = []
    for item in annotations[1:]:
        text = str(item.get("description") or "").strip()
        if not text:
            continue
        rect = _rect_from_vertices(_vertices(item))
        if rect is None:
            continue
        tokens.append({"page_number": page_number, "text": text, **rect})
    return tokens


def _read_word_text(word: dict[str, Any]) -> str:
    symbols = word.get("symbols") or []
    return "".join(str(symbol.get("text") or "") for symbol in symbols).strip()


def _extract_tokens_from_full_text(page: dict[str, Any], page_number: int) -> list[dict[str, Any]]:
    full_text = page.get("fullTextAnnotation") or page.get("full_text_annotation") or {}
    pages = full_text.get("pages") or []
    if not pages:
        return []

    tokens: list[dict[str, Any]] = []
    for page_node in pages:
        for block in page_node.get("blocks") or []:
            for paragraph in block.get("paragraphs") or []:
                for word in paragraph.get("words") or []:
                    text = _read_word_text(word)
                    if not text:
                        continue
                    rect = _rect_from_vertices(_vertices(word))
                    if rect is None:
                        continue
                    tokens.append({"page_number": page_number, "text": text, **rect})
    return tokens


def _extract_page_tokens(page_payload: dict[str, Any], page_number: int) -> list[dict[str, Any]]:
    tokens = _extract_tokens_from_text_annotations(page_payload, page_number)
    if tokens:
        return tokens
    return _extract_tokens_from_full_text(page_payload, page_number)


def _line_tolerance(tokens: list[dict[str, Any]]) -> float:
    heights = [token["height"] for token in tokens if token.get("height")]
    if not heights:
        return 10.0
    return max(7.0, min(24.0, float(median(heights) * 0.85)))


def _vertical_overlap_ratio(a: dict[str, Any], b: dict[str, Any]) -> float:
    overlap = max(0.0, min(a["y2"], b["y2"]) - max(a["y1"], b["y1"]))
    if overlap <= 0:
        return 0.0
    base = min(a["height"], b["height"])
    return overlap / max(base, 1.0)


def _parse_amount(value: str | None) -> float | None:
    text = str(value or "").strip().replace(" ", "")
    if not text:
        return None
    text = text.replace("(", "-").replace(")", "")
    text = text.replace(",", "")
    if text.count(".") > 1:
        return None
    try:
        amount = float(text)
    except ValueError:
        return None
    return round(amount, 2)


def _is_date_like(text: str) -> bool:
    raw = str(text or "").strip()
    if not raw:
        return False
    return bool(_DATE_NO_YEAR.match(raw) or _DATE_WITH_YEAR.match(raw) or _DATE_TEXTUAL.match(raw))


def _normalize_spaces(value: str) -> str:
    return " ".join(str(value or "").split())


def _is_textual_month(value: str) -> bool:
    return str(value or "").strip().lower() in _MONTH_MAP


def _looks_like_day_of_month(value: str) -> bool:
    text = str(value or "").strip()
    if not text.isdigit():
        return False
    day = int(text)
    return 1 <= day <= 31


def _infer_statement_year(tokens: list[dict[str, Any]]) -> int | None:
    years: list[int] = []
    for token in tokens:
        text = str(token.get("text") or "")
        for match in _YEAR_PATTERN.finditer(text):
            year = int(match.group(0))
            if 1900 <= year <= 2100:
                years.append(year)
    if not years:
        return None
    return Counter(years).most_common(1)[0][0]


def _normalize_date_iso(value: str, inferred_year: int | None) -> str | None:
    raw = str(value or "").strip()
    if not raw:
        return None

    m_no_year = _DATE_NO_YEAR.match(raw)
    if m_no_year:
        year = inferred_year
        if year is None:
            return None
        month = int(m_no_year.group("m"))
        day = int(m_no_year.group("d"))
        try:
            return dt.date(year, month, day).isoformat()
        except ValueError:
            return None

    m_full = _DATE_WITH_YEAR.match(raw)
    if m_full:
        a = int(m_full.group("a"))
        b = int(m_full.group("b"))
        c = int(m_full.group("c"))
        year_first = len(m_full.group("a")) == 4
        year_last = len(m_full.group("c")) == 4

        try:
            if year_first:
                return dt.date(a, b, c).isoformat()
            if year_last:
                # Treat month/day/year for statement rows.
                if a > 12 and b <= 12:
                    return dt.date(c, b, a).isoformat()
                return dt.date(c, a, b).isoformat()
        except ValueError:
            return None

    m_text = _DATE_TEXTUAL.match(raw)
    if not m_text:
        return None

    if m_text.group("mon1"):
        month_key = str(m_text.group("mon1") or "").strip().lower()
        day = int(m_text.group("day1"))
        year_text = str(m_text.group("year1") or "").strip()
    else:
        month_key = str(m_text.group("mon2") or "").strip().lower()
        day = int(m_text.group("day2"))
        year_text = str(m_text.group("year2") or "").strip()

    month = _MONTH_MAP.get(month_key)
    if month is None:
        return None

    if year_text:
        year = int(year_text)
        if year < 100:
            year += 2000
    else:
        year = inferred_year
    if year is None:
        return None

    try:
        return dt.date(int(year), month, day).isoformat()
    except ValueError:
        return None


def _extract_embedded_date_iso(text: str, inferred_year: int | None) -> str | None:
    raw = _normalize_spaces(text)
    if not raw:
        return None
    direct = _normalize_date_iso(raw, inferred_year)
    if direct:
        return direct
    numeric = _LINE_DATE_PATTERN.search(raw)
    if numeric:
        embedded_numeric = _normalize_date_iso(numeric.group(0), inferred_year)
        if embedded_numeric:
            return embedded_numeric
    textual = _DATE_TEXTUAL_EMBEDDED.search(raw)
    if not textual:
        return None
    return _normalize_date_iso(textual.group(0), inferred_year)


def _looks_like_header_text(text: str) -> bool:
    normalized = _normalize_spaces(text).lower()
    return normalized in {"date", "description", "debit", "credit", "balance"}


def _is_non_transaction_description(text: str) -> bool:
    normalized = _normalize_spaces(text).lower()
    if not normalized:
        return False
    if normalized in _NON_TXN_DESCRIPTION_MARKERS:
        return True
    return normalized.startswith("page ") and "total" in normalized


def _build_rough_rows(tokens: list[dict[str, Any]], start_row_number: int) -> list[dict[str, Any]]:
    """Pass 1: Layout reconstruction."""
    if not tokens:
        return []

    tolerance = _line_tolerance(tokens)
    ordered = sorted(tokens, key=lambda item: (item["cy"], item["x1"]))
    clusters: list[dict[str, Any]] = []
    for token in ordered:
        best_cluster: dict[str, Any] | None = None
        best_score = 0.0
        for cluster in reversed(clusters):
            anchor = cluster["anchor"]
            overlap = _vertical_overlap_ratio(token, anchor)
            if overlap >= 0.3:
                best_cluster = cluster
                best_score = overlap
                break
            if abs(token["cy"] - anchor["cy"]) <= tolerance:
                if overlap > best_score:
                    best_cluster = cluster
                    best_score = overlap
        if best_cluster is None:
            clusters.append({"items": [token], "anchor": token})
            continue
        best_cluster["items"].append(token)
        members = best_cluster["items"]
        best_cluster["anchor"] = {
            "y1": min(item["y1"] for item in members),
            "y2": max(item["y2"] for item in members),
            "height": max(1.0, max(item["y2"] for item in members) - min(item["y1"] for item in members)),
            "cy": sum(item["cy"] for item in members) / len(members),
        }

    rows: list[dict[str, Any]] = []
    next_row = start_row_number
    for cluster in clusters:
        row_tokens = sorted(cluster["items"], key=lambda item: item["x1"])
        if not row_tokens:
            continue
        date_token = next((t for t in row_tokens if _is_date_like(t["text"])), None)
        if date_token and _looks_like_header_text(date_token["text"]):
            continue

        number_tokens: list[dict[str, Any]] = []
        description_tokens: list[str] = []
        has_textual_month = any(_is_textual_month(token.get("text")) for token in row_tokens)
        for token in row_tokens:
            text = str(token.get("text") or "").strip()
            if not text:
                continue
            if token is date_token:
                continue
            if has_textual_month and _looks_like_day_of_month(text):
                description_tokens.append(text)
                continue
            amount = _parse_amount(text)
            if amount is not None:
                number_tokens.append({"value": amount, "x": token["x1"], "has_decimal": "." in text})
                continue
            if _looks_like_header_text(text):
                continue
            description_tokens.append(text)

        numbers = [item["value"] for item in sorted(number_tokens, key=lambda item: item["x"])]
        row = {
            "row_number": next_row,
            "date_raw": str(date_token["text"]).strip() if date_token else "",
            "description_raw": _normalize_spaces(" ".join(description_tokens)),
            "numbers": numbers,
            "number_decimal_count": int(sum(1 for item in number_tokens if item.get("has_decimal"))),
            "page_number": int(row_tokens[0].get("page_number") or 0),
            "anchor_y": float(sum(token["cy"] for token in row_tokens) / len(row_tokens)),
        }
        has_substance = bool(row["date_raw"] or row["description_raw"] or row["numbers"])
        if not has_substance:
            continue
        rows.append(row)
        next_row += 1
    return rows


def _reconstruct_ledger(rough_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Pass 2: Ledger reconstruction using running balance."""
    out: list[dict[str, Any]] = []
    previous_balance: float | None = None
    debit_markers = {"withdraw", "debit", "payment", "charge", "atm", "transfer out"}
    credit_markers = {"deposit", "credit", "incoming", "interest", "transfer in"}

    for row in rough_rows:
        nums = [float(value) for value in row.get("numbers") or []]
        description = _normalize_spaces(row.get("description_raw"))
        balance: float | None = None
        tx_hint: float | None = None
        debit = 0.0
        credit = 0.0

        if len(nums) >= 2:
            balance = nums[-1]
            tx_hint = nums[-2]
        elif len(nums) == 1:
            if previous_balance is None:
                tx_hint = nums[0]
            else:
                balance = nums[0]

        if previous_balance is not None and balance is not None:
            delta = round(balance - previous_balance, 2)
            if delta < 0:
                debit = abs(delta)
                credit = 0.0
            elif delta > 0:
                credit = delta
                debit = 0.0
        elif tx_hint is not None:
            lowered = description.lower()
            if any(marker in lowered for marker in credit_markers):
                credit = abs(tx_hint)
                debit = 0.0
            elif any(marker in lowered for marker in debit_markers):
                debit = abs(tx_hint)
                credit = 0.0

        if balance is None and previous_balance is not None:
            if credit > 0:
                balance = round(previous_balance + credit, 2)
            elif debit > 0:
                balance = round(previous_balance - debit, 2)

        if balance is not None:
            previous_balance = balance

        out.append(
            {
                "row_number": row.get("row_number"),
                "page_number": int(row.get("page_number") or 1),
                "date_raw": row.get("date_raw") or "",
                "description_raw": description,
                "number_tokens": len(nums),
                "number_decimal_count": int(row.get("number_decimal_count") or 0),
                "tx_hint": round(tx_hint, 2) if tx_hint is not None else None,
                "debit": round(debit, 2),
                "credit": round(credit, 2),
                "balance": round(balance, 2) if balance is not None else None,
            }
        )
    return out


def _semantic_cleanup(ledger_rows: list[dict[str, Any]], inferred_year: int | None) -> list[dict[str, Any]]:
    """Pass 3: Semantic cleanup and output normalization."""
    cleaned: list[dict[str, Any]] = []
    previous_balance: float | None = None
    previous_date_iso: str = ""

    for row in ledger_rows:
        date_iso = _normalize_date_iso(str(row.get("date_raw") or ""), inferred_year)
        description = _normalize_spaces(row.get("description_raw") or "")
        if not date_iso and description:
            date_iso = _extract_embedded_date_iso(description, inferred_year)

        if not date_iso:
            has_numeric_content = bool(
                (row.get("balance") is not None)
                or (_parse_amount(row.get("debit")) or 0) > 0
                or (_parse_amount(row.get("credit")) or 0) > 0
                or int(row.get("number_decimal_count") or 0) > 0
                or int(row.get("number_tokens") or 0) >= 2
            )
            if description and cleaned and not _is_non_transaction_description(description) and not has_numeric_content:
                cleaned[-1]["description"] = _normalize_spaces(f"{cleaned[-1]['description']} {description}")
                continue
            if not has_numeric_content or _is_non_transaction_description(description):
                continue
            fallback_description = description.strip() or "Transaction"
            debit = _parse_amount(row.get("debit"))
            credit = _parse_amount(row.get("credit"))
            balance = row.get("balance")
            fallback = {
                "row_number": int(len(cleaned) + 1),
                "page_number": int(row.get("page_number") or 1),
                "date": previous_date_iso,
                "description": fallback_description,
                "debit": round(debit or 0.0, 2),
                "credit": round(credit or 0.0, 2),
                "balance": round(float(balance), 2) if balance is not None else None,
            }
            cleaned.append(fallback)
            if fallback["balance"] is not None:
                previous_balance = fallback["balance"]
            continue

        if _is_non_transaction_description(description):
            continue

        balance = row.get("balance")
        if previous_balance is not None and balance is not None and round(balance - previous_balance, 2) == 0:
            continue

        merged_description = description.strip()
        if not merged_description:
            merged_description = "Transaction"

        debit = _parse_amount(row.get("debit"))
        credit = _parse_amount(row.get("credit"))
        normalized = {
            "row_number": int(len(cleaned) + 1),
            "page_number": int(row.get("page_number") or 1),
            "date": date_iso,
            "description": merged_description,
            "debit": round(debit or 0.0, 2),
            "credit": round(credit or 0.0, 2),
            "balance": round(float(balance), 2) if balance is not None else None,
        }
        cleaned.append(normalized)
        previous_date_iso = date_iso
        if normalized["balance"] is not None:
            previous_balance = normalized["balance"]

    return cleaned


def _pass1_layout_reconstruct_from_text(text: str, start_row_number: int, page_number: int) -> list[dict[str, Any]]:
    """Pass 1 for pdftotext: rough row extraction from text lines."""
    rows: list[dict[str, Any]] = []
    next_row = start_row_number
    for line in str(text or "").splitlines():
        raw = _normalize_spaces(line)
        if not raw:
            continue
        date_match = _LINE_DATE_PATTERN.search(raw)
        date_raw = date_match.group(0) if date_match else ""
        numbers = [_parse_amount(match.group(0)) for match in _AMOUNT_PATTERN.finditer(raw)]
        numbers = [value for value in numbers if value is not None]

        description = raw
        if date_raw:
            description = description.replace(date_raw, " ", 1)
        for match in _AMOUNT_PATTERN.findall(raw):
            description = description.replace(match, " ", 1)
        description = _normalize_spaces(description)

        if not date_raw and not numbers and not description:
            continue

        rows.append(
            {
                "row_number": next_row,
                "page_number": int(page_number),
                "date_raw": date_raw,
                "description_raw": description,
                "numbers": numbers,
            }
        )
        next_row += 1
    return rows


def parse_transactions_from_pdftotext_text(text: str) -> list[dict[str, Any]]:
    """Parse pdftotext output via the same 3-pass strategy."""
    rough_rows: list[dict[str, Any]] = []
    next_row = 1
    for page_index, chunk in enumerate(str(text or "").split("\f"), start=1):
        page_rows = _pass1_layout_reconstruct_from_text(chunk, start_row_number=next_row, page_number=page_index)
        if page_rows:
            next_row = int(page_rows[-1]["row_number"]) + 1
            rough_rows.extend(page_rows)
    if not rough_rows:
        return []
    inferred_year = _infer_statement_year([{"text": line} for line in str(text or "").splitlines()])
    ledger_rows = _reconstruct_ledger(rough_rows)
    return _semantic_cleanup(ledger_rows, inferred_year)


def parse_transactions_from_ocr_raw(raw_payload: dict[str, Any] | None) -> list[dict[str, Any]]:
    """Parse transactions from stored Google Vision OCR payload via 3 passes."""
    if not raw_payload:
        return []

    pages = raw_payload.get("pages") or []
    all_tokens: list[dict[str, Any]] = []
    rough_rows: list[dict[str, Any]] = []
    next_row = 1

    for page in pages:
        response = page.get("response") or {}
        page_number = int(page.get("page_number") or 0)
        tokens = _extract_page_tokens(response, page_number)
        if not tokens:
            continue
        all_tokens.extend(tokens)
        page_rows = _build_rough_rows(tokens, next_row)
        rough_rows.extend(page_rows)
        if page_rows:
            next_row = int(page_rows[-1]["row_number"]) + 1

    if not rough_rows:
        return []

    inferred_year = _infer_statement_year(all_tokens)
    ledger_rows = _reconstruct_ledger(rough_rows)
    return _semantic_cleanup(ledger_rows, inferred_year)
