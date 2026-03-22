"""BDO OCR parser based on header bounds and leftmost row numbers."""

from __future__ import annotations

from collections import defaultdict
from statistics import median
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import re
from typing import Any

COLUMN_ORDER = ["row_number", "date", "description", "debit", "credit", "balance"]
HEADER_KEYWORDS = {
    "date": {"date"},
    "description": {"tc"},
    "debit": {"debit"},
    "credit": {"credit"},
    "balance": {"balance", "bal"},
}
DATE_RE = re.compile(r"(?P<a>\d{1,2})[-/](?P<b>\d{1,2})[-/](?P<year>\d{2,4})")
ROW_RE = re.compile(r"\b\d{1,4}\b")
AMOUNT_CHUNK_RE = re.compile(r"\d[\d,.]*\d")
BALANCE_TOLERANCE = Decimal("0.50")


def _normalize_token(text: str) -> str:
    """Normalize OCR token text for keyword matching."""
    return re.sub(r"[^a-z0-9]+", "", (text or "").lower())


def _vertices(node: dict[str, Any]) -> list[dict[str, Any]]:
    """Return OCR polygon vertices for snake/camel payloads."""
    poly = node.get("boundingPoly") or node.get("bounding_poly") or {}
    return poly.get("vertices") or []


def _rect_from_vertices(vertices: list[dict[str, Any]]) -> dict[str, float] | None:
    """Build rectangle geometry from OCR polygon vertices."""
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
    """Read word text from Vision symbol arrays."""
    return "".join(str(symbol.get("text") or "") for symbol in (word.get("symbols") or [])).strip()


def _extract_tokens(page_payload: dict[str, Any], page_number: int) -> list[dict[str, Any]]:
    """Extract OCR tokens with geometry from a page payload."""
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
    for page in full_text.get("pages") or []:
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
    """Locate BDO header anchor tokens and header y-level."""
    if not tokens:
        return {}, 0.0

    min_y = min(token["cy"] for token in tokens)
    max_y = max(token["cy"] for token in tokens)
    cutoff = min_y + ((max_y - min_y) * 0.35)
    header_area = [token for token in tokens if token["cy"] <= cutoff]

    found: dict[str, dict[str, Any]] = {}
    for column, keywords in HEADER_KEYWORDS.items():
        matches = [token for token in header_area if token["norm"] in keywords]
        if matches:
            found[column] = sorted(matches, key=lambda item: (item["cy"], item["cx"]))[0]

    if {"date", "debit"} <= set(found) and "description" not in found:
        found["description"] = {
            "cx": (found["date"]["cx"] + found["debit"]["cx"]) / 2,
            "cy": (found["date"]["cy"] + found["debit"]["cy"]) / 2,
            "height": max(found["date"]["height"], found["debit"]["height"]),
        }

    header_y = median(item["cy"] for item in found.values()) if found else 0.0
    return found, header_y


def _build_bounds(tokens: list[dict[str, Any]], headers: dict[str, dict[str, Any]]) -> dict[str, tuple[float, float]]:
    """Build column bounds for row/date/description/debit/credit/balance."""
    required = {"date", "debit", "credit", "balance"}
    if not required.issubset(headers):
        return {}

    min_x = min(token["x1"] for token in tokens)
    max_x = max(token["x2"] for token in tokens)

    description_center = headers.get("description", {}).get("cx", (headers["date"]["cx"] + headers["debit"]["cx"]) / 2)
    centers = {
        "row_number": min_x + ((headers["date"]["cx"] - min_x) / 2),
        "date": headers["date"]["cx"],
        "description": description_center,
        "debit": headers["debit"]["cx"],
        "credit": headers["credit"]["cx"],
        "balance": headers["balance"]["cx"],
    }
    ordered = sorted(((column, centers[column]) for column in COLUMN_ORDER), key=lambda item: item[1])

    bounds: dict[str, tuple[float, float]] = {}
    for index, (column, center_x) in enumerate(ordered):
        left = min_x if index == 0 else (ordered[index - 1][1] + center_x) / 2
        right = max_x if index == len(ordered) - 1 else (center_x + ordered[index + 1][1]) / 2
        bounds[column] = (left, right)
    return bounds


def _row_tolerance(tokens: list[dict[str, Any]]) -> float:
    """Compute y-axis grouping tolerance for token rows."""
    heights = [token["height"] for token in tokens if token.get("height")]
    if not heights:
        return 10.0
    return max(8.0, min(22.0, float(median(heights) * 0.8)))


def _parse_row_number(value: str) -> int | None:
    """Parse the leftmost visual row number."""
    match = ROW_RE.search(value or "")
    if not match:
        return None
    return int(match.group(0))


def _parse_date_mmddyyyy(value: str) -> str | None:
    """Parse `dd-mm-yy` style date into `mm/dd/yyyy`."""
    match = DATE_RE.search((value or "").strip())
    if not match:
        return None

    first = int(match.group("a"))
    second = int(match.group("b"))
    year = int(match.group("year"))
    year = 2000 + year if year < 100 else year

    # BDO statements in this flow use day-month-year.
    day = first
    month = second
    if not 1 <= month <= 12 or not 1 <= day <= 31:
        month = first
        day = second
    if not 1 <= month <= 12 or not 1 <= day <= 31:
        return None

    return f"{month:02d}/{day:02d}/{year:04d}"


def _normalize_amount_token(token: str) -> str:
    """Normalize OCR amount token to comma thousands + 2dp decimal format.

    Rule enforced:
    - Only one decimal point is allowed, at the third position from the end.
    - Any separator before that is treated as a thousands separator.
    """
    compact = re.sub(r"[^0-9.,]", "", (token or ""))
    if not compact:
        return ""
    if "." not in compact and "," not in compact:
        return ""

    decimal_index: int | None = None
    if len(compact) >= 3 and compact[-3] in ".,":  # canonical xx.yy tail
        decimal_index = len(compact) - 3
    else:
        # Fallback for one-decimal OCR tails like "7,497,428.4".
        one_decimal = re.search(r"[.,]\d$", compact)
        if one_decimal:
            decimal_index = one_decimal.start()

    if decimal_index is None:
        return ""

    integer_digits = re.sub(r"[.,]", "", compact[:decimal_index])
    fraction_digits = re.sub(r"[.,]", "", compact[decimal_index + 1 :])
    if not integer_digits:
        return ""
    if not fraction_digits:
        fraction_digits = "00"
    elif len(fraction_digits) == 1:
        fraction_digits = f"{fraction_digits}0"
    else:
        fraction_digits = fraction_digits[:2]

    return f"{int(integer_digits):,}.{fraction_digits}"


def _extract_amount_candidates(value: str) -> list[str]:
    """Extract normalized amount candidates from OCR text."""
    cleaned = (value or "").replace("*", " ")
    candidates: list[str] = []
    for chunk in AMOUNT_CHUNK_RE.findall(cleaned):
        normalized = _normalize_amount_token(chunk)
        if normalized:
            candidates.append(normalized)
    return candidates


def _extract_amount(value: str, prefer_last: bool = False) -> str:
    """Extract monetary amount and strip masking asterisks."""
    candidates = _extract_amount_candidates(value)
    if not candidates:
        return ""
    return candidates[-1] if prefer_last else candidates[0]


def _to_decimal(value: str | None) -> Decimal | None:
    """Parse an amount string into Decimal for guardrail checks."""
    if not value:
        return None
    try:
        return Decimal(str(value).replace(",", "").strip())
    except (InvalidOperation, ValueError):
        return None


def _extract_largest_amount(value: str) -> str:
    """Extract the numerically largest monetary token from OCR text."""
    matches = _extract_amount_candidates(value)
    if not matches:
        return ""
    return max(matches, key=lambda item: float(item.replace(",", "")))


def _is_balance_forwarded(text: str) -> bool:
    """Return True for opening-balance rows."""
    normalized = re.sub(r"\s+", " ", (text or "").upper()).strip()
    return "BALANCE FORWARDED" in normalized


def _matches_expected_balance(
    prev_balance: Decimal | None,
    debit: Decimal | None,
    credit: Decimal | None,
    target_balance: Decimal | None,
) -> bool:
    """Return True when debit/credit arithmetic matches the target balance."""
    if prev_balance is None or target_balance is None:
        return False
    expected = prev_balance - (debit or Decimal("0")) + (credit or Decimal("0"))
    return abs(expected - target_balance) <= BALANCE_TOLERANCE


def _apply_amount_balance_shift_guardrail(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Move mis-shifted amount+balance from next row into an empty current row.

    Pattern handled:
    - Current row has no debit/credit/balance.
    - Next row has a balance and at least one amount.
    - Running-balance math indicates the next row's amount+balance belongs to current.
    """
    if len(rows) < 2:
        return rows

    adjusted = [dict(row) for row in rows]
    for index in range(len(adjusted) - 1):
        current = adjusted[index]
        next_row = adjusted[index + 1]

        if current.get("description") == "BALANCE FORWARDED":
            continue
        if current.get("debit") or current.get("credit") or current.get("balance"):
            continue

        prev_balance = _to_decimal(adjusted[index - 1].get("balance")) if index > 0 else None
        next_balance = _to_decimal(next_row.get("balance"))
        if prev_balance is None or next_balance is None:
            continue

        next_debit_text = next_row.get("debit")
        next_credit_text = next_row.get("credit")
        next_debit = _to_decimal(next_debit_text)
        next_credit = _to_decimal(next_credit_text)

        candidates: list[tuple[str, str]] = []
        if _matches_expected_balance(prev_balance, next_debit, None, next_balance) and next_debit_text:
            candidates.append(("debit", next_debit_text))
        if _matches_expected_balance(prev_balance, None, next_credit, next_balance) and next_credit_text:
            candidates.append(("credit", next_credit_text))

        # Apply only when there is one unambiguous match.
        if len(candidates) != 1:
            continue

        column, value = candidates[0]
        current[column] = value
        current["balance"] = next_row.get("balance")
        # Consume from donor row so the downstream row can claim its own values.
        next_row[column] = ""
        next_row["balance"] = ""

    return adjusted


def _apply_missing_balance_guardrail(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Fill missing balance from the next row when arithmetic suggests a skew."""
    if len(rows) < 2:
        return rows

    adjusted = [dict(row) for row in rows]
    for index, current in enumerate(adjusted):
        if current.get("description") == "BALANCE FORWARDED":
            continue
        if current.get("balance"):
            continue
        if not (current.get("debit") or current.get("credit")):
            continue
        if index + 1 >= len(adjusted):
            continue

        next_row = adjusted[index + 1]
        next_balance_text = next_row.get("balance")
        next_balance = _to_decimal(next_balance_text)
        if next_balance is None:
            continue

        prev_balance = _to_decimal(adjusted[index - 1].get("balance")) if index > 0 else None
        if prev_balance is None:
            continue

        debit = _to_decimal(current.get("debit"))
        credit = _to_decimal(current.get("credit"))
        if _matches_expected_balance(prev_balance, debit, credit, next_balance):
            current["balance"] = next_balance_text
            # Consume donor value so the next row can borrow its own balance.
            next_row["balance"] = ""

    return adjusted


def _validate_balance_chain(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Check running-balance correctness for rows with enough numeric data."""
    mismatch_rows: list[int] = []
    checked_rows = 0

    prev_balance: Decimal | None = None
    for row in rows:
        balance = _to_decimal(row.get("balance"))
        debit = _to_decimal(row.get("debit"))
        credit = _to_decimal(row.get("credit"))

        if prev_balance is None:
            if balance is not None:
                prev_balance = balance
            continue
        if balance is None or (debit is None and credit is None):
            continue

        checked_rows += 1
        if not _matches_expected_balance(prev_balance, debit, credit, balance):
            mismatch_rows.append(int(row.get("row_number") or 0))
        prev_balance = balance

    return {"checked_rows": checked_rows, "mismatch_rows": mismatch_rows, "is_valid": not mismatch_rows}


def _format_amount(value: Decimal) -> str:
    """Format a Decimal amount as a positive 2dp string with commas."""
    quantized = abs(value).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return f"{quantized:,.2f}"


def _align_rows_to_balance_sequence(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Assign balances to transactions strictly by appearance sequence.

    Rule requested: number of transactions should follow balance order 1:1.
    """
    if not rows:
        return rows

    balances = [str(row.get("balance") or "").strip() for row in rows if str(row.get("balance") or "").strip()]
    if not balances:
        return rows

    aligned = [dict(row) for row in rows]
    if len(aligned) < len(balances):
        next_row_number = max((int(item.get("row_number") or 0) for item in aligned), default=0) + 1
        for _ in range(len(balances) - len(aligned)):
            aligned.append(
                {
                    "row_number": next_row_number,
                    "date": "",
                    "description": "BALANCE_ONLY",
                    "debit": "",
                    "credit": "",
                    "balance": "",
                }
            )
            next_row_number += 1

    target_count = min(len(aligned), len(balances))
    aligned = aligned[:target_count]
    for index, row in enumerate(aligned):
        row["balance"] = balances[index]
    return aligned


def _recompute_amounts_from_balances(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Repair debit/credit using running-balance deltas when values do not justify balance."""
    if len(rows) < 2:
        return rows

    adjusted = [dict(row) for row in rows]
    previous_balance: Decimal | None = None

    for row in adjusted:
        current_balance = _to_decimal(row.get("balance"))
        if current_balance is None:
            continue

        if previous_balance is None or row.get("description") == "BALANCE FORWARDED":
            previous_balance = current_balance
            continue

        current_debit = _to_decimal(row.get("debit"))
        current_credit = _to_decimal(row.get("credit"))
        expected = previous_balance - (current_debit or Decimal("0")) + (current_credit or Decimal("0"))

        if abs(expected - current_balance) > BALANCE_TOLERANCE:
            delta = current_balance - previous_balance
            # Replace existing amounts with delta-derived truth from balances.
            row["debit"] = ""
            row["credit"] = ""
            if delta > BALANCE_TOLERANCE:
                row["credit"] = _format_amount(delta)
            elif delta < -BALANCE_TOLERANCE:
                row["debit"] = _format_amount(-delta)

        previous_balance = current_balance

    return adjusted


def _apply_expected_balance_fill_guardrail(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Fill remaining missing balances using running-balance arithmetic.

    This is a last-resort correction used only when debit/credit exists and
    prior row balance is known.
    """
    if len(rows) < 2:
        return rows

    adjusted = [dict(row) for row in rows]
    for index, current in enumerate(adjusted):
        if index == 0 or current.get("balance"):
            continue
        if not (current.get("debit") or current.get("credit")):
            continue

        prev_balance = _to_decimal(adjusted[index - 1].get("balance"))
        if prev_balance is None:
            continue
        debit = _to_decimal(current.get("debit"))
        credit = _to_decimal(current.get("credit"))
        expected = prev_balance - (debit or Decimal("0")) + (credit or Decimal("0"))
        if expected < Decimal("0"):
            continue

        current["balance"] = f"{expected:.2f}"
    return adjusted


def _group_column_entries(tokens: list[dict[str, Any]], tolerance: float) -> list[dict[str, Any]]:
    """Group tokens into ordered entries for one column."""
    if not tokens:
        return []

    ordered = sorted(tokens, key=lambda item: (item["cy"], item["cx"]))
    groups: list[dict[str, Any]] = []
    column_tolerance = max(6.0, tolerance * 0.8)

    for token in ordered:
        if not groups or abs(token["cy"] - groups[-1]["anchor_y"]) > column_tolerance:
            groups.append({"anchor_y": token["cy"], "items": [token]})
        else:
            groups[-1]["items"].append(token)
            groups[-1]["anchor_y"] = (groups[-1]["anchor_y"] + token["cy"]) / 2

    entries: list[dict[str, Any]] = []
    for group in groups:
        text = " ".join(item["text"] for item in sorted(group["items"], key=lambda item: item["cx"])).strip()
        if not text:
            continue
        entries.append({"text": text, "cy": group["anchor_y"]})
    return entries


def _column_text(entries: list[dict[str, Any]], index: int) -> str:
    """Read text entry by index from a column sequence."""
    if 0 <= index < len(entries):
        return str(entries[index].get("text") or "").strip()
    return ""


def _nearest_column_text(entries: list[dict[str, Any]], target_cy: float) -> str:
    """Read text entry nearest to a target y-position."""
    if not entries:
        return ""
    nearest = min(entries, key=lambda item: abs(float(item.get("cy") or 0.0) - target_cy))
    return str(nearest.get("text") or "").strip()


def _build_rows_from_page_by_column_sequence(
    tokens: list[dict[str, Any]],
    bounds: dict[str, tuple[float, float]],
    header_y: float,
    next_row_seed: int,
) -> list[dict[str, Any]]:
    """Build rows by parsing each column independently and aligning by sequence.

    Requested behavior:
    - Detect column bounds from headers.
    - Parse tokens inside each column bound in visual order.
    - Use balance sequence as row anchor so each balance gets a row.
    """
    if not bounds:
        return []

    tolerance = _row_tolerance(tokens)
    content_tokens = [token for token in tokens if token["cy"] > header_y + tolerance]
    by_column: dict[str, list[dict[str, Any]]] = {column: [] for column in COLUMN_ORDER}
    for token in content_tokens:
        for column, (left, right) in bounds.items():
            if left <= token["cx"] <= right:
                by_column[column].append(token)
                break

    row_entries = _group_column_entries(by_column["row_number"], tolerance)
    date_entries = _group_column_entries(by_column["date"], tolerance)
    description_entries = _group_column_entries(by_column["description"], tolerance)
    debit_entries = _group_column_entries(by_column["debit"], tolerance)
    credit_entries = _group_column_entries(by_column["credit"], tolerance)
    balance_entries = _group_column_entries(by_column["balance"], tolerance)

    # Keep only real balance-like entries and preserve order.
    balance_entries = [
        entry
        for entry in balance_entries
        if _extract_amount(entry.get("text") or "", prefer_last=True)
    ]
    if not balance_entries:
        return []

    rows: list[dict[str, Any]] = []
    _ = next_row_seed  # row numbering now relies on OCR-provided row markers
    opening_balance_offset = False
    if date_entries and balance_entries:
        first_date_y = float(date_entries[0].get("cy") or 0.0)
        first_balance_y = float(balance_entries[0].get("cy") or 0.0)
        if first_date_y > first_balance_y + tolerance:
            opening_balance_offset = True

    for index, balance_entry in enumerate(balance_entries):
        source_index = index - 1 if opening_balance_offset else index
        row_number_raw = _column_text(row_entries, source_index)
        date_raw = _column_text(date_entries, source_index)
        description_raw = _column_text(description_entries, source_index)
        debit_raw = _column_text(debit_entries, source_index)
        credit_raw = _column_text(credit_entries, source_index)
        balance_raw = str(balance_entry.get("text") or "").strip()
        balance_cy = float(balance_entry.get("cy") or 0.0)

        parsed_date = _parse_date_mmddyyyy(date_raw)
        row_number = _parse_row_number(row_number_raw)
        if row_number is None:
            row_number = _parse_row_number(date_raw)
        if row_number is None:
            row_number = _parse_row_number(_nearest_column_text(row_entries, balance_cy))
        if row_number is None and index == 0 and opening_balance_offset:
            # Opening balance row frequently has no explicit row marker in OCR.
            row_number = 1
        if row_number is None:
            continue

        description = description_raw.strip()
        if _is_balance_forwarded(description_raw):
            description = "BALANCE FORWARDED"
        elif index == 0 and opening_balance_offset:
            # First sequence item with no date is the opening balance row.
            description = "BALANCE FORWARDED"
        elif not description:
            description = "UNLABELED"

        rows.append(
            {
                "row_number": row_number,
                "date": parsed_date or "",
                "description": description,
                "debit": _extract_amount(debit_raw, prefer_last=True),
                "credit": _extract_amount(credit_raw, prefer_last=True),
                "balance": _extract_amount(balance_raw, prefer_last=True),
            }
        )
    return rows


def _pass1_layout_reconstruction(
    tokens: list[dict[str, Any]],
    bounds: dict[str, tuple[float, float]],
    header_y: float,
    page_number: int,
) -> tuple[list[dict[str, Any]], list[str], str | None]:
    """Pass 1: build rough rows and ordered balance stream from OCR layout."""
    if not bounds:
        return [], [], None

    tolerance = _row_tolerance(tokens)
    content_tokens = [token for token in tokens if token["cy"] > header_y + tolerance]
    assigned: list[dict[str, Any]] = []
    for token in content_tokens:
        for column, (left, right) in bounds.items():
            if left <= token["cx"] <= right:
                assigned.append({**token, "column": column})
                break

    if not assigned:
        return [], [], None

    assigned.sort(key=lambda token: (token["cy"], token["cx"]))
    grouped: list[dict[str, Any]] = []
    for token in assigned:
        if not grouped or abs(token["cy"] - grouped[-1]["anchor_y"]) > tolerance:
            grouped.append({"anchor_y": token["cy"], "items": [token]})
        else:
            grouped[-1]["items"].append(token)
            grouped[-1]["anchor_y"] = (grouped[-1]["anchor_y"] + token["cy"]) / 2

    rough_rows: list[dict[str, Any]] = []
    for group in grouped:
        by_column: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for token in group["items"]:
            by_column[token["column"]].append(token)

        row_text = " ".join(token["text"] for token in group["items"])
        row_number_raw = " ".join(item["text"] for item in sorted(by_column["row_number"], key=lambda t: t["cx"]))
        date_raw = " ".join(item["text"] for item in sorted(by_column["date"], key=lambda t: t["cx"]))
        description_raw = " ".join(item["text"] for item in sorted(by_column["description"], key=lambda t: t["cx"])).strip()
        debit_raw = " ".join(item["text"] for item in sorted(by_column["debit"], key=lambda t: t["cx"]))
        credit_raw = " ".join(item["text"] for item in sorted(by_column["credit"], key=lambda t: t["cx"]))
        balance_raw = " ".join(item["text"] for item in sorted(by_column["balance"], key=lambda t: t["cx"]))

        numbers: list[str] = []
        for chunk in (debit_raw, credit_raw, balance_raw):
            numbers.extend(_extract_amount_candidates(chunk))

        deduped_numbers: list[str] = []
        seen: set[str] = set()
        for number in numbers:
            if number not in seen:
                seen.add(number)
                deduped_numbers.append(number)

        row_number = _parse_row_number(row_number_raw)
        if row_number is None:
            row_number = _parse_row_number(date_raw)

        rough_rows.append(
            {
                "anchor_y": group["anchor_y"],
                "page_number": int(page_number),
                "row_number": row_number,
                "date": _parse_date_mmddyyyy(date_raw),
                "description": description_raw,
                "numbers": deduped_numbers,
                "is_opening": _is_balance_forwarded(row_text),
            }
        )

    balance_tokens = [token for token in assigned if token["column"] == "balance"]
    balance_entries = _group_column_entries(balance_tokens, tolerance)
    extracted_balances = [
        _extract_amount(entry.get("text") or "", prefer_last=True)
        for entry in balance_entries
        if _extract_amount(entry.get("text") or "", prefer_last=True)
    ]
    if not extracted_balances:
        return rough_rows, [], None

    first_date_y = min((float(row["anchor_y"]) for row in rough_rows if row.get("date")), default=None)
    first_balance_y = float(balance_entries[0].get("cy") or 0.0) if balance_entries else None
    has_explicit_opening = any(bool(row.get("is_opening")) for row in rough_rows)
    has_visual_opening = (
        first_date_y is not None
        and first_balance_y is not None
        and first_balance_y < (first_date_y - (tolerance * 0.5))
    )

    opening_balance: str | None = None
    start_index = 0
    if has_explicit_opening or has_visual_opening:
        opening_balance = extracted_balances[0]
        start_index = 1

    return rough_rows, extracted_balances[start_index:], opening_balance


def _pass2_ledger_reconstruction(
    rough_rows: list[dict[str, Any]],
    balance_stream: list[str],
    initial_opening_balance: str | None = None,
) -> list[dict[str, Any]]:
    """Pass 2: reconstruct debit/credit from sequential balances and deltas."""
    if not rough_rows or not balance_stream:
        return []

    # Keep date-bearing rows even when OCR missed numeric tokens;
    # Pass 2 reconstructs debit/credit from balance deltas.
    rows_with_date = [row for row in rough_rows if row.get("date")]
    if not rows_with_date:
        return []

    balances = list(balance_stream)
    opening_balance: Decimal | None = _to_decimal(initial_opening_balance)

    ledger_rows: list[dict[str, Any]] = []
    previous_balance = opening_balance
    balance_index = 0
    for row in rows_with_date:
        if balance_index >= len(balances):
            break

        row_number = row.get("row_number")
        if row_number is None:
            # Never invent row numbers; skip when OCR row marker is missing.
            continue

        balance_text = balances[balance_index]
        balance_index += 1
        balance_value = _to_decimal(balance_text)
        if balance_value is None:
            continue

        debit = ""
        credit = ""
        if previous_balance is not None:
            delta = balance_value - previous_balance
            if delta < -BALANCE_TOLERANCE:
                debit = _format_amount(-delta)
            elif delta > BALANCE_TOLERANCE:
                credit = _format_amount(delta)

        ledger_rows.append(
            {
                "row_number": int(row_number),
                "page_number": int(row.get("page_number") or 1),
                "date": row.get("date") or "",
                "description": (row.get("description") or "UNLABELED").strip(),
                "debit": debit,
                "credit": credit,
                "balance": balance_text,
            }
        )
        previous_balance = balance_value

    return ledger_rows


def _pass3_semantic_cleanup(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Pass 3: cleanup and filter non-transaction artifacts."""
    cleaned: list[dict[str, Any]] = []
    previous_balance: Decimal | None = None

    for row in rows:
        date_value = str(row.get("date") or "").strip()
        if not date_value:
            continue

        balance_value = _to_decimal(row.get("balance"))
        if balance_value is None:
            continue

        description = (row.get("description") or "").strip() or "UNLABELED"
        debit = _extract_amount(row.get("debit") or "", prefer_last=True)
        credit = _extract_amount(row.get("credit") or "", prefer_last=True)

        if previous_balance is not None and abs(balance_value - previous_balance) <= BALANCE_TOLERANCE and not debit and not credit:
            continue

        cleaned.append(
            {
                "row_number": int(row["row_number"]),
                "page_number": int(row.get("page_number") or 1),
                "date": date_value,
                "description": description,
                "debit": debit,
                "credit": credit,
                "balance": _extract_amount(row.get("balance") or "", prefer_last=True),
            }
        )
        previous_balance = balance_value

    return cleaned


def _build_rows_from_page(
    tokens: list[dict[str, Any]],
    bounds: dict[str, tuple[float, float]],
    header_y: float,
    next_row_seed: int,
) -> list[dict[str, Any]]:
    """Assign tokens to bounds, group by row, and parse BDO fields."""
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
    balance_tokens = [token for token in assigned if token["column"] == "balance"]

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

        row_text = " ".join(token["text"] for token in group["items"])
        row_number_raw = " ".join(item["text"] for item in sorted(by_column["row_number"], key=lambda t: t["cx"]))
        date_raw = " ".join(item["text"] for item in sorted(by_column["date"], key=lambda t: t["cx"]))
        description_raw = " ".join(
            item["text"] for item in sorted(by_column["description"], key=lambda t: t["cx"])
        ).strip()
        debit_raw = " ".join(item["text"] for item in sorted(by_column["debit"], key=lambda t: t["cx"]))
        credit_raw = " ".join(item["text"] for item in sorted(by_column["credit"], key=lambda t: t["cx"]))
        balance_raw = " ".join(item["text"] for item in sorted(by_column["balance"], key=lambda t: t["cx"]))

        parsed_row_number = _parse_row_number(row_number_raw)
        parsed_date = _parse_date_mmddyyyy(date_raw)
        debit = _extract_amount(debit_raw)
        credit = _extract_amount(credit_raw)
        balance = _extract_amount(balance_raw, prefer_last=True)

        if _is_balance_forwarded(row_text):
            balance = _extract_largest_amount(balance_raw)
            if not balance:
                nearby = [
                    token
                    for token in balance_tokens
                    if abs(token["cy"] - group["anchor_y"]) <= (tolerance * 3)
                ]
                nearby_text = " ".join(token["text"] for token in sorted(nearby, key=lambda t: (t["cy"], t["cx"])))
                balance = _extract_largest_amount(nearby_text)
            balance = balance or _extract_largest_amount(row_text)
            if not balance:
                continue
            parsed_date = None
            description = "BALANCE FORWARDED"
        else:
            description = description_raw
            # Artifact filter: keep only rows with real transactional signal.
            if parsed_date and (debit or credit or balance):
                pass
            elif balance and not debit and not credit:
                # Keep balance-only lines so sequence alignment can map balances 1:1.
                description = description or "BALANCE_ONLY"
            else:
                continue

        row_number = parsed_row_number
        if row_number is None or row_number in used_row_numbers:
            row_number = next_row
        used_row_numbers.add(row_number)
        next_row = max(next_row, row_number + 1)

        if (
            description == "BALANCE_ONLY"
            and rows
            and rows[-1].get("description") == "BALANCE FORWARDED"
            and str(rows[-1].get("balance") or "").strip() == str(balance or "").strip()
        ):
            # Drop duplicated opening balance token emitted as a standalone row.
            continue

        rows.append(
            {
                "row_number": row_number,
                "date": parsed_date or "",
                "description": description,
                "debit": debit,
                "credit": credit,
                "balance": balance,
            }
        )
    return rows


def parse_transactions_from_ocr_raw(raw_payload: dict[str, Any] | None) -> list[dict[str, Any]]:
    """Parse BDO transactions from Google Vision OCR raw payload."""
    if not raw_payload:
        return []

    pages = raw_payload.get("pages") or []
    rough_rows: list[dict[str, Any]] = []
    balance_stream: list[str] = []
    initial_opening_balance: str | None = None
    for page in pages:
        response = page.get("response") or {}
        page_number = int(page.get("page_number") or 0)
        tokens = _extract_tokens(response, page_number)
        if not tokens:
            continue
        headers, header_y = _find_headers(tokens)
        bounds = _build_bounds(tokens, headers)
        page_rough_rows, page_balance_stream, page_opening_balance = _pass1_layout_reconstruction(
            tokens,
            bounds,
            header_y,
            page_number,
        )
        if not page_rough_rows:
            continue
        if initial_opening_balance is None and page_opening_balance:
            initial_opening_balance = page_opening_balance
        rough_rows.extend(page_rough_rows)
        balance_stream.extend(page_balance_stream)

    pass2_rows = _pass2_ledger_reconstruction(rough_rows, balance_stream, initial_opening_balance)
    return _pass3_semantic_cleanup(pass2_rows)
