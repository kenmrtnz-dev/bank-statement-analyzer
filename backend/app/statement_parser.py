import datetime as dt
import re
from typing import Dict, List, Optional, Tuple

from app.bank_profiles import BankProfile, PROFILES


DATE_PATTERNS = {
    "mdy": [
        re.compile(r"\b(\d{1,2})/(\d{1,2})/(\d{2,4})\b"),
        re.compile(r"\b(\d{1,2})-(\d{1,2})-(\d{2,4})\b"),
        re.compile(r"\b(\d{1,2})\.(\d{1,2})\.(\d{2,4})\b"),
    ],
    "dmy": [
        re.compile(r"\b(\d{1,2})\s+([A-Za-z]{3})\s+(\d{2,4})\b"),
        re.compile(r"\b(\d{1,2})([A-Za-z]{3})(\d{2,4})\b"),
        re.compile(r"\b(\d{1,2})-(\d{1,2})-(\d{2,4})\b"),
        re.compile(r"\b(\d{1,2})\.(\d{1,2})\.(\d{2,4})\b"),
    ],
    "ymd": [
        re.compile(r"\b(\d{4})/(\d{1,2})/(\d{1,2})\b"),
        re.compile(r"\b(\d{4})-(\d{1,2})-(\d{1,2})\b"),
        re.compile(r"\b(\d{4})\.(\d{1,2})\.(\d{1,2})\b"),
    ],
}

MONTHS = {
    "jan": 1,
    "feb": 2,
    "mar": 3,
    "apr": 4,
    "may": 5,
    "jun": 6,
    "jul": 7,
    "aug": 8,
    "sep": 9,
    "oct": 10,
    "nov": 11,
    "dec": 12,
}
MONTH_ABBRS = [k.upper() for k in MONTHS.keys()]
OCR_DAY_DIGIT_MAP = {
    "O": "0", "Q": "0", "D": "0",
    "I": "1", "L": "1",
    "Z": "2",
    "S": "5",
    "B": "3",
    "T": "7", "Y": "7",
}
OCR_YEAR_DIGIT_MAP = {
    "O": "0", "Q": "0", "D": "0",
    "I": "1", "L": "1",
    "Z": "2",
    "S": "5",
    "B": "8",
    "T": "7", "Y": "7",
}
OCR_MONTH_CHAR_MAP = {
    "0": "O",
    "1": "I",
    "2": "Z",
    "5": "S",
    "8": "B",
    "6": "G",
    "4": "A",
    "7": "T",
}

AMOUNT_RE = re.compile(r"(?<![A-Za-z0-9])\(?-?\s*\$?\s*[\d,]+(?:\.\d{2})?\)?(?![A-Za-z0-9])")
HEADER_SYNONYMS = {
    "row": ["ln"],
    "date": ["date", "value date", "posting date"],
    "description": ["description", "details", "particulars", "transaction"],
    "debit": ["debit", "withdrawal"],
    "credit": ["credit", "deposit"],
    "balance": ["balance", "bal"],
}
NON_TRANSACTION_BALANCE_TOKENS = [
    "beginning balance",
    "beggining balance",
    "opening balance",
    "begin balance",
    "balance brought forward",
    "brought forward",
    "balance forward",
    "balance forwarded",
    "forwarded balance",
    "balance carried forward",
    "carried forward",
    "carry forward",
]


def normalize_amount(value: str) -> Optional[str]:
    text = (value or "").strip()
    if not text:
        return None
    # Do not treat date-like tokens as monetary amounts.
    if re.search(r"\b\d{1,4}[/-]\d{1,2}[/-]\d{1,4}\b", text):
        return None
    text = text.replace("₱", "").replace("PHP", "").replace("php", "")
    text = text.replace("—", "").replace("–", "").replace("-", "-")
    text = text.strip()
    if text in {"", "-", "--"}:
        return None

    neg = False
    if text.startswith("(") and text.endswith(")"):
        neg = True
        text = text[1:-1].strip()

    text = text.replace(" ", "")
    text = text.replace(",", "")
    text = re.sub(r"[^0-9.\-]", "", text)
    if text in {"", ".", "-"}:
        return None

    try:
        num = float(text)
    except ValueError:
        return None

    if neg and num > 0:
        num *= -1

    return f"{num:.2f}"


def normalize_date(value: str, order: List[str]) -> Optional[str]:
    text = (value or "").strip()
    if not text:
        return None

    # Trim timestamp tails like ', 11:10 A' or ' 11:10 AM'.
    text = re.sub(r",?\s+\d{1,2}:\d{2}(?::\d{2})?\s*[APMapm]{0,2}$", "", text).strip()
    text = re.sub(r"(?<=\d)[Oo](?=\d)", "0", text)

    for mode in order:
        for pattern in DATE_PATTERNS.get(mode, []):
            m = pattern.search(text)
            if not m:
                continue
            parsed = _match_to_date(m.groups(), mode)
            if parsed is not None:
                return parsed.isoformat()

    # OCR fallback: allow month/day without year.
    m = re.search(r"\b(\d{1,2})/(\d{1,2})\b", text)
    if m:
        try:
            guess = dt.date(dt.date.today().year, int(m.group(1)), int(m.group(2)))
            return guess.isoformat()
        except Exception:
            pass

    ocr_compact = _parse_ocr_compact_month_date(text)
    if ocr_compact is not None:
        return ocr_compact

    numeric_compact = _parse_ocr_compact_numeric_date(text, order)
    if numeric_compact is not None:
        return numeric_compact

    return None


def _match_to_date(groups: Tuple[str, ...], mode: str) -> Optional[dt.date]:
    try:
        if mode == "ymd":
            year = _normalize_year(groups[0])
            month = int(groups[1])
            day = int(groups[2])
            return dt.date(year, month, day)

        if mode == "mdy":
            month = int(groups[0])
            day = int(groups[1])
            year = _normalize_year(groups[2])
            return dt.date(year, month, day)

        if mode == "dmy":
            day = int(groups[0])
            if groups[1].isalpha():
                month = MONTHS.get(groups[1].strip().lower()[:3])
                if not month:
                    return None
                year = _normalize_year(groups[2])
            else:
                month = int(groups[1])
                year = _normalize_year(groups[2])
            return dt.date(year, month, day)
    except Exception:
        return None

    return None


def _normalize_year(raw: str) -> int:
    year = int(raw)
    if year < 100:
        year = 2000 + year
    # OCR on statements can misread 2025 as 1925.
    # Promote 19xx values to 20xx only when the result stays near present day.
    now_limit = dt.date.today().year + 1
    if 1900 <= year < 2000 and (year + 100) <= now_limit:
        year += 100
    return year


def _parse_ocr_compact_month_date(text: str) -> Optional[str]:
    tokens = re.findall(r"[A-Za-z0-9]{6,10}", text or "")
    for token in tokens:
        upper = token.upper()
        for i in range(0, len(upper) - 2):
            win = upper[i:i + 3]
            month_key = "".join(OCR_MONTH_CHAR_MAP.get(ch, ch) for ch in win)
            if month_key.lower()[:3] not in MONTHS:
                continue

            day_raw = upper[:i]
            year_raw = upper[i + 3:]
            if not day_raw or not year_raw:
                continue

            day_digits = []
            for ch in day_raw:
                mapped = OCR_DAY_DIGIT_MAP.get(ch, ch)
                if mapped.isdigit():
                    day_digits.append(mapped)
            year_digits = []
            for ch in year_raw:
                mapped = OCR_YEAR_DIGIT_MAP.get(ch, ch)
                if mapped.isdigit():
                    year_digits.append(mapped)

            if len(day_digits) == 0 or len(year_digits) < 2:
                continue

            day_txt = "".join(day_digits[-2:])
            year_txt = "".join(year_digits[:4])

            day = int(day_txt)
            if len(year_txt) >= 4:
                year = _normalize_year(year_txt[:4])
            else:
                year = _normalize_year(year_txt[:2])
            month = MONTHS[month_key.lower()[:3]]
            try:
                if 1 <= day <= 31:
                    return dt.date(year, month, day).isoformat()
                # OCR fallback when day is corrupted but month/year is visible.
                return dt.date(year, month, 1).isoformat()
            except Exception:
                continue
    return None


def _parse_ocr_compact_numeric_date(text: str, order: List[str]) -> Optional[str]:
    tokens = re.findall(r"\d{6,8}", text or "")
    for token in tokens:
        for mode in order:
            try:
                if mode == "ymd" and len(token) == 8:
                    year = _normalize_year(token[:4])
                    month = int(token[4:6])
                    day = int(token[6:8])
                    return dt.date(year, month, day).isoformat()
                if mode == "mdy":
                    if len(token) == 8:
                        month = int(token[:2])
                        day = int(token[2:4])
                        year = _normalize_year(token[4:8])
                    else:
                        month = int(token[:2])
                        day = int(token[2:4])
                        year = _normalize_year(token[4:6])
                    return dt.date(year, month, day).isoformat()
                if mode == "dmy":
                    if len(token) == 8:
                        day = int(token[:2])
                        month = int(token[2:4])
                        year = _normalize_year(token[4:8])
                    else:
                        day = int(token[:2])
                        month = int(token[2:4])
                        year = _normalize_year(token[4:6])
                    return dt.date(year, month, day).isoformat()
            except Exception:
                continue
    return None


def parse_words_page(
    words: List[Dict],
    page_width: float,
    page_height: float,
    profile: BankProfile,
    header_hint: Optional[Dict] = None,
    last_date_hint: Optional[str] = None,
) -> Tuple[List[Dict], List[Dict], Dict]:
    rows = []
    bounds = []
    attempted_bounded_parse = False

    grouped = _group_words_by_line(words)
    detected_header = _find_header_anchors(grouped, profile)
    diagnostics = {
        "header_detected": bool(detected_header),
        "header_y": detected_header["y"] if detected_header else None,
        "header_hint_used": False,
        "row_candidates": 0,
    }
    if detected_header:
        attempted_bounded_parse = True
        rows, bounds = _parse_grouped_lines_with_header(
            grouped_lines=grouped,
            page_width=page_width,
            page_height=page_height,
            profile=profile,
            header=detected_header,
            diagnostics=diagnostics,
            skip_before_header=True,
            last_date_hint=last_date_hint,
        )
        diagnostics["header_anchors"] = _serialize_header_anchors(detected_header)
        if rows:
            return rows, bounds, diagnostics
    elif _is_valid_header_hint(header_hint):
        attempted_bounded_parse = True
        hint_header = dict(header_hint or {})
        hint_header["y"] = float("-inf")
        rows, bounds = _parse_grouped_lines_with_header(
            grouped_lines=grouped,
            page_width=page_width,
            page_height=page_height,
            profile=profile,
            header=hint_header,
            diagnostics=diagnostics,
            skip_before_header=False,
            last_date_hint=last_date_hint,
        )
        diagnostics["header_hint_used"] = True
        diagnostics["fallback_mode"] = "header_hint_reuse"
        diagnostics["header_anchors"] = _serialize_header_anchors(hint_header)
        if rows:
            return rows, bounds, diagnostics

    if attempted_bounded_parse:
        diagnostics["fallback_mode"] = diagnostics.get("fallback_mode") or "bounded_parse_no_rows"
        return rows, bounds, diagnostics

    rows, bounds = _parse_rows_without_header(grouped, page_width, page_height, profile, last_date_hint=last_date_hint)
    diagnostics["fallback_mode"] = "no_header_line_parse"
    diagnostics["row_candidates"] = len(grouped)
    return rows, bounds, diagnostics


def _parse_grouped_lines_with_header(
    grouped_lines: List[Dict],
    page_width: float,
    page_height: float,
    profile: BankProfile,
    header: Dict,
    diagnostics: Dict,
    *,
    skip_before_header: bool,
    last_date_hint: Optional[str] = None,
) -> Tuple[List[Dict], List[Dict]]:
    rows: List[Dict] = []
    bounds: List[Dict] = []
    last_date_iso: Optional[str] = last_date_hint

    date_x = header["date"]
    description_x = header.get("description")
    debit_x = header["debit"]
    credit_x = header["credit"]
    balance_x = header["balance"]
    column_ranges = _build_header_column_ranges(header)

    for line in grouped_lines:
        y = line["cy"]
        if skip_before_header and y <= header["y"] + 2:
            continue

        line_text = " ".join(w["text"] for w in line["words"])
        if _is_noise(line_text, profile):
            continue

        diagnostics["row_candidates"] += 1
        date_txt = _nearest_text(line["words"], date_x)
        debit_txt, credit_txt, balance_txt = _assign_amount_columns(
            line["words"],
            debit_x,
            credit_x,
            balance_x,
            profile,
            debit_range=column_ranges.get("debit"),
            credit_range=column_ranges.get("credit"),
            balance_range=column_ranges.get("balance"),
        )

        # Parse dates from the full line first so multi-token dates
        # like "02 MAY 24" are handled consistently.
        date_iso = normalize_date(line_text, profile.date_order)
        if date_iso is None and date_txt:
            date_iso = normalize_date(date_txt, profile.date_order)
        debit = debit_txt
        credit = credit_txt
        balance = balance_txt
        description = _extract_description_from_header_line(
            line["words"],
            line_text,
            profile,
            header,
            column_ranges,
        )

        # With explicit header bounds, do not promote description numbers into amount columns.
        if balance is None and not any(column_ranges.get(key) is not None for key in ("debit", "credit", "balance")):
            line_amounts = _extract_line_amounts(line_text)
            if line_amounts:
                balance = line_amounts[-1]
                if len(line_amounts) >= 2 and debit is None and credit is None:
                    second = line_amounts[-2]
                    if second.startswith("-"):
                        debit = second
                    else:
                        credit = second

        if _is_opening_balance_line(line_text):
            debit = None
            credit = None

        description, debit, credit = _sanitize_profile_flow_values(
            profile,
            line_text,
            description,
            debit,
            credit,
        )
        if is_non_transaction_balance_line(description) or is_non_transaction_balance_line(line_text):
            continue

        if date_iso is None and _should_reuse_last_date_for_transaction(
            last_date_iso=last_date_iso,
            description=description,
            debit=debit,
            credit=credit,
            balance=balance,
            line_text=line_text,
            profile=profile,
        ):
            date_iso = last_date_iso

        if not (date_iso and balance):
            continue

        last_date_iso = date_iso
        row_id = f"{len(rows) + 1:03}"
        rows.append({
            "row_id": row_id,
            "date": date_iso,
            "description": description,
            "debit": debit,
            "credit": credit,
            "balance": balance,
        })

        row_bounds = _compute_tight_row_bounds(
            line_words=line["words"],
            page_width=page_width,
            page_height=page_height,
            left_hint=min(date_x, description_x if description_x is not None else date_x),
            right_hint=balance_x,
        )
        row_bounds["row_id"] = row_id
        bounds.append(row_bounds)

    return rows, bounds


def _serialize_header_anchors(header: Dict) -> Dict:
    out: Dict[str, float] = {}
    for key in ("row", "date", "description", "debit", "credit", "balance"):
        value = header.get(key)
        if value is None:
            continue
        try:
            out[key] = float(value)
        except Exception:
            continue
    for key in ("row", "date", "description", "debit", "credit", "balance"):
        span = header.get(f"{key}_span")
        if isinstance(span, dict):
            try:
                out[f"{key}_span"] = {
                    "x1": float(span.get("x1")),
                    "x2": float(span.get("x2")),
                    "cx": float(span.get("cx")),
                }
            except Exception:
                pass
    column_ranges = _build_header_column_ranges(header)
    if column_ranges:
        out["column_ranges"] = column_ranges
    return out


def _is_valid_header_hint(header_hint: Optional[Dict]) -> bool:
    if not isinstance(header_hint, dict):
        return False
    required = ("date", "debit", "credit", "balance")
    for key in required:
        value = header_hint.get(key)
        try:
            float(value)
        except Exception:
            return False
    return True


def parse_page_with_profile_fallback(
    words: List[Dict],
    page_width: float,
    page_height: float,
    detected_profile: BankProfile,
    header_hint: Optional[Dict] = None,
    last_date_hint: Optional[str] = None,
) -> Tuple[List[Dict], List[Dict], Dict]:
    base_rows, base_bounds, base_diag = parse_words_page(
        words,
        page_width,
        page_height,
        detected_profile,
        header_hint=header_hint,
        last_date_hint=last_date_hint,
    )
    base_ratio = _rows_conversion_ratio(base_rows, base_diag)

    selected_rows = base_rows
    selected_bounds = base_bounds
    selected_diag = dict(base_diag)
    selected_profile = detected_profile.name
    fallback_applied = False
    fallback_reason = None

    if _should_retry_generic(base_rows, base_diag):
        generic_profile = PROFILES["GENERIC"]
        fb_rows, fb_bounds, fb_diag = parse_words_page(
            words,
            page_width,
            page_height,
            generic_profile,
            header_hint=header_hint,
            last_date_hint=last_date_hint,
        )
        fb_ratio = _rows_conversion_ratio(fb_rows, fb_diag)

        choose_fallback = False
        if len(fb_rows) > len(base_rows):
            choose_fallback = True
        elif len(fb_rows) == len(base_rows) and fb_ratio > base_ratio:
            choose_fallback = True

        if choose_fallback:
            selected_rows = fb_rows
            selected_bounds = fb_bounds
            selected_diag = dict(fb_diag)
            selected_profile = generic_profile.name
            fallback_applied = True
            fallback_reason = "low_yield_detected_profile"

    selected_diag["profile_detected"] = detected_profile.name
    selected_diag["profile_selected"] = selected_profile
    selected_diag["fallback_applied"] = fallback_applied
    if fallback_reason:
        selected_diag["fallback_reason"] = fallback_reason

    return selected_rows, selected_bounds, selected_diag


def evaluate_quality(rows: List[Dict]) -> Dict:
    total = len(rows)
    if total == 0:
        return {
            "rows": 0,
            "date_ratio": 0.0,
            "balance_ratio": 0.0,
            "flow_ratio": 0.0,
            "passes": False,
            "reasons": ["no_rows"],
        }

    date_ok = sum(1 for r in rows if r.get("date"))
    balance_ok = sum(1 for r in rows if r.get("balance"))
    flow_ok = sum(1 for r in rows if r.get("debit") or r.get("credit"))

    date_ratio = date_ok / total
    balance_ratio = balance_ok / total
    flow_ratio = flow_ok / total

    reasons = []
    if total < 3:
        reasons.append("few_rows")
    if date_ratio < 0.8:
        reasons.append("low_date_ratio")
    if balance_ratio < 0.8:
        reasons.append("low_balance_ratio")

    return {
        "rows": total,
        "date_ratio": round(date_ratio, 3),
        "balance_ratio": round(balance_ratio, 3),
        "flow_ratio": round(flow_ratio, 3),
        "passes": len(reasons) == 0,
        "reasons": reasons,
    }


def is_transaction_row(row: Dict, profile: BankProfile) -> bool:
    if not row.get("date") or not row.get("balance"):
        return False

    description = str(row.get("description") or "").strip()
    lower_desc = description.lower()

    if description and is_non_transaction_balance_line(description):
        return False

    if not row.get("debit") and not row.get("credit"):
        return False

    if lower_desc:
        header_tokens = set(
            profile.date_tokens
            + profile.description_tokens
            + profile.debit_tokens
            + profile.credit_tokens
            + profile.balance_tokens
        )
        header_hits = sum(1 for token in header_tokens if token and token in lower_desc)
        if header_hits >= 2:
            return False

    return True


def should_fallback_to_ocr(
    text_word_count: int,
    rows: List[Dict],
    diagnostics: Dict,
) -> Tuple[bool, Optional[str]]:
    if text_word_count <= 0:
        return True, "no_text_layer"

    # Text-first speed mode: any text layer keeps the page in text parsing.
    # OCR is reserved only for pages without extractable text.
    return False, None


def _rows_conversion_ratio(rows: List[Dict], diagnostics: Dict) -> float:
    row_candidates = int(diagnostics.get("row_candidates") or 0)
    if row_candidates <= 0:
        return float(len(rows))
    return len(rows) / float(max(row_candidates, 1))


def _should_retry_generic(rows: List[Dict], diagnostics: Dict) -> bool:
    row_candidates = int(diagnostics.get("row_candidates") or 0)
    rows_count = len(rows)
    if row_candidates < 20:
        return False
    if rows_count <= 5:
        return True
    ratio = rows_count / float(max(row_candidates, 1))
    return ratio < 0.35


def _group_words_by_line(words: List[Dict]) -> List[Dict]:
    if not words:
        return []
    sorted_words = sorted(words, key=lambda w: (((w["y1"] + w["y2"]) / 2.0), w["x1"]))
    heights = [max(1.0, w["y2"] - w["y1"]) for w in sorted_words]
    median_h = sorted(heights)[len(heights) // 2]
    y_tol = max(2.0, median_h * 0.7)

    lines = []
    current = []
    current_y = None
    for w in sorted_words:
        cy = (w["y1"] + w["y2"]) / 2.0
        if not current:
            current = [w]
            current_y = cy
            continue
        if abs(cy - current_y) <= y_tol:
            current.append(w)
            current_y = (current_y + cy) / 2.0
        else:
            current = sorted(current, key=lambda x: x["x1"])
            lines.append({"words": current, "cy": current_y})
            current = [w]
            current_y = cy
    if current:
        current = sorted(current, key=lambda x: x["x1"])
        lines.append({"words": current, "cy": current_y})

    return lines


def _find_header_anchors(grouped_lines: List[Dict], profile: BankProfile) -> Optional[Dict]:
    search_window = grouped_lines[:80]
    for line in search_window:
        header = _detect_header_from_words(line["words"], profile, header_y=line["cy"])
        if header:
            return header

    for idx in range(len(search_window) - 1):
        upper = search_window[idx]
        lower = search_window[idx + 1]
        if not _header_lines_are_mergeable(upper, lower):
            continue
        combined_words = sorted(
            [*upper["words"], *lower["words"]],
            key=lambda word: (float(word["x1"]), float(word["y1"]), float(word["x2"])),
        )
        header = _detect_header_from_words(combined_words, profile, header_y=max(upper["cy"], lower["cy"]))
        if header:
            return header

    return None


def _detect_header_from_words(words: List[Dict], profile: BankProfile, *, header_y: float) -> Optional[Dict]:
    segments = _identify_header_segments(words, profile)
    core_hits = {segment["key"] for segment in segments if segment.get("source") == "core"}
    if "date" not in core_hits or "balance" not in core_hits:
        return None
    if "debit" not in core_hits and "credit" not in core_hits:
        return None
    if len(core_hits) < 3:
        return None

    header = _build_header_from_segments(header_y, segments)
    date_x = header.get("date")
    debit_x = header.get("debit")
    credit_x = header.get("credit")
    balance_x = header.get("balance")
    if date_x is None or balance_x is None:
        return None
    if debit_x is None:
        debit_x = (date_x + balance_x) / 2.0
        header["debit"] = debit_x
    if credit_x is None:
        credit_x = (debit_x + balance_x) / 2.0
        header["credit"] = credit_x
    return header


def _header_lines_are_mergeable(upper: Dict, lower: Dict) -> bool:
    upper_y = float(upper.get("cy") or 0.0)
    lower_y = float(lower.get("cy") or 0.0)
    if lower_y <= upper_y:
        return False
    return (lower_y - upper_y) <= 24.0


def _header_token_map(profile: Optional[BankProfile]) -> Dict[str, List[str]]:
    token_map = {key: list(values) for key, values in HEADER_SYNONYMS.items()}
    if profile:
        profile_tokens = {
            "date": profile.date_tokens,
            "description": profile.description_tokens,
            "debit": profile.debit_tokens,
            "credit": profile.credit_tokens,
            "balance": profile.balance_tokens,
        }
        for key, values in profile_tokens.items():
            bucket = token_map.setdefault(key, [])
            for value in values:
                normalized = str(value or "").strip().lower()
                if normalized and normalized not in bucket:
                    bucket.append(normalized)
    return token_map


def _identify_header_segments(words: List[Dict], profile: Optional[BankProfile]) -> List[Dict]:
    if not words:
        return []

    lowered = [str(w.get("text") or "").strip().lower() for w in words]
    token_specs: List[Tuple[int, str, List[str]]] = []
    for key, tokens in _header_token_map(profile).items():
        for token in tokens:
            parts = [part for part in token.split() if part]
            if parts:
                token_specs.append((len(parts), key, parts))
    token_specs.sort(key=lambda item: (-item[0], item[2]))

    matched_indices: set[int] = set()
    segments: List[Dict] = []
    i = 0
    while i < len(words):
        matched = False
        for width, key, parts in token_specs:
            if i + width > len(words):
                continue
            if any(idx in matched_indices for idx in range(i, i + width)):
                continue
            if lowered[i:i + width] != parts:
                continue
            segments.append(_make_header_segment(words, i, i + width - 1, key, source="core"))
            matched_indices.update(range(i, i + width))
            i += width
            matched = True
            break
        if not matched:
            i += 1

    extra_start: Optional[int] = None
    for idx in range(len(words)):
        token = lowered[idx]
        if not token:
            continue
        if idx in matched_indices:
            if extra_start is not None:
                segments.append(_make_header_segment(words, extra_start, idx - 1, "description", source="extra"))
                extra_start = None
            continue
        if extra_start is None:
            extra_start = idx
    if extra_start is not None:
        segments.append(_make_header_segment(words, extra_start, len(words) - 1, "description", source="extra"))

    return sorted(segments, key=lambda segment: (segment["x1"], segment["x2"]))


def _make_header_segment(words: List[Dict], start_idx: int, end_idx: int, key: str, *, source: str) -> Dict:
    left = float(words[start_idx]["x1"])
    right = float(words[end_idx]["x2"])
    cx = (left + right) / 2.0
    return {
        "key": key,
        "x1": left,
        "x2": right,
        "cx": cx,
        "source": source,
    }


def _build_header_from_segments(header_y: float, segments: List[Dict]) -> Dict:
    header: Dict = {"y": header_y, "segments": [dict(segment) for segment in segments]}
    merged_spans: Dict[str, Dict[str, float]] = {}

    for segment in segments:
        key = segment["key"]
        span = {"x1": float(segment["x1"]), "x2": float(segment["x2"]), "cx": float(segment["cx"])}
        if key not in header:
            header[key] = span["cx"]
        if key not in merged_spans:
            merged_spans[key] = dict(span)
            continue
        merged = merged_spans[key]
        merged["x1"] = min(merged["x1"], span["x1"])
        merged["x2"] = max(merged["x2"], span["x2"])
        merged["cx"] = (merged["x1"] + merged["x2"]) / 2.0

    for key, span in merged_spans.items():
        header[f"{key}_span"] = span

    return header


def _nearest_text(words: List[Dict], target_x: float) -> Optional[str]:
    if not words:
        return None
    # Use words left of the debit anchor as date candidate when date not isolated.
    candidates = sorted(words, key=lambda w: abs(((w["x1"] + w["x2"]) / 2.0) - target_x))
    for w in candidates[:3]:
        t = w["text"].strip()
        if t:
            return t
    return None


def _assign_amount_columns(
    words: List[Dict],
    debit_x: float,
    credit_x: float,
    balance_x: float,
    profile: Optional[BankProfile] = None,
    *,
    debit_range: Optional[Tuple[float, float]] = None,
    credit_range: Optional[Tuple[float, float]] = None,
    balance_range: Optional[Tuple[float, float]] = None,
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    amount_words = []
    flow_left = min(debit_x, credit_x, balance_x)
    strict_column_ranges = any(bounds is not None for bounds in (debit_range, credit_range, balance_range))
    for w in words:
        token = str(w.get("text", "")).strip()
        cx = (w["x1"] + w["x2"]) / 2.0
        if _is_profile_amount_zone_restricted(profile) and cx < (flow_left - 4.0):
            continue
        if _is_profile_reference_code(profile, token):
            continue
        if _is_date_like_token(w.get("text", "")):
            continue
        if _is_short_integer_amount_fragment(token):
            continue
        if _is_unformatted_long_integer_token(token):
            continue
        norm = normalize_amount(w["text"])
        if norm is None:
            continue
        amount_words.append({"cx": cx, "value": norm})

    if not amount_words:
        return None, None, None

    balance_candidates = [
        (i, amount)
        for i, amount in enumerate(amount_words)
        if _x_in_column_range(amount["cx"], balance_range)
    ]
    if balance_candidates:
        balance_idx = min(balance_candidates, key=lambda item: abs(item[1]["cx"] - balance_x))[0]
    elif strict_column_ranges:
        return None, None, None
    else:
        balance_idx = min(range(len(amount_words)), key=lambda i: abs(amount_words[i]["cx"] - balance_x))
    balance = amount_words[balance_idx]["value"]
    remaining = [a for i, a in enumerate(amount_words) if i != balance_idx]

    debit = None
    credit = None
    if not remaining:
        return debit, credit, balance

    debit_candidates = [amount for amount in remaining if _x_in_column_range(amount["cx"], debit_range)]
    credit_candidates = [amount for amount in remaining if _x_in_column_range(amount["cx"], credit_range)]
    used_candidates: list[dict] = []

    if debit_candidates:
        debit_pick = min(debit_candidates, key=lambda amount: abs(amount["cx"] - debit_x))
        debit = debit_pick["value"]
        used_candidates.append(debit_pick)
        credit_candidates = [amount for amount in credit_candidates if amount is not debit_pick]
    if credit_candidates:
        credit_pick = min(credit_candidates, key=lambda amount: abs(amount["cx"] - credit_x))
        credit = credit_pick["value"]
        used_candidates.append(credit_pick)

    residual_candidates = [amount for amount in remaining if amount not in used_candidates]
    if residual_candidates and debit_range is None and credit_range is None:
        if debit is None and credit is not None:
            debit_pick = min(residual_candidates, key=lambda amount: abs(amount["cx"] - debit_x))
            debit = debit_pick["value"]
            used_candidates.append(debit_pick)
            residual_candidates = [amount for amount in residual_candidates if amount is not debit_pick]
        elif credit is None and debit is not None:
            credit_pick = min(residual_candidates, key=lambda amount: abs(amount["cx"] - credit_x))
            credit = credit_pick["value"]
            used_candidates.append(credit_pick)
            residual_candidates = [amount for amount in residual_candidates if amount is not credit_pick]
        elif debit is None and credit is None:
            if len(residual_candidates) == 1:
                cand = residual_candidates[0]
                if abs(cand["cx"] - debit_x) <= abs(cand["cx"] - credit_x):
                    debit = cand["value"]
                else:
                    credit = cand["value"]
            else:
                d_idx = min(range(len(residual_candidates)), key=lambda i: abs(residual_candidates[i]["cx"] - debit_x))
                c_idx = min(range(len(residual_candidates)), key=lambda i: abs(residual_candidates[i]["cx"] - credit_x))

                if d_idx == c_idx:
                    cand = residual_candidates[d_idx]
                    if abs(cand["cx"] - debit_x) <= abs(cand["cx"] - credit_x):
                        debit = cand["value"]
                    else:
                        credit = cand["value"]
                    return debit, credit, balance

                debit = residual_candidates[d_idx]["value"]
                credit = residual_candidates[c_idx]["value"]

    if debit is None and credit is None and debit_range is None and credit_range is None:
        d_idx = min(range(len(remaining)), key=lambda i: abs(remaining[i]["cx"] - debit_x))
        c_idx = min(range(len(remaining)), key=lambda i: abs(remaining[i]["cx"] - credit_x))

        if d_idx == c_idx:
            cand = remaining[d_idx]
            if abs(cand["cx"] - debit_x) <= abs(cand["cx"] - credit_x):
                debit = cand["value"]
            else:
                credit = cand["value"]
            return debit, credit, balance

        debit = remaining[d_idx]["value"]
        credit = remaining[c_idx]["value"]
    return debit, credit, balance


def _is_date_like_token(token: str) -> bool:
    text = (token or "").strip()
    if not text:
        return False
    return bool(re.search(r"\b\d{1,4}[/-]\d{1,2}[/-]\d{1,4}\b", text))


def _is_short_integer_amount_fragment(token: str) -> bool:
    text = str(token or "").strip().replace(",", "")
    if not text:
        return False
    if any(ch in text for ch in ".()"):
        return False
    stripped = text.lstrip("-")
    return stripped.isdigit() and len(stripped) <= 2


def _is_unformatted_long_integer_token(token: str) -> bool:
    text = str(token or "").strip()
    if not text or any(ch in text for ch in ",.()"):
        return False
    digits_only = re.sub(r"[^0-9]", "", text.lstrip("-"))
    return digits_only.isdigit() and len(digits_only) >= 5


def _extract_description_from_header_line(
    words: List[Dict],
    line_text: str,
    profile: BankProfile,
    header: Dict,
    column_ranges: Dict[str, Tuple[float, float]],
) -> Optional[str]:
    if not words:
        return _extract_description_without_header(line_text, profile)

    segment_ranges = column_ranges.get("_segment_ranges") or []
    description_ranges = [
        segment["range"]
        for segment in segment_ranges
        if segment.get("key") == "description"
    ]
    reserved_segments = [
        segment
        for segment in segment_ranges
        if segment.get("key") in {"row", "date", "debit", "credit", "balance"}
    ]
    flow_ranges = [column_ranges.get(key) for key in ("debit", "credit", "balance") if column_ranges.get(key)]

    if not description_ranges and not flow_ranges:
        return _extract_description_without_header(line_text, profile)

    picked: List[str] = []
    for w in words:
        cx = (w["x1"] + w["x2"]) / 2.0
        token = (w.get("text") or "").strip()
        if not token:
            continue
        if _is_placeholder_description_token(token):
            continue
        if any(
            _word_in_column_range(w, segment["range"], slack=2.0)
            if segment.get("key") == "row"
            else _x_in_column_range(cx, segment["range"], slack=2.0)
            for segment in reserved_segments
        ):
            continue
        if (
            normalize_amount(token) is not None
            and not _is_profile_reference_code(profile, token)
            and any(_x_in_column_range(cx, column_ranges.get(key)) for key in ("debit", "credit", "balance"))
        ):
            continue
        if normalize_date(token, profile.date_order) is not None:
            continue
        if description_ranges and any(_word_in_column_range(w, bounds, slack=2.0) for bounds in description_ranges):
            picked.append(token)
            continue
        picked.append(token)

    if picked:
        desc = re.sub(r"\s+", " ", " ".join(picked)).strip(" -:|,")
        if desc and not _is_noise(desc, profile):
            return desc

    from_words = _extract_description_from_words(words, profile)
    if from_words:
        return from_words

    return _extract_description_without_header(line_text, profile)


def _extract_description_without_header(line_text: str, profile: BankProfile) -> Optional[str]:
    text = (line_text or "").strip()
    if not text:
        return None

    search_order = profile.date_order + [m for m in ("mdy", "dmy", "ymd") if m not in profile.date_order]
    for mode in search_order:
        for pattern in DATE_PATTERNS.get(mode, []):
            m = pattern.search(text)
            if m:
                text = f"{text[:m.start()]} {text[m.end():]}"
                break
        else:
            continue
        break

    text = re.sub(r",?\s+\d{1,2}:\d{2}(?::\d{2})?\s*[APMapm]{0,2}$", "", text)
    text = AMOUNT_RE.sub(" ", text)
    text = _normalize_description_text(text)
    if not text:
        return None
    if _is_noise(text, profile):
        return None
    return text


def _extract_description_from_words(words: List[Dict], profile: BankProfile) -> Optional[str]:
    if not words:
        return None

    ignored_tokens = {
        *(t.lower() for t in profile.date_tokens),
        *(t.lower() for t in profile.description_tokens),
        *(t.lower() for t in profile.debit_tokens),
        *(t.lower() for t in profile.credit_tokens),
        *(t.lower() for t in profile.balance_tokens),
    }

    parts: List[str] = []
    for w in sorted(words, key=lambda item: item.get("x1", 0.0)):
        token = (w.get("text") or "").strip()
        if not token:
            continue
        if _is_placeholder_description_token(token):
            continue
        lower = token.lower()
        if lower in ignored_tokens:
            continue
        if normalize_amount(token) is not None and not _is_profile_reference_code(profile, token):
            continue
        if normalize_date(token, profile.date_order) is not None:
            continue
        parts.append(token)

    text = _normalize_description_text(" ".join(parts))
    if not text:
        return None
    if _is_noise(text, profile):
        return None
    return text


def _is_placeholder_description_token(token: str) -> bool:
    cleaned = re.sub(r"\s+", "", str(token or ""))
    if not cleaned:
        return True
    return all(ch in "-–—_:|." for ch in cleaned)


def _normalize_description_text(text: str) -> Optional[str]:
    raw_tokens = [token for token in re.split(r"\s+", str(text or "").strip()) if token]
    tokens = [token for token in raw_tokens if not _is_placeholder_description_token(token)]
    normalized = re.sub(r"\s+", " ", " ".join(tokens)).strip(" -:|,")
    return normalized or None


def _extract_line_amounts(line_text: str) -> List[str]:
    out = []
    for m in AMOUNT_RE.findall(line_text):
        token = (m or "").strip()
        if not token:
            continue
        # Skip short integer fragments (often date/code OCR noise).
        plain = token.replace(",", "").replace("(", "").replace(")", "").replace("-", "").replace("$", "").strip()
        if "." not in token and plain.isdigit() and len(plain) <= 2:
            continue
        norm = normalize_amount(m)
        if norm is not None:
            out.append(norm)
    return out


def _should_reuse_last_date_for_transaction(
    *,
    last_date_iso: Optional[str],
    description: Optional[str],
    debit: Optional[str],
    credit: Optional[str],
    balance: Optional[str],
    line_text: str,
    profile: BankProfile,
) -> bool:
    if not last_date_iso:
        return False
    if not balance:
        return False
    if not (debit or credit):
        return False
    if is_non_transaction_balance_line(line_text):
        return False

    desc = str(description or "").strip()
    if not desc:
        return False
    if _is_noise(desc, profile):
        return False

    header_tokens = set(
        profile.date_tokens
        + profile.description_tokens
        + profile.debit_tokens
        + profile.credit_tokens
        + profile.balance_tokens
    )
    lower_desc = desc.lower()
    header_hits = sum(1 for token in header_tokens if token and token in lower_desc)
    if header_hits >= 2:
        return False

    return True


def _is_profile_reference_code(profile: Optional[BankProfile], token: str) -> bool:
    if not profile:
        return False
    if profile.name != "AUTO_BUSINESS_BANKING_GROWIDE":
        return False
    cleaned = (token or "").strip().replace(",", "")
    # BDO digital profile contains long reference-like numeric codes (often ending in .00)
    # inside description; do not treat them as debit/credit amounts.
    return bool(re.fullmatch(r"\d{10,}(?:\.00)?", cleaned))


def _is_profile_amount_zone_restricted(profile: Optional[BankProfile]) -> bool:
    return bool(profile and profile.name == "AUTO_BUSINESS_BANKING_GROWIDE")


def _sanitize_profile_flow_values(
    profile: Optional[BankProfile],
    line_text: str,
    description: Optional[str],
    debit: Optional[str],
    credit: Optional[str],
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    if not profile or profile.name != "AUTO_BUSINESS_BANKING_GROWIDE":
        return description, debit, credit

    desc = str(description or "").strip()

    def strip_ref_amount(value: Optional[str]) -> Optional[str]:
        nonlocal desc
        if not value:
            return value
        raw = str(value).strip()
        if not re.fullmatch(r"\d{10,}\.00", raw):
            return value
        code = raw[:-3]
        if code and code not in desc and code in re.sub(r"[^0-9]", "", line_text or ""):
            desc = (f"{desc} {code}").strip()
        return None

    debit = strip_ref_amount(debit)
    credit = strip_ref_amount(credit)
    return (desc or None), debit, credit


def _parse_rows_without_header(
    grouped_lines: List[Dict],
    page_width: float,
    page_height: float,
    profile: BankProfile,
    *,
    last_date_hint: Optional[str] = None,
) -> Tuple[List[Dict], List[Dict]]:
    rows = []
    bounds = []
    i = 0
    last_date_iso: Optional[str] = last_date_hint
    while i < len(grouped_lines):
        line = grouped_lines[i]
        line_text = " ".join(w["text"] for w in line["words"])
        if _is_noise(line_text, profile):
            i += 1
            continue

        date_iso = normalize_date(line_text, profile.date_order)

        line_words = list(line["words"])
        amounts = _extract_line_amounts(line_text)
        j = i + 1
        # OCR often emits one transaction across multiple short lines.
        while len(amounts) < 2 and j < len(grouped_lines) and j <= i + 3:
            next_line = grouped_lines[j]
            next_text = " ".join(w["text"] for w in next_line["words"])
            if normalize_date(next_text, profile.date_order):
                break
            next_amounts = _extract_line_amounts(next_text)
            if next_amounts:
                amounts.extend(next_amounts)
                line_words.extend(next_line["words"])
            j += 1

        if not amounts:
            i += 1
            continue

        balance = amounts[-1]
        debit = None
        credit = None
        if len(amounts) >= 2:
            flow = amounts[-2]
            lower = line_text.lower()
            if flow.startswith("-") or any(t in lower for t in ["withdraw", "debit", "db"]):
                debit = flow
            elif any(t in lower for t in ["deposit", "credit", "cr"]):
                credit = flow
            else:
                debit = flow

        combined_text = " ".join(w["text"] for w in line_words)
        if is_non_transaction_balance_line(combined_text):
            debit = None
            credit = None

        description = _extract_description_from_words(line_words, profile) or _extract_description_without_header(combined_text, profile)
        if is_non_transaction_balance_line(description) or is_non_transaction_balance_line(combined_text):
            i = max(i + 1, j)
            continue
        if date_iso is None and _should_reuse_last_date_for_transaction(
            last_date_iso=last_date_iso,
            description=description,
            debit=debit,
            credit=credit,
            balance=balance,
            line_text=combined_text,
            profile=profile,
        ):
            date_iso = last_date_iso

        if not date_iso:
            i = max(i + 1, j)
            continue

        last_date_iso = date_iso
        row_id = f"{len(rows) + 1:03}"
        rows.append({
            "row_id": row_id,
            "date": date_iso,
            "description": description,
            "debit": debit,
            "credit": credit,
            "balance": balance,
        })

        row_bounds = _compute_tight_row_bounds(
            line_words=line_words,
            page_width=page_width,
            page_height=page_height,
        )
        row_bounds["row_id"] = row_id
        bounds.append(row_bounds)

        i = max(i + 1, j)

    return rows, bounds


def _is_noise(line_text: str, profile: BankProfile) -> bool:
    lower = (line_text or "").lower()
    if not lower.strip():
        return True
    for token in profile.noise_tokens:
        if token in lower:
            return True
    return False


def _is_opening_balance_line(text: str) -> bool:
    return is_non_transaction_balance_line(text)


def is_non_transaction_balance_line(text: str) -> bool:
    lower = (text or "").lower()
    if not lower.strip():
        return False
    normalized = re.sub(r"[^a-z0-9]+", " ", lower)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return any(
        token in normalized
        for token in NON_TRANSACTION_BALANCE_TOKENS
    )


def _compute_tight_row_bounds(
    line_words: List[Dict],
    page_width: float,
    page_height: float,
    left_hint: Optional[float] = None,
    right_hint: Optional[float] = None,
) -> Dict[str, float]:
    words = [w for w in (line_words or []) if str(w.get("text") or "").strip()]
    if not words:
        return {"x1": 0.0, "y1": 0.0, "x2": 0.0, "y2": 0.0}

    if left_hint is not None and right_hint is not None:
        left = min(float(left_hint), float(right_hint))
        right = max(float(left_hint), float(right_hint))
        x_pad = max(10.0, page_width * 0.025)
        zone_left = max(0.0, left - x_pad)
        zone_right = min(float(max(page_width, 1.0)), right + x_pad)
        if zone_right > zone_left:
            zone_words = [w for w in words if float(w.get("x2") or 0) >= zone_left and float(w.get("x1") or 0) <= zone_right]
            if len(zone_words) >= 2:
                words = zone_words

    heights = sorted(max(1.0, float(w["y2"]) - float(w["y1"])) for w in words)
    median_h = heights[len(heights) // 2] if heights else 1.0
    y_centers = sorted((float(w["y1"]) + float(w["y2"])) / 2.0 for w in words)
    median_cy = y_centers[len(y_centers) // 2] if y_centers else 0.0
    y_tol = max(3.0, median_h * 1.15)
    core_words = [
        w
        for w in words
        if abs(((float(w["y1"]) + float(w["y2"])) / 2.0) - median_cy) <= y_tol
    ]
    if len(core_words) >= 2:
        words = core_words

    x1 = min(float(w["x1"]) for w in words)
    y1 = min(float(w["y1"]) for w in words)
    x2 = max(float(w["x2"]) for w in words)
    y2 = max(float(w["y2"]) for w in words)

    pad_x = max(2.0, page_width * 0.0035)
    pad_y = max(1.0, page_height * 0.0025)
    x1 = max(0.0, x1 - pad_x)
    y1 = max(0.0, y1 - pad_y)
    x2 = min(float(max(page_width, 1.0)), x2 + pad_x)
    y2 = min(float(max(page_height, 1.0)), y2 + pad_y)

    return {
        "x1": _clamp01(x1 / max(page_width, 1.0)),
        "y1": _clamp01(y1 / max(page_height, 1.0)),
        "x2": _clamp01(x2 / max(page_width, 1.0)),
        "y2": _clamp01(y2 / max(page_height, 1.0)),
    }


def _clamp01(v: float) -> float:
    return max(0.0, min(1.0, float(v)))


def _build_header_column_ranges(header: Dict) -> Dict[str, Tuple[float, float]]:
    segments = header.get("segments")
    if isinstance(segments, list) and segments:
        ordered_segments = sorted(
            [
                {
                    "key": str(segment.get("key") or ""),
                    "x1": float(segment.get("x1")),
                    "x2": float(segment.get("x2")),
                    "cx": float(segment.get("cx")),
                }
                for segment in segments
                if segment.get("key") is not None
            ],
            key=lambda item: (item["x1"], item["x2"]),
        )
        if ordered_segments:
            ranges: Dict[str, Tuple[float, float]] = {}
            segment_ranges: List[Dict] = []
            for idx, segment in enumerate(ordered_segments):
                key = segment["key"]
                left = float(segment["x1"]) if key in {"debit", "credit", "balance"} else float(segment["cx"])
                right = float("inf")
                if key in {"debit", "credit", "balance"}:
                    for future in ordered_segments[idx + 1:]:
                        if future["key"] in {"debit", "credit", "balance"} and float(future["x1"]) > left:
                            right = float(future["x1"])
                            break
                elif idx + 1 < len(ordered_segments):
                    right = float(ordered_segments[idx + 1]["x1"])
                bounds = (left, right)
                segment_ranges.append({"key": segment["key"], "range": bounds})
                if key not in ranges:
                    ranges[key] = bounds
                else:
                    current_left, current_right = ranges[key]
                    merged_right = right if current_right == float("inf") or right == float("inf") else max(current_right, right)
                    ranges[key] = (min(current_left, left), merged_right)
            ranges["_segment_ranges"] = segment_ranges
            return ranges

    ordered_keys = ("date", "description", "debit", "credit", "balance")
    anchors = []
    for key in ordered_keys:
        center = header.get(key)
        if center is None:
            continue
        span = header.get(f"{key}_span")
        if isinstance(span, dict):
            left = float(span.get("x1", center))
            right = float(span.get("x2", center))
        else:
            left = float(center)
            right = float(center)
        anchors.append((key, left, right, float(center)))

    if not anchors:
        return {}

    ranges: Dict[str, Tuple[float, float]] = {}
    for idx, (key, left, right, center) in enumerate(anchors):
        prev_anchor = anchors[idx - 1] if idx > 0 else None
        next_anchor = anchors[idx + 1] if idx + 1 < len(anchors) else None

        if prev_anchor is None:
            range_left = float("-inf")
        else:
            prev_right = prev_anchor[2]
            range_left = (prev_right + left) / 2.0 if prev_right <= left else (prev_anchor[3] + center) / 2.0

        if next_anchor is None:
            range_right = float("inf")
        else:
            next_left = next_anchor[1]
            range_right = (right + next_left) / 2.0 if right <= next_left else (center + next_anchor[3]) / 2.0

        ranges[key] = (float(range_left), float(range_right))

    return ranges


def _x_in_column_range(x: float, bounds: Optional[Tuple[float, float]], slack: float = 4.0) -> bool:
    if bounds is None:
        return False
    left, right = bounds
    return (x >= (left - slack)) and (x <= (right + slack))


def _word_in_column_range(word: Dict, bounds: Optional[Tuple[float, float]], slack: float = 4.0) -> bool:
    if bounds is None:
        return False
    left, right = bounds
    x1 = float(word.get("x1") or 0.0)
    x2 = float(word.get("x2") or 0.0)
    return x2 >= (left - slack) and x1 <= (right + slack)
