from __future__ import annotations

import json
import math
import os
import datetime as dt
import shutil
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Callable, Dict, List

import cv2
from pdf2image import convert_from_path, pdfinfo_from_path
from PIL import Image

from app.bank_profiles import detect_bank_profile
from app.pdf_text_extract import extract_pdf_layout_pages
from app.services.ocr.router import (
    build_scanned_ocr_router,
    resolve_document_parse_mode,
    scanned_render_dpi,
)
from app.statement_parser import (
    is_non_transaction_balance_line,
    is_transaction_row,
    normalize_date,
    parse_page_with_profile_fallback,
)

OCR_BACKEND = "google_vision"
PREVIEW_MAX_PIXELS = int(os.getenv("PREVIEW_MAX_PIXELS", "6000000"))
OPENAI_OCR_USE_STRUCTURED_ROWS = str(os.getenv("OPENAI_OCR_USE_STRUCTURED_ROWS", "true")).strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
OPENAI_OCR_PAGE_BATCH_SIZE = max(1, int(os.getenv("OPENAI_OCR_PAGE_BATCH_SIZE", "25")))
OCR_ROW_FILTER_LENIENT = str(os.getenv("OCR_ROW_FILTER_LENIENT", "true")).strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
FLOW_AMOUNT_TOLERANCE = Decimal("0.005")

ProgressReporter = Callable[[str, str, int], None]


def normalize_parse_mode(mode: str | None) -> str:
    raw = str(mode or "").strip().lower()
    if raw in {"text", "ocr", "auto", "google_vision", "pdftotext"}:
        return raw
    return "auto"


def resolve_parse_mode(input_pdf: str, requested_mode: str | None) -> str:
    return resolve_document_parse_mode(input_pdf=input_pdf, requested_mode=normalize_parse_mode(requested_mode))


def run_pipeline(job_dir: str | Path, parse_mode: str, report: ProgressReporter) -> Dict:
    root = Path(job_dir)
    input_pdf = root / "input" / "document.pdf"
    pages_dir = root / "pages"
    cleaned_dir = root / "cleaned"
    ocr_dir = root / "ocr"
    result_dir = root / "result"

    pages_dir.mkdir(parents=True, exist_ok=True)
    cleaned_dir.mkdir(parents=True, exist_ok=True)
    ocr_dir.mkdir(parents=True, exist_ok=True)
    result_dir.mkdir(parents=True, exist_ok=True)

    selected_mode = normalize_parse_mode(parse_mode)
    if selected_mode == "auto":
        selected_mode = resolve_parse_mode(str(input_pdf), "auto")

    effective_mode = "text" if selected_mode in {"text", "pdftotext"} else "ocr"

    if effective_mode == "text":
        try:
            parsed_output, bounds_output, diagnostics = _run_text_pipeline(
                input_pdf=input_pdf,
                ocr_dir=ocr_dir,
                report=report,
            )
        except Exception as exc:
            selected_mode = "google_vision"
            parsed_output, bounds_output, diagnostics = _run_ocr_pipeline(
                input_pdf=input_pdf,
                pages_dir=pages_dir,
                cleaned_dir=cleaned_dir,
                ocr_dir=ocr_dir,
                report=report,
            )
            diagnostics.setdefault("job", {})["text_fallback_error"] = str(exc or "")[:500]
    else:
        parsed_output, bounds_output, diagnostics = _run_ocr_pipeline(
            input_pdf=input_pdf,
            pages_dir=pages_dir,
            cleaned_dir=cleaned_dir,
            ocr_dir=ocr_dir,
            report=report,
        )

    job_diag = diagnostics.setdefault("job", {})
    job_diag["parse_mode"] = selected_mode
    job_diag.setdefault("ocr_backend", OCR_BACKEND)
    job_diag["pages"] = len(parsed_output)

    _write_json_atomic(result_dir / "parsed_rows.json", parsed_output)
    _write_json_atomic(result_dir / "bounds.json", bounds_output)
    _write_json_atomic(result_dir / "parse_diagnostics.json", diagnostics)

    return {
        "parse_mode": selected_mode,
        "pages": len(parsed_output),
        "parsed_rows": parsed_output,
        "bounds": bounds_output,
        "diagnostics": diagnostics,
    }


def _run_text_pipeline(input_pdf: Path, ocr_dir: Path, report: ProgressReporter) -> tuple[Dict, Dict, Dict]:
    report("processing", "text_extraction", 10)
    layout_pages = extract_pdf_layout_pages(str(input_pdf))
    if not layout_pages:
        raise RuntimeError("text_layer_not_found")

    parsed_output: Dict[str, List[Dict]] = {}
    bounds_output: Dict[str, List[Dict]] = {}
    diagnostics: Dict[str, Dict] = {"job": {"source_type": "text"}, "pages": {}}
    header_hints_by_profile: Dict[str, Dict] = {}
    last_header_hint: Dict | None = None
    last_date_hint: str = ""
    last_balance_hint = None
    previous_page_rows: List[Dict] = []
    seen_row_signatures: set[tuple] = set()

    total = len(layout_pages)
    for idx, layout in enumerate(layout_pages, start=1):
        page_name = f"page_{idx:03}"
        words = layout.get("words") or []
        page_w = float(layout.get("width") or 1)
        page_h = float(layout.get("height") or 1)
        text = str(layout.get("text") or "")

        profile = detect_bank_profile(text)
        page_rows, page_bounds, parser_diag = parse_page_with_profile_fallback(
            words,
            page_w,
            page_h,
            profile,
            header_hint=header_hints_by_profile.get(profile.name) or last_header_hint,
            last_date_hint=last_date_hint or None,
        )
        filtered_rows, filtered_bounds = _filter_rows_and_bounds(page_rows, page_bounds, profile)
        filtered_rows, filtered_bounds = _dedupe_page_overlap(
            previous_rows=previous_page_rows,
            current_rows=filtered_rows,
            current_bounds=filtered_bounds,
        )
        filtered_rows, filtered_bounds = _dedupe_document_rows(
            seen_signatures=seen_row_signatures,
            current_rows=filtered_rows,
            current_bounds=filtered_bounds,
        )
        selected_profile = str(parser_diag.get("profile_selected") or profile.name)
        header_anchors = parser_diag.get("header_anchors")
        if isinstance(header_anchors, dict) and header_anchors:
            header_hints_by_profile[selected_profile] = dict(header_anchors)
            last_header_hint = dict(header_anchors)

        parsed_output[page_name] = filtered_rows
        bounds_output[page_name] = filtered_bounds
        previous_page_rows = filtered_rows
        page_last_date = _last_row_date(filtered_rows)
        if page_last_date:
            last_date_hint = page_last_date
        page_last_balance = _last_row_balance(filtered_rows)
        if page_last_balance is not None:
            last_balance_hint = page_last_balance
        diagnostics["pages"][page_name] = {
            "source_type": "text",
            "bank_profile": profile.name,
            "rows_parsed": len(filtered_rows),
            "profile_detected": parser_diag.get("profile_detected", profile.name),
            "profile_selected": selected_profile,
            "fallback_applied": bool(parser_diag.get("fallback_applied", False)),
            "header_detected": bool(parser_diag.get("header_detected", False)),
            "header_hint_used": bool(parser_diag.get("header_hint_used", False)),
            "fallback_mode": parser_diag.get("fallback_mode"),
        }
        _write_json_atomic(ocr_dir / f"{page_name}.json", [])

        progress = 15 + int((idx / max(total, 1)) * 75)
        report("processing", "text_parsing", progress)

    return parsed_output, bounds_output, diagnostics


def _run_ocr_pipeline(
    input_pdf: Path,
    pages_dir: Path,
    cleaned_dir: Path,
    ocr_dir: Path,
    report: ProgressReporter,
) -> tuple[Dict, Dict, Dict]:
    page_files = prepare_ocr_pages(
        input_pdf=input_pdf,
        pages_dir=pages_dir,
        cleaned_dir=cleaned_dir,
        report=report,
    )
    ocr_router = build_scanned_ocr_router(page_count=len(page_files))

    parsed_output: Dict[str, List[Dict]] = {}
    bounds_output: Dict[str, List[Dict]] = {}
    diagnostics: Dict[str, Dict] = {"job": {"source_type": "ocr", "ocr_backend": ocr_router.engine_name}, "pages": {}}
    header_hints_by_profile: Dict[str, Dict] = {}
    last_date_hint: str = ""
    last_balance_hint = None

    for batch_start in range(0, len(page_files), OPENAI_OCR_PAGE_BATCH_SIZE):
        batch = page_files[batch_start:batch_start + OPENAI_OCR_PAGE_BATCH_SIZE]
        for inner_idx, page_file in enumerate(batch, start=1):
            idx = batch_start + inner_idx
            page_name, page_rows, page_bounds, page_diag = process_ocr_page(
                page_file=page_file,
                cleaned_dir=cleaned_dir,
                ocr_dir=ocr_dir,
                ocr_router=ocr_router,
                header_hint=header_hints_by_profile.get("last"),
                last_date_hint=last_date_hint or None,
            )
            parsed_output[page_name] = page_rows
            bounds_output[page_name] = page_bounds
            diagnostics["pages"][page_name] = page_diag
            header_anchors = page_diag.get("header_anchors")
            if isinstance(header_anchors, dict) and header_anchors:
                header_hints_by_profile["last"] = dict(header_anchors)
            page_last_date = _last_row_date(page_rows)
            if page_last_date:
                last_date_hint = page_last_date
            page_last_balance = _last_row_balance(page_rows)
            if page_last_balance is not None:
                last_balance_hint = page_last_balance

            progress = 45 + int((idx / max(len(page_files), 1)) * 45)
            report("processing", "ocr_parsing", progress)

    return parsed_output, bounds_output, diagnostics


def prepare_ocr_pages(
    input_pdf: Path,
    pages_dir: Path,
    cleaned_dir: Path,
    report: ProgressReporter | None = None,
) -> List[str]:
    if report is not None:
        report("processing", "pdf_to_images", 5)
    page_files = _render_pdf_pages(input_pdf=input_pdf, pages_dir=pages_dir, dpi=scanned_render_dpi())
    if not page_files:
        raise RuntimeError("no_pages_rendered")

    cleaned_dir.mkdir(parents=True, exist_ok=True)
    if report is not None:
        report("processing", "image_copy", 20)
    for idx, page_file in enumerate(page_files, start=1):
        src = pages_dir / page_file
        dst = cleaned_dir / page_file
        shutil.copyfile(src, dst)
        if report is not None:
            progress = 20 + int((idx / max(len(page_files), 1)) * 20)
            report("processing", "image_copy", progress)
    return page_files


def process_ocr_page(
    page_file: str,
    cleaned_dir: Path,
    ocr_dir: Path,
    *,
    ocr_router=None,
    rate_limit_heartbeat=None,
    header_hint: Dict | None = None,
    last_date_hint: str | None = None,
) -> tuple[str, List[Dict], List[Dict], Dict]:
    page_name = page_file.replace(".png", "")
    page_path = cleaned_dir / page_file
    page_h, page_w = _image_size(page_path)

    if ocr_router is None:
        ocr_router = build_scanned_ocr_router(page_count=1)

    if OPENAI_OCR_USE_STRUCTURED_ROWS and ocr_router.engine_name == "openai_vision" and ocr_router.openai_client is not None:
        try:
            structured = ocr_router.openai_client.extract_structured_rows(page_path, rate_limit_heartbeat=rate_limit_heartbeat)
            raw_openai = ocr_router.openai_client.consume_last_openai_response()
            if raw_openai is not None:
                _write_json_atomic(ocr_dir / f"{page_name}.openai_raw.json", raw_openai)
            ai_rows, ai_bounds = _normalize_structured_ai_rows(
                structured_rows=structured.get("rows") or [],
                page_width=page_w,
                page_height=page_h,
                last_date_hint=last_date_hint or "",
            )
            if ai_rows:
                _write_json_atomic(ocr_dir / f"{page_name}.json", [])
                diag = {
                    "source_type": "ocr",
                    "ocr_backend": ocr_router.engine_name,
                    "row_extraction": "openai_structured_rows",
                    "rows_parsed": len(ai_rows),
                    "batch_size": OPENAI_OCR_PAGE_BATCH_SIZE,
                }
                return page_name, ai_rows, ai_bounds, diag
        except Exception:
            # Fall back to token OCR + local parser path.
            pass

    ocr_items = ocr_router.ocr_page(page_path)
    _write_json_atomic(ocr_dir / f"{page_name}.json", ocr_items)
    if ocr_router.engine_name == "openai_vision" and ocr_router.openai_client is not None:
        raw_openai = ocr_router.openai_client.consume_last_openai_response()
        if raw_openai is not None:
            _write_json_atomic(ocr_dir / f"{page_name}.openai_raw.json", raw_openai)

    ocr_words = _ocr_items_to_words(ocr_items)
    text = " ".join((item.get("text") or "") for item in ocr_items)
    profile = detect_bank_profile(text)

    page_rows, page_bounds, parser_diag = parse_page_with_profile_fallback(
        ocr_words,
        page_w,
        page_h,
        profile,
        header_hint=header_hint,
        last_date_hint=last_date_hint,
    )
    filtered_rows, filtered_bounds = _filter_rows_and_bounds(page_rows, page_bounds, profile)
    diag = {
        "source_type": "ocr",
        "ocr_backend": ocr_router.engine_name,
        "bank_profile": profile.name,
        "rows_parsed": len(filtered_rows),
        "profile_detected": parser_diag.get("profile_detected", profile.name),
        "profile_selected": parser_diag.get("profile_selected", profile.name),
        "fallback_applied": bool(parser_diag.get("fallback_applied", False)),
        "header_detected": bool(parser_diag.get("header_detected", False)),
        "header_hint_used": bool(parser_diag.get("header_hint_used", False)),
    }
    if isinstance(parser_diag.get("header_anchors"), dict):
        diag["header_anchors"] = parser_diag["header_anchors"]
    return page_name, filtered_rows, filtered_bounds, diag


def _filter_rows_and_bounds(page_rows: List[Dict], page_bounds: List[Dict], profile) -> tuple[List[Dict], List[Dict]]:
    filtered_rows: List[Dict] = []
    for row in page_rows:
        row_type = _classify_row_type(row, profile)
        if not row_type:
            continue
        row_copy = dict(row)
        row_copy["row_type"] = row_type
        filtered_rows.append(row_copy)

    return _renumber_rows_and_bounds(filtered_rows, page_bounds)


def _renumber_rows_and_bounds(page_rows: List[Dict], page_bounds: List[Dict]) -> tuple[List[Dict], List[Dict]]:
    id_map: Dict[str, str] = {}
    normalized_rows: List[Dict] = []
    for idx, row in enumerate(page_rows, start=1):
        old_id = str(row.get("row_id") or idx)
        new_id = f"{idx:03}"
        id_map[old_id] = new_id
        normalized_rows.append(
            {
                "row_id": new_id,
                "date": row.get("date"),
                "description": row.get("description"),
                "debit": row.get("debit"),
                "credit": row.get("credit"),
                "balance": row.get("balance"),
                "row_type": row.get("row_type") or "transaction",
            }
        )

    normalized_bounds: List[Dict] = []
    for bound in page_bounds:
        old_id = str(bound.get("row_id") or "")
        if old_id not in id_map:
            continue
        normalized_bounds.append(
            {
                "row_id": id_map[old_id],
                "x1": bound.get("x1"),
                "y1": bound.get("y1"),
                "x2": bound.get("x2"),
                "y2": bound.get("y2"),
            }
        )

    return normalized_rows, normalized_bounds


def _dedupe_page_overlap(
    previous_rows: List[Dict],
    current_rows: List[Dict],
    current_bounds: List[Dict],
    *,
    max_overlap: int = 5,
) -> tuple[List[Dict], List[Dict]]:
    if not previous_rows or not current_rows:
        return current_rows, current_bounds

    overlap_limit = min(max_overlap, len(previous_rows), len(current_rows))
    overlap = 0
    for size in range(overlap_limit, 0, -1):
        prev_slice = previous_rows[-size:]
        curr_slice = current_rows[:size]
        if all(_row_signature(prev_row) == _row_signature(curr_row) for prev_row, curr_row in zip(prev_slice, curr_slice)):
            overlap = size
            break

    if overlap <= 0:
        return current_rows, current_bounds

    kept_rows = current_rows[overlap:]
    kept_ids = {str(row.get("row_id") or "") for row in kept_rows}
    kept_bounds = [bound for bound in current_bounds if str(bound.get("row_id") or "") in kept_ids]
    return _renumber_rows_and_bounds(kept_rows, kept_bounds)


def _dedupe_document_rows(
    seen_signatures: set[tuple],
    current_rows: List[Dict],
    current_bounds: List[Dict],
) -> tuple[List[Dict], List[Dict]]:
    if not current_rows:
        return current_rows, current_bounds

    kept_rows: List[Dict] = []
    kept_ids: set[str] = set()
    for row in current_rows:
        signature = _row_signature(row)
        if signature in seen_signatures:
            continue
        seen_signatures.add(signature)
        kept_rows.append(row)
        kept_ids.add(str(row.get("row_id") or ""))

    if len(kept_rows) == len(current_rows):
        return current_rows, current_bounds

    kept_bounds = [bound for bound in current_bounds if str(bound.get("row_id") or "") in kept_ids]
    return _renumber_rows_and_bounds(kept_rows, kept_bounds)


def _row_signature(row: Dict) -> tuple:
    return (
        str(row.get("date") or "").strip(),
        str(row.get("description") or "").strip(),
        str(row.get("debit") or "").strip(),
        str(row.get("credit") or "").strip(),
        str(row.get("balance") or "").strip(),
        str(row.get("row_type") or "transaction").strip().lower(),
    )


def _normalize_structured_ai_rows(
    structured_rows: List[Dict],
    page_width: int,
    page_height: int,
    *,
    last_date_hint: str = "",
) -> tuple[List[Dict], List[Dict]]:
    rows: List[Dict] = []
    bounds: List[Dict] = []
    max_w = float(max(page_width, 1))
    max_h = float(max(page_height, 1))
    last_date: str = str(last_date_hint or "").strip()

    for idx, row in enumerate(structured_rows, start=1):
        if not isinstance(row, dict):
            continue
        b = row.get("bounds")
        if not isinstance(b, dict):
            continue
        try:
            x1 = float(b.get("x1"))
            y1 = float(b.get("y1"))
            x2 = float(b.get("x2"))
            y2 = float(b.get("y2"))
        except Exception:
            continue
        x1 = max(0.0, min(max_w, x1))
        y1 = max(0.0, min(max_h, y1))
        x2 = max(0.0, min(max_w, x2))
        y2 = max(0.0, min(max_h, y2))
        if x2 < x1:
            x1, x2 = x2, x1
        if y2 < y1:
            y1, y2 = y2, y1

        row_id = f"{len(rows) + 1:03}"
        normalized_date = _normalize_structured_row_date(str(row.get("date") or "").strip())
        description = str(row.get("description") or "").strip()
        debit = _normalize_amount_value(row.get("debit"))
        credit = _normalize_amount_value(row.get("credit"))
        balance = _normalize_amount_value(row.get("balance"))
        row_type = str(row.get("row_type") or "transaction").strip().lower() or "transaction"
        if not normalized_date and _should_carry_forward_structured_row_date(
            last_date=last_date,
            description=description,
            debit=debit,
            credit=credit,
            balance=balance,
            row_type=row_type,
        ):
            normalized_date = last_date
        if normalized_date:
            last_date = normalized_date
        rownumber_value = _infer_row_number_from_row(row)
        rows.append(
            {
                "row_id": row_id,
                "rownumber": _normalize_row_number_value(rownumber_value),
                "row_number": str(_normalize_row_number_value(rownumber_value) or ""),
                "date": normalized_date,
                "description": description,
                "debit": debit,
                "credit": credit,
                "balance": balance,
                "row_type": row_type,
            }
        )
        bounds.append(
            {
                "row_id": row_id,
                "x1": max(0.0, min(1.0, x1 / max_w)),
                "y1": max(0.0, min(1.0, y1 / max_h)),
                "x2": max(0.0, min(1.0, x2 / max_w)),
                "y2": max(0.0, min(1.0, y2 / max_h)),
            }
        )
    return rows, bounds


def _normalize_row_number_value(value) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    text = str(value).strip()
    if not text:
        return None
    digits = "".join(ch for ch in text if ch.isdigit())
    if not digits:
        return None
    try:
        return int(digits)
    except Exception:
        return None


def _infer_row_number_from_row(row: Dict) -> int | None:
    direct = _normalize_row_number_value(row.get("rownumber"))
    if direct is not None:
        return direct
    direct = _normalize_row_number_value(row.get("row_number"))
    if direct is not None:
        return direct
    return None


def _should_carry_forward_structured_row_date(
    *,
    last_date: str,
    description: str,
    debit,
    credit,
    balance,
    row_type: str,
) -> bool:
    if not last_date:
        return False
    if row_type not in {"transaction", "balance_only", "opening_balance", "closing_balance"}:
        return False
    if balance is None:
        return False
    if debit is None and credit is None:
        return False
    if not str(description or "").strip():
        return False
    return True


def _last_row_date(rows: List[Dict]) -> str:
    for row in reversed(rows):
        value = str(row.get("date") or "").strip()
        if value:
            return value
    return ""


def _last_row_balance(rows: List[Dict]):
    for row in reversed(rows):
        value = _amount_to_decimal(row.get("balance"))
        if value is not None:
            return value
    return None


def _amounts_match(left: Decimal | None, right: Decimal | None, tolerance: Decimal = Decimal("0.01")) -> bool:
    if left is None or right is None:
        return False
    return abs(left - right) <= tolerance


def _flow_description_bias(description: str | None) -> str | None:
    lowered = str(description or "").strip().lower()
    if not lowered:
        return None

    debit_markers = (
        "withdrawal",
        "withdraw",
        "service fee",
        "fee",
        "bills payment",
        "bill payment",
        "instapay-send",
        "instapay send",
        "payment",
        "send",
    )
    credit_markers = (
        "reversal",
        "refund",
        "deposit",
        "interest",
        "incoming",
        "received",
        "transfer in",
        "cash in",
    )

    has_debit = any(marker in lowered for marker in debit_markers)
    has_credit = any(marker in lowered for marker in credit_markers)
    if has_debit and not has_credit:
        return "debit"
    if has_credit and not has_debit:
        return "credit"
    return None


def _infer_balance_flow_direction(page_rows: List[Dict], previous_balance_hint=None) -> str | None:
    prev_balance = _amount_to_decimal(previous_balance_hint)
    ascending_hits = 0
    descending_hits = 0

    for row in page_rows:
        curr_balance = _amount_to_decimal(row.get("balance"))
        debit = _normalize_flow_amount_decimal(row.get("debit"))
        credit = _normalize_flow_amount_decimal(row.get("credit"))

        if prev_balance is not None and curr_balance is not None:
            if debit is not None and credit is None:
                if _amounts_match(prev_balance - debit, curr_balance):
                    ascending_hits += 1
                if _amounts_match(prev_balance + debit, curr_balance):
                    descending_hits += 1
            elif credit is not None and debit is None:
                if _amounts_match(prev_balance + credit, curr_balance):
                    ascending_hits += 1
                if _amounts_match(prev_balance - credit, curr_balance):
                    descending_hits += 1

        if curr_balance is not None:
            prev_balance = curr_balance

    if ascending_hits > descending_hits:
        return "ascending"
    if descending_hits > ascending_hits:
        return "descending"
    return None


def _expected_balance_for_flow(prev_balance: Decimal, debit: Decimal | None, credit: Decimal | None, flow_direction: str) -> Decimal:
    debit_value = debit or Decimal("0")
    credit_value = credit or Decimal("0")
    if flow_direction == "descending":
        return prev_balance + debit_value - credit_value
    return prev_balance - debit_value + credit_value


def _normalize_flow_amount_decimal(value):
    amount = _amount_to_decimal(value)
    if amount is None:
        return None
    if abs(amount) <= FLOW_AMOUNT_TOLERANCE:
        return None
    return amount


def _normalize_flow_amount_value(value):
    amount = _normalize_amount_value(value)
    if amount is None:
        return None
    if abs(amount) <= float(FLOW_AMOUNT_TOLERANCE):
        return None
    return amount


def _sanitize_row_flow_amounts(
    row_copy: Dict,
    *,
    prev_balance: Decimal | None,
    curr_balance: Decimal | None,
    flow_direction: str | None,
) -> tuple[Decimal | None, Decimal | None]:
    debit = _normalize_flow_amount_decimal(row_copy.get("debit"))
    credit = _normalize_flow_amount_decimal(row_copy.get("credit"))
    if debit is None:
        row_copy["debit"] = None
    if credit is None:
        row_copy["credit"] = None
    if debit is None or credit is None:
        return debit, credit

    if prev_balance is not None and curr_balance is not None and flow_direction:
        debit_matches = _amounts_match(
            _expected_balance_for_flow(prev_balance, debit, None, flow_direction),
            curr_balance,
        )
        credit_matches = _amounts_match(
            _expected_balance_for_flow(prev_balance, None, credit, flow_direction),
            curr_balance,
        )
        if debit_matches and not credit_matches:
            row_copy["credit"] = None
            return debit, None
        if credit_matches and not debit_matches:
            row_copy["debit"] = None
            return None, credit

    if prev_balance is not None and curr_balance is not None:
        delta = abs(curr_balance - prev_balance)
        debit_matches_delta = _amounts_match(delta, abs(debit))
        credit_matches_delta = _amounts_match(delta, abs(credit))
        if debit_matches_delta and not credit_matches_delta:
            row_copy["credit"] = None
            return debit, None
        if credit_matches_delta and not debit_matches_delta:
            row_copy["debit"] = None
            return None, credit

    description_bias = _flow_description_bias(row_copy.get("description"))
    if description_bias == "debit":
        row_copy["credit"] = None
        return debit, None
    if description_bias == "credit":
        row_copy["debit"] = None
        return None, credit

    # When the parser cannot justify one side, emit neither rather than
    # preserving an impossible dual-sided transaction row.
    row_copy["debit"] = None
    row_copy["credit"] = None
    return None, None


def _repair_page_flow_columns(page_rows: List[Dict], previous_balance_hint=None) -> List[Dict]:
    if not page_rows:
        return page_rows

    fixed_rows: List[Dict] = []
    prev_balance = _amount_to_decimal(previous_balance_hint)
    flow_direction = _infer_balance_flow_direction(page_rows, previous_balance_hint=previous_balance_hint)
    for row in page_rows:
        row_copy = dict(row)
        curr_balance = _amount_to_decimal(row_copy.get("balance"))
        debit, credit = _sanitize_row_flow_amounts(
            row_copy,
            prev_balance=prev_balance,
            curr_balance=curr_balance,
            flow_direction=flow_direction,
        )
        description_bias = _flow_description_bias(row_copy.get("description"))

        if prev_balance is not None and curr_balance is not None and flow_direction:
            if debit is not None and credit is None:
                expected_current = _expected_balance_for_flow(prev_balance, debit, None, flow_direction)
                swapped_flow = "descending" if flow_direction == "ascending" else "ascending"
                expected_swapped = _expected_balance_for_flow(prev_balance, debit, None, swapped_flow)
                if (
                    description_bias != "debit"
                    and not _amounts_match(expected_current, curr_balance)
                    and _amounts_match(expected_swapped, curr_balance)
                ):
                    row_copy["credit"] = row_copy.get("debit")
                    row_copy["debit"] = None
                    credit = debit
                    debit = None
            elif credit is not None and debit is None:
                expected_current = _expected_balance_for_flow(prev_balance, None, credit, flow_direction)
                swapped_flow = "descending" if flow_direction == "ascending" else "ascending"
                expected_swapped = _expected_balance_for_flow(prev_balance, None, credit, swapped_flow)
                if (
                    description_bias != "credit"
                    and not _amounts_match(expected_current, curr_balance)
                    and _amounts_match(expected_swapped, curr_balance)
                ):
                    row_copy["debit"] = row_copy.get("credit")
                    row_copy["credit"] = None
                    debit = credit
                    credit = None

        fixed_rows.append(row_copy)
        if curr_balance is not None:
            prev_balance = curr_balance

    return fixed_rows


def _amount_to_decimal(value):
    if value is None:
        return None
    text = str(value).strip().replace(",", "")
    if not text:
        return None
    try:
        return Decimal(text)
    except (InvalidOperation, ValueError):
        return None


def _normalize_amount_value(value):
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text:
        return None
    cleaned = "".join(ch for ch in text if ch.isdigit() or ch in ".-")
    if cleaned in {"", "-", ".", "-."}:
        return None
    try:
        return float(cleaned)
    except Exception:
        return None


def _normalize_structured_row_date(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""

    iso_value = normalize_date(raw, ["mdy", "dmy", "ymd"])
    if iso_value:
        try:
            parsed = dt.datetime.strptime(iso_value, "%Y-%m-%d").date()
            return parsed.strftime("%m/%d/%Y")
        except Exception:
            pass

    for fmt in ("%m/%d/%Y", "%d/%m/%Y", "%m-%d-%Y", "%d-%m-%Y", "%Y-%m-%d"):
        try:
            parsed = dt.datetime.strptime(raw, fmt).date()
            parsed = _coerce_statement_century(parsed)
            return parsed.strftime("%m/%d/%Y")
        except Exception:
            continue
    return raw


def _coerce_statement_century(value: dt.date) -> dt.date:
    year = int(value.year)
    now_limit = dt.date.today().year + 1
    if 1900 <= year < 2000 and (year + 100) <= now_limit:
        try:
            return value.replace(year=year + 100)
        except ValueError:
            return value
    return value


def _classify_row_type(row: Dict, profile) -> str | None:
    if not OCR_ROW_FILTER_LENIENT:
        return "transaction" if is_transaction_row(row, profile) else None

    date = str(row.get("date") or "").strip()
    description = str(row.get("description") or "").strip()
    lower_desc = description.lower()
    debit = str(row.get("debit") or "").strip()
    credit = str(row.get("credit") or "").strip()
    balance = str(row.get("balance") or "").strip()
    has_flow = bool(debit or credit)
    has_balance = bool(balance)

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
            return None

    if not date:
        return None

    if description and is_non_transaction_balance_line(description):
        return None
    if description and ("closing balance" in lower_desc or "ending balance" in lower_desc):
        return "closing_balance" if has_balance else None

    if has_flow:
        return "transaction"
    if has_balance:
        return "balance_only"
    return None


def _ocr_items_to_words(ocr_items: List[Dict]) -> List[Dict]:
    words = []
    for item in ocr_items:
        bbox = item.get("bbox") or []
        text = (item.get("text") or "").strip()
        if len(bbox) != 4 or not text:
            continue
        xs = [pt[0] for pt in bbox]
        ys = [pt[1] for pt in bbox]
        words.append(
            {
                "text": text,
                "x1": float(min(xs)),
                "y1": float(min(ys)),
                "x2": float(max(xs)),
                "y2": float(max(ys)),
            }
        )
    return words


def _render_pdf_pages(input_pdf: Path, pages_dir: Path, dpi: int) -> List[str]:
    pages_dir.mkdir(parents=True, exist_ok=True)
    total_pages = 0
    try:
        info = pdfinfo_from_path(str(input_pdf))
        total_pages = int(info.get("Pages") or 0)
    except Exception:
        total_pages = 0

    if total_pages <= 0:
        pages = convert_from_path(str(input_pdf), dpi=dpi, fmt="png")
        files = []
        for idx, page in enumerate(pages, start=1):
            name = f"page_{idx:03}.png"
            _save_preview_page(page, pages_dir / name)
            files.append(name)
        return files

    files = []
    for idx in range(1, total_pages + 1):
        page_list = convert_from_path(
            str(input_pdf),
            dpi=dpi,
            fmt="png",
            first_page=idx,
            last_page=idx,
        )
        if not page_list:
            continue
        name = f"page_{idx:03}.png"
        _save_preview_page(page_list[0], pages_dir / name)
        files.append(name)
    return files


def _save_preview_page(page: Image.Image, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    w, h = page.size
    pixels = max(1, w * h)
    if pixels > PREVIEW_MAX_PIXELS:
        scale = math.sqrt(PREVIEW_MAX_PIXELS / float(pixels))
        page = page.resize(
            (max(1, int(w * scale)), max(1, int(h * scale))),
            resample=Image.Resampling.BILINEAR,
        )
    page.save(path, format="PNG")


def _image_size(path: Path) -> tuple[int, int]:
    img = cv2.imread(str(path))
    if img is None:
        return 1, 1
    h, w = img.shape[:2]
    return h, w


def _write_json_atomic(path: Path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with open(tmp_path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)
    os.replace(tmp_path, path)
