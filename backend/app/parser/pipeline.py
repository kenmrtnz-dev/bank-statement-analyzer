"""Parser pipeline orchestration."""

from decimal import Decimal
import logging
from pathlib import Path

from app.parser import bdo_parser, simple_column_parser, sterling_bank_of_asia_parser
from app.parser import bank_detector, template_loader
from app.parser.extractors import google_vision, pdf_text_extractor
from app.parser.normalize import normalizer
from app.parser.structure import column_detector, row_detector, table_builder
from app.parser.transactions import description_merger, transaction_extractor
from app.parser.validation import balance_validator

logger = logging.getLogger(__name__)
SUPPORTED_PARSER_PROFILES = {"sterling_bank_of_asia", "bdo", "generic"}


def _build_summary(transactions: list[dict]) -> dict:
    """Build a simple summary from normalized transactions."""
    total_debit = sum((item.get("debit") or Decimal("0")) for item in transactions)
    total_credit = sum((item.get("credit") or Decimal("0")) for item in transactions)
    ending_balance = transactions[-1]["balance"] if transactions else None
    return {
        "total_rows": len(transactions),
        "total_debit": total_debit,
        "total_credit": total_credit,
        "ending_balance": ending_balance,
    }


def _normalize_parser_profile(parser_profile: str | None) -> str:
    raw = str(parser_profile or "").strip().lower()
    if raw in SUPPORTED_PARSER_PROFILES:
        return raw
    return "auto"


def _resolve_selected_parser(requested_parser: str | None, detected_bank: str) -> str:
    normalized_requested = _normalize_parser_profile(requested_parser)
    if normalized_requested in SUPPORTED_PARSER_PROFILES:
        return normalized_requested
    normalized_detected = str(detected_bank or "").strip().lower()
    if normalized_detected in SUPPORTED_PARSER_PROFILES:
        return normalized_detected
    return "generic"


def _extract_text_from_google_vision_raw(raw_payload: dict | None) -> str:
    if not isinstance(raw_payload, dict):
        return ""
    chunks: list[str] = []
    for page in raw_payload.get("pages") or []:
        if not isinstance(page, dict):
            continue
        response = page.get("response") if isinstance(page.get("response"), dict) else {}
        annotations = response.get("textAnnotations") or response.get("text_annotations") or []
        if isinstance(annotations, list) and annotations:
            first = annotations[0]
            if isinstance(first, dict):
                top_text = str(first.get("description") or "").strip()
                if top_text:
                    chunks.append(top_text)
                    continue
        full = response.get("fullTextAnnotation") or response.get("full_text_annotation") or {}
        if isinstance(full, dict):
            text = str(full.get("text") or "").strip()
            if text:
                chunks.append(text)
    return "\n".join(chunks).strip()


def _infer_bank_from_google_vision_raw(raw_payload: dict | None) -> str:
    text = _extract_text_from_google_vision_raw(raw_payload).lower()
    if not text:
        return "generic"

    bdo_signals = [
        "bdo",
        "check no",
        "balance forwarded",
        "account number",
    ]
    sterling_signals = [
        "sterling bank of asia",
        "sba",
        "ledger",
    ]
    bdo_score = sum(1 for marker in bdo_signals if marker in text)
    sterling_score = sum(1 for marker in sterling_signals if marker in text)
    if bdo_score >= max(1, sterling_score):
        return "bdo"
    if sterling_score > bdo_score:
        return "sterling_bank_of_asia"
    return "generic"


def _parse_google_vision_transactions(raw_payload: dict | None, parser_profile: str) -> list[dict]:
    if not isinstance(raw_payload, dict):
        return []
    if parser_profile == "sterling_bank_of_asia":
        return sterling_bank_of_asia_parser.parse_transactions_from_ocr_raw(raw_payload)
    if parser_profile == "bdo":
        return bdo_parser.parse_transactions_from_ocr_raw(raw_payload)
    return simple_column_parser.parse_transactions_from_ocr_raw(raw_payload)


def _score_transaction_rows(rows: list[dict] | None) -> tuple[int, int, int]:
    if not isinstance(rows, list) or not rows:
        return (0, 0, 0)
    valid = 0
    dated = 0
    balanced = 0
    for row in rows:
        if not isinstance(row, dict):
            continue
        has_date = bool(str(row.get("date") or "").strip())
        has_balance = row.get("balance") is not None and str(row.get("balance") or "").strip() != ""
        has_amount = (
            row.get("debit") is not None and str(row.get("debit") or "").strip() != ""
        ) or (
            row.get("credit") is not None and str(row.get("credit") or "").strip() != ""
        )
        if has_date:
            dated += 1
        if has_balance:
            balanced += 1
        if has_date and (has_balance or has_amount):
            valid += 1
    return (valid, dated, balanced)


def _auto_select_google_vision_parser(raw_payload: dict | None, detected_bank: str) -> tuple[str, list[dict]]:
    hint = str(detected_bank or "").strip().lower()
    if hint in SUPPORTED_PARSER_PROFILES and hint != "generic":
        hinted_rows = _parse_google_vision_transactions(raw_payload, hint)
        if _score_transaction_rows(hinted_rows)[0] > 0:
            return hint, hinted_rows

    candidates = ("bdo", "sterling_bank_of_asia", "generic")
    best_parser = "generic"
    best_rows: list[dict] = []
    best_score = (0, 0, 0)
    for parser_name in candidates:
        candidate_rows = _parse_google_vision_transactions(raw_payload, parser_name)
        score = _score_transaction_rows(candidate_rows)
        if score > best_score:
            best_score = score
            best_parser = parser_name
            best_rows = candidate_rows

    if best_score[0] <= 0:
        generic_rows = _parse_google_vision_transactions(raw_payload, "generic")
        return "generic", generic_rows
    return best_parser, best_rows


def parse_google_vision_raw_payload(
    raw_payload: dict | None,
    parser_profile: str | None = None,
    detected_bank: str = "generic",
) -> tuple[list[dict], str]:
    normalized_requested = _normalize_parser_profile(parser_profile)
    selected_bank = str(detected_bank or "").strip().lower()
    if normalized_requested == "auto":
        inferred_bank = selected_bank
        if inferred_bank in {"", "generic", "unknown"}:
            inferred_bank = _infer_bank_from_google_vision_raw(raw_payload)
        selected_parser, rows = _auto_select_google_vision_parser(raw_payload, inferred_bank)
        return rows, selected_parser

    selected_parser = _resolve_selected_parser(normalized_requested, selected_bank)
    rows = _parse_google_vision_transactions(raw_payload, selected_parser)
    return rows, selected_parser


def process_document(file_path: str | Path, ocr_engine: str = "auto", parser_profile: str | None = None) -> dict:
    """Process a single statement document and return parsed output."""
    path = Path(file_path)
    logger.info("Starting parse pipeline for %s", path)
    ocr_source = "pdftotext"
    ocr_raw = None

    if ocr_engine == "pdftotext":
        logger.info("Forced pdftotext mode for %s", path.name)
        text = pdf_text_extractor.extract_text(path)
    elif ocr_engine == "google_vision":
        logger.info("Forced Google Vision mode for %s", path.name)
        ocr_source = "google_vision"
        ocr_result = google_vision.extract_text_with_details(path)
        text = ocr_result.get("text", "")
        ocr_raw = ocr_result.get("raw")
        if not str(text or "").strip():
            details = ""
            if isinstance(ocr_raw, dict):
                details = str(ocr_raw.get("error") or "").strip()
            raise RuntimeError(f"google_vision_ocr_failed:{details or 'empty_text'}")
    else:
        # Auto mode decides between fast embedded extraction and OCR.
        if pdf_text_extractor.has_embedded_text(path):
            logger.info("Detected embedded PDF text for %s", path.name)
            text = pdf_text_extractor.extract_text(path)
        else:
            logger.info("Detected scanned PDF for %s", path.name)
            ocr_source = "google_vision"
            ocr_result = google_vision.extract_text_with_details(path)
            text = ocr_result.get("text", "")
            ocr_raw = ocr_result.get("raw")

    bank = bank_detector.detect_bank(text)
    logger.info("Detected bank template: %s", bank)
    selected_parser = _resolve_selected_parser(parser_profile, bank)
    parser_strategy = "template_pipeline"

    parsed_from_ocr_raw = []
    parsed_from_pdftotext = []
    if ocr_source == "google_vision":
        parsed_from_ocr_raw, selected_parser = parse_google_vision_raw_payload(
            ocr_raw,
            parser_profile=parser_profile,
            detected_bank=bank,
        )
        if parsed_from_ocr_raw:
            parser_strategy = "google_vision_raw_parser"
    elif ocr_source == "pdftotext" and selected_parser == "generic":
        parsed_from_pdftotext = simple_column_parser.parse_transactions_from_pdftotext_text(text)
        if parsed_from_pdftotext:
            parser_strategy = "pdftotext_layered_parser"

    # Each stage stays isolated so bank-specific logic can be improved later
    # without collapsing parsing into one large opaque function.
    template = template_loader.load_template(bank)
    if parsed_from_ocr_raw:
        extracted_rows = parsed_from_ocr_raw
    elif parsed_from_pdftotext:
        extracted_rows = parsed_from_pdftotext
    else:
        rows = row_detector.detect_rows(text)
        column_map = column_detector.detect_columns(template)
        raw_table = table_builder.build_table(rows, column_map)
        merged_rows = description_merger.merge_descriptions(raw_table)
        extracted_rows = transaction_extractor.extract_transactions(merged_rows)
    normalized_rows = normalizer.normalize_transactions(extracted_rows)
    validation = balance_validator.validate_balances(normalized_rows)
    summary = _build_summary(normalized_rows)

    logger.info("Completed parse pipeline for %s", path.name)
    return {
        "bank": bank,
        "ocr_engine_requested": ocr_engine,
        "ocr_source": ocr_source,
        "ocr_raw": ocr_raw,
        "parser_profile_requested": _normalize_parser_profile(parser_profile),
        "parser_profile_used": selected_parser,
        "parser_strategy": parser_strategy,
        "transactions": normalized_rows,
        "summary": summary,
        "validation": validation,
    }
