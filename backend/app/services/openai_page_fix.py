from __future__ import annotations

import base64
import json
import os
from pathlib import Path
from typing import Any, Dict, List

import httpx


class OpenAIPageFixError(RuntimeError):
    """Raised when the page-fix request fails or returns invalid data."""


class OpenAIPageFixNotConfiguredError(OpenAIPageFixError):
    """Raised when page-fix is disabled or missing credentials."""


def _env_bool(name: str, default: bool) -> bool:
    raw = str(os.getenv(name, str(default))).strip().lower()
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    return default


def is_openai_page_fix_available() -> bool:
    if not _env_bool("OPENAI_PAGE_FIX_ENABLED", True):
        return False
    return bool(str(os.getenv("OPENAI_API_KEY", "")).strip())


def _image_media_type(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix in {".jpg", ".jpeg"}:
        return "image/jpeg"
    if suffix == ".webp":
        return "image/webp"
    return "image/png"


def _build_image_data_url(image_path: Path) -> str:
    payload = base64.b64encode(image_path.read_bytes()).decode("ascii")
    return f"data:{_image_media_type(image_path)};base64,{payload}"


def _response_output_text(payload: Dict[str, Any]) -> str:
    text = payload.get("output_text")
    if isinstance(text, str) and text.strip():
        return text.strip()

    output = payload.get("output")
    if isinstance(output, list):
        for item in output:
            if not isinstance(item, dict):
                continue
            content = item.get("content")
            if not isinstance(content, list):
                continue
            for block in content:
                if not isinstance(block, dict):
                    continue
                text_value = block.get("text")
                if isinstance(text_value, str) and text_value.strip():
                    return text_value.strip()
                if isinstance(block.get("json"), dict):
                    return json.dumps(block["json"])
    raise OpenAIPageFixError("page_ai_fix_invalid_response")


def _normalize_summary(value: Any) -> Dict[str, Any]:
    payload = value if isinstance(value, dict) else {}
    issues = payload.get("issues_found")
    return {
        "changed": bool(payload.get("changed", False)),
        "issues_found": [str(item).strip() for item in issues] if isinstance(issues, list) else [],
        "rationale": str(payload.get("rationale") or "").strip(),
    }


def repair_page_rows_with_openai(
    *,
    page_name: str,
    parsed_rows: List[Dict[str, Any]],
    raw_payload: Any,
    raw_source: str,
    image_path: str | Path,
) -> Dict[str, Any]:
    if not _env_bool("OPENAI_PAGE_FIX_ENABLED", True):
        raise OpenAIPageFixNotConfiguredError("page_ai_fix_disabled")

    api_key = str(os.getenv("OPENAI_API_KEY", "")).strip()
    if not api_key:
        raise OpenAIPageFixNotConfiguredError("openai_api_key_missing")

    model = str(os.getenv("OPENAI_PAGE_FIX_MODEL", "gpt-4.1-mini")).strip() or "gpt-4.1-mini"
    timeout_seconds = max(5, int(str(os.getenv("OPENAI_PAGE_FIX_TIMEOUT_SECONDS", "45")).strip() or "45"))
    base_url = str(os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1")).strip() or "https://api.openai.com/v1"
    image_ref = _build_image_data_url(Path(image_path))

    schema = {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "rows": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "row_id": {"type": "string"},
                        "rownumber": {"type": ["integer", "null"]},
                        "row_number": {"type": "string"},
                        "date": {"type": "string"},
                        "description": {"type": "string"},
                        "debit": {"type": ["number", "string", "null"]},
                        "credit": {"type": ["number", "string", "null"]},
                        "balance": {"type": ["number", "string", "null"]},
                        "row_type": {"type": "string"},
                    },
                    "required": ["row_id", "rownumber", "row_number", "date", "description", "debit", "credit", "balance", "row_type"],
                },
            },
            "summary": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "changed": {"type": "boolean"},
                    "issues_found": {"type": "array", "items": {"type": "string"}},
                    "rationale": {"type": "string"},
                },
                "required": ["changed", "issues_found", "rationale"],
            },
        },
        "required": ["rows", "summary"],
    }

    context_payload = {
        "page": page_name,
        "raw_source": raw_source,
        "repair_goal": (
            "Cross-check three inputs for the same page: "
            "1) the source image, 2) the current parsed rows, and 3) the canonical saved raw result. "
            "The parsed rows may be incorrect and should be treated as a draft to verify and repair."
        ),
        "rules": [
            "Repair only the parsed rows for this single page.",
            "Treat the image and canonical raw result as the primary evidence.",
            "Treat the current parsed rows as a possibly incorrect draft.",
            "Fix OCR typos, wrong dates, wrong amounts, wrong row numbers, missing values, and obvious split/merge mistakes when supported by the evidence.",
            "Preserve and correct row_type. Valid row types are transaction, balance_only, opening_balance, and closing_balance.",
            "Beginning balance, opening balance, balance forwarded, brought forward, carried forward, and similar top balance lines should not be treated as normal transactions.",
            "Do not invent rows or values unsupported by the image or canonical raw result.",
            "Preserve row order unless the evidence clearly shows a row should be moved or merged.",
            "If you keep a field unchanged, it should be because the evidence supports it, not because it was already present in the parsed rows.",
            "If no improvement can be made after checking all three inputs, return the current rows unchanged and summary.changed=false.",
            "Return only JSON matching the provided schema.",
        ],
        "canonical_raw_result": raw_payload,
        "current_parsed_rows": parsed_rows,
    }

    request_payload = {
        "model": model,
        "store": False,
        "input": [
            {
                "role": "system",
                "content": [
                    {
                        "type": "input_text",
                        "text": (
                            "You repair parsed bank-statement rows for one page by auditing the current parsed rows "
                            "against the source image and the canonical saved raw result. "
                            "Do not anchor on the current parsed rows when the other evidence disagrees. "
                            "Return only the requested JSON schema."
                        ),
                    }
                ],
            },
            {
                "role": "user",
                "content": [
                    {"type": "input_text", "text": json.dumps(context_payload, ensure_ascii=True)},
                    {"type": "input_image", "image_url": image_ref, "detail": "high"},
                ],
            },
        ],
        "text": {
            "format": {
                "type": "json_schema",
                "name": "page_fix_result",
                "schema": schema,
                "strict": True,
            }
        },
    }

    try:
        with httpx.Client(timeout=timeout_seconds) as client:
            response = client.post(
                f"{base_url.rstrip('/')}/responses",
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
                json=request_payload,
            )
            response.raise_for_status()
            payload = response.json()
    except httpx.HTTPStatusError as exc:
        raise OpenAIPageFixError(f"page_ai_fix_http_error:{exc.response.status_code}") from exc
    except Exception as exc:
        raise OpenAIPageFixError("page_ai_fix_request_failed") from exc

    try:
        parsed = json.loads(_response_output_text(payload))
    except OpenAIPageFixError:
        raise
    except Exception as exc:
        raise OpenAIPageFixError("page_ai_fix_invalid_json") from exc

    rows = parsed.get("rows")
    if not isinstance(rows, list):
        raise OpenAIPageFixError("page_ai_fix_invalid_rows")

    return {
        "rows": rows,
        "summary": _normalize_summary(parsed.get("summary")),
    }


__all__ = [
    "OpenAIPageFixError",
    "OpenAIPageFixNotConfiguredError",
    "is_openai_page_fix_available",
    "repair_page_rows_with_openai",
]
