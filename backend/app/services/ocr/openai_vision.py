from __future__ import annotations

import base64
import hashlib
import io
import json
import os
import re
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Tuple

import httpx
from PIL import Image
from redis import Redis

SYSTEM_PROMPT = (
    "You are a high-accuracy OCR engine. Extract text exactly as written. "
    "Do not summarize. Preserve numbers, dates, spacing, and formatting."
)
USER_PROMPT = "Extract all visible text from this bank statement page. Return plain text only."
STRUCTURED_OCR_SYSTEM_PROMPT = (
    "You are a high-accuracy OCR engine. Return structured OCR tokens with bounding boxes. "
    "Do not summarize. Keep token text exact."
)
STRUCTURED_ROWS_SYSTEM_PROMPT = (
    "You are a bank statement extraction engine. Return structured bank statement rows with tight bounds. "
    "Include transactions and balance lines, but exclude table headers and page furniture. "
    "Return rows in this schema: rownumber, date, description, debit, credit, balance, bounds. "
    "rownumber is the serial number in the first/leftmost column before the date (e.g., 3, 4, 5). "
    "Do NOT use check/reference/document numbers from description as rownumber. "
    "rownumber must be integer when visible, otherwise null. "
    "date must be MM/DD/YYYY when recognizable, otherwise empty string. "
    "debit/credit/balance must be number or null."
)
MAX_IMAGE_BYTES = 2 * 1024 * 1024
STRUCTURED_ROWS_CACHE_VERSION = "v2"


class TruncatedOCRResponse(RuntimeError):
    """Raised when the model output appears truncated."""


@dataclass(frozen=True)
class OCRImageConfig:
    max_dimension: int
    jpeg_quality: int


def _env_int(name: str, default: int) -> int:
    try:
        return int(str(os.getenv(name, default)).strip())
    except Exception:
        return default


def _flatten_message_content(value) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        chunks: List[str] = []
        for item in value:
            if isinstance(item, str):
                chunks.append(item)
                continue
            if isinstance(item, dict):
                text = item.get("text")
                if isinstance(text, str):
                    chunks.append(text)
        return "".join(chunks)
    return str(value or "")


def plain_text_to_ocr_items(raw_text: str, page_width: int, page_height: int) -> List[Dict]:
    text = str(raw_text or "")
    lines = text.splitlines()
    if not lines:
        return []

    step = float(max(page_height, 1)) / float(max(len(lines), 1))
    row_height = max(8.0, step * 0.8)

    items: List[Dict] = []
    item_id = 1
    for row_index, line in enumerate(lines):
        if not line.strip():
            continue
        line_len = max(len(line), 1)
        y1 = min(float(page_height), float(row_index) * step)
        y2 = min(float(page_height), y1 + row_height)

        for match in re.finditer(r"\S+", line):
            token = match.group(0)
            x1 = (float(match.start()) / float(line_len)) * float(max(page_width, 1))
            x2 = (float(match.end()) / float(line_len)) * float(max(page_width, 1))
            if x2 <= x1:
                x2 = min(float(page_width), x1 + 1.0)
            bbox = [
                [float(x1), float(y1)],
                [float(x2), float(y1)],
                [float(x2), float(y2)],
                [float(x1), float(y2)],
            ]
            items.append(
                {
                    "id": item_id,
                    "text": token,
                    "confidence": 1.0,
                    "bbox": bbox,
                }
            )
            item_id += 1
    return items


class OpenAIVisionOCR:
    def __init__(
        self,
        api_key: str,
        model: str,
        timeout_seconds: int,
        cache_dir: Path,
        base_url: str = "https://api.openai.com/v1",
    ):
        self.api_key = api_key
        self.model = model
        self.timeout_seconds = max(5, int(timeout_seconds))
        self.cache_dir = Path(cache_dir)
        self.base_url = base_url.rstrip("/")
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self._last_openai_response: Dict[str, Any] | None = None
        self._rate_redis: Redis | None = None
        self._rate_redis_init_failed = False
        self._rate_limit_per_window = max(1, _env_int("OPENAI_OCR_RPM_LIMIT", 60))
        self._rate_window_seconds = max(1, _env_int("OPENAI_OCR_RATE_WINDOW_SECONDS", 60))
        self._rate_wait_timeout_seconds = max(1, _env_int("OPENAI_OCR_RATE_WAIT_TIMEOUT_SECONDS", 120))
        self._rate_key = str(os.getenv("OPENAI_OCR_RATE_KEY", "openai:ocr:rpm")).strip() or "openai:ocr:rpm"

    @classmethod
    def from_env(cls) -> "OpenAIVisionOCR":
        api_key = str(os.getenv("OPENAI_API_KEY", "")).strip()
        if not api_key:
            raise RuntimeError("openai_api_key_missing")

        model = str(os.getenv("OPENAI_OCR_MODEL", "gpt-4o-mini")).strip() or "gpt-4o-mini"
        timeout_seconds = _env_int("OPENAI_TIMEOUT_SECONDS", 60)
        data_dir = Path(os.getenv("DATA_DIR", "./data"))
        cache_dir = Path(os.getenv("OPENAI_OCR_CACHE_DIR", str(data_dir / "ocr_cache")))
        base_url = str(os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1")).strip()
        if not base_url:
            base_url = "https://api.openai.com/v1"
        return cls(
            api_key=api_key,
            model=model,
            timeout_seconds=timeout_seconds,
            cache_dir=cache_dir,
            base_url=base_url,
        )

    def extract_ocr_items(self, image_path: str | Path) -> List[Dict]:
        path = Path(image_path)
        with Image.open(path) as img:
            page_w, page_h = img.size
            image = img.convert("RGB")

        try:
            return self._extract_structured_ocr_items(image=image, page_width=page_w, page_height=page_h)
        except Exception:
            # Keep a soft fallback for resilience when the model returns invalid JSON.
            text = self.extract_text(path)
            return plain_text_to_ocr_items(text, page_w, page_h)

    def consume_last_openai_response(self) -> Dict[str, Any] | None:
        payload = self._last_openai_response
        self._last_openai_response = None
        return payload

    def extract_structured_rows(
        self,
        image_path: str | Path,
        *,
        rate_limit_heartbeat: Callable[[float], None] | None = None,
    ) -> Dict[str, Any]:
        path = Path(image_path)
        with Image.open(path) as img:
            page_w, page_h = img.size
            image = img.convert("RGB")

        configs = [
            OCRImageConfig(max_dimension=2000, jpeg_quality=78),
            OCRImageConfig(max_dimension=1400, jpeg_quality=65),
        ]
        last_error: Exception | None = None
        for idx, config in enumerate(configs):
            allow_retry = idx < len(configs) - 1
            try:
                rows = self._extract_structured_rows_with_config(
                    image=image,
                    page_width=page_w,
                    page_height=page_h,
                    config=config,
                    allow_retry=allow_retry,
                    rate_limit_heartbeat=rate_limit_heartbeat,
                )
                return {"rows": rows, "page_width": page_w, "page_height": page_h}
            except TruncatedOCRResponse as exc:
                last_error = exc
                continue
            except Exception as exc:
                last_error = exc
                continue

        if last_error:
            raise last_error
        raise RuntimeError("openai_ocr_empty_output")

    def extract_text(self, image_path: str | Path) -> str:
        path = Path(image_path)
        with Image.open(path) as src:
            image = src.convert("RGB")

        try:
            return self._extract_with_config(image, OCRImageConfig(max_dimension=2000, jpeg_quality=78))
        except TruncatedOCRResponse:
            pass

        try:
            return self._extract_with_config(image, OCRImageConfig(max_dimension=1400, jpeg_quality=65))
        except TruncatedOCRResponse:
            pass

        top, bottom = self._split_vertical_halves(image)
        top_text = self._extract_with_config(top, OCRImageConfig(max_dimension=1400, jpeg_quality=65), allow_retry=False)
        bottom_text = self._extract_with_config(
            bottom,
            OCRImageConfig(max_dimension=1400, jpeg_quality=65),
            allow_retry=False,
        )
        merged = "\n".join(part for part in [top_text, bottom_text] if part)
        if not merged.strip():
            raise RuntimeError("openai_ocr_empty_output")
        return merged

    def _extract_with_config(
        self,
        image: Image.Image,
        config: OCRImageConfig,
        *,
        allow_retry: bool = True,
    ) -> str:
        encoded = self._encode_image(image, config)
        cache_key = hashlib.sha256(encoded).hexdigest()
        cached = self._cache_get(cache_key)
        if cached is not None:
            return cached

        try:
            text = self._call_openai(encoded)
        except TruncatedOCRResponse:
            if allow_retry:
                raise
            raise RuntimeError("openai_ocr_truncated_after_retries")

        self._cache_put(cache_key, text)
        return text

    def _extract_structured_ocr_items(self, image: Image.Image, page_width: int, page_height: int) -> List[Dict]:
        configs = [
            OCRImageConfig(max_dimension=2000, jpeg_quality=78),
            OCRImageConfig(max_dimension=1400, jpeg_quality=65),
        ]
        last_error: Exception | None = None

        for idx, config in enumerate(configs):
            allow_retry = idx < len(configs) - 1
            try:
                return self._extract_structured_with_config(
                    image=image,
                    page_width=page_width,
                    page_height=page_height,
                    config=config,
                    allow_retry=allow_retry,
                )
            except TruncatedOCRResponse as exc:
                last_error = exc
                continue
            except Exception as exc:
                last_error = exc
                continue

        top, bottom = self._split_vertical_halves(image)
        top_h = top.size[1]
        top_items = self._extract_structured_with_config(
            image=top,
            page_width=page_width,
            page_height=max(1, top_h),
            config=OCRImageConfig(max_dimension=1400, jpeg_quality=65),
            allow_retry=False,
        )
        bottom_items = self._extract_structured_with_config(
            image=bottom,
            page_width=page_width,
            page_height=max(1, page_height - top_h),
            config=OCRImageConfig(max_dimension=1400, jpeg_quality=65),
            allow_retry=False,
        )

        merged: List[Dict] = []
        item_id = 1
        for item in top_items:
            merged.append({
                "id": item_id,
                "text": item.get("text"),
                "confidence": item.get("confidence", 1.0),
                "bbox": item.get("bbox"),
            })
            item_id += 1

        for item in bottom_items:
            bbox = item.get("bbox") or []
            shifted_bbox = []
            for point in bbox:
                if not isinstance(point, list) or len(point) != 2:
                    continue
                shifted_bbox.append([float(point[0]), float(point[1]) + float(top_h)])
            if len(shifted_bbox) != 4:
                continue
            merged.append({
                "id": item_id,
                "text": item.get("text"),
                "confidence": item.get("confidence", 1.0),
                "bbox": shifted_bbox,
            })
            item_id += 1

        if merged:
            return merged
        if last_error:
            raise last_error
        raise RuntimeError("openai_ocr_empty_output")

    def _extract_structured_with_config(
        self,
        image: Image.Image,
        page_width: int,
        page_height: int,
        config: OCRImageConfig,
        *,
        allow_retry: bool = True,
    ) -> List[Dict]:
        encoded = self._encode_image(image, config)
        cache_hash = hashlib.sha256(encoded).hexdigest()
        cache_key = f"{cache_hash}.items"
        raw_cache_key = f"{cache_hash}.items.raw"
        cached_items = self._cache_get_json(cache_key)
        if isinstance(cached_items, list) and cached_items:
            cached_raw = self._cache_get_json(raw_cache_key)
            if isinstance(cached_raw, dict):
                self._last_openai_response = cached_raw
                return cached_items
            # Old cache entries may only have token items. Refresh once so raw response can be captured.
            try:
                refreshed_items = self._call_openai_structured(encoded, page_width=page_width, page_height=page_height)
                self._cache_put_json(cache_key, refreshed_items)
                if isinstance(self._last_openai_response, dict):
                    self._cache_put_json(raw_cache_key, self._last_openai_response)
                return refreshed_items
            except Exception:
                return cached_items

        try:
            items = self._call_openai_structured(encoded, page_width=page_width, page_height=page_height)
        except TruncatedOCRResponse:
            if allow_retry:
                raise
            raise RuntimeError("openai_ocr_truncated_after_retries")

        self._cache_put_json(cache_key, items)
        if isinstance(self._last_openai_response, dict):
            self._cache_put_json(raw_cache_key, self._last_openai_response)
        return items

    def _extract_structured_rows_with_config(
        self,
        image: Image.Image,
        page_width: int,
        page_height: int,
        config: OCRImageConfig,
        *,
        allow_retry: bool = True,
        rate_limit_heartbeat: Callable[[float], None] | None = None,
    ) -> List[Dict]:
        encoded = self._encode_image(image, config)
        cache_hash = hashlib.sha256(encoded).hexdigest()
        rows_cache_key = f"{cache_hash}.rows.{STRUCTURED_ROWS_CACHE_VERSION}"
        raw_cache_key = f"{cache_hash}.rows.raw.{STRUCTURED_ROWS_CACHE_VERSION}"

        cached_rows = self._cache_get_json(rows_cache_key)
        if isinstance(cached_rows, list) and cached_rows:
            cached_raw = self._cache_get_json(raw_cache_key)
            if isinstance(cached_raw, dict):
                self._last_openai_response = cached_raw
            return cached_rows

        try:
            rows = self._call_openai_structured_rows(
                encoded,
                page_width=page_width,
                page_height=page_height,
                rate_limit_heartbeat=rate_limit_heartbeat,
            )
        except TruncatedOCRResponse:
            if allow_retry:
                raise
            raise RuntimeError("openai_ocr_truncated_after_retries")

        self._cache_put_json(rows_cache_key, rows)
        if isinstance(self._last_openai_response, dict):
            self._cache_put_json(raw_cache_key, self._last_openai_response)
        return rows

    def _call_openai(self, image_bytes: bytes) -> str:
        data_url = "data:image/jpeg;base64," + base64.b64encode(image_bytes).decode("ascii")
        payload = {
            "model": self.model,
            "temperature": 0,
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": USER_PROMPT},
                        {"type": "image_url", "image_url": {"url": data_url}},
                    ],
                },
            ],
            "max_tokens": _env_int("OPENAI_OCR_MAX_TOKENS", 4096),
        }
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

        self._wait_for_rate_limit()
        with httpx.Client(timeout=self.timeout_seconds) as client:
            response = client.post(f"{self.base_url}/chat/completions", headers=headers, json=payload)

        if response.status_code >= 400:
            detail = self._extract_error_detail(response)
            lowered = detail.lower()
            if "context_length_exceeded" in lowered or "maximum context length" in lowered:
                raise TruncatedOCRResponse(detail)
            raise RuntimeError(f"openai_ocr_http_error:{response.status_code}:{detail}")

        body = response.json()
        self._last_openai_response = {
            "mode": "plain_text",
            "model": self.model,
            "response": body,
        }
        choices = body.get("choices") or []
        if not choices:
            raise RuntimeError("openai_ocr_empty_response")

        choice = choices[0] or {}
        finish_reason = str(choice.get("finish_reason") or "").strip().lower()
        message = choice.get("message") or {}
        content = _flatten_message_content(message.get("content"))
        if finish_reason == "length":
            raise TruncatedOCRResponse("openai_ocr_output_truncated")
        if not content.strip():
            raise RuntimeError("openai_ocr_empty_output")
        return content

    def _call_openai_structured(self, image_bytes: bytes, page_width: int, page_height: int) -> List[Dict]:
        data_url = "data:image/jpeg;base64," + base64.b64encode(image_bytes).decode("ascii")
        user_prompt = (
            "Extract OCR tokens with tight bounding boxes from this bank statement page. "
            f"Coordinates must be in absolute pixels for image width={int(page_width)}, height={int(page_height)}. "
            "Return one token per word-like unit."
        )
        payload = {
            "model": self.model,
            "temperature": 0,
            "messages": [
                {"role": "system", "content": STRUCTURED_OCR_SYSTEM_PROMPT},
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": user_prompt},
                        {"type": "image_url", "image_url": {"url": data_url}},
                    ],
                },
            ],
            "response_format": {
                "type": "json_schema",
                "json_schema": {
                    "name": "ocr_tokens",
                    "strict": True,
                    "schema": {
                        "type": "object",
                        "properties": {
                            "tokens": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "text": {"type": "string"},
                                        "x1": {"type": "number"},
                                        "y1": {"type": "number"},
                                        "x2": {"type": "number"},
                                        "y2": {"type": "number"},
                                        "confidence": {"type": "number"},
                                    },
                                    "required": ["text", "x1", "y1", "x2", "y2", "confidence"],
                                    "additionalProperties": False,
                                },
                            }
                        },
                        "required": ["tokens"],
                        "additionalProperties": False,
                    },
                },
            },
            "max_tokens": _env_int("OPENAI_OCR_MAX_TOKENS", 4096),
        }
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

        self._wait_for_rate_limit()
        with httpx.Client(timeout=self.timeout_seconds) as client:
            response = client.post(f"{self.base_url}/chat/completions", headers=headers, json=payload)

        if response.status_code >= 400:
            detail = self._extract_error_detail(response)
            lowered = detail.lower()
            if "context_length_exceeded" in lowered or "maximum context length" in lowered:
                raise TruncatedOCRResponse(detail)
            raise RuntimeError(f"openai_ocr_http_error:{response.status_code}:{detail}")

        body = response.json()
        self._last_openai_response = {
            "mode": "structured_tokens",
            "model": self.model,
            "page_width": int(page_width),
            "page_height": int(page_height),
            "response": body,
        }
        choices = body.get("choices") or []
        if not choices:
            raise RuntimeError("openai_ocr_empty_response")

        choice = choices[0] or {}
        finish_reason = str(choice.get("finish_reason") or "").strip().lower()
        message = choice.get("message") or {}
        content = _flatten_message_content(message.get("content"))
        if finish_reason == "length":
            raise TruncatedOCRResponse("openai_ocr_output_truncated")
        if not content.strip():
            raise RuntimeError("openai_ocr_empty_output")

        parsed = self._safe_parse_json(content)
        if not isinstance(parsed, dict):
            raise RuntimeError("openai_ocr_invalid_json")
        raw_tokens = parsed.get("tokens")
        if not isinstance(raw_tokens, list):
            raise RuntimeError("openai_ocr_invalid_tokens")
        return self._normalize_structured_tokens(raw_tokens, page_width=page_width, page_height=page_height)

    def _call_openai_structured_rows(
        self,
        image_bytes: bytes,
        page_width: int,
        page_height: int,
        rate_limit_heartbeat: Callable[[float], None] | None = None,
    ) -> List[Dict]:
        data_url = "data:image/jpeg;base64," + base64.b64encode(image_bytes).decode("ascii")
        user_prompt = (
            "Extract bank statement rows. Return JSON only. "
            "Each row must contain rownumber, date, description, debit, credit, balance, and tight bounding box. "
            f"Coordinates are absolute pixels for image width={int(page_width)}, height={int(page_height)}. "
            "rownumber is the serial number in the first column before date; "
            "do not use check/document/reference numbers from description. "
            "date must be MM/DD/YYYY when recognized, otherwise empty string. "
            "rownumber is the transaction/passbook number as integer, otherwise null. "
            "debit/credit/balance must be number or null."
        )
        payload = {
            "model": self.model,
            "temperature": 0,
            "messages": [
                {"role": "system", "content": STRUCTURED_ROWS_SYSTEM_PROMPT},
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": user_prompt},
                        {"type": "image_url", "image_url": {"url": data_url}},
                    ],
                },
            ],
            "response_format": {
                "type": "json_schema",
                "json_schema": {
                    "name": "bank_statement_rows",
                    "strict": True,
                    "schema": {
                        "type": "object",
                        "properties": {
                            "rows": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "rownumber": {"type": ["integer", "null"]},
                                        "date": {"type": "string"},
                                        "description": {"type": "string"},
                                        "debit": {"type": ["number", "null"]},
                                        "credit": {"type": ["number", "null"]},
                                        "balance": {"type": ["number", "null"]},
                                        "bounds": {
                                            "type": "object",
                                            "properties": {
                                                "x1": {"type": "number"},
                                                "y1": {"type": "number"},
                                                "x2": {"type": "number"},
                                                "y2": {"type": "number"},
                                            },
                                            "required": ["x1", "y1", "x2", "y2"],
                                            "additionalProperties": False,
                                        },
                                    },
                                    "required": ["rownumber", "date", "description", "debit", "credit", "balance", "bounds"],
                                    "additionalProperties": False,
                                },
                            }
                        },
                        "required": ["rows"],
                        "additionalProperties": False,
                    },
                },
            },
            "max_tokens": _env_int("OPENAI_OCR_MAX_TOKENS", 4096),
        }
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

        self._wait_for_rate_limit(on_wait=rate_limit_heartbeat)
        with httpx.Client(timeout=self.timeout_seconds) as client:
            response = client.post(f"{self.base_url}/chat/completions", headers=headers, json=payload)

        if response.status_code >= 400:
            detail = self._extract_error_detail(response)
            lowered = detail.lower()
            if "context_length_exceeded" in lowered or "maximum context length" in lowered:
                raise TruncatedOCRResponse(detail)
            raise RuntimeError(f"openai_ocr_http_error:{response.status_code}:{detail}")

        body = response.json()
        self._last_openai_response = {
            "mode": "structured_rows",
            "model": self.model,
            "page_width": int(page_width),
            "page_height": int(page_height),
            "response": body,
        }
        choices = body.get("choices") or []
        if not choices:
            raise RuntimeError("openai_ocr_empty_response")

        choice = choices[0] or {}
        finish_reason = str(choice.get("finish_reason") or "").strip().lower()
        message = choice.get("message") or {}
        content = _flatten_message_content(message.get("content"))
        if finish_reason == "length":
            raise TruncatedOCRResponse("openai_ocr_output_truncated")
        if not content.strip():
            raise RuntimeError("openai_ocr_empty_output")

        parsed = self._safe_parse_json(content)
        if not isinstance(parsed, dict):
            raise RuntimeError("openai_ocr_invalid_json")
        rows = parsed.get("rows")
        if not isinstance(rows, list):
            raise RuntimeError("openai_ocr_invalid_rows")
        return self._normalize_structured_rows(rows, page_width=page_width, page_height=page_height)

    def _normalize_structured_tokens(self, raw_tokens: List[Any], page_width: int, page_height: int) -> List[Dict]:
        max_w = float(max(page_width, 1))
        max_h = float(max(page_height, 1))
        items: List[Dict] = []
        item_id = 1
        for token in raw_tokens:
            if not isinstance(token, dict):
                continue
            text = str(token.get("text") or "").strip()
            if not text:
                continue
            try:
                x1 = float(token.get("x1"))
                y1 = float(token.get("y1"))
                x2 = float(token.get("x2"))
                y2 = float(token.get("y2"))
            except Exception:
                continue
            conf = token.get("confidence", 1.0)
            try:
                confidence = float(conf)
            except Exception:
                confidence = 1.0

            x1 = max(0.0, min(max_w, x1))
            y1 = max(0.0, min(max_h, y1))
            x2 = max(0.0, min(max_w, x2))
            y2 = max(0.0, min(max_h, y2))
            if x2 < x1:
                x1, x2 = x2, x1
            if y2 < y1:
                y1, y2 = y2, y1
            if (x2 - x1) < 0.5 or (y2 - y1) < 0.5:
                continue

            bbox = [[x1, y1], [x2, y1], [x2, y2], [x1, y2]]
            items.append(
                {
                    "id": item_id,
                    "text": text,
                    "confidence": max(0.0, min(1.0, confidence)),
                    "bbox": bbox,
                }
            )
            item_id += 1

        if not items:
            raise RuntimeError("openai_ocr_no_valid_tokens")
        return items

    def _normalize_structured_rows(self, raw_rows: List[Any], page_width: int, page_height: int) -> List[Dict]:
        max_w = float(max(page_width, 1))
        max_h = float(max(page_height, 1))
        out: List[Dict] = []
        for item in raw_rows:
            if not isinstance(item, dict):
                continue
            bounds = item.get("bounds")
            if not isinstance(bounds, dict):
                continue
            try:
                x1 = float(bounds.get("x1"))
                y1 = float(bounds.get("y1"))
                x2 = float(bounds.get("x2"))
                y2 = float(bounds.get("y2"))
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
            if (x2 - x1) < 0.5 or (y2 - y1) < 0.5:
                continue

            date = str(item.get("date") or "").strip()
            rownumber = self._coerce_row_number(item.get("rownumber"), fallback=item.get("row_number"))
            description = str(item.get("description") or "").strip()
            debit = self._coerce_nullable_amount(item.get("debit"))
            credit = self._coerce_nullable_amount(item.get("credit"))
            balance = self._coerce_nullable_amount(item.get("balance"))
            row_type = str(item.get("row_type") or "transaction").strip().lower() or "transaction"
            if not date and not description and balance is None:
                continue

            out.append(
                {
                    "rownumber": rownumber,
                    "date": date,
                    "description": description,
                    "debit": debit,
                    "credit": credit,
                    "balance": balance,
                    "row_type": row_type,
                    "bounds": {"x1": x1, "y1": y1, "x2": x2, "y2": y2},
                }
            )
        if not out:
            raise RuntimeError("openai_ocr_no_valid_rows")
        return out

    @staticmethod
    def _coerce_row_number(value: Any, fallback: Any = None) -> int | None:
        candidate = value if value is not None else fallback
        if candidate is None:
            return None
        text = str(candidate).strip()
        if not text:
            return None
        if any(ch.isalpha() for ch in text):
            return None
        digits = "".join(ch for ch in text if ch.isdigit())
        if not digits:
            return None
        try:
            return int(digits)
        except Exception:
            return None

    @staticmethod
    def _coerce_nullable_amount(value: Any) -> float | None:
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

    def _encode_image(self, image: Image.Image, config: OCRImageConfig) -> bytes:
        prepared = image.convert("L")
        prepared = self._downscale(prepared, max_dimension=max(512, min(2000, config.max_dimension)))
        quality = max(50, min(95, int(config.jpeg_quality)))

        while True:
            payload = self._jpeg_bytes(prepared, quality=quality)
            if len(payload) <= MAX_IMAGE_BYTES:
                return payload
            if quality > 55:
                quality -= 5
                continue

            width, height = prepared.size
            if max(width, height) <= 700:
                return payload
            prepared = prepared.resize(
                (max(1, int(width * 0.88)), max(1, int(height * 0.88))),
                resample=Image.Resampling.BILINEAR,
            )

    def _downscale(self, image: Image.Image, max_dimension: int) -> Image.Image:
        width, height = image.size
        largest = max(width, height)
        if largest <= max_dimension:
            return image
        ratio = max_dimension / float(largest)
        target = (max(1, int(width * ratio)), max(1, int(height * ratio)))
        return image.resize(target, resample=Image.Resampling.BILINEAR)

    def _jpeg_bytes(self, image: Image.Image, quality: int) -> bytes:
        out = io.BytesIO()
        image.save(out, format="JPEG", quality=quality, optimize=True)
        return out.getvalue()

    def _split_vertical_halves(self, image: Image.Image) -> Tuple[Image.Image, Image.Image]:
        width, height = image.size
        mid = max(1, height // 2)
        top = image.crop((0, 0, width, mid))
        bottom = image.crop((0, mid, width, height))
        return top, bottom

    def _cache_get(self, key: str) -> str | None:
        path = self.cache_dir / f"{key}.txt"
        if not path.exists():
            return None
        try:
            return path.read_text(encoding="utf-8")
        except Exception:
            return None

    def _cache_put(self, key: str, text: str):
        path = self.cache_dir / f"{key}.txt"
        tmp = path.with_suffix(".txt.tmp")
        tmp.write_text(text, encoding="utf-8")
        os.replace(tmp, path)

    def _cache_get_json(self, key: str) -> Any | None:
        path = self.cache_dir / f"{key}.json"
        if not path.exists():
            return None
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return None

    def _cache_put_json(self, key: str, payload: Any):
        path = self.cache_dir / f"{key}.json"
        tmp = path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        os.replace(tmp, path)

    def _safe_parse_json(self, content: str) -> Any:
        text = str(content or "").strip()
        if not text:
            return None
        try:
            return json.loads(text)
        except Exception:
            pass
        fenced = re.search(r"```(?:json)?\s*(\{.*\})\s*```", text, flags=re.DOTALL | re.IGNORECASE)
        if fenced:
            try:
                return json.loads(fenced.group(1))
            except Exception:
                return None
        return None

    def _extract_error_detail(self, response: httpx.Response) -> str:
        try:
            payload = response.json()
        except Exception:
            return response.text.strip() or "unknown_error"

        err = payload.get("error")
        if isinstance(err, dict):
            parts = []
            code = str(err.get("code") or "").strip()
            message = str(err.get("message") or "").strip()
            if code:
                parts.append(code)
            if message:
                parts.append(message)
            if parts:
                return " | ".join(parts)
        return json.dumps(payload, ensure_ascii=True)

    def _rate_redis_client(self) -> Redis | None:
        if self._rate_redis is not None:
            return self._rate_redis
        if self._rate_redis_init_failed:
            return None
        try:
            redis_url = str(os.getenv("REDIS_URL", os.getenv("CELERY_BROKER_URL", "redis://redis:6379/0"))).strip()
            if not redis_url:
                redis_url = "redis://redis:6379/0"
            self._rate_redis = Redis.from_url(redis_url, decode_responses=True, socket_timeout=5, socket_connect_timeout=5)
            self._rate_redis.ping()
            return self._rate_redis
        except Exception:
            self._rate_redis_init_failed = True
            return None

    def _wait_for_rate_limit(self, on_wait: Callable[[float], None] | None = None):
        client = self._rate_redis_client()
        if client is None:
            return
        limit = max(1, int(self._rate_limit_per_window))
        window_ms = max(1000, int(self._rate_window_seconds * 1000))
        timeout_s = max(1.0, float(self._rate_wait_timeout_seconds))
        deadline = time.monotonic() + timeout_s
        script = """
local key = KEYS[1]
local now_ms = tonumber(ARGV[1])
local window_ms = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])
local member = ARGV[4]
redis.call('ZREMRANGEBYSCORE', key, 0, now_ms - window_ms)
local count = redis.call('ZCARD', key)
if count < limit then
  redis.call('ZADD', key, now_ms, member)
  redis.call('EXPIRE', key, math.floor(window_ms / 1000) + 5)
  return {1, 0}
end
local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
if oldest[2] == nil then
  return {0, 50}
end
local wait_ms = window_ms - (now_ms - tonumber(oldest[2]))
if wait_ms < 0 then wait_ms = 0 end
return {0, wait_ms}
"""
        while True:
            now_ms = int(time.time() * 1000)
            member = f"{now_ms}-{uuid.uuid4().hex}"
            ok = False
            wait_ms = 0
            try:
                result = client.eval(script, 1, self._rate_key, now_ms, window_ms, limit, member)
                ok = bool(result and int(result[0]) == 1)
                if not ok:
                    wait_ms = int(result[1] or 0)
            except Exception:
                return
            if ok:
                return
            wait_s = max(0.05, min(1.0, wait_ms / 1000.0 if wait_ms > 0 else 0.2))
            if on_wait is not None:
                try:
                    on_wait(wait_s)
                except Exception:
                    pass
            if time.monotonic() + wait_s > deadline:
                raise RuntimeError("openai_rate_limit_wait_timeout")
            time.sleep(wait_s)
