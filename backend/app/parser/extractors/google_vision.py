"""Google Vision OCR integration for scanned statements."""

import base64
from functools import lru_cache
import json
import logging
import os
from pathlib import Path
import subprocess
from typing import Any
from urllib.error import HTTPError
from urllib import parse, request

try:
    from google.protobuf.json_format import MessageToDict
except ImportError:  # pragma: no cover
    MessageToDict = None

try:
    from google.cloud import vision
except ImportError:  # pragma: no cover
    vision = None

logger = logging.getLogger(__name__)
VISION_MAX_BATCH_SIZE = 16


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name, str(default))
    try:
        return int(str(raw).strip())
    except (TypeError, ValueError):
        return default


def _google_vision_pdf_dpi() -> int:
    return max(72, _env_int("GOOGLE_VISION_PDF_DPI", 120))


def _google_vision_batch_size() -> int:
    return max(1, min(VISION_MAX_BATCH_SIZE, _env_int("GOOGLE_VISION_BATCH_SIZE", 5)))


def _google_vision_api_key() -> str:
    return str(os.getenv("GOOGLE_VISION_API_KEY", "") or "").strip()


@lru_cache(maxsize=1)
def _get_client() -> Any:
    """Create and cache the Vision API client."""
    if vision is None:
        raise RuntimeError("google-cloud-vision is not installed.")
    return vision.ImageAnnotatorClient()


def _convert_pdf_to_images(file_path: Path) -> list[Path]:
    """Convert each PDF page to a PNG image and return generated file paths."""
    output_dir = file_path.parent / f"{file_path.stem}_pages"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_prefix = output_dir / "page"
    existing_images = sorted(output_dir.glob("page-*.png"))
    if existing_images:
        logger.info("Reusing %s existing OCR page images for %s", len(existing_images), file_path.name)
        return existing_images

    try:
        result = subprocess.run(
            [
                "pdftoppm",
                "-png",
                "-r",
                str(_google_vision_pdf_dpi()),
                str(file_path),
                str(output_prefix),
            ],
            capture_output=True,
            text=True,
            check=False,
        )
    except FileNotFoundError:
        logger.warning("pdftoppm is not installed or not available in PATH.")
        return []

    if result.returncode != 0:
        logger.warning("pdftoppm failed for %s: %s", file_path.name, result.stderr.strip())
        return []

    image_paths = sorted(output_dir.glob("page-*.png"))
    if not image_paths:
        logger.warning("No page images were generated for %s.", file_path.name)
    return image_paths


def _extract_full_text(response_payload: dict[str, Any]) -> str:
    """Pull full OCR text from a Vision response object."""
    if "fullTextAnnotation" in response_payload:
        return (response_payload.get("fullTextAnnotation") or {}).get("text", "")
    return (response_payload.get("full_text_annotation") or {}).get("text", "")


def _chunk_page_images(page_images: list[Path], batch_size: int) -> list[list[tuple[int, Path]]]:
    """Split page image paths into indexed batches while preserving order."""
    bounded_size = max(1, min(batch_size, VISION_MAX_BATCH_SIZE))
    indexed_images = list(enumerate(page_images, start=1))
    return [
        indexed_images[index : index + bounded_size]
        for index in range(0, len(indexed_images), bounded_size)
    ]


def _extract_batch_with_api_key(
    batch_pages: list[tuple[int, Path]], api_key: str
) -> list[tuple[int, Path, dict[str, Any]]]:
    """Call Vision REST API once for a batch of page images."""
    requests_payload = []
    for _, image_path in batch_pages:
        encoded_image = base64.b64encode(image_path.read_bytes()).decode("utf-8")
        requests_payload.append(
            {
                "image": {"content": encoded_image},
                "features": [{"type": "DOCUMENT_TEXT_DETECTION"}],
            }
        )

    payload = {
        "requests": requests_payload,
    }
    endpoint = f"https://vision.googleapis.com/v1/images:annotate?key={parse.quote(api_key)}"
    req = request.Request(
        endpoint,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with request.urlopen(req, timeout=30) as response:  # noqa: S310
        response_payload = json.loads(response.read().decode("utf-8"))

    responses = response_payload.get("responses") or []
    if len(responses) != len(batch_pages):
        raise RuntimeError(
            "Vision API response count mismatch: "
            f"expected {len(batch_pages)}, got {len(responses)}."
        )

    page_payloads: list[tuple[int, Path, dict[str, Any]]] = []
    for response_index, (page_number, image_path) in enumerate(batch_pages):
        page_response = responses[response_index]
        error = page_response.get("error")
        if error:
            message = error.get("message", "Vision API request failed.")
            raise RuntimeError(f"Vision API failed on page {page_number}: {message}")
        page_payloads.append((page_number, image_path, page_response))

    return page_payloads


def _extract_batch_resilient_with_api_key(
    batch_pages: list[tuple[int, Path]], api_key: str
) -> list[tuple[int, Path, dict[str, Any]]]:
    """Process a batch and auto-split it when request payload is too large."""
    try:
        return _extract_batch_with_api_key(batch_pages, api_key)
    except HTTPError as exc:
        if len(batch_pages) == 1:
            raise
        logger.warning(
            "Vision batch failed (%s) for %s pages; retrying with smaller chunks.",
            exc,
            len(batch_pages),
        )
    except Exception:  # noqa: BLE001
        if len(batch_pages) == 1:
            raise
        logger.exception(
            "Vision batch failed for %s pages; retrying with smaller chunks.",
            len(batch_pages),
        )

    midpoint = len(batch_pages) // 2
    left = _extract_batch_resilient_with_api_key(batch_pages[:midpoint], api_key)
    right = _extract_batch_resilient_with_api_key(batch_pages[midpoint:], api_key)
    return left + right


def _extract_text_with_api_key(
    page_images: list[Path], api_key: str, batch_size: int = 5
) -> tuple[str, dict[str, Any]]:
    """Run OCR for page images using batched Vision REST API calls."""
    texts: list[str] = []
    raw_pages: list[dict[str, Any]] = []
    failed_pages: list[dict[str, Any]] = []
    page_batches = _chunk_page_images(page_images, batch_size)
    logger.info(
        "Starting Vision OCR with API key in %s batches (batch_size=%s, pages=%s)",
        len(page_batches),
        max(1, min(batch_size, VISION_MAX_BATCH_SIZE)),
        len(page_images),
    )

    for batch_index, batch_pages in enumerate(page_batches, start=1):
        try:
            page_payloads = _extract_batch_resilient_with_api_key(batch_pages, api_key)
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Batch %s/%s failed (%s). Retrying this batch per-page.",
                batch_index,
                len(page_batches),
                exc,
            )
            page_payloads = []
            for page_index, image_path in batch_pages:
                try:
                    single_payload = _extract_batch_with_api_key([(page_index, image_path)], api_key)
                    page_payloads.extend(single_payload)
                except Exception as single_exc:  # noqa: BLE001
                    logger.warning("Vision OCR failed for page %s: %s", page_index, single_exc)
                    failed_pages.append(
                        {
                            "page_number": page_index,
                            "image_file": image_path.name,
                            "error": str(single_exc),
                        }
                    )

        for page_index, image_path, page_payload in page_payloads:
            page_text = _extract_full_text(page_payload)
            logger.info(
                "Vision API key OCR completed for page %s (batch %s/%s)",
                page_index,
                batch_index,
                len(page_batches),
            )
            if page_text.strip():
                texts.append(page_text)
            raw_pages.append(
                {
                    "page_number": page_index,
                    "image_file": image_path.name,
                    "response": page_payload,
                }
            )

    raw_pages.sort(key=lambda item: item["page_number"])
    raw_payload = {
        "provider": "google_vision",
        "mode": "api_key",
        "page_count": len(page_images),
        "batch_size": max(1, min(batch_size, VISION_MAX_BATCH_SIZE)),
        "pages": raw_pages,
        "failed_pages": failed_pages,
    }
    return "\n".join(texts), raw_payload


def _response_to_dict(response: Any) -> dict[str, Any]:
    """Convert Vision client response objects into plain dictionaries."""
    if MessageToDict is not None and hasattr(response, "_pb"):
        return MessageToDict(response._pb, preserving_proto_field_name=True)
    if hasattr(response, "to_dict"):
        try:
            return response.to_dict()
        except Exception:  # noqa: BLE001
            pass
    if hasattr(response.__class__, "to_json"):
        try:
            return json.loads(response.__class__.to_json(response))
        except Exception:  # noqa: BLE001
            pass
    return {"raw_repr": str(response)}


def _extract_text_with_client(page_images: list[Path]) -> tuple[str, dict[str, Any]]:
    """Run OCR for each page image using the Google Cloud Vision client."""
    if vision is None:
        raise RuntimeError("google-cloud-vision is not installed.")

    client = _get_client()
    texts: list[str] = []
    raw_pages: list[dict[str, Any]] = []
    for page_index, image_path in enumerate(page_images, start=1):
        image = vision.Image(content=image_path.read_bytes())
        response = client.document_text_detection(image=image)
        if response.error.message:
            raise RuntimeError(f"Vision OCR failed on page {page_index}: {response.error.message}")
        response_payload = _response_to_dict(response)
        page_text = _extract_full_text(response_payload)
        logger.info("Vision client OCR completed for page %s", page_index)
        if page_text.strip():
            texts.append(page_text)
        raw_pages.append(
            {
                "page_number": page_index,
                "image_file": image_path.name,
                "response": response_payload,
            }
        )
    raw_payload = {
        "provider": "google_vision",
        "mode": "service_account",
        "page_count": len(page_images),
        "pages": raw_pages,
    }
    return "\n".join(texts), raw_payload


def extract_text_with_details(file_path: str | Path) -> dict[str, Any]:
    """Convert scanned PDF pages to images and return text plus raw Vision payloads."""
    path = Path(file_path)
    image_paths = _convert_pdf_to_images(path)
    if not image_paths:
        return {
            "text": "",
            "raw": {
                "provider": "google_vision",
                "mode": "unavailable",
                "page_count": 0,
                "pages": [],
                "error": "No page images generated from PDF.",
            },
        }

    api_key = _google_vision_api_key()
    if api_key:
        try:
            text, raw = _extract_text_with_api_key(
                image_paths,
                api_key,
                _google_vision_batch_size(),
            )
            return {"text": text, "raw": raw}
        except Exception as exc:  # noqa: BLE001
            logger.warning("Google Vision OCR (API key) failed for %s: %s", path.name, exc)
            return {
                "text": "",
                "raw": {
                    "provider": "google_vision",
                    "mode": "api_key",
                    "page_count": len(image_paths),
                    "pages": [],
                    "error": str(exc),
                },
            }

    try:
        text, raw = _extract_text_with_client(image_paths)
        return {"text": text, "raw": raw}
    except Exception as exc:  # noqa: BLE001
        logger.warning("Google Vision OCR failed for %s: %s", path.name, exc)
        return {
            "text": "",
            "raw": {
                "provider": "google_vision",
                "mode": "service_account",
                "page_count": len(image_paths),
                "pages": [],
                "error": str(exc),
            },
        }


def extract_text(file_path: str | Path) -> str:
    """Compatibility wrapper that returns OCR text only."""
    return extract_text_with_details(file_path).get("text", "")
