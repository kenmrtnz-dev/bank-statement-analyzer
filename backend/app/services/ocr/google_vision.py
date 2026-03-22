from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List
from urllib import parse, request

from app.parser.extractors import google_vision as legacy_google_vision


def _vertices_to_bbox(vertices: list[dict[str, Any]]) -> list[list[float]]:
    points = []
    for point in vertices[:4]:
        points.append([float(point.get("x", 0) or 0), float(point.get("y", 0) or 0)])
    while len(points) < 4:
        points.append([0.0, 0.0])
    return points


def _response_to_ocr_items(payload: dict[str, Any]) -> List[Dict]:
    annotations = payload.get("textAnnotations") or payload.get("text_annotations") or []
    if not isinstance(annotations, list):
        return []

    items: List[Dict] = []
    start_index = 1 if len(annotations) > 1 else 0
    for idx, item in enumerate(annotations[start_index:], start=1):
        if not isinstance(item, dict):
            continue
        text = str(item.get("description") or item.get("text") or "").strip()
        if not text:
            continue
        poly = item.get("boundingPoly") or item.get("bounding_poly") or {}
        vertices = poly.get("vertices") or []
        if not isinstance(vertices, list) or not vertices:
            continue
        items.append(
            {
                "id": idx,
                "text": text,
                "confidence": float(item.get("confidence") or 1.0),
                "bbox": _vertices_to_bbox(vertices),
            }
        )
    return items


class GoogleVisionOCR:
    @classmethod
    def from_env(cls) -> "GoogleVisionOCR":
        api_key = legacy_google_vision._google_vision_api_key()
        if api_key:
            return cls()
        if legacy_google_vision.vision is None:
            raise RuntimeError("google_vision_not_configured")
        return cls()

    def extract_ocr_items(self, image_path: str | Path) -> List[Dict]:
        path = Path(image_path)
        if not path.exists():
            raise RuntimeError(f"google_vision_missing_image:{path}")

        api_key = legacy_google_vision._google_vision_api_key()
        if api_key:
            payload = self._extract_with_api_key(path, api_key)
        else:
            payload = self._extract_with_client(path)
        return _response_to_ocr_items(payload)

    def _extract_with_api_key(self, image_path: Path, api_key: str) -> dict[str, Any]:
        encoded_image = legacy_google_vision.base64.b64encode(image_path.read_bytes()).decode("utf-8")
        payload = {
            "requests": [
                {
                    "image": {"content": encoded_image},
                    "features": [{"type": "DOCUMENT_TEXT_DETECTION"}],
                }
            ]
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
        if not responses:
            raise RuntimeError("google_vision_empty_response")
        page_response = responses[0]
        error = page_response.get("error")
        if error:
            raise RuntimeError(f"google_vision_failed:{error.get('message', 'unknown')}")
        return page_response

    def _extract_with_client(self, image_path: Path) -> dict[str, Any]:
        if legacy_google_vision.vision is None:
            raise RuntimeError("google_vision_not_configured")
        client = legacy_google_vision._get_client()
        image = legacy_google_vision.vision.Image(content=image_path.read_bytes())
        response = client.document_text_detection(image=image)
        if response.error.message:
            raise RuntimeError(f"google_vision_failed:{response.error.message}")
        return legacy_google_vision._response_to_dict(response)
