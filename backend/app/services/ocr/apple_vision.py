from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from hashlib import sha256
from pathlib import Path
from typing import Dict, List

from app.paths import get_data_dir


class AppleVisionOCR:
    def __init__(self, *, cache_dir: Path):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.binary_path = self.cache_dir / "apple_vision_ocr"
        self.source_path = Path(__file__).with_name("apple_vision_ocr.swift")

    @classmethod
    def is_available(cls) -> bool:
        return sys.platform == "darwin" and bool(shutil.which("swiftc"))

    @classmethod
    def from_env(cls) -> "AppleVisionOCR":
        if not cls.is_available():
            raise RuntimeError("apple_vision_ocr_unavailable")
        data_dir = get_data_dir()
        cache_dir = Path(os.getenv("APPLE_VISION_OCR_CACHE_DIR", str(data_dir / "apple_vision_ocr")))
        return cls(cache_dir=cache_dir)

    def extract_ocr_items(self, image_path: str | Path) -> List[Dict]:
        path = Path(image_path)
        if not path.exists():
            raise RuntimeError(f"apple_vision_ocr_missing_image:{path}")

        cache_key = sha256(path.read_bytes()).hexdigest()
        cache_file = self.cache_dir / f"{cache_key}.json"
        if cache_file.exists():
            payload = json.loads(cache_file.read_text(encoding="utf-8"))
            if isinstance(payload, list):
                return payload

        self._ensure_binary()
        result = subprocess.run(
            [str(self.binary_path), str(path)],
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )
        if result.returncode != 0:
            detail = (result.stderr or result.stdout or "").strip()
            raise RuntimeError(f"apple_vision_ocr_failed:{detail[:500]}")

        payload = json.loads(result.stdout or "[]")
        if not isinstance(payload, list):
            raise RuntimeError("apple_vision_ocr_invalid_payload")

        cache_file.write_text(json.dumps(payload), encoding="utf-8")
        return payload

    def _ensure_binary(self) -> None:
        if not self.source_path.exists():
            raise RuntimeError(f"apple_vision_ocr_source_missing:{self.source_path}")

        needs_compile = not self.binary_path.exists()
        if not needs_compile:
            try:
                needs_compile = self.binary_path.stat().st_mtime < self.source_path.stat().st_mtime
            except OSError:
                needs_compile = True
        if not needs_compile:
            return

        result = subprocess.run(
            ["swiftc", str(self.source_path), "-o", str(self.binary_path)],
            capture_output=True,
            text=True,
            timeout=120,
            check=False,
        )
        if result.returncode != 0:
            detail = (result.stderr or result.stdout or "").strip()
            raise RuntimeError(f"apple_vision_ocr_compile_failed:{detail[:500]}")

