from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict


class JobsRepository:
    def __init__(self, data_dir: str | Path):
        self.data_dir = Path(data_dir)
        self.jobs_dir = self.data_dir / "jobs"
        self.exports_dir = self.data_dir / "exports"
        self.jobs_dir.mkdir(parents=True, exist_ok=True)
        self.exports_dir.mkdir(parents=True, exist_ok=True)

    def job_dir(self, job_id: str) -> Path:
        return self.jobs_dir / str(job_id)

    def ensure_job_layout(self, job_id: str):
        root = self.job_dir(job_id)
        for part in ("input", "result", "pages", "cleaned", "ocr", "preview"):
            (root / part).mkdir(parents=True, exist_ok=True)
        return root

    def job_exists(self, job_id: str) -> bool:
        return (self.job_dir(job_id) / "input" / "document.pdf").exists()

    def path(self, job_id: str, *parts: str) -> Path:
        return self.job_dir(job_id).joinpath(*parts)

    def write_bytes(self, path: Path, data: bytes):
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "wb") as handle:
            handle.write(data)

    def write_json(self, path: Path, payload: Any):
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        with open(tmp, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2)
        os.replace(tmp, path)

    def read_json(self, path: Path, default: Any):
        if not path.exists():
            return default
        with open(path, encoding="utf-8") as handle:
            return json.load(handle)

    def read_status(self, job_id: str) -> Dict[str, Any]:
        return self.read_json(self.path(job_id, "status.json"), default={})

    def write_status(self, job_id: str, payload: Dict[str, Any]):
        self.write_json(self.path(job_id, "status.json"), payload)

    def list_png(self, job_id: str, folder: str) -> list[str]:
        target = self.path(job_id, folder)
        if not target.exists():
            return []
        return sorted(item.name for item in target.iterdir() if item.is_file() and item.suffix.lower() == ".png")


__all__ = ["JobsRepository"]
