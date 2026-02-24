from __future__ import annotations

import os
import shutil
from pathlib import Path

DATA_DIR = Path(os.getenv("DATA_DIR", "./data"))


def clear_jobs_and_exports() -> dict:
    jobs_dir = DATA_DIR / "jobs"
    exports_dir = DATA_DIR / "exports"

    removed_jobs = 0
    if jobs_dir.exists():
        for item in jobs_dir.iterdir():
            if item.is_dir():
                shutil.rmtree(item, ignore_errors=True)
                removed_jobs += 1

    removed_exports = 0
    if exports_dir.exists():
        for item in exports_dir.iterdir():
            if item.is_file():
                item.unlink(missing_ok=True)
                removed_exports += 1
            elif item.is_dir():
                shutil.rmtree(item, ignore_errors=True)
                removed_exports += 1

    jobs_dir.mkdir(parents=True, exist_ok=True)
    exports_dir.mkdir(parents=True, exist_ok=True)
    return {"cleared_jobs": removed_jobs, "cleared_exports": removed_exports}

