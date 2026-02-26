from __future__ import annotations

import os
import shutil
from pathlib import Path

def _data_dir() -> Path:
    return Path(os.getenv("DATA_DIR", "./data"))


def _settings_file() -> Path:
    return _data_dir() / "config" / "admin_settings.json"


def get_ui_settings() -> dict:
    path = _settings_file()
    if not path.exists():
        return {"upload_testing_enabled": False}
    try:
        import json

        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
        return {"upload_testing_enabled": bool(payload.get("upload_testing_enabled", False))}
    except Exception:
        return {"upload_testing_enabled": False}


def set_upload_testing_enabled(enabled: bool) -> dict:
    import json

    path = _settings_file()
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"upload_testing_enabled": bool(enabled)}
    tmp = path.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)
    os.replace(tmp, path)
    return payload


def clear_jobs_and_exports() -> dict:
    root = _data_dir()
    jobs_dir = root / "jobs"
    exports_dir = root / "exports"

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
