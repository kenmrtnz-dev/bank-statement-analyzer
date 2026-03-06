"""Shared filesystem path resolution for runtime data."""

from __future__ import annotations

import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DATA_DIR = PROJECT_ROOT / "storage"


def get_data_dir() -> Path:
    configured = str(os.getenv("DATA_DIR", "") or "").strip()
    if configured:
        return Path(configured)
    return DEFAULT_DATA_DIR
