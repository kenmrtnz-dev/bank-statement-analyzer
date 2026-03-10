"""Shared filesystem path resolution for runtime data."""

from __future__ import annotations

from pathlib import Path

from app.settings import default_data_dir

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DATA_DIR = default_data_dir()


def get_data_dir() -> Path:
    return default_data_dir()
