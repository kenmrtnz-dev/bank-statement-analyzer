"""Shared filesystem path resolution for runtime data."""

from __future__ import annotations

from pathlib import Path

from app.settings import default_data_dir

VOLUME_STORAGE_DIRNAME = ".volume_sets"


def get_project_root() -> Path:
    source_root = Path(__file__).resolve().parents[2]
    if (source_root / "pyproject.toml").exists():
        return source_root
    cwd = Path.cwd()
    if (cwd / "pyproject.toml").exists() and (cwd / "backend").exists():
        return cwd
    return source_root


PROJECT_ROOT = get_project_root()
DEFAULT_DATA_DIR = default_data_dir()


def get_data_dir() -> Path:
    return default_data_dir()


def get_volume_storage_dir() -> Path:
    return get_data_dir() / VOLUME_STORAGE_DIRNAME


def get_legacy_volume_storage_dir() -> Path:
    return get_project_root() / "storage"
