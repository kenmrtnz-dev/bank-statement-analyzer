"""Typed runtime settings for repo-root installs and prod validation."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _env_bool(name: str, default: bool) -> bool:
    raw = str(os.getenv(name, str(default))).strip().lower()
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    return default


def _env_int(name: str, default: int) -> int:
    raw = str(os.getenv(name, str(default))).strip()
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


def _repo_root() -> Path:
    source_root = Path(__file__).resolve().parents[2]
    if (source_root / "pyproject.toml").exists():
        return source_root
    cwd = Path.cwd()
    if (cwd / "pyproject.toml").exists() and (cwd / "backend").exists():
        return cwd
    return source_root


def default_data_dir() -> Path:
    configured = str(os.getenv("DATA_DIR", "") or "").strip()
    if configured:
        return Path(configured)
    return _repo_root() / "storage"


@dataclass(frozen=True)
class AppSettings:
    app_env: str
    data_dir: Path
    database_url: str
    jwt_secret: str
    seed_default_users: bool
    admin_username: str
    admin_password: str
    redis_url: str
    celery_broker_url: str
    celery_result_backend: str
    celery_task_default_queue: str
    session_cookie_secure: bool

    @property
    def is_prod(self) -> bool:
        return self.app_env == "prod"

    def validate_for_api(self) -> None:
        self._require_database_url()
        self._require_prod_secret()

    def validate_for_worker(self) -> None:
        self._require_database_url()
        self._require_celery_broker_url()
        self._require_prod_secret()

    def _require_database_url(self) -> None:
        if not self.database_url:
            raise RuntimeError("DATABASE_URL is required. Configure Postgres explicitly.")

    def _require_celery_broker_url(self) -> None:
        if not self.celery_broker_url:
            raise RuntimeError("CELERY_BROKER_URL or REDIS_URL is required for Celery workers.")

    def _require_prod_secret(self) -> None:
        if self.is_prod and (not self.jwt_secret or self.jwt_secret == "change-me"):
            raise RuntimeError("JWT_SECRET must be set to a non-placeholder value when APP_ENV=prod.")


def load_settings() -> AppSettings:
    app_env = str(os.getenv("APP_ENV", "dev") or "dev").strip().lower() or "dev"
    is_prod = app_env == "prod"
    jwt_secret = str(os.getenv("JWT_SECRET", "change-me") or "").strip()
    database_url = str(os.getenv("DATABASE_URL", "") or "").strip()

    default_seed = not is_prod
    seed_default_users = _env_bool("SEED_DEFAULT_USERS", default_seed)

    redis_url = str(os.getenv("REDIS_URL", "") or "").strip()
    if not redis_url and not is_prod:
        redis_url = "redis://redis:6379/0"

    celery_broker_url = str(os.getenv("CELERY_BROKER_URL", "") or "").strip() or redis_url
    celery_result_backend = str(os.getenv("CELERY_RESULT_BACKEND", "") or "").strip() or celery_broker_url
    session_cookie_secure = _env_bool("SESSION_COOKIE_SECURE", is_prod)

    return AppSettings(
        app_env=app_env,
        data_dir=default_data_dir(),
        database_url=database_url,
        jwt_secret=jwt_secret,
        seed_default_users=seed_default_users,
        admin_username=str(os.getenv("ADMIN_USERNAME", "admin") or "admin").strip() or "admin",
        admin_password=str(os.getenv("ADMIN_PASSWORD", "admin123") or "admin123"),
        redis_url=redis_url,
        celery_broker_url=celery_broker_url,
        celery_result_backend=celery_result_backend,
        celery_task_default_queue=str(os.getenv("CELERY_TASK_DEFAULT_QUEUE", "jobs") or "jobs").strip() or "jobs",
        session_cookie_secure=session_cookie_secure,
    )


__all__ = ["AppSettings", "default_data_dir", "load_settings", "_env_bool", "_env_int"]
