import os
import uuid
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import pytest
from app import main
from app.auth import service as auth_service
from app.jobs import repository as jobs_repository
from app.jobs import service as jobs_service
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text


DEFAULT_TEST_DATABASE_URL = "postgresql+psycopg://ocr:ocrpass@localhost:5433/ocr"


def _build_schema_database_url(base_url: str, schema_name: str) -> str:
    parts = urlsplit(base_url)
    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    query["options"] = f"-csearch_path={schema_name}"
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))


@pytest.fixture
def app_with_temp_data(monkeypatch, tmp_path: Path):
    base_database_url = str(os.getenv("TEST_DATABASE_URL") or os.getenv("DATABASE_URL") or DEFAULT_TEST_DATABASE_URL).strip()
    schema_name = f"test_{uuid.uuid4().hex}"
    admin_engine = create_engine(base_database_url, future=True)
    with admin_engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA "{schema_name}"'))

    database_url = _build_schema_database_url(base_database_url, schema_name)
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DATABASE_URL", database_url)
    monkeypatch.setenv("DB_AUTO_CREATE_SCHEMA", "true")
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(jobs_service, "DATA_DIR", str(tmp_path))
    monkeypatch.setattr(auth_service, "DATA_DIR", tmp_path)
    monkeypatch.setattr(auth_service, "AUTH_DIR", tmp_path / "auth")
    monkeypatch.setattr(auth_service, "USERS_FILE", (tmp_path / "auth" / "users.json"))
    monkeypatch.setattr(auth_service, "DEFAULT_ADMIN_USERNAME", "admin")
    monkeypatch.setattr(auth_service, "DEFAULT_ADMIN_PASSWORD", "admin123")
    auth_service.clear_all_sessions()

    (tmp_path / "jobs").mkdir(parents=True, exist_ok=True)
    (tmp_path / "exports").mkdir(parents=True, exist_ok=True)

    try:
        yield main.app, tmp_path
    finally:
        with jobs_repository._DB_ENGINE_CACHE_GUARD:
            engine = jobs_repository._DB_ENGINE_CACHE.pop(database_url, None)
            jobs_repository._DB_SCHEMA_READY = {
                key for key in jobs_repository._DB_SCHEMA_READY if not key.startswith(database_url)
            }
        if engine is not None:
            engine.dispose()
        with admin_engine.begin() as conn:
            conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE'))
        admin_engine.dispose()


@pytest.fixture
def client(app_with_temp_data):
    app, _tmp_path = app_with_temp_data
    with TestClient(app) as test_client:
        ensure_admin = test_client.post("/auth/login", data={"username": "admin", "password": "admin123"})
        assert ensure_admin.status_code == 200
        create_eval = test_client.post(
            "/admin/evaluators",
            json={"username": "eval_test", "password": "evalpass1"},
        )
        if create_eval.status_code not in (200, 400):
            raise AssertionError(f"unexpected evaluator create status: {create_eval.status_code}")
        test_client.post("/auth/logout")
        login_res = test_client.post("/auth/login", data={"username": "eval_test", "password": "evalpass1"})
        assert login_res.status_code == 200
        yield test_client
