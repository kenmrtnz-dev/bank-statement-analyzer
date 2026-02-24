from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app import main
from app.modules.auth import service as auth_service
from app.modules.jobs import service as jobs_service


@pytest.fixture
def app_with_temp_data(monkeypatch, tmp_path: Path):
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

    yield main.app, tmp_path


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
