import importlib
import io
import json
import zipfile
from pathlib import Path

from app.jobs import service as jobs_service

volume_router_module = importlib.import_module("app.volume.router")


def _patch_volume_storage_roots(monkeypatch, primary_root: Path, legacy_root: Path | None = None) -> Path:
    resolved_legacy_root = legacy_root or (primary_root.parent / "legacy_volume_storage")
    monkeypatch.setattr(volume_router_module, "STORAGE_ROOT", primary_root)
    monkeypatch.setattr(volume_router_module, "LEGACY_STORAGE_ROOT", resolved_legacy_root)
    monkeypatch.setattr(jobs_service, "VOLUME_STORAGE_ROOT", primary_root)
    monkeypatch.setattr(jobs_service, "LEGACY_VOLUME_STORAGE_ROOT", resolved_legacy_root)
    return resolved_legacy_root


def test_volume_page_loads_for_authenticated_user(client, monkeypatch, tmp_path: Path):
    storage_root = tmp_path / "storage"
    target_dir = storage_root / "set-alpha"
    target_dir.mkdir(parents=True, exist_ok=True)
    (target_dir / "one.pdf").write_bytes(b"file-one")
    _patch_volume_storage_roots(monkeypatch, storage_root)

    res = client.get("/volume")
    assert res.status_code == 200
    assert "Volume Upload" in res.text
    assert "set_name" in res.text
    assert '"set_name": "set-alpha"' in res.text


def test_volume_upload_saves_multiple_files(client, monkeypatch, tmp_path: Path):
    storage_root = tmp_path / "storage"
    _patch_volume_storage_roots(monkeypatch, storage_root)

    res = client.post(
        "/volume/upload",
        data={"set_name": "batch-001"},
        files=[
            ("files", ("one.pdf", b"%PDF-1.4\nfile-one", "application/pdf")),
            ("files", ("two.pdf", b"%PDF-1.4\nfile-two", "application/pdf")),
        ],
    )

    assert res.status_code == 200
    payload = res.json()
    assert payload["ok"] is True
    assert payload["set_name"] == "batch-001"
    assert payload["saved_count"] == 2
    assert payload["uploader_username"] == "eval_test"
    assert payload["uploader_role"] == "evaluator"

    target_dir = storage_root / "batch-001"
    assert target_dir.is_dir()
    assert (target_dir / "one.pdf").read_bytes() == b"%PDF-1.4\nfile-one"
    assert (target_dir / "two.pdf").read_bytes() == b"%PDF-1.4\nfile-two"


def test_volume_upload_rejects_invalid_set_name(client):
    res = client.post(
        "/volume/upload",
        data={"set_name": "../escape"},
        files=[("files", ("one.pdf", b"%PDF-1.4\nfile-one", "application/pdf"))],
    )

    assert res.status_code == 400
    assert res.json()["detail"] == "invalid_set_name"


def test_volume_sets_list_and_download(client, monkeypatch, tmp_path: Path):
    storage_root = tmp_path / "storage"
    target_dir = storage_root / "set-alpha"
    target_dir.mkdir(parents=True, exist_ok=True)
    (target_dir / "one.pdf").write_bytes(b"file-one")
    (target_dir / "two.pdf").write_bytes(b"file-two")
    ocr_dir = storage_root / "apple_vision_ocr"
    ocr_dir.mkdir(parents=True, exist_ok=True)
    (ocr_dir / "ocr.json").write_text("{}", encoding="utf-8")
    _patch_volume_storage_roots(monkeypatch, storage_root)

    list_res = client.get("/volume/sets")
    assert list_res.status_code == 200
    payload = list_res.json()
    assert payload["ok"] is True
    assert len(payload["items"]) == 1
    assert payload["items"][0]["set_name"] == "set-alpha"
    assert payload["items"][0]["file_count"] == 2

    download_res = client.get("/volume/sets/set-alpha/download")
    assert download_res.status_code == 200
    assert download_res.headers["content-type"].startswith("application/zip")
    assert "set-alpha.zip" in download_res.headers.get("content-disposition", "")

    archive = zipfile.ZipFile(io.BytesIO(download_res.content))
    assert sorted(archive.namelist()) == ["set-alpha/one.pdf", "set-alpha/two.pdf"]
    assert archive.read("set-alpha/one.pdf") == b"file-one"
    assert archive.read("set-alpha/two.pdf") == b"file-two"


def test_admin_can_start_volume_set_and_vt_job_shows_for_uploader(client, monkeypatch, tmp_path: Path):
    storage_root = tmp_path / "storage"
    _patch_volume_storage_roots(monkeypatch, storage_root)
    monkeypatch.setattr(jobs_service, "_start_job_worker", lambda *_args, **_kwargs: True)

    upload_res = client.post(
        "/volume/upload",
        data={"set_name": "batch-001"},
        files=[
            ("files", ("one.pdf", b"%PDF-1.4\nfile-one", "application/pdf")),
            ("files", ("two.pdf", b"%PDF-1.4\nfile-two", "application/pdf")),
        ],
    )
    assert upload_res.status_code == 200

    sets_res = client.get("/volume/sets")
    assert sets_res.status_code == 200
    sets_payload = sets_res.json()
    assert sets_payload["items"][0]["uploader_username"] == "eval_test"
    assert sets_payload["items"][0]["pending_count"] == 2

    client.post("/auth/logout")
    admin_login = client.post("/auth/login", data={"username": "admin", "password": "admin123"})
    assert admin_login.status_code == 200

    start_res = client.post("/admin/volume-sets/batch-001/start-next")
    assert start_res.status_code == 200
    start_payload = start_res.json()
    job_id = start_payload["job_id"]
    assert start_payload["set_name"] == "batch-001"
    assert start_payload["file_name"] == "one.pdf"
    assert start_payload["owner_username"] == "eval_test"
    assert start_payload["owner_role"] == "evaluator"

    detail_res = client.get("/admin/volume-sets/batch-001")
    assert detail_res.status_code == 200
    detail_payload = detail_res.json()["item"]
    first_file = detail_payload["files"][0]
    assert first_file["file_name"] == "one.pdf"
    assert first_file["job_id"] == job_id
    assert first_file["volume_status"] == "queued"

    meta_path = tmp_path / "jobs" / job_id / "meta.json"
    job_meta = json.loads(meta_path.read_text(encoding="utf-8"))
    assert job_meta["created_by"] == "eval_test"
    assert job_meta["created_role"] == "evaluator"
    assert job_meta["source_tag"] == "VT"
    assert job_meta["source_category"] == "volume_test"
    assert job_meta["volume_set_name"] == "batch-001"
    assert job_meta["volume_file_name"] == "one.pdf"

    client.post("/auth/logout")
    evaluator_login = client.post("/auth/login", data={"username": "eval_test", "password": "evalpass1"})
    assert evaluator_login.status_code == 200

    mine_res = client.get("/jobs/mine")
    assert mine_res.status_code == 200
    mine_payload = mine_res.json()
    vt_row = next(row for row in mine_payload["rows"] if row["job_id"] == job_id)
    assert vt_row["owner_username"] == "eval_test"
    assert vt_row["source_tag"] == "VT"
    assert vt_row["source_category"] == "volume_test"
    assert vt_row["volume_set_name"] == "batch-001"
    assert vt_row["volume_file_name"] == "one.pdf"


def test_completed_volume_job_auto_starts_next_pending_file(client, monkeypatch, tmp_path: Path):
    storage_root = tmp_path / "storage"
    _patch_volume_storage_roots(monkeypatch, storage_root)
    monkeypatch.setattr(jobs_service, "_start_job_worker", lambda *_args, **_kwargs: True)

    upload_res = client.post(
        "/volume/upload",
        data={"set_name": "batch-002"},
        files=[
            ("files", ("one.pdf", b"%PDF-1.4\nfile-one", "application/pdf")),
            ("files", ("two.pdf", b"%PDF-1.4\nfile-two", "application/pdf")),
        ],
    )
    assert upload_res.status_code == 200

    client.post("/auth/logout")
    admin_login = client.post("/auth/login", data={"username": "admin", "password": "admin123"})
    assert admin_login.status_code == 200

    start_res = client.post("/admin/volume-sets/batch-002/start-next")
    assert start_res.status_code == 200
    first_job_id = start_res.json()["job_id"]

    repo = jobs_service.JobsRepository(jobs_service.DATA_DIR)
    repo.write_json(
        jobs_service._pages_manifest_path(repo, first_job_id),
        {"pages": ["page_001.png"], "updated_at": "2026-03-26T00:00:00Z"},
    )
    jobs_service._write_page_fragment(
        repo,
        first_job_id,
        "page_001",
        page_rows=[
            {
                "row_id": "001",
                "row_number": "1",
                "date": "03/01/2026",
                "description": "VT Deposit",
                "debit": None,
                "credit": "1500.25",
                "balance": "1500.25",
                "row_type": "transaction",
            }
        ],
        page_bounds=[],
        page_diag={"source_type": "ocr", "rows_parsed": 1, "ocr_backend": "google_vision"},
    )

    final_payload = jobs_service.finalize_job_processing(job_id=first_job_id, parse_mode="auto", task_id="finalize-task")
    assert final_payload["status"] == "completed"
    assert final_payload["volume_next_file_name"] == "two.pdf"
    second_job_id = final_payload["volume_next_job_id"]
    assert second_job_id
    assert second_job_id != first_job_id

    detail_res = client.get("/admin/volume-sets/batch-002")
    assert detail_res.status_code == 200
    files = detail_res.json()["item"]["files"]
    assert files[0]["file_name"] == "one.pdf"
    assert files[0]["job_id"] == first_job_id
    assert files[0]["volume_status"] == "completed"
    assert files[1]["file_name"] == "two.pdf"
    assert files[1]["job_id"] == second_job_id
    assert files[1]["volume_status"] == "queued"
    assert files[1]["can_start"] is False

    second_meta = repo.read_json(repo.path(second_job_id, "meta.json"), default={})
    assert second_meta["source_tag"] == "VT"
    assert second_meta["source_category"] == "volume_test"
    assert second_meta["volume_set_name"] == "batch-002"
    assert second_meta["volume_file_name"] == "two.pdf"


def test_admin_can_delete_individual_volume_set(client, monkeypatch, tmp_path: Path):
    storage_root = tmp_path / "storage"
    target_dir = storage_root / "delete-me"
    target_dir.mkdir(parents=True, exist_ok=True)
    (target_dir / "one.pdf").write_bytes(b"file-one")
    (target_dir / "two.pdf").write_bytes(b"file-two")
    _patch_volume_storage_roots(monkeypatch, storage_root)

    client.post("/auth/logout")
    admin_login = client.post("/auth/login", data={"username": "admin", "password": "admin123"})
    assert admin_login.status_code == 200

    delete_res = client.delete("/admin/volume-sets/delete-me")
    assert delete_res.status_code == 200
    payload = delete_res.json()
    assert payload["ok"] is True
    assert payload["set_name"] == "delete-me"
    assert payload["deleted"] is True
    assert payload["deleted_files"] == 2
    assert not target_dir.exists()

    list_res = client.get("/admin/volume-sets")
    assert list_res.status_code == 200
    assert list_res.json()["items"] == []


def test_admin_can_clear_all_volume_sets(client, monkeypatch, tmp_path: Path):
    storage_root = tmp_path / "storage"
    legacy_root = tmp_path / "legacy-storage"
    first_dir = storage_root / "batch-a"
    second_dir = legacy_root / "batch-b"
    first_dir.mkdir(parents=True, exist_ok=True)
    second_dir.mkdir(parents=True, exist_ok=True)
    (first_dir / "one.pdf").write_bytes(b"file-one")
    (second_dir / "two.pdf").write_bytes(b"file-two")
    _patch_volume_storage_roots(monkeypatch, storage_root, legacy_root)

    client.post("/auth/logout")
    admin_login = client.post("/auth/login", data={"username": "admin", "password": "admin123"})
    assert admin_login.status_code == 200

    clear_res = client.delete("/admin/volume-sets")
    assert clear_res.status_code == 200
    payload = clear_res.json()
    assert payload["ok"] is True
    assert payload["deleted"] is True
    assert payload["cleared_sets"] == 2
    assert payload["cleared_files"] == 2
    assert not first_dir.exists()
    assert not second_dir.exists()

    list_res = client.get("/admin/volume-sets")
    assert list_res.status_code == 200
    assert list_res.json()["items"] == []


def test_volume_sets_migrate_from_repo_storage_to_persistent_data_dir(client, monkeypatch, tmp_path: Path):
    storage_root = tmp_path / "persistent_data" / ".volume_sets"
    legacy_root = tmp_path / "repo_storage"
    target_dir = legacy_root / "legacy-batch"
    target_dir.mkdir(parents=True, exist_ok=True)
    (target_dir / "one.pdf").write_bytes(b"legacy-file")
    (target_dir / ".volume-set.json").write_text(
        json.dumps({"set_name": "legacy-batch", "uploader_username": "eval_test", "uploader_role": "evaluator"}),
        encoding="utf-8",
    )
    _patch_volume_storage_roots(monkeypatch, storage_root, legacy_root)

    list_res = client.get("/volume/sets")
    assert list_res.status_code == 200
    payload = list_res.json()
    assert payload["ok"] is True
    assert payload["items"][0]["set_name"] == "legacy-batch"
    assert payload["items"][0]["uploader_username"] == "eval_test"

    migrated_dir = storage_root / "legacy-batch"
    assert migrated_dir.is_dir()
    assert (migrated_dir / "one.pdf").read_bytes() == b"legacy-file"
    assert not target_dir.exists()
