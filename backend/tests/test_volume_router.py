import importlib
import io
import json
import zipfile
from pathlib import Path

from app.jobs import service as jobs_service

volume_router_module = importlib.import_module("app.volume.router")


def test_volume_page_loads_for_authenticated_user(client, monkeypatch, tmp_path: Path):
    storage_root = tmp_path / "storage"
    target_dir = storage_root / "set-alpha"
    target_dir.mkdir(parents=True, exist_ok=True)
    (target_dir / "one.pdf").write_bytes(b"file-one")
    monkeypatch.setattr(volume_router_module, "STORAGE_ROOT", storage_root)

    res = client.get("/volume")
    assert res.status_code == 200
    assert "Volume Upload" in res.text
    assert "set_name" in res.text
    assert '"set_name": "set-alpha"' in res.text


def test_volume_upload_saves_multiple_files(client, monkeypatch, tmp_path: Path):
    storage_root = tmp_path / "storage"
    monkeypatch.setattr(volume_router_module, "STORAGE_ROOT", storage_root)

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
    monkeypatch.setattr(volume_router_module, "STORAGE_ROOT", storage_root)

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
    monkeypatch.setattr(volume_router_module, "STORAGE_ROOT", storage_root)
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
