import importlib
import io
import zipfile
from pathlib import Path

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
    monkeypatch.setattr(volume_router_module, "STORAGE_ROOT", storage_root)

    list_res = client.get("/volume/sets")
    assert list_res.status_code == 200
    payload = list_res.json()
    assert payload["ok"] is True
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
