from pathlib import Path

from app.modules.jobs.repository import JobsRepository


def test_read_json_returns_default_for_invalid_json(tmp_path: Path):
    repo = JobsRepository(tmp_path)
    target = repo.jobs_dir / "broken.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("{not valid json", encoding="utf-8")

    payload = repo.read_json(target, default={"ok": True})
    assert payload == {"ok": True}
