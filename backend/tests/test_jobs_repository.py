from pathlib import Path
from decimal import Decimal

from app.jobs.repository import JobsRepository


def test_read_json_returns_default_for_invalid_json(tmp_path: Path):
    repo = JobsRepository(tmp_path)
    target = repo.jobs_dir / "broken.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("{not valid json", encoding="utf-8")

    payload = repo.read_json(target, default={"ok": True})
    assert payload == {"ok": True}


def test_write_json_serializes_decimal_values(tmp_path: Path):
    repo = JobsRepository(tmp_path)
    target = repo.jobs_dir / "decimal.json"
    payload = {"score": Decimal("1.25"), "nested": {"amount": Decimal("42.00")}}

    repo.write_json(target, payload)
    loaded = repo.read_json(target, default={})

    assert loaded["score"] == 1.25
    assert loaded["nested"]["amount"] == 42.0
