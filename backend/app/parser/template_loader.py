"""Bank template loading helpers."""

from pathlib import Path
import json

TEMPLATE_DIR = Path(__file__).resolve().parent / "templates"


def load_template(bank_name: str) -> dict:
    """Load a bank template or fall back to the generic template."""
    requested = TEMPLATE_DIR / f"{bank_name}.json"
    fallback = TEMPLATE_DIR / "generic.json"
    target = requested if requested.exists() else fallback
    with target.open("r", encoding="utf-8") as handle:
        return json.load(handle)
