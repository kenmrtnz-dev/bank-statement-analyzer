"""Helpers for producing JSON-safe payloads."""

from __future__ import annotations

import math
from decimal import Decimal
from typing import Any


def make_json_safe(value: Any) -> Any:
    """Recursively normalize values that Python allows but strict JSON rejects."""
    if isinstance(value, dict):
        return {key: make_json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [make_json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [make_json_safe(item) for item in value]
    if isinstance(value, set):
        return [make_json_safe(item) for item in value]
    if isinstance(value, Decimal):
        if not value.is_finite():
            return None
        return float(value)
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    return value


def json_default(value: Any) -> Any:
    """Normalize supported non-standard JSON types during serialization."""
    if isinstance(value, Decimal):
        if not value.is_finite():
            return None
        return float(value)
    raise TypeError(f"Object of type {value.__class__.__name__} is not JSON serializable")
