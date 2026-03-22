from __future__ import annotations

from typing import Any

__all__ = ["app", "create_app"]


def __getattr__(name: str) -> Any:
    if name in {"app", "create_app"}:
        from app.main import app, create_app

        exports = {"app": app, "create_app": create_app}
        return exports[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
