"""Authentication helpers for simple file-backed users and in-memory sessions."""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import secrets
from threading import Lock
from typing import Dict, Optional

from fastapi import HTTPException
from app.paths import get_data_dir
from app.settings import load_settings

DATA_DIR = get_data_dir()
AUTH_DIR = DATA_DIR / "auth"
USERS_FILE = AUTH_DIR / "users.json"

DEFAULT_ADMIN_USERNAME = load_settings().admin_username
DEFAULT_ADMIN_PASSWORD = load_settings().admin_password
SESSION_COOKIE = "bank_stmt_session"

_SESSIONS: dict[str, dict] = {}
_LOCK = Lock()


def _hash_password(password: str, salt_hex: str) -> str:
    """Derive a PBKDF2 hash so stored passwords are not persisted in plain text."""
    salt = bytes.fromhex(salt_hex)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, 150_000)
    return digest.hex()


def _load_users() -> Dict[str, dict]:
    """Load the user store, creating an empty file the first time the app starts."""
    AUTH_DIR.mkdir(parents=True, exist_ok=True)
    if not USERS_FILE.exists():
        _write_users({})
    with open(USERS_FILE, encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        return {}
    return payload


def _write_users(users: Dict[str, dict]):
    """Persist the full user map atomically so partial writes do not corrupt auth data."""
    AUTH_DIR.mkdir(parents=True, exist_ok=True)
    tmp = USERS_FILE.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as handle:
        json.dump(users, handle, indent=2)
    os.replace(tmp, USERS_FILE)


def _normalize_username(username: str) -> str:
    cleaned = str(username or "").strip()
    if not cleaned:
        raise HTTPException(status_code=400, detail="username_required")
    if " " in cleaned:
        raise HTTPException(status_code=400, detail="username_no_spaces")
    return cleaned


def _validate_password(password: str | None, *, required: bool) -> str | None:
    normalized = None if password is None else str(password)
    if not required and (normalized is None or normalized == ""):
        return None
    if len(normalized or "") < 6:
        raise HTTPException(status_code=400, detail="password_too_short")
    return normalized


def _count_admins(users: Dict[str, dict]) -> int:
    return sum(1 for item in users.values() if str(item.get("role") or "").strip().lower() == "admin")


def _build_user_row(
    username: str,
    user: dict,
    *,
    acting_username: str | None = None,
    admin_count: int | None = None,
) -> dict:
    role = str(user.get("role") or "").strip().lower()
    total_admins = admin_count if admin_count is not None else (1 if role == "admin" else 0)
    is_current_user = bool(acting_username) and username == acting_username
    is_admin = role == "admin"
    is_last_admin = is_admin and total_admins <= 1
    return {
        "username": username,
        "role": role,
        "is_current_user": is_current_user,
        "is_admin": is_admin,
        "is_last_admin": is_last_admin,
        "can_change_role": not is_current_user and not is_last_admin,
        "can_delete": not is_current_user and not is_last_admin,
    }


def _sync_sessions_for_user(
    username: str,
    *,
    new_username: str | None = None,
    new_role: str | None = None,
    delete: bool = False,
):
    for token, payload in list(_SESSIONS.items()):
        if str(payload.get("username") or "") != username:
            continue
        if delete:
            _SESSIONS.pop(token, None)
            continue
        if new_username is not None:
            payload["username"] = new_username
        if new_role is not None:
            payload["role"] = new_role


def ensure_admin_exists():
    """Seed the default admin account once so a fresh install is always accessible."""
    if not load_settings().seed_default_users:
        return
    with _LOCK:
        users = _load_users()
        if DEFAULT_ADMIN_USERNAME in users:
            return
        salt = secrets.token_hex(16)
        users[DEFAULT_ADMIN_USERNAME] = {
            "username": DEFAULT_ADMIN_USERNAME,
            "role": "admin",
            "salt": salt,
            "password_hash": _hash_password(DEFAULT_ADMIN_PASSWORD, salt),
        }
        _write_users(users)


def authenticate_user(username: str, password: str) -> dict:
    """Validate credentials and return only the public identity fields needed by the app."""
    ensure_admin_exists()
    users = _load_users()
    user = users.get(username)
    if not user:
        raise HTTPException(status_code=401, detail="invalid_credentials")
    expected = user.get("password_hash", "")
    actual = _hash_password(password, user.get("salt", ""))
    if not hmac.compare_digest(expected, actual):
        raise HTTPException(status_code=401, detail="invalid_credentials")
    return {"username": user["username"], "role": user["role"]}


def create_session(user: dict) -> str:
    """Create a process-local session token for the authenticated user."""
    token = secrets.token_urlsafe(32)
    _SESSIONS[token] = {"username": user["username"], "role": user["role"]}
    return token


def destroy_session(token: str):
    """Invalidate a session token if it exists."""
    _SESSIONS.pop(token, None)


def get_user_by_session(token: str | None) -> Optional[dict]:
    """Resolve a session cookie token back to the current user payload."""
    if not token:
        return None
    payload = _SESSIONS.get(token)
    if not payload:
        return None
    return {"username": payload["username"], "role": payload["role"]}


def create_evaluator_account(username: str, password: str):
    """Validate and store a new evaluator login in the JSON-backed user store."""
    cleaned = _normalize_username(username)
    validated_password = _validate_password(password, required=True)

    with _LOCK:
        users = _load_users()
        if cleaned in users:
            raise HTTPException(status_code=400, detail="username_exists")
        salt = secrets.token_hex(16)
        users[cleaned] = {
            "username": cleaned,
            "role": "evaluator",
            "salt": salt,
            "password_hash": _hash_password(validated_password or "", salt),
        }
        _write_users(users)


def list_users(*, acting_username: str | None = None) -> list[dict]:
    ensure_admin_exists()
    with _LOCK:
        users = _load_users()
        admin_count = _count_admins(users)
        rows = [
            _build_user_row(username, user, acting_username=acting_username, admin_count=admin_count)
            for username, user in users.items()
        ]
    return sorted(rows, key=lambda item: (item["role"] != "admin", item["username"].lower()))


def update_user_account(
    target_username: str,
    *,
    acting_username: str,
    next_username: str | None = None,
    next_password: str | None = None,
    next_role: str | None = None,
) -> dict:
    ensure_admin_exists()
    cleaned_target = _normalize_username(target_username)
    cleaned_username = _normalize_username(next_username) if next_username is not None else None
    validated_password = _validate_password(next_password, required=False)
    normalized_role = None if next_role is None else str(next_role or "").strip().lower()
    if normalized_role is not None and normalized_role not in {"admin", "evaluator"}:
        raise HTTPException(status_code=400, detail="invalid_role")

    with _LOCK:
        users = _load_users()
        existing = users.get(cleaned_target)
        if not existing:
            raise HTTPException(status_code=404, detail="user_not_found")

        current_role = str(existing.get("role") or "").strip().lower()
        desired_role = normalized_role if normalized_role is not None else current_role
        desired_username = cleaned_username or cleaned_target

        if desired_username != cleaned_target and desired_username in users:
            raise HTTPException(status_code=400, detail="username_exists")

        if cleaned_target == acting_username and desired_role != current_role:
            raise HTTPException(status_code=400, detail="current_admin_role_change_forbidden")

        admin_count = _count_admins(users)
        if current_role == "admin" and desired_role != "admin" and admin_count <= 1:
            raise HTTPException(status_code=400, detail="last_admin_role_change_forbidden")

        updated = dict(existing)
        updated["username"] = desired_username
        updated["role"] = desired_role
        if validated_password:
            salt = secrets.token_hex(16)
            updated["salt"] = salt
            updated["password_hash"] = _hash_password(validated_password, salt)

        if desired_username != cleaned_target:
            users.pop(cleaned_target, None)
        users[desired_username] = updated
        _write_users(users)
        _sync_sessions_for_user(
            cleaned_target,
            new_username=desired_username if desired_username != cleaned_target else None,
            new_role=desired_role if desired_role != current_role else None,
        )
        return _build_user_row(
            desired_username,
            updated,
            acting_username=desired_username if acting_username == cleaned_target else acting_username,
            admin_count=_count_admins(users),
        )


def delete_user_account(target_username: str, *, acting_username: str) -> None:
    ensure_admin_exists()
    cleaned_target = _normalize_username(target_username)

    with _LOCK:
        users = _load_users()
        existing = users.get(cleaned_target)
        if not existing:
            raise HTTPException(status_code=404, detail="user_not_found")
        if cleaned_target == acting_username:
            raise HTTPException(status_code=400, detail="current_admin_delete_forbidden")

        role = str(existing.get("role") or "").strip().lower()
        if role == "admin" and _count_admins(users) <= 1:
            raise HTTPException(status_code=400, detail="last_admin_delete_forbidden")

        users.pop(cleaned_target, None)
        _write_users(users)
        _sync_sessions_for_user(cleaned_target, delete=True)


def clear_all_sessions():
    """Drop every active in-memory session, typically after admin maintenance actions."""
    _SESSIONS.clear()
