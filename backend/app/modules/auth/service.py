from __future__ import annotations

import hashlib
import hmac
import json
import os
import secrets
from pathlib import Path
from threading import Lock
from typing import Dict, Optional

from fastapi import HTTPException

DATA_DIR = Path(os.getenv("DATA_DIR", "./data"))
AUTH_DIR = DATA_DIR / "auth"
USERS_FILE = AUTH_DIR / "users.json"

DEFAULT_ADMIN_USERNAME = os.getenv("ADMIN_USERNAME", "admin")
DEFAULT_ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "admin123")
SESSION_COOKIE = "bank_stmt_session"

_SESSIONS: dict[str, dict] = {}
_LOCK = Lock()


def _hash_password(password: str, salt_hex: str) -> str:
    salt = bytes.fromhex(salt_hex)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, 150_000)
    return digest.hex()


def _load_users() -> Dict[str, dict]:
    AUTH_DIR.mkdir(parents=True, exist_ok=True)
    if not USERS_FILE.exists():
        _write_users({})
    with open(USERS_FILE, encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        return {}
    return payload


def _write_users(users: Dict[str, dict]):
    AUTH_DIR.mkdir(parents=True, exist_ok=True)
    tmp = USERS_FILE.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as handle:
        json.dump(users, handle, indent=2)
    os.replace(tmp, USERS_FILE)


def ensure_admin_exists():
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
    token = secrets.token_urlsafe(32)
    _SESSIONS[token] = {"username": user["username"], "role": user["role"]}
    return token


def destroy_session(token: str):
    _SESSIONS.pop(token, None)


def get_user_by_session(token: str | None) -> Optional[dict]:
    if not token:
        return None
    payload = _SESSIONS.get(token)
    if not payload:
        return None
    return {"username": payload["username"], "role": payload["role"]}


def create_evaluator_account(username: str, password: str):
    cleaned = str(username or "").strip()
    if not cleaned:
        raise HTTPException(status_code=400, detail="username_required")
    if " " in cleaned:
        raise HTTPException(status_code=400, detail="username_no_spaces")
    if len(password or "") < 6:
        raise HTTPException(status_code=400, detail="password_too_short")

    with _LOCK:
        users = _load_users()
        if cleaned in users:
            raise HTTPException(status_code=400, detail="username_exists")
        salt = secrets.token_hex(16)
        users[cleaned] = {
            "username": cleaned,
            "role": "evaluator",
            "salt": salt,
            "password_hash": _hash_password(password, salt),
        }
        _write_users(users)


def clear_all_sessions():
    _SESSIONS.clear()

