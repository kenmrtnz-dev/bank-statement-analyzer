from __future__ import annotations

from fastapi import Cookie, Depends, HTTPException

from app.modules.auth.service import SESSION_COOKIE, get_user_by_session


def get_current_user(session_token: str | None = Cookie(default=None, alias=SESSION_COOKIE)):
    user = get_user_by_session(session_token)
    if not user:
        raise HTTPException(status_code=401, detail="not_authenticated")
    return user


def require_evaluator_or_admin(user=Depends(get_current_user)):
    role = user.get("role")
    if role not in {"admin", "evaluator"}:
        raise HTTPException(status_code=403, detail="forbidden")
    return user


def require_evaluator(user=Depends(get_current_user)):
    role = user.get("role")
    if role != "evaluator":
        raise HTTPException(status_code=403, detail="evaluator_only")
    return user


def require_admin(user=Depends(get_current_user)):
    role = user.get("role")
    if role != "admin":
        raise HTTPException(status_code=403, detail="admin_only")
    return user
