from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse

from app.modules.admin.service import get_ui_settings
from app.modules.auth.deps import get_current_user, require_admin, require_evaluator_or_admin
from app.modules.auth.service import (
    SESSION_COOKIE,
    authenticate_user,
    create_session,
    destroy_session,
    ensure_admin_exists,
)

router = APIRouter()


@router.get("/login")
def login_page():
    static_file = Path(__file__).resolve().parents[2] / "static" / "login.html"
    response = FileResponse(static_file)
    response.headers["Cache-Control"] = "no-store"
    return response


@router.post("/auth/login")
def login(username: str = Form(...), password: str = Form(...)):
    ensure_admin_exists()
    user = authenticate_user(username=username, password=password)
    token = create_session(user)
    response = JSONResponse({"ok": True, "username": user["username"], "role": user["role"]})
    response.set_cookie(
        key=SESSION_COOKIE,
        value=token,
        httponly=True,
        samesite="lax",
        secure=False,
        max_age=60 * 60 * 12,
    )
    return response


@router.post("/auth/logout")
def logout(request: Request):
    token = request.cookies.get(SESSION_COOKIE)
    if token:
        destroy_session(token)
    out = JSONResponse({"ok": True})
    out.delete_cookie(SESSION_COOKIE)
    return out


@router.get("/auth/me")
def me(user=Depends(get_current_user)):
    return user


@router.get("/ui/settings")
def ui_settings(_user=Depends(require_evaluator_or_admin)):
    return {"ok": True, **get_ui_settings()}


@router.get("/admin")
def admin_page(_user=Depends(require_admin)):
    static_file = Path(__file__).resolve().parents[2] / "static" / "admin.html"
    response = FileResponse(static_file)
    response.headers["Cache-Control"] = "no-store"
    return response


@router.get("/logout")
def logout_redirect():
    response = RedirectResponse(url="/login", status_code=303)
    response.delete_cookie(SESSION_COOKIE)
    return response
