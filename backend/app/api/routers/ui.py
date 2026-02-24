from pathlib import Path

from fastapi import APIRouter, Request
from fastapi.responses import FileResponse, RedirectResponse

from app.modules.auth.service import SESSION_COOKIE, ensure_admin_exists, get_user_by_session

router = APIRouter()


def _render_app_or_redirect(request: Request):
    ensure_admin_exists()
    token = request.cookies.get(SESSION_COOKIE)
    user = get_user_by_session(token)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    if str(user.get("role") or "").lower() == "admin":
        return RedirectResponse(url="/admin", status_code=303)
    static_index = Path(__file__).resolve().parents[2] / "static" / "index.html"
    response = FileResponse(static_index)
    response.headers["Cache-Control"] = "no-store"
    return response


@router.get("/")
def home(request: Request):
    return _render_app_or_redirect(request)


@router.get("/uploads")
def uploads_page(request: Request):
    return _render_app_or_redirect(request)


@router.get("/processing")
def processing_page(request: Request):
    return _render_app_or_redirect(request)


@router.get("/evaluator")
def evaluator_page(request: Request):
    return _render_app_or_redirect(request)


@router.get("/health")
def health():
    return {"ok": True}
