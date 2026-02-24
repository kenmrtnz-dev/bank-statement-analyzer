from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path
import os

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from app.api.routers.ui import router as ui_router
from app.modules.admin.router import router as admin_router
from app.modules.auth.router import router as auth_router
from app.modules.crm.router import router as crm_router
from app.modules.jobs.router import router as jobs_router

DATA_DIR = Path(os.getenv("DATA_DIR", "./data"))


def _bootstrap_dirs():
    root = Path(DATA_DIR)
    (root / "jobs").mkdir(parents=True, exist_ok=True)
    (root / "exports").mkdir(parents=True, exist_ok=True)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    _bootstrap_dirs()
    yield


def create_app() -> FastAPI:
    app = FastAPI(title="Bank Statement Analyzer API", lifespan=lifespan)

    static_dir = Path(__file__).resolve().parent / "static"
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

    app.include_router(ui_router)
    app.include_router(auth_router)
    app.include_router(jobs_router)
    app.include_router(crm_router)
    app.include_router(admin_router)

    return app


app = create_app()
