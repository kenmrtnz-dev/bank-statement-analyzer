"""Application entrypoint for the FastAPI API and bundled static UI."""

from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from app.api.routers.ui import router as ui_router
from app.admin.router import router as admin_router
from app.auth.router import router as auth_router
from app.crm.router import router as crm_router
from app.jobs.repository import ensure_job_pages_schema, ensure_job_results_raw_schema, ensure_jobs_schema, ensure_transactions_schema
from app.jobs.router import router as jobs_router
from app.paths import get_data_dir
from app.settings import load_settings
from app.volume.router import router as volume_router

DATA_DIR = get_data_dir()


def _bootstrap_dirs():
    """Create the on-disk folders and DB tables the app expects at runtime."""
    load_settings().validate_for_api()
    root = Path(DATA_DIR)
    (root / "jobs").mkdir(parents=True, exist_ok=True)
    (root / "exports").mkdir(parents=True, exist_ok=True)
    ensure_jobs_schema(root)
    ensure_job_pages_schema(root)
    ensure_transactions_schema(root)
    ensure_job_results_raw_schema(root)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    """Run startup bootstrapping once before the app begins serving requests."""
    _bootstrap_dirs()
    yield


def create_app() -> FastAPI:
    """Assemble the FastAPI app, static assets, and feature routers."""
    app = FastAPI(title="Bank Statement Analyzer API", lifespan=lifespan)

    # The frontend is served from the same process so the app can run as a single deployable unit.
    static_dir = Path(__file__).resolve().parent / "static"
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

    app.include_router(ui_router)
    app.include_router(auth_router)
    app.include_router(jobs_router)
    app.include_router(crm_router)
    app.include_router(admin_router)
    app.include_router(volume_router)

    return app


app = create_app()
