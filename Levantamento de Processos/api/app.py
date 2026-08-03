"""FastAPI application — stable entrypoint for the web UI."""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.auth import get_expected_api_key
from api.jobs.store import JobStore
from api.jobs.worker import SearchJobWorker
from api.routes.searches import router as searches_router
from tjsp_pipeline.config import Settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def _cors_origins() -> list[str]:
    """Comma-separated API_CORS_ORIGINS; default empty (platform uses server-side proxy)."""
    raw = (os.getenv("API_CORS_ORIGINS") or "").strip()
    if not raw:
        return []
    if raw == "*":
        return ["*"]
    return [o.strip() for o in raw.split(",") if o.strip()]


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = Settings.load()
    settings.ensure_dirs()
    store = JobStore()
    worker = SearchJobWorker(store, settings)
    app.state.settings = settings
    app.state.job_store = store
    app.state.worker = worker
    app.state.api_token = get_expected_api_key()
    worker.start()
    allow_ips = bool((os.getenv("API_ALLOWED_IPS") or "").strip())
    logger.info(
        "API ready | final_output=%s refactor=%s calculo=%s api_key=%s ip_allowlist=%s",
        settings.final_output_dir,
        settings.refactor_path,
        settings.calculo_api_configured,
        "set" if app.state.api_token else "MISSING",
        "on" if allow_ips else "off",
    )
    try:
        yield
    finally:
        worker.stop()


app = FastAPI(
    title="dashboard-backend TJSP API",
    version="1.0.0",
    description=(
        "Async search jobs over the e-SAJ scrape + REFACTOR pipeline. "
        "POST /api/v1/searches then poll GET /api/v1/searches/{job_id}."
    ),
    lifespan=lifespan,
)

_origins = _cors_origins()
if _origins:
    # Browser clients only; platform BFF does not need CORS.
    app.add_middleware(
        CORSMiddleware,
        allow_origins=_origins,
        allow_credentials=_origins != ["*"],
        allow_methods=["GET", "POST", "OPTIONS"],
        allow_headers=["Authorization", "Content-Type", "X-API-Key", "X-API-Token"],
    )

app.include_router(searches_router, prefix="/api/v1", tags=["searches"])
