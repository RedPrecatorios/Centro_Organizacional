"""FastAPI application — stable entrypoint for the web UI."""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.jobs.store import JobStore
from api.jobs.worker import SearchJobWorker
from api.routes.searches import router as searches_router
from tjsp_pipeline.config import Settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = Settings.load()
    settings.ensure_dirs()
    store = JobStore()
    worker = SearchJobWorker(store, settings)
    app.state.settings = settings
    app.state.job_store = store
    app.state.worker = worker
    app.state.api_token = (os.getenv("API_TOKEN") or "").strip()
    worker.start()
    logger.info(
        "API ready | final_output=%s refactor=%s calculo=%s token=%s",
        settings.final_output_dir,
        settings.refactor_path,
        settings.calculo_api_configured,
        "set" if app.state.api_token else "MISSING",
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

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(searches_router, prefix="/api/v1", tags=["searches"])
