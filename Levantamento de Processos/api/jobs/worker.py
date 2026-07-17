"""Background worker that drains queued search jobs."""

from __future__ import annotations

import logging
import threading
import time
from typing import TYPE_CHECKING

from api.jobs.store import JobStore
from api.services.orchestrator import run_search_job
from tjsp_pipeline.config import Settings

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


class SearchJobWorker:
    def __init__(
        self,
        store: JobStore,
        settings: Settings,
        *,
        poll_seconds: float = 1.0,
    ) -> None:
        self.store = store
        self.settings = settings
        self.poll_seconds = poll_seconds
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._loop,
            name="search-job-worker",
            daemon=True,
        )
        self._thread.start()
        logger.info("SearchJobWorker started")

    def stop(self, timeout: float = 5.0) -> None:
        self._stop.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=timeout)
        logger.info("SearchJobWorker stopped")

    def _loop(self) -> None:
        while not self._stop.is_set():
            job = self.store.claim_next_queued()
            if job is None:
                self._stop.wait(self.poll_seconds)
                continue
            logger.info(
                "Job claimed | id=%s kind=%s query=%s",
                job.job_id,
                job.search_kind,
                job.nome,
            )

            def on_progress(msg: str, _job_id: str = job.job_id) -> None:
                self.store.update(_job_id, progress=msg)

            try:
                result = run_search_job(
                    self.settings,
                    search_kind=job.search_kind,
                    query_value=job.query_value or job.nome,
                    filter_nome=job.filter_nome,
                    on_progress=on_progress,
                )
                self.store.update(
                    job.job_id,
                    status="done",
                    progress="done",
                    result_json=result,
                    error=None,
                )
                logger.info(
                    "Job done | id=%s aptos=%s inaptos=%s",
                    job.job_id,
                    len(result.get("processos_aptos") or []),
                    len(result.get("processos_inaptos") or []),
                )
            except Exception as exc:
                logger.exception("Job failed | id=%s", job.job_id)
                self.store.update(
                    job.job_id,
                    status="failed",
                    progress="failed",
                    error=str(exc),
                    result_json={
                        "errors": [str(exc)],
                        "processos_aptos": [],
                        "processos_inaptos": [],
                        "skipped": [],
                    },
                )
            # Brief pause between jobs to avoid hammering Chrome/proxy.
            time.sleep(0.5)
