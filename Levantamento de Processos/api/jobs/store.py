"""SQLite-backed async search jobs."""

from __future__ import annotations

import json
import sqlite3
import threading
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from tjsp_pipeline.config import PROJECT_ROOT


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


@dataclass
class JobRecord:
    job_id: str
    status: str
    nome: str  # label da query (compat)
    search_kind: str
    query_value: str
    filter_nome: str | None
    progress: str | None
    result_json: dict[str, Any]
    error: str | None
    created_at: str
    updated_at: str


class JobStore:
    def __init__(self, db_path: Path | None = None) -> None:
        self.db_path = db_path or (PROJECT_ROOT / "logs" / "api_jobs.sqlite3")
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS search_jobs (
                        job_id TEXT PRIMARY KEY,
                        status TEXT NOT NULL,
                        nome TEXT NOT NULL,
                        search_kind TEXT NOT NULL DEFAULT 'nome',
                        query_value TEXT NOT NULL DEFAULT '',
                        filter_nome TEXT,
                        progress TEXT,
                        result_json TEXT NOT NULL DEFAULT '{}',
                        error TEXT,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL
                    )
                    """
                )
                cols = {
                    r[1]
                    for r in conn.execute("PRAGMA table_info(search_jobs)").fetchall()
                }
                if "search_kind" not in cols:
                    conn.execute(
                        "ALTER TABLE search_jobs ADD COLUMN search_kind TEXT NOT NULL DEFAULT 'nome'"
                    )
                if "query_value" not in cols:
                    conn.execute(
                        "ALTER TABLE search_jobs ADD COLUMN query_value TEXT NOT NULL DEFAULT ''"
                    )
                if "filter_nome" not in cols:
                    conn.execute(
                        "ALTER TABLE search_jobs ADD COLUMN filter_nome TEXT"
                    )
                conn.commit()

    def create(
        self,
        *,
        label: str,
        search_kind: str = "nome",
        query_value: str = "",
        filter_nome: str | None = None,
    ) -> JobRecord:
        job_id = uuid.uuid4().hex
        now = _utc_now()
        kind = (search_kind or "nome").strip().lower() or "nome"
        value = (query_value or label or "").strip()
        filtro = (filter_nome or "").strip() or None
        record = JobRecord(
            job_id=job_id,
            status="queued",
            nome=label,
            search_kind=kind,
            query_value=value,
            filter_nome=filtro,
            progress="queued",
            result_json={},
            error=None,
            created_at=now,
            updated_at=now,
        )
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO search_jobs
                    (job_id, status, nome, search_kind, query_value, filter_nome,
                     progress, result_json, error, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        record.job_id,
                        record.status,
                        record.nome,
                        record.search_kind,
                        record.query_value,
                        record.filter_nome,
                        record.progress,
                        "{}",
                        None,
                        record.created_at,
                        record.updated_at,
                    ),
                )
                conn.commit()
        return record

    def get(self, job_id: str) -> JobRecord | None:
        with self._lock:
            with self._connect() as conn:
                row = conn.execute(
                    "SELECT * FROM search_jobs WHERE job_id = ?",
                    (job_id,),
                ).fetchone()
        if row is None:
            return None
        return self._row_to_record(row)

    def claim_next_queued(self) -> JobRecord | None:
        """Atomically pick the oldest queued job and mark it running."""
        with self._lock:
            with self._connect() as conn:
                row = conn.execute(
                    """
                    SELECT * FROM search_jobs
                    WHERE status = 'queued'
                    ORDER BY created_at ASC
                    LIMIT 1
                    """
                ).fetchone()
                if row is None:
                    return None
                now = _utc_now()
                conn.execute(
                    """
                    UPDATE search_jobs
                    SET status = 'running', progress = ?, updated_at = ?
                    WHERE job_id = ? AND status = 'queued'
                    """,
                    ("running", now, row["job_id"]),
                )
                conn.commit()
                updated = conn.execute(
                    "SELECT * FROM search_jobs WHERE job_id = ?",
                    (row["job_id"],),
                ).fetchone()
        return self._row_to_record(updated) if updated else None

    def update(
        self,
        job_id: str,
        *,
        status: str | None = None,
        progress: str | None = None,
        result_json: dict[str, Any] | None = None,
        error: str | None = None,
    ) -> None:
        fields: list[str] = ["updated_at = ?"]
        values: list[Any] = [_utc_now()]
        if status is not None:
            fields.append("status = ?")
            values.append(status)
        if progress is not None:
            fields.append("progress = ?")
            values.append(progress)
        if result_json is not None:
            fields.append("result_json = ?")
            values.append(json.dumps(result_json, ensure_ascii=False))
        if error is not None:
            fields.append("error = ?")
            values.append(error)
        values.append(job_id)
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    f"UPDATE search_jobs SET {', '.join(fields)} WHERE job_id = ?",
                    values,
                )
                conn.commit()

    @staticmethod
    def _row_to_record(row: sqlite3.Row) -> JobRecord:
        raw = row["result_json"] or "{}"
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            payload = {}
        keys = row.keys()
        kind = row["search_kind"] if "search_kind" in keys else "nome"
        qv = row["query_value"] if "query_value" in keys else row["nome"]
        filtro = row["filter_nome"] if "filter_nome" in keys else None
        return JobRecord(
            job_id=row["job_id"],
            status=row["status"],
            nome=row["nome"],
            search_kind=(kind or "nome"),
            query_value=(qv or row["nome"] or ""),
            filter_nome=(str(filtro).strip() if filtro else None),
            progress=row["progress"],
            result_json=payload if isinstance(payload, dict) else {},
            error=row["error"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )
