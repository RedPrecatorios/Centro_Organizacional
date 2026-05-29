"""
Jobs de análise processual (REFACTOR_TJSP) para a página Memória de Cálculo.
"""

from __future__ import annotations

import json
import os
import subprocess
import threading
import time
import uuid
from pathlib import Path
from typing import Any

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
_DEFAULT_REFACTOR_ROOT = _PROJECT_ROOT.parent / "REFACTOR_TJSP"


def _refactor_root() -> Path:
    raw = (os.getenv("REFACTOR_TJSP_PATH") or "").strip()
    if raw:
        return Path(raw).resolve()
    return _DEFAULT_REFACTOR_ROOT.resolve()


def _refactor_python() -> Path:
    raw = (os.getenv("REFACTOR_TJSP_PYTHON") or "").strip()
    if raw:
        return Path(raw).resolve()
    venv_py = _refactor_root() / "venv" / "bin" / "python"
    if venv_py.is_file():
        return venv_py
    return Path("python3")


def _jobs_dir() -> Path:
    d = _refactor_root() / "output" / "analise_processual_jobs"
    d.mkdir(parents=True, exist_ok=True)
    return d


_lock = threading.Lock()
_jobs: dict[str, dict[str, Any]] = {}


def _read_json(path: Path) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None
    return data if isinstance(data, dict) else None


def _run_subprocess(
    job_id: str,
    *,
    processo: str,
    incidente: str,
    progress_path: Path,
    result_path: Path,
) -> None:
    root = _refactor_root()
    script = root / "run_single_case.py"
    python = _refactor_python()
    cmd = [
        str(python),
        str(script),
        "--processo",
        processo,
        "--incidente",
        incidente,
        "--progress-file",
        str(progress_path),
        "--result-file",
        str(result_path),
    ]
    try:
        proc = subprocess.run(
            cmd,
            cwd=str(root),
            capture_output=True,
            text=True,
            timeout=int((os.getenv("REFACTOR_ANALISE_TIMEOUT") or "1800").strip() or "1800"),
        )
        with _lock:
            job = _jobs.get(job_id)
            if job is None:
                return
            job["returncode"] = proc.returncode
            if proc.returncode != 0 and not result_path.is_file():
                job["error"] = (proc.stderr or proc.stdout or "Falha na análise")[:2000]
                job["status"] = "error"
                job["done"] = True
    except subprocess.TimeoutExpired:
        with _lock:
            job = _jobs.get(job_id)
            if job:
                job["status"] = "error"
                job["done"] = True
                job["error"] = "Tempo máximo excedido na análise processual."
    except Exception as exc:
        with _lock:
            job = _jobs.get(job_id)
            if job:
                job["status"] = "error"
                job["done"] = True
                job["error"] = str(exc)
    finally:
        with _lock:
            job = _jobs.get(job_id)
            if job and result_path.is_file():
                result = _read_json(result_path)
                if result:
                    job["result"] = result
                    job["done"] = True
                    job["status"] = "done" if result.get("ok") else "error"
                    if not result.get("ok"):
                        job["error"] = result.get("error") or "Análise falhou"
            elif job and job.get("status") == "running":
                job["done"] = True
                job["status"] = "error"
                job["error"] = job.get("error") or "Análise encerrada sem resultado."


def start_job(*, processo: str, incidente: str) -> dict[str, Any]:
    root = _refactor_root()
    if not (root / "run_single_case.py").is_file():
        raise FileNotFoundError(
            f"REFACTOR_TJSP não encontrado em {root}. Defina REFACTOR_TJSP_PATH no .env."
        )

    job_id = uuid.uuid4().hex[:16]
    job_dir = _jobs_dir() / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    progress_path = job_dir / "progress.json"
    result_path = job_dir / "result.json"

    progress_path.write_text(
        json.dumps({"message": "A iniciar…", "percent": 0.0, "done": False}, ensure_ascii=False),
        encoding="utf-8",
    )

    job: dict[str, Any] = {
        "job_id": job_id,
        "status": "running",
        "done": False,
        "processo": processo,
        "incidente": incidente,
        "progress_path": str(progress_path),
        "result_path": str(result_path),
        "created_at": time.time(),
        "error": None,
        "result": None,
    }
    with _lock:
        _jobs[job_id] = job

    thread = threading.Thread(
        target=_run_subprocess,
        kwargs={
            "job_id": job_id,
            "processo": processo,
            "incidente": incidente,
            "progress_path": progress_path,
            "result_path": result_path,
        },
        daemon=True,
    )
    thread.start()
    return {"job_id": job_id, "status": "running"}


def get_job_status(job_id: str) -> dict[str, Any] | None:
    with _lock:
        job = _jobs.get(job_id)
        if job is None:
            return None
        snapshot = dict(job)

    progress_path = Path(snapshot.get("progress_path") or "")
    progress = _read_json(progress_path) if progress_path else None
    if progress:
        snapshot["message"] = progress.get("message")
        snapshot["percent"] = progress.get("percent", 0.0)
        if progress.get("done") and snapshot.get("status") == "running":
            snapshot["percent"] = max(float(snapshot.get("percent") or 0), 99.0)

    result_path = Path(snapshot.get("result_path") or "")
    if snapshot.get("result") is None and result_path.is_file():
        result = _read_json(result_path)
        if result:
            snapshot["result"] = result
            snapshot["done"] = True
            snapshot["status"] = "done" if result.get("ok") else "error"
            if not result.get("ok"):
                snapshot["error"] = result.get("error")
    elif (
        progress
        and progress.get("done")
        and result_path.is_file()
        and not snapshot.get("done")
    ):
        result = _read_json(result_path)
        if result:
            snapshot["result"] = result
            snapshot["done"] = True
            snapshot["status"] = "done" if result.get("ok") else "error"
            if not result.get("ok"):
                snapshot["error"] = result.get("error")
            with _lock:
                live = _jobs.get(job_id)
                if live is not None:
                    live["result"] = result
                    live["done"] = True
                    live["status"] = snapshot["status"]
                    if snapshot.get("error"):
                        live["error"] = snapshot["error"]

    return snapshot
