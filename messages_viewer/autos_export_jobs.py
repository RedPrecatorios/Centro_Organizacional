"""Jobs de exportação de autos (REFACTOR_TJSP) — cumprimento, incidente e DEPRE."""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import threading
import time
import uuid
from pathlib import Path
from typing import Any

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
_DEFAULT_REFACTOR_ROOT = _PROJECT_ROOT.parent / "REFACTOR_TJSP"
_SAFE_JOB_ID = re.compile(r"^[a-zA-Z0-9_-]{8,32}$")
_DEFAULT_RETAIN_HOURS = 24
_MAX_RETAIN_HOURS = 168


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
    raw = (os.getenv("REFACTOR_AUTOS_EXPORT_JOBS_DIR") or "").strip()
    if raw:
        d = Path(raw).resolve()
    else:
        d = (_PROJECT_ROOT / "instance" / "autos_export_jobs").resolve()
    d.mkdir(parents=True, exist_ok=True)
    return d


def retain_hours() -> int:
    raw = (os.getenv("AUTOS_EXPORT_RETAIN_HOURS") or str(_DEFAULT_RETAIN_HOURS)).strip()
    try:
        hours = int(float(raw))
    except ValueError:
        hours = _DEFAULT_RETAIN_HOURS
    return max(1, min(hours, _MAX_RETAIN_HOURS))


def retain_seconds() -> int:
    return retain_hours() * 3600


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _as_user_id(value: Any) -> int | None:
    try:
        uid = int(value)
    except (TypeError, ValueError):
        return None
    return uid if uid > 0 else None


def _as_optional_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def user_can_access_job(
    job: dict[str, Any] | None,
    *,
    user_id: int | None,
    is_admin: bool,
) -> bool:
    if not job:
        return False
    owner = _as_user_id(job.get("user_id"))
    if owner is None:
        return True
    if is_admin:
        return True
    return owner == _as_user_id(user_id)


def configured() -> bool:
    root = _refactor_root()
    script = root / "run_autos_export.py"
    if not script.is_file():
        return False
    py = (os.getenv("REFACTOR_TJSP_PYTHON") or "").strip()
    if py:
        return Path(py).is_file()
    return (_refactor_root() / "venv" / "bin" / "python").is_file()


_lock = threading.Lock()
_jobs: dict[str, dict[str, Any]] = {}


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _read_json(path: Path) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None
    return data if isinstance(data, dict) else None


def _job_dir_for(job_id: str) -> Path | None:
    if not _SAFE_JOB_ID.match(job_id or ""):
        return None
    path = _jobs_dir() / job_id
    return path if path.is_dir() else None


def _persist_job_meta(job_dir: Path, **fields: Any) -> None:
    meta = _read_json(job_dir / "meta.json") or {}
    meta.update(fields)
    _write_json(job_dir / "meta.json", meta)


def _persist_job_failure(result_path: Path, error: str) -> None:
    if not result_path.is_file():
        _write_json(result_path, {"ok": False, "error": error, "outcome": None})
    _persist_job_meta(result_path.parent, status="error", error=error)


def _is_running_job(job_dir: Path, meta: dict[str, Any]) -> bool:
    if (job_dir / "result.json").is_file():
        return False
    return str(meta.get("status") or "running") == "running"


def _delete_job_dir(job_id: str) -> bool:
    job_dir = _job_dir_for(job_id)
    with _lock:
        _jobs.pop(job_id, None)
    if job_dir is None:
        return False
    shutil.rmtree(job_dir, ignore_errors=True)
    return True


def _cleanup_old_jobs() -> None:
    root = _jobs_dir()
    cutoff = time.time() - retain_seconds()
    for child in root.iterdir():
        if not child.is_dir() or not _SAFE_JOB_ID.match(child.name):
            continue
        meta = _read_json(child / "meta.json") or {}
        if _is_running_job(child, meta):
            continue
        created = _as_float(meta.get("created_at"))
        if created <= 0:
            try:
                created = child.stat().st_mtime
            except OSError:
                created = 0
        if created and created < cutoff:
            _delete_job_dir(child.name)


def _refactor_logs_dir() -> Path:
    raw = (os.getenv("REFACTOR_AUTOS_EXPORT_LOGS_DIR") or os.getenv("REFACTOR_ANALISE_LOGS_DIR") or "").strip()
    if raw:
        d = Path(raw).resolve()
    else:
        d = (_PROJECT_ROOT / "instance" / "refactor_logs").resolve()
    d.mkdir(parents=True, exist_ok=True)
    return d


def _refactor_subprocess_env() -> dict[str, str]:
    env = os.environ.copy()
    env["REFACTOR_LOGS_PATH"] = str(_refactor_logs_dir())
    for key in (
        "WEBSHARE_PROXIES_LIST",
        "HTTP_PROXY_MAX_ATTEMPTS",
        "HTTP_PROXY_URL",
        "HTTP_USE_PROXY",
    ):
        value = (os.getenv(key) or "").strip()
        if value:
            env[key] = value
    if not (env.get("WEBSHARE_PROXIES_LIST") or "").strip():
        default_list = _refactor_root() / "webshare-proxies-list.txt"
        if default_list.is_file():
            env["WEBSHARE_PROXIES_LIST"] = str(default_list)
    env.setdefault("HTTP_USE_PROXY", "true")
    env.pop("HTTP_PROXY", None)
    env.pop("HTTPS_PROXY", None)
    # Só este subprocesso (aba Autos e-SAJ): não enfileira cálculo.
    env["AUTOS_EXPORT_MODE"] = "1"
    env["USE_CALCULO_API"] = "0"
    env["ENABLE_MEMORIA_CALCULO_SYNC"] = "0"
    return env


def _run_subprocess(
    job_id: str,
    *,
    processo: str,
    incidente: str,
    progress_path: Path,
    result_path: Path,
    output_dir: Path,
) -> None:
    root = _refactor_root()
    script = root / "run_autos_export.py"
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
        "--output-dir",
        str(output_dir),
        "--job-id",
        job_id,
    ]
    timeout = int((os.getenv("REFACTOR_AUTOS_EXPORT_TIMEOUT") or "3600").strip() or "3600")
    try:
        env = _refactor_subprocess_env()
        env["TEMP_RUN_ID"] = job_id
        proc = subprocess.run(
            cmd,
            cwd=str(root),
            capture_output=True,
            text=True,
            env=env,
            start_new_session=True,
            timeout=timeout,
        )
        with _lock:
            job = _jobs.get(job_id)
            if job is not None:
                job["returncode"] = proc.returncode
                if proc.returncode != 0 and not result_path.is_file():
                    job["error"] = (proc.stderr or proc.stdout or "Falha na exportação")[:2000]
                    job["status"] = "error"
                    job["done"] = True
        if proc.returncode != 0 and not result_path.is_file():
            err = (proc.stderr or proc.stdout or "Falha na exportação")[:2000]
            _persist_job_failure(result_path, err)
    except subprocess.TimeoutExpired:
        err = "Tempo máximo excedido na exportação de autos."
        with _lock:
            job = _jobs.get(job_id)
            if job:
                job["status"] = "error"
                job["done"] = True
                job["error"] = err
        _persist_job_failure(result_path, err)
    except Exception as exc:
        err = str(exc)
        with _lock:
            job = _jobs.get(job_id)
            if job:
                job["status"] = "error"
                job["done"] = True
                job["error"] = err
        _persist_job_failure(result_path, err)
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
                        job["error"] = result.get("error") or "Exportação falhou"
                    _persist_job_meta(
                        result_path.parent,
                        status=job["status"],
                        error=job.get("error"),
                    )
            elif job and job.get("status") == "running":
                job["done"] = True
                job["status"] = "error"
                job["error"] = job.get("error") or "Exportação encerrada sem resultado."
                _persist_job_failure(result_path, str(job["error"]))


def start_job(
    *,
    processo: str,
    incidente: str,
    user_id: int | None = None,
    user_name: str = "",
) -> dict[str, Any]:
    root = _refactor_root()
    if not (root / "run_autos_export.py").is_file():
        raise FileNotFoundError(
            f"REFACTOR_TJSP não encontrado em {root}. Defina REFACTOR_TJSP_PATH no .env."
        )
    _cleanup_old_jobs()
    job_id = uuid.uuid4().hex[:16]
    job_dir = _jobs_dir() / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    progress_path = job_dir / "progress.json"
    result_path = job_dir / "result.json"
    progress_path.write_text(
        json.dumps({"message": "A iniciar…", "percent": 0.0, "done": False}, ensure_ascii=False),
        encoding="utf-8",
    )
    created_at = time.time()
    owner_id = _as_user_id(user_id)
    owner_name = str(user_name or "").strip()[:160]
    _write_json(
        job_dir / "meta.json",
        {
            "job_id": job_id,
            "processo": processo,
            "incidente": incidente,
            "created_at": created_at,
            "status": "running",
            "user_id": owner_id,
            "user_name": owner_name,
        },
    )
    job: dict[str, Any] = {
        "job_id": job_id,
        "status": "running",
        "done": False,
        "processo": processo,
        "incidente": incidente,
        "progress_path": str(progress_path),
        "result_path": str(result_path),
        "created_at": created_at,
        "user_id": owner_id,
        "user_name": owner_name,
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
            "output_dir": job_dir,
        },
        daemon=True,
    )
    thread.start()
    return {"job_id": job_id, "status": "running"}


def _snapshot_from_disk(job_id: str) -> dict[str, Any] | None:
    job_dir = _job_dir_for(job_id)
    if job_dir is None:
        return None
    meta = _read_json(job_dir / "meta.json") or {}
    progress = _read_json(job_dir / "progress.json") or {}
    result = _read_json(job_dir / "result.json")
    error = meta.get("error")
    if result is not None:
        done = True
        if result.get("ok"):
            status = "done"
        else:
            status = "error"
            error = result.get("error") or error or "Exportação falhou"
    else:
        done = False
        status = "running"
    return _apply_availability(
        {
            "job_id": job_id,
            "status": status,
            "done": done,
            "processo": meta.get("processo"),
            "incidente": meta.get("incidente"),
            "message": progress.get("message") or "A processar…",
            "percent": float(progress.get("percent") or 0.0),
            "current": _as_optional_int(progress.get("current")),
            "total": _as_optional_int(progress.get("total")),
            "result": result,
            "error": error,
            "created_at": meta.get("created_at"),
            "user_id": meta.get("user_id"),
            "user_name": meta.get("user_name"),
            "has_zip": (job_dir / "autos_export.zip").is_file(),
        },
        job_dir,
    )


def get_job_status(job_id: str) -> dict[str, Any] | None:
    with _lock:
        job = _jobs.get(job_id)
        snapshot = dict(job) if job is not None else None
    if snapshot is None:
        return _snapshot_from_disk(job_id)

    progress_path = Path(snapshot.get("progress_path") or "")
    progress = _read_json(progress_path) if progress_path else None
    if progress:
        snapshot["message"] = progress.get("message")
        snapshot["percent"] = progress.get("percent", 0.0)
        snapshot["current"] = _as_optional_int(progress.get("current"))
        snapshot["total"] = _as_optional_int(progress.get("total"))
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
    snapshot["has_zip"] = (result_path.parent / "autos_export.zip").is_file() if result_path else False
    if snapshot.get("user_id") is None:
        meta = _read_json(result_path.parent / "meta.json") if result_path else None
        if meta:
            snapshot["user_id"] = meta.get("user_id")
            snapshot.setdefault("user_name", meta.get("user_name"))
    job_dir = _job_dir_for(str(snapshot.get("job_id") or ""))
    return _apply_availability(snapshot, job_dir)


def _apply_availability(
    snapshot: dict[str, Any],
    job_dir: Path | None = None,
) -> dict[str, Any]:
    created = _as_float(snapshot.get("created_at"))
    if created <= 0 and job_dir is not None:
        try:
            created = float(job_dir.stat().st_mtime)
            snapshot["created_at"] = created
        except OSError:
            created = 0.0
    snapshot["retain_hours"] = retain_hours()
    if created <= 0:
        snapshot["expires_at"] = None
        snapshot["files_available"] = True
        return snapshot
    expires = created + retain_seconds()
    available = time.time() < expires
    snapshot["expires_at"] = expires
    snapshot["files_available"] = available
    if not available:
        snapshot["has_zip"] = False
    return snapshot


def _file_count(snapshot: dict[str, Any]) -> int | None:
    result = snapshot.get("result") if isinstance(snapshot.get("result"), dict) else {}
    outcome = result.get("outcome") if isinstance(result.get("outcome"), dict) else {}
    raw = outcome.get("file_count")
    if raw is None:
        files = outcome.get("files")
        if isinstance(files, list):
            raw = len(files)
    try:
        return int(raw) if raw is not None else None
    except (TypeError, ValueError):
        return None


def list_item(snapshot: dict[str, Any]) -> dict[str, Any]:
    available = bool(snapshot.get("files_available"))
    return {
        "job_id": snapshot.get("job_id"),
        "status": snapshot.get("status"),
        "done": bool(snapshot.get("done")),
        "processo": snapshot.get("processo"),
        "incidente": snapshot.get("incidente"),
        "message": snapshot.get("message"),
        "percent": float(snapshot.get("percent") or 0.0),
        "current": _as_optional_int(snapshot.get("current")),
        "total": _as_optional_int(snapshot.get("total")),
        "error": snapshot.get("error"),
        "created_at": snapshot.get("created_at"),
        "expires_at": snapshot.get("expires_at"),
        "files_available": available,
        "has_zip": bool(snapshot.get("has_zip")) and available,
        "file_count": _file_count(snapshot) if snapshot.get("done") else None,
        "user_id": snapshot.get("user_id"),
        "user_name": snapshot.get("user_name"),
    }


def list_jobs(*, user_id: int | None = None, is_admin: bool = False) -> list[dict[str, Any]]:
    _cleanup_old_jobs()
    items: list[dict[str, Any]] = []
    root = _jobs_dir()
    for child in root.iterdir():
        if not child.is_dir() or not _SAFE_JOB_ID.match(child.name):
            continue
        snapshot = get_job_status(child.name)
        if snapshot is None:
            continue
        if not user_can_access_job(snapshot, user_id=user_id, is_admin=is_admin):
            continue
        if (
            snapshot.get("done")
            and not snapshot.get("files_available")
            and snapshot.get("status") != "running"
        ):
            _delete_job_dir(str(snapshot.get("job_id") or child.name))
            continue
        items.append(list_item(snapshot))
    items.sort(key=lambda row: _as_float(row.get("created_at")), reverse=True)
    return items


def delete_job(
    job_id: str,
    *,
    user_id: int | None = None,
    is_admin: bool = False,
) -> tuple[dict[str, Any], int]:
    snapshot = get_job_status(job_id)
    if snapshot is None or not user_can_access_job(
        snapshot, user_id=user_id, is_admin=is_admin
    ):
        return {"ok": False, "error": "Solicitação não encontrada."}, 404
    if not snapshot.get("done") and snapshot.get("status") == "running":
        return {
            "ok": False,
            "error": "Só é possível excluir solicitações já concluídas ou com erro.",
        }, 409
    if not _delete_job_dir(job_id):
        return {"ok": False, "error": "Não foi possível excluir a solicitação."}, 500
    return {"ok": True, "job_id": job_id}, 200


def resolve_job_file(job_id: str, relpath: str) -> Path | None:
    snapshot = get_job_status(job_id)
    if snapshot is None or not snapshot.get("files_available"):
        return None
    job_dir = _job_dir_for(job_id)
    if job_dir is None:
        return None
    cleaned = (relpath or "").replace("\\", "/").lstrip("/")
    if not cleaned or ".." in cleaned.split("/"):
        return None
    files_root = (job_dir / "files").resolve()
    target = (files_root / cleaned).resolve()
    try:
        target.relative_to(files_root)
    except ValueError:
        return None
    return target if target.is_file() else None


def resolve_job_zip(job_id: str) -> Path | None:
    snapshot = get_job_status(job_id)
    if snapshot is None or not snapshot.get("files_available"):
        return None
    job_dir = _job_dir_for(job_id)
    if job_dir is None:
        return None
    zip_path = job_dir / "autos_export.zip"
    return zip_path if zip_path.is_file() else None
