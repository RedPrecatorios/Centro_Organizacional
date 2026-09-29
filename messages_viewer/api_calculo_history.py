"""Histórico leve de jobs da API_CALCULO, no lado do Centro.

Não copia payload nem resultado: uma linha por ``job_id``. O total de
concluídos sobrevive à poda dos ~200 ficheiros que a API mantém.
"""
from __future__ import annotations

import json
import os
import re
import threading
from pathlib import Path
from typing import Any, Iterable

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
_LOCK = threading.Lock()
_JOB_ID_RE = re.compile(r"^[a-fA-F0-9]{16,64}$")
_LOG_DONE_RE = re.compile(r"Calculo job_id=([a-fA-F0-9]{16,64}) concluido")
_LOG_FAIL_RE = re.compile(r"Calculo job_id=([a-fA-F0-9]{16,64}) falhou")
_SEEN_LINE_RE = re.compile(
    r"^([a-fA-F0-9]{16,64})\s+(done|error)(?:\s+(\S+))?$"
)

_seen_cache: dict[str, tuple[str, str]] | None = None
_seen_cache_size = -1


def _history_dir() -> Path:
    raw = (os.getenv("API_CALCULO_HISTORY_DIR") or "").strip()
    if raw:
        d = Path(raw).resolve()
    else:
        d = (_PROJECT_ROOT / "instance" / "api_calculo_history").resolve()
    d.mkdir(parents=True, exist_ok=True)
    return d


def _meta_path() -> Path:
    return _history_dir() / "meta.json"


def _seen_path() -> Path:
    return _history_dir() / "seen_ids.txt"


def _empty_meta() -> dict[str, Any]:
    return {
        "version": 1,
        "log_offset": 0,
        "log_size": 0,
        "bootstrapped": False,
        "updated_at": None,
    }


def _read_meta() -> dict[str, Any]:
    path = _meta_path()
    if not path.is_file():
        return _empty_meta()
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return _empty_meta()
    if not isinstance(data, dict):
        return _empty_meta()
    base = _empty_meta()
    base.update(data)
    return base


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    tmp.replace(path)


def _load_seen() -> dict[str, tuple[str, str]]:
    global _seen_cache, _seen_cache_size
    path = _seen_path()
    size = path.stat().st_size if path.is_file() else 0
    if _seen_cache is not None and size == _seen_cache_size:
        return _seen_cache
    seen: dict[str, tuple[str, str]] = {}
    if path.is_file():
        try:
            for line in path.read_text(encoding="utf-8").splitlines():
                match = _SEEN_LINE_RE.match(line.strip())
                if not match:
                    job_id = line.strip()
                    if _JOB_ID_RE.fullmatch(job_id):
                        seen[job_id] = ("done", "-")
                    continue
                seen[match.group(1)] = (match.group(2), match.group(3) or "-")
        except OSError:
            pass
    _seen_cache = seen
    _seen_cache_size = size
    return seen


def _append_seen(rows: list[tuple[str, str, str]]) -> None:
    global _seen_cache, _seen_cache_size
    if not rows:
        return
    path = _seen_path()
    with path.open("a", encoding="utf-8") as handle:
        for job_id, status, source_ip in rows:
            handle.write(f"{job_id} {status} {source_ip or '-'}\n")
    if _seen_cache is None:
        _seen_cache = {}
    for job_id, status, source_ip in rows:
        _seen_cache[job_id] = (status, source_ip or "-")
    try:
        _seen_cache_size = path.stat().st_size
    except OSError:
        _seen_cache_size = -1


def _counts_from_seen(seen: dict[str, tuple[str, str]]) -> dict[str, Any]:
    done = 0
    error = 0
    by_source: dict[str, dict[str, int]] = {}
    for status, source_ip in seen.values():
        if status == "done":
            done += 1
        elif status == "error":
            error += 1
        else:
            continue
        ip = source_ip if source_ip and source_ip != "-" else ""
        if not ip:
            continue
        bucket = by_source.setdefault(ip, {"done": 0, "error": 0})
        bucket[status] = bucket.get(status, 0) + 1
    return {"done": done, "error": error, "by_source": by_source, "tracked": len(seen)}


def _parse_log_chunk(text: str) -> list[tuple[str, str]]:
    found: dict[str, str] = {}
    for job_id in _LOG_FAIL_RE.findall(text):
        found[job_id] = "error"
    for job_id in _LOG_DONE_RE.findall(text):
        found[job_id] = "done"
    return list(found.items())


def _tail_api_log(log_path: Path | None, meta: dict[str, Any]) -> list[tuple[str, str]]:
    if log_path is None or not log_path.is_file():
        return []
    try:
        size = log_path.stat().st_size
    except OSError:
        return []
    offset = int(meta.get("log_offset") or 0)
    last_size = int(meta.get("log_size") or 0)
    if size < last_size or offset > size:
        offset = 0
    if offset >= size:
        meta["log_offset"] = size
        meta["log_size"] = size
        return []
    try:
        with log_path.open("r", encoding="utf-8", errors="replace") as handle:
            handle.seek(offset)
            chunk = handle.read()
            meta["log_offset"] = handle.tell()
    except OSError:
        return []
    meta["log_size"] = size
    return _parse_log_chunk(chunk)


def ingest_finished_jobs(
    jobs: Iterable[dict[str, Any]],
    *,
    log_path: Path | None = None,
    now_iso: str | None = None,
) -> dict[str, Any]:
    """Regista jobs finalizados ainda não vistos. Devolve os totais persistidos."""
    with _LOCK:
        meta = _read_meta()
        seen = _load_seen()
        new_rows: list[tuple[str, str, str]] = []

        if not meta.get("bootstrapped") and log_path is not None and log_path.is_file():
            try:
                bootstrap = _parse_log_chunk(
                    log_path.read_text(encoding="utf-8", errors="replace")
                )
                meta["log_offset"] = log_path.stat().st_size
                meta["log_size"] = meta["log_offset"]
            except OSError:
                bootstrap = []
            for job_id, status in bootstrap:
                if job_id in seen:
                    continue
                seen[job_id] = (status, "-")
                new_rows.append((job_id, status, "-"))
            meta["bootstrapped"] = True
        else:
            for job_id, status in _tail_api_log(log_path, meta):
                if job_id in seen:
                    continue
                seen[job_id] = (status, "-")
                new_rows.append((job_id, status, "-"))

        for job in jobs:
            job_id = str(job.get("job_id") or "").strip()
            status = str(job.get("status") or "").strip()
            if status not in {"done", "error"} or not _JOB_ID_RE.fullmatch(job_id):
                continue
            ip = str(job.get("source_ip") or "").strip() or "-"
            if job_id in seen:
                continue
            seen[job_id] = (status, ip)
            new_rows.append((job_id, status, ip))

        if new_rows:
            _append_seen(new_rows)
        if now_iso:
            meta["updated_at"] = now_iso
        _atomic_write_json(_meta_path(), meta)
        counts = _counts_from_seen(seen)
        counts["updated_at"] = meta.get("updated_at")
        return counts
