"""Monitor da fila da API_CALCULO (planilha DEPRE / memória).

Lê ``calculo_jobs/`` no disco (mesmo volume) para não depender do HTTP
sob carga. Opcionalmente consulta ``systemctl`` e o ``.env`` da API.
"""
from __future__ import annotations

import json
import logging
import os
import subprocess
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

_SP = ZoneInfo("America/Sao_Paulo")

DEFAULT_API_CALCULO_ROOT = Path(
    "/mnt/volume_nyc1_1778499775066/API_CALCULO"
)

CLOUD_LABELS: dict[str, str] = {
    "165.227.67.80": "ubuntu-c-2-nyc3-01",
    "146.190.223.251": "ubuntu-s-2vcpu-8gb-nyc1",
    "192.81.217.95": "clone-04122025-nyc1",
    "157.230.170.25": "ubuntu-c-2-sfo2-01",
    "143.198.129.232": "ubuntu-c-2-sfo3-01",
    "68.183.23.91": "InternalAutomation (este host)",
    "159.65.103.229": "ubuntu-c-2-sfo2-01 (prioritário)",
    "174.138.62.59": "pesquisa2-nyc3",
    "178.128.13.58": "ubuntu-c-2-sfo2-01 (alt)",
}


def api_calculo_root() -> Path:
    raw = (os.getenv("API_CALCULO_ROOT") or "").strip()
    if raw:
        return Path(raw)
    return DEFAULT_API_CALCULO_ROOT


def api_calculo_jobs_dir() -> Path:
    raw = (os.getenv("API_CALCULO_JOBS_DIR") or "").strip()
    if raw:
        return Path(raw)
    return api_calculo_root() / "calculo_jobs"


def api_calculo_monitor_configured() -> bool:
    return api_calculo_jobs_dir().is_dir()


def poll_interval_ms() -> int:
    try:
        value = int((os.getenv("API_CALCULO_POLL_INTERVAL_MS") or "8000").strip())
    except ValueError:
        value = 8000
    return max(3000, min(value, 120_000))


def _cloud_label(ip: str) -> str:
    ip = (ip or "").strip() or "-"
    return CLOUD_LABELS.get(ip, ip)


def _now_iso() -> str:
    return datetime.now(_SP).isoformat(timespec="seconds")


def _read_json(path: Path) -> dict[str, Any] | None:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return data if isinstance(data, dict) else None


def _extract_case(payload: Any) -> dict[str, str]:
    if not isinstance(payload, dict):
        return {"requerente": "", "processo": "", "incidente": ""}
    record = payload.get("record") if isinstance(payload.get("record"), dict) else {}
    processo_obj = (
        payload.get("processo") if isinstance(payload.get("processo"), dict) else {}
    )
    requerente = str(
        record.get("Requerente")
        or record.get("requerente")
        or payload.get("requerente")
        or ""
    ).strip()
    processo = str(
        record.get("Numero_de_Processo")
        or record.get("numero_processo")
        or processo_obj.get("numero_processo")
        or payload.get("processo")
        or ""
    ).strip()
    if isinstance(payload.get("processo"), dict):
        processo = str(
            processo_obj.get("numero_processo")
            or processo_obj.get("Numero_de_Processo")
            or ""
        ).strip() or processo
    incidente = str(
        record.get("Numero_do_Incidente")
        or record.get("numero_incidente")
        or processo_obj.get("numero_incidente")
        or payload.get("incidente")
        or ""
    ).strip()
    return {
        "requerente": requerente[:120],
        "processo": processo[:80],
        "incidente": incidente[:40],
    }


def _job_summary(job: dict[str, Any]) -> dict[str, Any]:
    ip = str(job.get("source_ip") or "").strip() or "-"
    case = _extract_case(job.get("payload"))
    try:
        rank = int(job.get("priority_rank"))
    except (TypeError, ValueError):
        rank = 2
    return {
        "job_id": str(job.get("job_id") or ""),
        "status": str(job.get("status") or ""),
        "source_ip": ip,
        "host": _cloud_label(ip),
        "priority_rank": rank,
        "enqueued_at": job.get("enqueued_at"),
        "started_at": job.get("started_at"),
        "finished_at": job.get("finished_at"),
        "error": (str(job.get("error") or "")[:200] or None),
        **case,
    }


def _read_libreoffice_max() -> int | None:
    env_path = api_calculo_root() / ".env"
    if not env_path.is_file():
        return None
    try:
        for line in env_path.read_text(encoding="utf-8", errors="replace").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, val = line.partition("=")
            if key.strip() == "LIBREOFFICE_MAX_CONCURRENT":
                return max(1, int(val.strip().strip("\"'")))
    except (OSError, ValueError):
        return None
    return None


def _systemctl_active(unit: str) -> str:
    try:
        proc = subprocess.run(
            ["systemctl", "is-active", unit],
            capture_output=True,
            text=True,
            timeout=3,
            check=False,
        )
        return (proc.stdout or proc.stderr or "unknown").strip() or "unknown"
    except Exception:
        return "unknown"


def build_api_calculo_monitor_snapshot(
    *,
    running_limit: int = 20,
    queued_sample: int = 15,
    done_sample: int = 10,
) -> dict[str, Any]:
    jobs_dir = api_calculo_jobs_dir()
    if not jobs_dir.is_dir():
        return {
            "ok": False,
            "error": f"Pasta de jobs não encontrada: {jobs_dir}",
            "generated_at": _now_iso(),
            "jobs_dir": str(jobs_dir),
        }

    status_counts: Counter[str] = Counter()
    by_ip: dict[str, Counter[str]] = defaultdict(Counter)
    running: list[dict[str, Any]] = []
    queued: list[dict[str, Any]] = []
    done: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    unreadable = 0

    for path in jobs_dir.glob("*.json"):
        if path.name.startswith("_") or path.name.startswith("."):
            continue
        job = _read_json(path)
        if job is None:
            unreadable += 1
            continue
        status = str(job.get("status") or "?")
        status_counts[status] += 1
        ip = str(job.get("source_ip") or "").strip() or "-"
        by_ip[ip][status] += 1
        summary = _job_summary(job)
        if status == "running":
            running.append(summary)
        elif status == "queued":
            queued.append(summary)
        elif status == "done":
            done.append(summary)
        elif status == "error":
            errors.append(summary)

    def _sort_key_enq(item: dict[str, Any]) -> str:
        return str(item.get("enqueued_at") or "")

    def _sort_key_fin(item: dict[str, Any]) -> str:
        return str(item.get("finished_at") or item.get("enqueued_at") or "")

    running.sort(key=lambda j: str(j.get("started_at") or ""), reverse=True)
    queued.sort(key=_sort_key_enq)  # oldest first
    done.sort(key=_sort_key_fin, reverse=True)
    errors.sort(key=_sort_key_fin, reverse=True)

    by_source = []
    for ip, counts in by_ip.items():
        by_source.append(
            {
                "source_ip": ip,
                "host": _cloud_label(ip),
                "queued": int(counts.get("queued", 0)),
                "running": int(counts.get("running", 0)),
                "done": int(counts.get("done", 0)),
                "error": int(counts.get("error", 0)),
                "total": int(sum(counts.values())),
            }
        )
    by_source.sort(key=lambda row: (-row["queued"], -row["running"], row["host"]))

    rr_state = None
    rr_path = jobs_dir / "_rr_state.json"
    if rr_path.is_file():
        rr_state = _read_json(rr_path)

    index_len = None
    index_path = jobs_dir / "_queued_index.json"
    if index_path.is_file():
        try:
            idx = json.loads(index_path.read_text(encoding="utf-8"))
            if isinstance(idx, list):
                index_len = len(idx)
        except (OSError, json.JSONDecodeError):
            index_len = None

    lo_max = _read_libreoffice_max()
    return {
        "ok": True,
        "generated_at": _now_iso(),
        "jobs_dir": str(jobs_dir),
        "api_root": str(api_calculo_root()),
        "counts": {
            "queued": int(status_counts.get("queued", 0)),
            "running": int(status_counts.get("running", 0)),
            "done": int(status_counts.get("done", 0)),
            "error": int(status_counts.get("error", 0)),
            "other": int(
                sum(
                    v
                    for k, v in status_counts.items()
                    if k not in {"queued", "running", "done", "error"}
                )
            ),
            "unreadable": unreadable,
        },
        "index_queued": index_len,
        "libreoffice_max_concurrent": lo_max,
        "services": {
            "api_calculo": _systemctl_active("api-calculo"),
            "api_calculo_drive_worker": _systemctl_active("api-calculo-drive-worker"),
        },
        "rr_state": rr_state,
        "by_source": by_source,
        "running_jobs": running[: max(1, running_limit)],
        "queued_oldest": queued[: max(1, queued_sample)],
        "recent_done": done[: max(1, done_sample)],
        "recent_errors": errors[: max(1, done_sample)],
    }
