"""Monitor da fila da API_CALCULO (planilha DEPRE / memória).

Lê ``calculo_jobs/`` no disco (mesmo volume) para não depender do HTTP
sob carga. Opcionalmente consulta ``systemctl`` e o ``.env`` da API.
"""
from __future__ import annotations

import json
import logging
import os
import re
import socket
import subprocess
import threading
import time
import unicodedata
from collections import Counter, defaultdict
from datetime import datetime, timezone
from http.client import RemoteDisconnected
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

from messages_viewer.api_calculo_history import ingest_finished_jobs

logger = logging.getLogger(__name__)

_SP = ZoneInfo("America/Sao_Paulo")
_JOB_ID_RE = re.compile(r"^[a-fA-F0-9]{8,64}$")

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


def _pick_str(*values: Any, limit: int = 160) -> str:
    for value in values:
        if value is None or isinstance(value, (dict, list)):
            continue
        text = str(value).strip()
        if text:
            return text[:limit]
    return ""


def _extract_case(payload: Any) -> dict[str, str]:
    if not isinstance(payload, dict):
        return {"requerente": "", "processo": "", "incidente": ""}
    record = payload.get("record") if isinstance(payload.get("record"), dict) else {}
    processo_obj = (
        payload.get("processo") if isinstance(payload.get("processo"), dict) else {}
    )
    requerente = _pick_str(
        record.get("Requerente"),
        record.get("requerente"),
        payload.get("requerente"),
        limit=120,
    )
    processo = _pick_str(
        record.get("Numero_de_Processo"),
        record.get("numero_processo"),
        processo_obj.get("numero_processo"),
        payload.get("processo") if not isinstance(payload.get("processo"), dict) else None,
        limit=80,
    )
    if isinstance(payload.get("processo"), dict):
        processo = (
            _pick_str(
                processo_obj.get("numero_processo"),
                processo_obj.get("Numero_de_Processo"),
                limit=80,
            )
            or processo
        )
    incidente = _pick_str(
        record.get("Numero_do_Incidente"),
        record.get("numero_incidente"),
        processo_obj.get("numero_incidente"),
        payload.get("incidente"),
        limit=40,
    )
    return {
        "requerente": requerente,
        "processo": processo,
        "incidente": incidente,
    }


def _extract_highlights(payload: Any) -> dict[str, str]:
    if not isinstance(payload, dict):
        return {}
    record = payload.get("record") if isinstance(payload.get("record"), dict) else {}
    credor = payload.get("credor") if isinstance(payload.get("credor"), dict) else {}
    valores = payload.get("valores") if isinstance(payload.get("valores"), dict) else {}
    processo = payload.get("processo") if isinstance(payload.get("processo"), dict) else {}
    return {
        "requerente": _pick_str(record.get("Requerente"), credor.get("nome"), payload.get("requerente")),
        "cpf": _pick_str(record.get("CPF"), credor.get("cpf")),
        "processo": _pick_str(record.get("Numero_de_Processo"), processo.get("numero_processo")),
        "incidente": _pick_str(record.get("Numero_do_Incidente"), processo.get("numero_incidente")),
        "entidade": _pick_str(record.get("Entidade_Devedora"), processo.get("entidade_devedora")),
        "natureza": _pick_str(record.get("Natureza"), processo.get("natureza")),
        "data_base": _pick_str(record.get("Data_Base"), credor.get("data_base")),
        "data_nascimento": _pick_str(
            record.get("Data_de_Nascimento"), credor.get("data_de_nascimento")
        ),
        "data_decisao": _pick_str(record.get("Data_Decisao"), processo.get("data_decisao")),
        "principal_liquido": _pick_str(
            record.get("Principal_Liquido"), valores.get("principal_liquido")
        ),
        "juros_moratorios": _pick_str(
            record.get("Juros_Moratorio"), valores.get("juros_moratorios")
        ),
        "valor_requisitado": _pick_str(
            record.get("Valor_Requisitado"), valores.get("valor_requisitado")
        ),
        "numero_de_meses": _pick_str(
            record.get("Numero_de_Meses"), valores.get("numero_de_meses")
        ),
        "ip_hostname": _pick_str(payload.get("IP-HOSTNAME")),
        "prioridade_status": _pick_str(payload.get("prioridade_status")),
    }


def _resolved_job_path(job_id: str) -> Path | None:
    job_id = (job_id or "").strip()
    if not _JOB_ID_RE.fullmatch(job_id):
        return None
    jobs_dir = api_calculo_jobs_dir()
    try:
        jobs_root = jobs_dir.resolve()
        path = (jobs_dir / f"{job_id}.json").resolve()
        path.relative_to(jobs_root)
    except (OSError, ValueError):
        return None
    return path


def _api_env_value(key: str) -> str:
    env_path = api_calculo_root() / ".env"
    if not env_path.is_file():
        return ""
    try:
        for line in env_path.read_text(encoding="utf-8", errors="replace").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            name, _, val = line.partition("=")
            if name.strip() == key:
                return val.strip().strip("\"'")
    except OSError:
        return ""
    return ""


def api_calculo_http_base() -> str:
    raw = (os.getenv("API_CALCULO_URL") or "").strip()
    if raw:
        return raw.rstrip("/")
    port = (os.getenv("API_CALCULO_PORT") or _api_env_value("API_PORT") or "9487").strip()
    return f"http://127.0.0.1:{port}"


def api_calculo_http_token() -> str:
    return ((os.getenv("API_CALCULO_TOKEN") or "").strip() or _api_env_value("API_TOKEN"))


# Só marca a API 2 como fora depois deste intervalo sem um GET /health bem-sucedido.
_REMOTE_HEALTH_GRACE_SECONDS = 90
_remote_health_lock = threading.Lock()
_remote_health_state: dict[str, Any] = {"last_ok_at": 0.0, "last_instance": None}


def _remote_health_grace_seconds() -> float:
    raw = (os.getenv("API_CALCULO_REMOTE_OFF_AFTER_SECONDS") or "").strip()
    try:
        value = float(raw) if raw else _REMOTE_HEALTH_GRACE_SECONDS
    except ValueError:
        value = _REMOTE_HEALTH_GRACE_SECONDS
    return max(15.0, min(value, 600.0))


def _stabilize_remote_health(instance: dict[str, Any], health_ok: bool) -> dict[str, Any]:
    """Uma falha isolada de /health não tira a API 2 do ar."""
    now = time.monotonic()
    with _remote_health_lock:
        if health_ok:
            _remote_health_state["last_ok_at"] = now
            _remote_health_state["last_instance"] = dict(instance)
            return instance
        last_ok = float(_remote_health_state.get("last_ok_at") or 0)
        last = _remote_health_state.get("last_instance")
        if last_ok and (now - last_ok) < _remote_health_grace_seconds() and isinstance(last, dict):
            kept = dict(last)
            kept["reachable"] = True
            if not kept.get("service") or kept.get("service") == "inactive":
                kept["service"] = "active"
            return kept
    return instance


def api_calculo_remote_base() -> str:
    raw = (os.getenv("API_CALCULO_REMOTE_URL") or "http://192.81.217.95:9487").strip()
    return raw.rstrip("/")


def api_calculo_remote_label() -> str:
    raw = (os.getenv("API_CALCULO_REMOTE_LABEL") or "").strip()
    if raw:
        return raw
    host = api_calculo_remote_base().split("://", 1)[-1].split(":", 1)[0]
    return _cloud_label(host)


def _tcp_open(host: str, port: int, timeout: float = 1.5) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def _probe_http_health(base_url: str, timeout: float = 2.5) -> dict[str, Any]:
    url = base_url.rstrip("/") + "/health"
    started = time.perf_counter()
    try:
        req = Request(url, method="GET", headers={"Accept": "application/json"})
        with urlopen(req, timeout=timeout) as resp:
            raw = resp.read(2048)
            code = int(resp.getcode() or 0)
    except HTTPError as exc:
        return {
            "reachable": False,
            "http_status": int(exc.code),
            "health_status": None,
            "latency_ms": int((time.perf_counter() - started) * 1000),
            "error": f"HTTP {exc.code}",
        }
    except (
        URLError,
        RemoteDisconnected,
        ConnectionResetError,
        TimeoutError,
        socket.timeout,
        OSError,
    ) as exc:
        return {
            "reachable": False,
            "http_status": None,
            "health_status": None,
            "latency_ms": None,
            "error": str(exc)[:180],
        }
    latency_ms = int((time.perf_counter() - started) * 1000)
    health_status = None
    try:
        body = json.loads(raw.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        body = None
    if isinstance(body, dict):
        status_text = str(body.get("status") or "").strip()
        health_status = status_text or None
    return {
        "reachable": code == 200 and (health_status in {None, "ok"}),
        "http_status": code,
        "health_status": health_status,
        "latency_ms": latency_ms,
    }


def _local_instance(
    counts: dict[str, Any],
    services: dict[str, str],
    lo_max: int | None,
) -> dict[str, Any]:
    port_raw = (os.getenv("API_CALCULO_PORT") or _api_env_value("API_PORT") or "9487").strip()
    try:
        port = int(port_raw)
    except ValueError:
        port = 9487
    service = services.get("api_calculo") or "unknown"
    reachable = service == "active" or _tcp_open("127.0.0.1", port)
    return {
        "id": "local",
        "role": "esta máquina",
        "label": "InternalAutomation",
        "address": f"127.0.0.1:{port}",
        "reachable": reachable,
        "service": service,
        "drive_worker": services.get("api_calculo_drive_worker") or "unknown",
        "queue_visible": True,
        "latency_ms": None,
        "health_status": None,
        "counts": {
            "queued": int(counts.get("queued") or 0),
            "running": int(counts.get("running") or 0),
            "done": int(counts.get("done") or 0),
            "error": int(counts.get("error") or 0),
        },
        "libreoffice_max_concurrent": lo_max,
        "note": "Fila lida em calculo_jobs/ nesta máquina.",
    }


def _fetch_remote_fila(base_url: str, timeout: float = 4.0) -> dict[str, Any]:
    """GET /calculo/monitor na outra API. 404 significa que o código ainda não tem a rota."""
    token = api_calculo_http_token()
    url = base_url.rstrip("/") + "/calculo/monitor"
    if not token:
        return {"available": False, "error": "Token da API não configurado para ler a fila remota."}
    started = time.perf_counter()
    req = Request(
        url,
        method="GET",
        headers={
            "Accept": "application/json",
            "Authorization": f"Bearer {token}",
        },
    )
    try:
        with urlopen(req, timeout=timeout) as resp:
            raw = resp.read(2_000_000)
            code = int(resp.getcode() or 0)
    except HTTPError as exc:
        latency_ms = int((time.perf_counter() - started) * 1000)
        if int(exc.code) in (404, 405):
            return {
                "available": False,
                "http_status": int(exc.code),
                "latency_ms": latency_ms,
                "error": (
                    "GET /calculo/monitor respondeu 404. "
                    "O processo na porta 9487 ainda não tem a rota da fila."
                ),
            }
        if int(exc.code) == 401:
            return {
                "available": False,
                "http_status": 401,
                "latency_ms": latency_ms,
                "error": "A outra API recusou o token ao ler a fila.",
            }
        return {
            "available": False,
            "http_status": int(exc.code),
            "latency_ms": latency_ms,
            "error": f"HTTP {exc.code} em /calculo/monitor",
        }
    except (
        URLError,
        RemoteDisconnected,
        ConnectionResetError,
        TimeoutError,
        socket.timeout,
        OSError,
    ) as exc:
        return {"available": False, "error": str(exc)[:180]}
    latency_ms = int((time.perf_counter() - started) * 1000)
    try:
        body = json.loads(raw.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        return {
            "available": False,
            "http_status": code,
            "latency_ms": latency_ms,
            "error": "Resposta inválida de /calculo/monitor.",
        }
    if code != 200 or not isinstance(body, dict) or not body.get("ok"):
        return {
            "available": False,
            "http_status": code,
            "latency_ms": latency_ms,
            "error": "A outra API não devolveu a fila.",
        }
    body["available"] = True
    body["latency_ms"] = latency_ms
    return body


def _remote_instance() -> dict[str, Any]:
    base = api_calculo_remote_base()
    address = base.split("://", 1)[-1]
    health = _probe_http_health(base)
    fila = _fetch_remote_fila(base) if health.get("reachable") else {"available": False}
    queue_visible = bool(fila.get("available"))
    counts = fila.get("counts") if queue_visible and isinstance(fila.get("counts"), dict) else None
    if queue_visible:
        note = "Fila lida em GET /calculo/monitor desta instância."
    elif health.get("reachable") and fila.get("error"):
        note = str(fila.get("error"))
    elif health.get("reachable"):
        note = (
            "API no ar. A fila aparece aqui quando esta instância publicar GET /calculo/monitor."
        )
    else:
        note = "Sem resposta em GET /health."
    lo_max = fila.get("libreoffice_max_concurrent") if queue_visible else None
    try:
        lo_max = int(lo_max) if lo_max is not None else None
    except (TypeError, ValueError):
        lo_max = None
    remote_services = fila.get("services") if isinstance(fila.get("services"), dict) else {}
    drive_worker = remote_services.get("api_calculo_drive_worker") or "active"
    api_service = remote_services.get("api_calculo") or (
        "active" if health.get("reachable") else "inactive"
    )
    sources = []
    if queue_visible:
        for row in fila.get("by_source") or []:
            if not isinstance(row, dict):
                continue
            item = dict(row)
            item["host"] = _cloud_label(str(item.get("source_ip") or ""))
            sources.append(item)
    built = {
        "id": "remote",
        "role": "outro host",
        "label": api_calculo_remote_label(),
        "address": address,
        "base_url": base,
        "reachable": bool(health.get("reachable")),
        "service": api_service,
        "drive_worker": drive_worker,
        "queue_visible": queue_visible,
        "latency_ms": fila.get("latency_ms") if fila.get("latency_ms") is not None else health.get("latency_ms"),
        "health_status": health.get("health_status"),
        "http_status": health.get("http_status"),
        "error": None if queue_visible else (fila.get("error") or health.get("error")),
        "counts": counts,
        "libreoffice_max_concurrent": lo_max,
        "jobs_keep_count": fila.get("jobs_keep_count") if queue_visible else None,
        "index_queued": fila.get("index_queued") if queue_visible else None,
        "rr_last_source_key": fila.get("rr_last_source_key") if queue_visible else None,
        "by_source": sources,
        "running_jobs": fila.get("running_jobs") if queue_visible else [],
        "queued_oldest": fila.get("queued_oldest") if queue_visible else [],
        "recent_done": fila.get("recent_done") if queue_visible else [],
        "recent_errors": fila.get("recent_errors") if queue_visible else [],
        "generated_at": fila.get("generated_at") if queue_visible else None,
        "note": note,
    }
    return _stabilize_remote_health(built, bool(health.get("reachable")))


def _atomic_write_json(path: Path, data: dict[str, Any]) -> None:
    tmp = path.with_name(f".{path.name}.tmp")
    tmp.write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    tmp.replace(path)


def _api_error_text(api_out: dict[str, Any]) -> str:
    detail = api_out.get("detail") or api_out.get("error") or "A API recusou o reenvio."
    if isinstance(detail, list):
        parts = []
        for item in detail:
            if isinstance(item, dict):
                parts.append(str(item.get("msg") or item))
            else:
                parts.append(str(item))
        detail = "; ".join(parts)
    return str(detail)[:400]


def _post_calculo_fila(payload: dict[str, Any]) -> tuple[dict[str, Any], int]:
    token = api_calculo_http_token()
    if not token:
        return {"ok": False, "error": "Token da API de cálculo não configurado."}, 503
    url = api_calculo_http_base() + "/calculo/fila"
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = Request(
        url,
        data=body,
        method="POST",
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json; charset=utf-8",
            "Authorization": f"Bearer {token}",
        },
    )
    try:
        with urlopen(req, timeout=30) as resp:
            raw = resp.read()
            code = int(resp.getcode() or 0)
    except HTTPError as exc:
        raw = exc.read()
        code = int(exc.code)
    except (
        URLError,
        RemoteDisconnected,
        ConnectionResetError,
        TimeoutError,
        socket.timeout,
        OSError,
    ) as exc:
        return {
            "ok": False,
            "error": f"Não foi possível contactar a API de cálculo ({url}). {exc}",
        }, 502
    try:
        out = json.loads(raw.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        return {"ok": False, "error": "Resposta inválida da API de cálculo."}, 502
    if not isinstance(out, dict):
        return {"ok": False, "error": "Resposta inválida da API de cálculo."}, 502
    return out, code


def resend_api_calculo_job(
    job_id: str,
    payload_override: Any = None,
) -> tuple[dict[str, Any], int]:
    """Reenvia um job com erro via POST /calculo/fila e tira-o do quadro de erros."""
    path = _resolved_job_path(job_id)
    if path is None:
        return {"ok": False, "error": "job_id inválido"}, 400
    if not path.is_file():
        return {"ok": False, "error": "Job não encontrado"}, 404

    job = _read_json(path)
    if job is None:
        return {"ok": False, "error": "Arquivo do job ilegível"}, 500

    status = str(job.get("status") or "")
    if status == "resent":
        return {
            "ok": False,
            "error": "Este caso já foi reenviado.",
            "resent_job_id": job.get("resent_job_id"),
        }, 409
    if status != "error":
        return {"ok": False, "error": "Só é possível reenviar jobs com erro."}, 409

    if payload_override is not None:
        if not isinstance(payload_override, dict) or not payload_override:
            return {"ok": False, "error": "Payload editado inválido."}, 400
        payload = payload_override
    else:
        payload = job.get("payload")
        if not isinstance(payload, dict) or not payload:
            return {"ok": False, "error": "Este job não tem payload para reenviar."}, 400

    api_out, code = _post_calculo_fila(payload)
    new_id = str(api_out.get("job_id") or "")
    if code not in (200, 202) or not new_id:
        return {
            "ok": False,
            "error": _api_error_text(api_out),
        }, (code if code >= 400 else 502)

    try:
        current = _read_json(path) or job
        if str(current.get("status") or "") == "error":
            current["status"] = "resent"
            current["resent_at"] = _now_iso()
            current["resent_job_id"] = new_id
            _atomic_write_json(path, current)
    except OSError as exc:
        logger.warning("Reenvio ok mas não marcou o erro original %s: %s", job_id, exc)

    return {
        "ok": True,
        "job_id": job_id,
        "new_job_id": new_id,
        "status": api_out.get("status") or "queued",
        "enqueued_at": api_out.get("enqueued_at"),
        "position": api_out.get("position"),
    }, 200


def load_job_detail(job_id: str) -> tuple[dict[str, Any], int]:
    """Lê um job já gravado em ``calculo_jobs/{id}.json`` (somente leitura)."""
    path = _resolved_job_path(job_id)
    if path is None:
        return {"ok": False, "error": "job_id inválido"}, 400

    if not path.is_file():
        return {"ok": False, "error": "Job não encontrado"}, 404

    job = _read_json(path)
    if job is None:
        return {"ok": False, "error": "Arquivo do job ilegível"}, 500

    payload = job.get("payload")
    ip = str(job.get("source_ip") or "").strip() or "-"
    try:
        rank = int(job.get("priority_rank"))
    except (TypeError, ValueError):
        rank = 2
    try:
        job_http = int(job["http_status"]) if job.get("http_status") is not None else None
    except (TypeError, ValueError):
        job_http = job.get("http_status")

    return {
        "ok": True,
        "job_id": str(job.get("job_id") or job_id),
        "status": str(job.get("status") or ""),
        "http_status": job_http,
        "error": (str(job.get("error") or "").strip() or None),
        "source_ip": ip,
        "host": _cloud_label(ip),
        "priority_rank": rank,
        "enqueued_at": job.get("enqueued_at"),
        "started_at": job.get("started_at"),
        "finished_at": job.get("finished_at"),
        "destaques": _extract_highlights(payload),
        "payload": payload if isinstance(payload, (dict, list)) else None,
        "result": job.get("result"),
    }, 200


def _fold_case_part(value: Any) -> str:
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return "".join(ch for ch in text.casefold() if ch.isalnum())


def _error_case_key(job: dict[str, Any]) -> str:
    requerente = _fold_case_part(job.get("requerente"))
    processo = _fold_case_part(job.get("processo"))
    incidente = _fold_case_part(job.get("incidente"))
    if not (requerente or processo or incidente):
        return f"id:{(job.get('job_id') or '').strip()}"
    return f"{requerente}|{processo}|{incidente}"


def _group_error_jobs(errors: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Um item por requerente + processo + incidente; o mais recente representa o grupo."""
    grouped: dict[str, list[dict[str, Any]]] = {}
    order: list[str] = []
    for job in errors:
        key = _error_case_key(job)
        if key not in grouped:
            grouped[key] = []
            order.append(key)
        grouped[key].append(job)
    out: list[dict[str, Any]] = []
    for key in order:
        items = grouped[key]
        lead = dict(items[0])
        lead["error_count"] = len(items)
        lead["error_job_ids"] = [str(item.get("job_id") or "") for item in items]
        lead["error_occurrences"] = [
            {
                "job_id": item.get("job_id"),
                "finished_at": item.get("finished_at"),
                "error": item.get("error"),
                "host": item.get("host"),
                "source_ip": item.get("source_ip"),
            }
            for item in items
        ]
        out.append(lead)
    return out


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


def api_calculo_jobs_keep_count() -> int:
    raw = (
        os.getenv("CALCULO_JOBS_KEEP_COUNT") or _api_env_value("CALCULO_JOBS_KEEP_COUNT")
    ).strip()
    try:
        value = int(raw) if raw else 200
    except ValueError:
        value = 200
    return max(1, value)


def api_calculo_log_path() -> Path:
    raw = (os.getenv("API_CALCULO_LOG") or "").strip()
    if raw:
        return Path(raw)
    return api_calculo_root() / "logs" / "api.log"


def _read_libreoffice_max() -> int | None:
    raw = _api_env_value("LIBREOFFICE_MAX_CONCURRENT")
    if not raw:
        return None
    try:
        return max(1, int(raw))
    except ValueError:
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
    keep = api_calculo_jobs_keep_count()
    disk_done = int(status_counts.get("done", 0))
    disk_error = int(status_counts.get("error", 0))
    generated_at = _now_iso()
    history = ingest_finished_jobs(
        done + errors,
        log_path=api_calculo_log_path(),
        now_iso=generated_at,
    )
    history_by_source = history.get("by_source") or {}
    if isinstance(history_by_source, dict):
        for row in by_source:
            ip = str(row.get("source_ip") or "")
            hist = history_by_source.get(ip) if isinstance(history_by_source.get(ip), dict) else {}
            row["history_done"] = int(hist.get("done") or 0)
            row["history_error"] = int(hist.get("error") or 0)
    return {
        "ok": True,
        "generated_at": generated_at,
        "jobs_dir": str(jobs_dir),
        "api_root": str(api_calculo_root()),
        "counts": {
            "queued": int(status_counts.get("queued", 0)),
            "running": int(status_counts.get("running", 0)),
            "done": disk_done,
            "error": disk_error,
            "other": int(
                sum(
                    v
                    for k, v in status_counts.items()
                    if k not in {"queued", "running", "done", "error"}
                )
            ),
            "unreadable": unreadable,
        },
        "history": {
            "done": max(int(history.get("done") or 0), disk_done),
            "error": max(int(history.get("error") or 0), disk_error),
            "tracked": int(history.get("tracked") or 0),
            "updated_at": history.get("updated_at") or generated_at,
        },
        "retention": {
            "keep": keep,
            "finished_on_disk": disk_done + disk_error,
            "done_on_disk": disk_done,
            "error_on_disk": disk_error,
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
        "recent_errors": _group_error_jobs(errors),
        "instances": [
            _local_instance(
                {
                    "queued": int(status_counts.get("queued", 0)),
                    "running": int(status_counts.get("running", 0)),
                    "done": disk_done,
                    "error": disk_error,
                },
                {
                    "api_calculo": _systemctl_active("api-calculo"),
                    "api_calculo_drive_worker": _systemctl_active("api-calculo-drive-worker"),
                },
                lo_max,
            ),
            _remote_instance(),
        ],
    }
