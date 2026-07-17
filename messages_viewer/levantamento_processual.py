# -*- coding: utf-8 -*-
"""
Levantamento Processual: proxy para a API dashboard-backend (TJSP searches).

Env:
  TJSP_API_BASE_URL  — ex. http://127.0.0.1:8003
  TJSP_API_TOKEN     — Bearer (fallback: API_TOKEN)
  TJSP_API_TIMEOUT   — timeout HTTP em segundos (default 60)
  TJSP_POLL_INTERVAL_MS — intervalo sugerido ao frontend (default 5000)
"""

from __future__ import annotations

import os
import re
from typing import Any

import requests

_TERMINAL = frozenset({"done", "failed"})


def api_base() -> str | None:
    base = (os.getenv("TJSP_API_BASE_URL") or "").strip().rstrip("/")
    return base or None


def api_token() -> str | None:
    token = (
        (os.getenv("TJSP_API_TOKEN") or "").strip()
        or (os.getenv("API_TOKEN") or "").strip()
    )
    return token or None


def is_configured() -> bool:
    return bool(api_base() and api_token())


def poll_interval_ms() -> int:
    try:
        value = int((os.getenv("TJSP_POLL_INTERVAL_MS") or "5000").strip())
    except ValueError:
        value = 5000
    return max(2000, min(value, 60000))


def _timeout() -> float:
    try:
        value = float((os.getenv("TJSP_API_TIMEOUT") or "60").strip())
    except ValueError:
        value = 60.0
    return max(5.0, min(value, 120.0))


def _headers() -> dict[str, str]:
    return {
        "Authorization": f"Bearer {api_token()}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def _not_configured() -> tuple[dict[str, Any], int]:
    return (
        {
            "ok": False,
            "error": "API de levantamento não configurada. Defina TJSP_API_BASE_URL e TJSP_API_TOKEN no .env.",
        },
        503,
    )


def _map_http_error(response: requests.Response) -> tuple[dict[str, Any], int]:
    code = int(response.status_code)
    if code == 401:
        return {"ok": False, "error": "Token rejeitado pela API (401)."}, 401
    if code == 404:
        return {"ok": False, "error": "Job não encontrado (404)."}, 404
    detail: Any
    try:
        detail = response.json()
    except Exception:
        detail = (response.text or "")[:500]
    msg = f"API retornou HTTP {code}."
    if isinstance(detail, dict):
        d = detail.get("detail")
        if isinstance(d, str) and d.strip():
            msg = d.strip()
        elif isinstance(d, list) and d:
            msg = str(d[0])
    return (
        {
            "ok": False,
            "error": msg,
            "detail": detail,
        },
        code if code >= 400 else 502,
    )


def _request_error(exc: BaseException) -> tuple[dict[str, Any], int]:
    if isinstance(exc, requests.Timeout):
        return {"ok": False, "timeout": True, "error": "Timeout ao contactar a API."}, 504
    if isinstance(exc, requests.ConnectionError):
        return {
            "ok": False,
            "error": f"Não foi possível ligar à API ({api_base()}).",
        }, 503
    return {"ok": False, "error": str(exc) or "Erro ao contactar a API."}, 502


def _normalize_payload_body(
    *,
    nome: str | None = None,
    cpf: str | None = None,
    processo: str | None = None,
) -> tuple[dict[str, str] | None, str | None]:
    """Devolve body JSON da API ou (None, mensagem de erro)."""
    n = (nome or "").strip()
    c = re.sub(r"\D+", "", (cpf or "").strip())
    p = (processo or "").strip()

    if c:
        if n or p:
            return None, "CPF deve ser informado sozinho (sem nome nem processo)."
        if len(c) != 11:
            return None, "CPF deve ter 11 dígitos."
        return {"cpf": c}, None

    if p:
        if not n:
            return (
                None,
                "Ao pesquisar por processo, informe também o nome (Monday) "
                "para filtrar os incidentes na capa.",
            )
        if len(n) < 3:
            return None, "Informe um nome com pelo menos 3 caracteres."
        if len(p) < 8:
            return None, "Informe um número de processo válido."
        return {"processo": p, "nome": n}, None

    if not n:
        return None, "Informe nome, CPF, ou processo + nome."
    if len(n) < 3:
        return None, "Informe um nome com pelo menos 3 caracteres."
    return {"nome": n}, None


def extract_processos_aptos(payload: dict[str, Any] | None) -> list[Any]:
    if not isinstance(payload, dict):
        return []

    def _as_list(value: Any) -> list[Any] | None:
        return value if isinstance(value, list) else None

    for key in ("processos_aptos", "aptos", "processosAptos"):
        found = _as_list(payload.get(key))
        if found is not None:
            return found
    result = payload.get("result")
    if isinstance(result, dict):
        for key in ("processos_aptos", "aptos", "processosAptos"):
            found = _as_list(result.get(key))
            if found is not None:
                return found
    return []


def extract_processos_inaptos(payload: dict[str, Any] | None) -> list[Any]:
    if not isinstance(payload, dict):
        return []

    def _as_list(value: Any) -> list[Any] | None:
        return value if isinstance(value, list) else None

    for key in ("processos_inaptos", "inaptos", "processosInaptos"):
        found = _as_list(payload.get(key))
        if found is not None:
            return found

    result = payload.get("result")
    if isinstance(result, dict):
        for key in ("processos_inaptos", "inaptos"):
            found = _as_list(result.get(key))
            if found is not None:
                return found

    skipped = _as_list(payload.get("skipped"))
    if skipped is None and isinstance(result, dict):
        skipped = _as_list(result.get("skipped"))
    if not skipped:
        return []
    out: list[Any] = []
    for item in skipped:
        if not isinstance(item, dict):
            continue
        out.append(
            {
                "numero_de_processo": item.get("numero_de_processo"),
                "numero_do_incidente": item.get("numero_do_incidente"),
                "motivo": item.get("motivo") or item.get("reason") or "INAPTO",
                "status": "inapto",
                "source": item.get("source") or "pipeline",
                "record": item.get("record") or {},
            }
        )
    return out


def _enrich(payload: dict[str, Any]) -> dict[str, Any]:
    out = dict(payload)
    out["ok"] = True
    status = str(out.get("status") or "").strip().lower()
    out["terminal"] = status in _TERMINAL
    if status == "done":
        out["processos_aptos"] = extract_processos_aptos(out)
        out["processos_inaptos"] = extract_processos_inaptos(out)
    else:
        out.setdefault("processos_aptos", [])
        out.setdefault("processos_inaptos", [])
    return out


def api_health() -> tuple[dict[str, Any], int]:
    base = api_base()
    if not base:
        return _not_configured()
    try:
        response = requests.get(
            f"{base}/api/v1/health",
            timeout=min(10.0, _timeout()),
        )
    except Exception as exc:  # noqa: BLE001
        out, code = _request_error(exc)
        out["healthy"] = False
        return out, code
    if response.status_code >= 400:
        out, code = _map_http_error(response)
        out["healthy"] = False
        return out, code
    try:
        body = response.json()
    except Exception:
        body = {}
    healthy = response.status_code < 400
    if isinstance(body, dict):
        if "healthy" in body:
            healthy = bool(body.get("healthy"))
        elif str(body.get("status") or "").lower() in {"ok", "healthy", "up"}:
            healthy = True
        elif body.get("ok") is False:
            healthy = False
    return {"ok": healthy, "healthy": healthy, "api": body}, (200 if healthy else 503)


def create_search(
    nome: str | None = None,
    *,
    cpf: str | None = None,
    processo: str | None = None,
) -> tuple[dict[str, Any], int]:
    if not is_configured():
        return _not_configured()
    body, err = _normalize_payload_body(nome=nome, cpf=cpf, processo=processo)
    if err or not body:
        return {"ok": False, "error": err or "Pedido inválido."}, 400
    try:
        response = requests.post(
            f"{api_base()}/api/v1/searches",
            headers=_headers(),
            json=body,
            timeout=_timeout(),
        )
    except Exception as exc:  # noqa: BLE001
        return _request_error(exc)
    if response.status_code >= 400:
        return _map_http_error(response)
    try:
        payload = response.json()
    except Exception:
        return {"ok": False, "error": "Resposta inválida da API ao criar busca."}, 502
    if not isinstance(payload, dict):
        return {"ok": False, "error": "Resposta inválida da API ao criar busca."}, 502
    return _enrich(payload), 200


def get_search(job_id: str) -> tuple[dict[str, Any], int]:
    if not is_configured():
        return _not_configured()
    jid = (job_id or "").strip()
    if not jid:
        return {"ok": False, "error": "job_id em falta."}, 400
    try:
        response = requests.get(
            f"{api_base()}/api/v1/searches/{jid}",
            headers=_headers(),
            timeout=_timeout(),
        )
    except Exception as exc:  # noqa: BLE001
        return _request_error(exc)
    if response.status_code >= 400:
        return _map_http_error(response)
    try:
        body = response.json()
    except Exception:
        return {"ok": False, "error": "Resposta inválida da API ao consultar job."}, 502
    if not isinstance(body, dict):
        return {"ok": False, "error": "Resposta inválida da API ao consultar job."}, 502
    return _enrich(body), 200
