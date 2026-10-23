# -*- coding: utf-8 -*-
"""Cliente do Selo headless (TOTP) para o painel 2FA."""

from __future__ import annotations

import json
import os
import re
import urllib.error
import urllib.request
from typing import Any

_ACCOUNT_ID_RE = re.compile(r"^[0-9a-f]{32}$")


def selo_configured() -> bool:
    return bool((os.getenv("SELO_TOTP_URL") or "http://127.0.0.1:8766").strip())


def _base_url() -> str:
    return (os.getenv("SELO_TOTP_URL") or "http://127.0.0.1:8766").strip().rstrip("/")


def _timeout() -> float:
    try:
        return max(0.5, float(os.getenv("SELO_TOTP_TIMEOUT") or "2"))
    except ValueError:
        return 2.0


def _headers(*, json_body: bool = False) -> dict[str, str]:
    headers = {"Accept": "application/json"}
    if json_body:
        headers["Content-Type"] = "application/json"
    token = (os.getenv("SELO_API_TOKEN") or "").strip()
    if token:
        headers["X-Selo-Token"] = token
    return headers


def _request(
    method: str,
    path: str,
    *,
    body: dict | None = None,
) -> dict[str, Any]:
    url = f"{_base_url()}{path}"
    data = None
    headers = _headers(json_body=body is not None)
    if body is not None:
        data = json.dumps(body, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=_timeout()) as resp:
            raw = resp.read().decode("utf-8")
            payload = json.loads(raw) if raw else {}
            status = int(getattr(resp, "status", 200) or 200)
    except urllib.error.HTTPError as exc:
        try:
            raw = exc.read().decode("utf-8")
            detail = json.loads(raw) if raw else {}
        except Exception:
            detail = {}
        err = detail.get("error") if isinstance(detail, dict) else None
        return {
            "ok": False,
            "error": err or f"Selo HTTP {exc.code}",
            "status": int(exc.code),
            "accounts": [],
        }
    except Exception as exc:
        return {
            "ok": False,
            "error": f"Selo indisponível: {exc}",
            "status": 503,
            "accounts": [],
        }

    if not isinstance(payload, dict):
        return {"ok": False, "error": "Resposta inválida do Selo", "status": 502, "accounts": []}

    if not payload.get("ok", True):
        return {
            "ok": False,
            "error": payload.get("error") or "Falha no Selo",
            "status": status if status >= 400 else 400,
            "accounts": payload.get("accounts") or [],
        }

    payload["ok"] = True
    payload["status"] = status
    return payload


def fetch_codes() -> dict[str, Any]:
    """Lista contas e códigos atuais."""
    data = _request("GET", "/api/codes")
    if not data.get("ok"):
        return data
    accounts = data.get("accounts") or []
    if not isinstance(accounts, list):
        accounts = []
    return {
        "ok": True,
        "server_time": data.get("server_time"),
        "accounts": accounts,
        "status": 200,
    }


def health() -> dict[str, Any]:
    return _request("GET", "/api/health")


def add_account(secret: str, name: str = "", issuer: str = "") -> dict[str, Any]:
    return _request(
        "POST",
        "/api/accounts",
        body={
            "secret": (secret or "").strip(),
            "name": (name or "").strip(),
            "issuer": (issuer or "").strip(),
        },
    )


def delete_account(account_id: str) -> dict[str, Any]:
    aid = (account_id or "").strip().lower()
    if not _ACCOUNT_ID_RE.fullmatch(aid):
        return {"ok": False, "error": "Conta inválida.", "status": 400}
    return _request("DELETE", f"/api/accounts/{aid}")
