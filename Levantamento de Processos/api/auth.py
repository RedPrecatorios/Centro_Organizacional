"""API key gate for Levantamento (cloud → cloud).

Accepts any of:
  Authorization: Bearer <key>
  X-API-Token: <key>
  X-API-Key: <key>

Server secret (first non-empty wins):
  API_KEY  |  API_TOKEN

Optional IP allowlist (comma-separated):
  API_ALLOWED_IPS=203.0.113.10,198.51.100.20
  (when set, only these client IPs may call protected routes)
"""

from __future__ import annotations

import ipaddress
import os
import secrets
from typing import Annotated

from fastapi import Header, HTTPException, Request, status


def get_expected_api_key() -> str:
    """Shared secret — prefer API_KEY, fallback API_TOKEN (legacy)."""
    return (
        (os.getenv("API_KEY") or "").strip()
        or (os.getenv("API_TOKEN") or "").strip()
    )


# Back-compat alias used by older imports / app state
def get_expected_api_token() -> str:
    return get_expected_api_key()


def _parse_allowed_ips() -> list[
    ipaddress.IPv4Address
    | ipaddress.IPv6Address
    | ipaddress.IPv4Network
    | ipaddress.IPv6Network
]:
    raw = (os.getenv("API_ALLOWED_IPS") or "").strip()
    if not raw:
        return []
    out: list[
        ipaddress.IPv4Address
        | ipaddress.IPv6Address
        | ipaddress.IPv4Network
        | ipaddress.IPv6Network
    ] = []
    for part in raw.split(","):
        item = part.strip()
        if not item:
            continue
        try:
            if "/" in item:
                out.append(ipaddress.ip_network(item, strict=False))
            else:
                out.append(ipaddress.ip_address(item))
        except ValueError:
            continue
    return out


def _client_ip(request: Request) -> str:
    """Best-effort client IP (honours X-Forwarded-For when behind nginx)."""
    trust_proxy = (os.getenv("API_TRUST_PROXY") or "true").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    if trust_proxy:
        forwarded = (request.headers.get("x-forwarded-for") or "").split(",")[0].strip()
        if forwarded:
            return forwarded
        real_ip = (request.headers.get("x-real-ip") or "").strip()
        if real_ip:
            return real_ip
    if request.client and request.client.host:
        return request.client.host
    return ""


def _ip_allowed(client_ip: str) -> bool:
    allow = _parse_allowed_ips()
    if not allow:
        return True
    if not client_ip:
        return False
    try:
        addr = ipaddress.ip_address(client_ip)
    except ValueError:
        return False
    for rule in allow:
        if isinstance(rule, (ipaddress.IPv4Network, ipaddress.IPv6Network)):
            if addr in rule:
                return True
        elif addr == rule:
            return True
    return False


def _extract_provided_key(
    authorization: str | None,
    x_api_token: str | None,
    x_api_key: str | None,
) -> str:
    if authorization:
        parts = authorization.strip().split(None, 1)
        if len(parts) == 2 and parts[0].lower() == "bearer":
            return parts[1].strip()
        return authorization.strip()
    if x_api_key:
        return x_api_key.strip()
    if x_api_token:
        return x_api_token.strip()
    return ""


def require_api_token(
    request: Request,
    authorization: Annotated[str | None, Header()] = None,
    x_api_token: Annotated[str | None, Header(alias="X-API-Token")] = None,
    x_api_key: Annotated[str | None, Header(alias="X-API-Key")] = None,
) -> None:
    """
    Protect /searches* routes.

    Accept either:
      Authorization: Bearer <key>
      X-API-Key: <key>
      X-API-Token: <key>
    """
    expected = get_expected_api_key()
    if not expected:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="API_KEY/API_TOKEN nao configurado no servidor.",
        )

    client_ip = _client_ip(request)
    if not _ip_allowed(client_ip):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="IP do cliente nao autorizado.",
        )

    provided = _extract_provided_key(authorization, x_api_token, x_api_key)
    if not provided or not secrets.compare_digest(provided, expected):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="API key invalida ou ausente.",
            headers={"WWW-Authenticate": "Bearer"},
        )
