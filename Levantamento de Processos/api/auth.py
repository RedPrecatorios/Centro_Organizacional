"""Simple bearer-token gate for API v1 (shared secret with the cloud client)."""

from __future__ import annotations

import os
import secrets
from typing import Annotated

from fastapi import Header, HTTPException, status


def get_expected_api_token() -> str:
    return (os.getenv("API_TOKEN") or "").strip()


def require_api_token(
    authorization: Annotated[str | None, Header()] = None,
    x_api_token: Annotated[str | None, Header(alias="X-API-Token")] = None,
) -> None:
    """
    Accept either:
      Authorization: Bearer <token>
      X-API-Token: <token>
    """
    expected = get_expected_api_token()
    if not expected:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="API_TOKEN nao configurado no servidor.",
        )

    provided = ""
    if authorization:
        parts = authorization.strip().split(None, 1)
        if len(parts) == 2 and parts[0].lower() == "bearer":
            provided = parts[1].strip()
        else:
            provided = authorization.strip()
    elif x_api_token:
        provided = x_api_token.strip()

    if not provided or not secrets.compare_digest(provided, expected):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token invalido ou ausente.",
            headers={"WWW-Authenticate": "Bearer"},
        )
