"""Consulta Monday somente leitura, antes da atualização de cálculo."""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_CENTRO_ROOT = Path(__file__).resolve().parents[3]


def _ensure_centro_on_path() -> None:
    root = str(_CENTRO_ROOT)
    if root not in sys.path:
        sys.path.insert(0, root)


def lookup_incident(processo: str, incidente: str) -> dict[str, Any] | None:
    """Pré-checagem Monday sem mutação (não atribui rejeitado)."""
    try:
        _ensure_centro_on_path()
        from messages_viewer.monday_board import precheck_caso
    except Exception as exc:
        logger.warning("Monday lookup indisponível: %s", type(exc).__name__)
        return None
    try:
        return precheck_caso(
            processo=str(processo or ""),
            incidente=str(incidente or ""),
            mutate=False,
        )
    except Exception as exc:
        logger.warning(
            "Monday lookup falhou para %s/%s: %s",
            processo,
            incidente,
            type(exc).__name__,
        )
        return None
