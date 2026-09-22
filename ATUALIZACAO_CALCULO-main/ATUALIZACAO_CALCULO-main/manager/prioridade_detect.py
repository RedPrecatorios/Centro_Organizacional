# -*- coding: utf-8 -*-
"""Detecta prioridade DEPRE para prefixar PRIORI no nome da planilha.

A UI não pergunta. O script usa coleta_depre (valor_pago) e os JSON da
Análise Processual (extração / fanout), o mesmo sinal que já alimenta o cálculo.
"""
from __future__ import annotations

import json
import os
from typing import Any


_DEFAULT_REFACTOR_ROOT = "/mnt/volume_nyc1_1778499775066/REFACTOR_TJSP"
_STATUS_SEM_SALDO = frozenset({"sem_saldo", "sem saldo"})
_STATUS_EXTRAIDO = frozenset({"extraido", "completo", "somente_oficio"})


def _refactor_root() -> str:
    return (os.getenv("REFACTOR_TJSP_PATH") or _DEFAULT_REFACTOR_ROOT).rstrip("/")


def _case_key(processo: str | None, incidente: str | None) -> str:
    proc = str(processo or "").strip()
    inc = str(incidente or "").strip()
    if not proc:
        return ""
    return f"{proc}_{inc}" if inc else proc


def _read_json(path: str) -> dict[str, Any] | None:
    if not path or not os.path.isfile(path):
        return None
    try:
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)
    except (OSError, json.JSONDecodeError, TypeError):
        return None
    return data if isinstance(data, dict) else None


def _to_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def detect_from_coleta_depre(valor_pago: Any, saldo: Any) -> bool:
    """Pagamento de prioridade com saldo remanescente (regra antiga do script)."""
    pago = _to_float(valor_pago)
    if pago is None or pago <= 0:
        return False
    restante = _to_float(saldo)
    if restante is not None and restante == 0:
        return False
    return True


def _extracao_indica_prioridade(data: dict[str, Any]) -> bool:
    status = str(data.get("status") or "").strip().lower()
    if status in _STATUS_SEM_SALDO:
        return False
    extra = data.get("extracao") if isinstance(data.get("extracao"), dict) else data
    tem_valor = any(
        extra.get(key)
        for key in ("valor_principal", "sub_total", "total", "juros_moratorios")
    )
    if status in _STATUS_EXTRAIDO and tem_valor:
        return True
    if tem_valor and status in {"", "ok"}:
        return True
    return False


def _extra_infos_indica_prioridade(extra: Any) -> bool:
    if not isinstance(extra, dict):
        return False
    if extra.get("prioridade") is not True:
        return False
    status = str(extra.get("prioridade_status") or "").strip().lower()
    if status in _STATUS_SEM_SALDO:
        return False
    return True


def _payload_indica_prioridade(data: dict[str, Any] | None) -> bool:
    if not data:
        return False
    context = data.get("context") if isinstance(data.get("context"), dict) else {}
    if _extra_infos_indica_prioridade(context.get("extra_infos")):
        return True
    if _extra_infos_indica_prioridade(data.get("extra_infos")):
        return True
    records = data.get("records")
    if isinstance(records, list):
        for rec in records:
            if not isinstance(rec, dict):
                continue
            if rec.get("prioridade") is True:
                return True
            if _extra_infos_indica_prioridade(rec.get("extra_infos")):
                return True
    return False


def detect_from_analise_outputs(processo: str | None, incidente: str | None) -> bool:
    """JSON gravado pela Análise Processual (extração DEPRE / fanout)."""
    key = _case_key(processo, incidente)
    if not key:
        return False
    root = _refactor_root()
    extracao = _read_json(
        os.path.join(root, "output", "depre_prioridade", f"{key}_saldo_extracao.json")
    )
    if extracao and _extracao_indica_prioridade(extracao):
        return True
    fanout = _read_json(os.path.join(root, "output", "json", f"{key}_fanout.json"))
    if _payload_indica_prioridade(fanout):
        return True
    preenchimento = _read_json(os.path.join(root, "output", "json", f"{key}.json"))
    if _payload_indica_prioridade(preenchimento):
        return True
    return False


def resolve_planilha_prioridade(
    *,
    explicit: bool = False,
    valor_pago: Any = None,
    saldo: Any = None,
    processo: str | None = None,
    incidente: str | None = None,
) -> bool:
    """True se a planilha deve chamar-se PRIORI.

    ``explicit`` só força True (cálculo manual). ``False`` nunca desliga a
    deteção automática.
    """
    if explicit:
        return True
    if detect_from_coleta_depre(valor_pago, saldo):
        return True
    if detect_from_analise_outputs(processo, incidente):
        return True
    return False
