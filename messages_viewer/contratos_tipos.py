# -*- coding: utf-8 -*-
"""Tipos de contrato espelhando contratos.php / gerar_preca.php / gerar_rpv.php."""
from __future__ import annotations

from typing import Any


def template_from_cessionaria(cessionaria: str) -> str:
    name = (cessionaria or "").strip()
    if name == "RED PRECATÓRIOS E INTERMEDIAÇÃO LTDA":
        return "RED"
    if name == "GT PRECATÓRIOS E ASSESSORIA LTDA":
        return "GT"
    if not name:
        return ""
    return "NENHUM"


_PRECA_NORMAL: list[tuple[str, str]] = [
    ("0", "TODOS"),
    ("1", "ADITAMENTO AO INSTRUMENTO PARTICULAR DE CESSÃO DE CRÉDITO"),
    ("2", "INSTRUMENTO PARTICULAR DE CESSÃO DE CRÉDITO"),
    ("6", "DECLARAÇÃO DE RESPONSABILIDADE"),
    ("7", "COMUNICADO ENTE DEVEDOR"),
    ("9", "PEDIDO DE HABILITAÇÃO"),
    ("14", "PETIÇÃO DE SOLICITAÇÃO DE HABILITAÇÃO PGE"),
    ("15", "PROCURAÇÃO PGE"),
    ("16", "PROCURAÇÃO PODERES ESPECÍFICOS"),
    ("17", "PROCURAÇÃO SEM RESERVA"),
    ("19", "PROCURAÇÃO PÚBLICA"),
    ("21", "TERMO DE QUITAÇÃO"),
    ("22", "SUBSTABELECIMENTO"),
    ("23", "TRANSLADO ESCRITURA PÚBLICA"),
    ("24", "DECLARAÇÃO DE ENDEREÇO"),
    ("25", "PETIÇÃO DE COMUNICAÇÃO DE CESSÃO - INCIDENTE"),
    ("26", "INSTRUMENTO PROMESSA DE CESSÃO"),
]

_RPV_NORMAL: list[tuple[str, str]] = [
    ("0", "TODOS"),
    ("1", "DECLARAÇÃO DE RESPONSABILIDADE"),
    ("2", "INSTRUMENTO PARTICULAR DE CESSÃO DE CRÉDITO"),
    ("3", "ADITAMENTO AO INSTRUMENTO PARTICULAR DE CESSÃO DE CRÉDITO"),
    ("5", "PETIÇÃO DE HABILITAÇÃO"),
    ("6", "PROCURAÇÃO DE LEVANTAMENTO"),
    ("7", "DECLARAÇÃO DE LEVANTAMENTO"),
    ("8", "TERMO DE QUITAÇÃO"),
    ("9", "SUBSTABELECIMENTO"),
    ("10", "TRANSLADO ESCRITURA PÚBLICA"),
]

_RECUPERE: list[tuple[str, str]] = [
    ("1", "PROCURAÇÃO - M"),
    ("2", "PROCURAÇÃO - F"),
    ("3", "CONTRATO DE PRESTAÇÃO DE SERVIÇOS - M"),
    ("4", "CONTRATO DE PRESTAÇÃO DE SERVIÇOS - F"),
]


def listar_tipos(*, tipo: str = "preca", recupere: bool = False) -> list[dict[str, str]]:
    tipo_norm = (tipo or "preca").strip().lower()
    if recupere:
        source = _RECUPERE
    elif tipo_norm == "rpv":
        source = _RPV_NORMAL
    else:
        source = _PRECA_NORMAL
    return [{"id": i, "label": label} for i, label in source]


def tipos_payload(*, tipo: str, recupere: bool = False) -> dict[str, Any]:
    return {
        "ok": True,
        "tipo": tipo,
        "recupere": recupere,
        "contratos": listar_tipos(tipo=tipo, recupere=recupere),
    }
