# -*- coding: utf-8 -*-
"""Pré-checagem Monday: já atribuído vs Proposta Enviada=Rejeitado vs livre."""

from __future__ import annotations

from typing import Any

import requests

from messages_viewer.monday_board import (  # noqa: F401
    MondayPrecheckError,
    _col_text,
    _evaluate_item,
    _fetch_item_columns,
    _find_items,
    _format_cnj,
    _inc_search_values,
    _is_rejeitado,
    _item_matches,
    _livre_fallback,
    _norm_inc,
    _norm_proc,
    _precheck_unavailable,
    _proc_search_values,
    is_configured,
    lookup_si_processo,
    monday_block_message,
    resolve_column_ids,
)
from messages_viewer.plataforma_auth import solicitante_from_user


def precheck_caso(
    *,
    processo: str = "",
    incidente: str = "",
    numero_cumprimento: str = "",
    is_cumprimento: bool = False,
    user: dict | None = None,
    col_ids: dict[str, str] | None = None,
    mutate: bool = True,
) -> dict[str, Any]:
    sol = solicitante_from_user(user)
    requester_email = (sol or {}).get("email_solicitante") or ""
    requester_name = (sol or {}).get("nome_solicitante") or ""
    proc = numero_cumprimento if is_cumprimento else processo
    inc = "" if is_cumprimento else incidente
    base = {
        "processo": str(processo or "").strip(),
        "incidente": str(incidente or "").strip(),
        "numero_cumprimento": str(numero_cumprimento or "").strip(),
        "is_cumprimento": bool(is_cumprimento),
    }
    if not is_configured():
        return {
            **base,
            "outcome": "livre",
            "label": "Monday não configurada — segue para inclusão",
            "incluir": True,
            "monday_configured": False,
        }
    if not _norm_proc(proc):
        return {**base, "outcome": "erro", "label": "Processo em falta.", "incluir": False}
    si = lookup_si_processo(proc, inc)
    if si:
        si.update(base)
        return si
    try:
        ids = col_ids or resolve_column_ids()
        found = _find_items(ids, proc, inc)
        if not found:
            return {
                **base,
                "outcome": "livre",
                "label": "Livre — não está na Monday",
                "incluir": True,
                "monday_configured": True,
            }
        detailed = _fetch_item_columns([str(it.get("id")) for it in found], ids)
        matched = [it for it in detailed if _item_matches(it, ids, proc, inc)] or detailed
        best = matched[0]
        result = _evaluate_item(
            best,
            ids,
            requester_email=requester_email,
            requester_name=requester_name,
            assign_on_rejeitado=mutate,
        )
        result.update(base)
        result["monday_configured"] = True
        return result
    except requests.RequestException as exc:
        return _precheck_unavailable(base, exc)
    except MondayPrecheckError as exc:
        return _precheck_unavailable(base, exc)


def precheck_casos(
    casos: list[dict[str, Any]],
    user: dict | None,
    *,
    mutate: bool = True,
) -> dict[str, Any]:
    items = []
    for raw in casos:
        if not isinstance(raw, dict):
            continue
        items.append(
            precheck_caso(
                processo=str(raw.get("processo") or ""),
                incidente=str(raw.get("incidente") or ""),
                numero_cumprimento=str(raw.get("numero_cumprimento") or ""),
                is_cumprimento=bool(raw.get("is_cumprimento")),
                user=user,
                mutate=mutate,
            )
        )
    return {
        "ok": True,
        "monday_configured": is_configured(),
        "precheck_ok": bool(items) and all(not it.get("monday_error") for it in items),
        "items": items,
    }


def incluir_apos_precheck(
    casos: list[dict[str, Any]],
    user: dict | None,
    *,
    confirmar_duplicado: bool = False,
) -> tuple[dict[str, Any], int]:
    from messages_viewer.solicitacao_inclusao import criar

    clean = [c for c in casos if isinstance(c, dict)]
    checked = precheck_casos(clean, user)
    pre_items = list(checked.get("items") or [])
    if len(pre_items) < len(clean):
        pre_items.extend(_livre_fallback(raw) for raw in clean[len(pre_items) :])
    items_out: list[dict[str, Any]] = []
    claimed_once = False
    http_code = 200
    for raw, pre in zip(clean, pre_items):
        row = dict(pre or {})
        row["solicitacao"] = None
        if not row.get("incluir"):
            items_out.append(row)
            continue
        payload = {
            "processo": raw.get("processo"),
            "incidente": raw.get("incidente"),
            "cpf_credor": raw.get("cpf_credor") or raw.get("cpf"),
            "requerente": raw.get("requerente") or raw.get("nome") or raw.get("nome_credor"),
            "is_cumprimento": bool(raw.get("is_cumprimento")),
            "numero_cumprimento": raw.get("numero_cumprimento"),
        }
        if confirmar_duplicado:
            payload["confirmar_duplicado"] = True
        out, code = criar(payload, user, skip_cooldown=claimed_once)
        if code == 429:
            row["outcome"] = "cooldown"
            row["label"] = out.get("error") or "Aguarde para enviar outra solicitação."
            row["incluir"] = False
            row["solicitacao"] = out
            items_out.append(row)
            http_code = 429
            continue
        if code == 409 or (isinstance(out, dict) and out.get("code") == "duplicate_open"):
            row["outcome"] = "duplicado"
            row["label"] = out.get("error") or "Solicitação duplicada na fila."
            row["incluir"] = False
            row["solicitacao"] = out
            items_out.append(row)
            continue
        if code >= 400 or (isinstance(out, dict) and out.get("ok") is False):
            row["outcome"] = "erro"
            row["label"] = (out or {}).get("error") or f"Falha ao incluir (HTTP {code})."
            row["incluir"] = False
            row["solicitacao"] = out
            items_out.append(row)
            continue
        claimed_once = True
        row["outcome"] = "enviado"
        row["label"] = "Enviado para inclusão"
        row["solicitacao"] = out
        items_out.append(row)
    return {
        "ok": True,
        "monday_configured": is_configured(),
        "items": items_out,
    }, http_code
