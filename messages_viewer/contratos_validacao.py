# -*- coding: utf-8 -*-
"""Validação prévia de campos obrigatórios antes de gerar contratos."""
from __future__ import annotations

import re
from typing import Any

from messages_viewer.contratos_tipos import listar_tipos, template_from_cessionaria
from messages_viewer.pre_analise_ficha import carregar_ficha, field_label
from messages_viewer.pre_analise_ficha_widgets import LEGACY_REQUIRED_FIELDS, PROCURADORIA_BY_TIPO

# Espelha contratos.php (.required / borda amarela) + insert_process.php (save mínimo).
# Campos obrigatórios só na seção de herdeiros (validados em _validar_herdeiros).
_LEGACY_HERDEIRO_ONLY: frozenset[str] = frozenset(
    {
        "herdeiro_habilitado",
        "nome_herdeiro",
        "cpf_herdeiro",
        "parentesco_herdeiro",
        "nacionalidade_herdeiro",
        "estado_civil_herdeiro",
        "percentual_detido",
        "herdeiro_cedente",
    }
)

# insert_process.php — mínimo para salvar/gerar (legado não pré-valida por contrato).
_SAVE_MINIMUM: frozenset[str] = frozenset(
    {
        "cumprimento_de_sentenca",
        "incidente",
        "nome_credor",
        "cpf_credor",
        "cessionaria",
        "percentual_honorarios",
    }
)

# Campos .required sempre visíveis na ficha (sim/não ou select obrigatório).
_ALWAYS_REQUIRED: frozenset[str] = frozenset(
    {
        "estado_civil_credor",
        "nacionalidade_credor",
        "credor_falecido",
        "anuente_credor",
    }
)

_LEGACY_REQUIRED_BASE: tuple[str, ...] = tuple(
    k
    for k in LEGACY_REQUIRED_FIELDS
    if k not in _LEGACY_HERDEIRO_ONLY and k not in _SAVE_MINIMUM and k not in _ALWAYS_REQUIRED
)


def _cell(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() in ("none", "null", "nan"):
        return ""
    return text


def _field_missing(dados: dict[str, Any], key: str) -> bool:
    return not _cell(dados.get(key))


def _missing_fields(dados: dict[str, Any], keys: list[str]) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    seen: set[str] = set()
    for key in keys:
        if key in seen:
            continue
        seen.add(key)
        if _field_missing(dados, key):
            out.append({"key": key, "label": field_label(key)})
    return out


def _merge_dados_overlay(
    base: dict[str, Any], overlay: dict[str, Any] | None
) -> dict[str, Any]:
    out = dict(base)
    if not isinstance(overlay, dict):
        return out
    for key, value in overlay.items():
        text = _cell(value)
        if text:
            out[key] = text
    return out


def _apply_derived_fields(dados: dict[str, Any]) -> None:
    if _field_missing(dados, "procuradoria"):
        tipo = _cell(dados.get("procuradoria_tipo"))
        proc = PROCURADORIA_BY_TIPO.get(tipo)
        if proc:
            dados["procuradoria"] = proc

    if _cell(dados.get("cessionaria")):
        if _field_missing(dados, "template"):
            dados["template"] = template_from_cessionaria(dados["cessionaria"])

    if _field_missing(dados, "percentual_de_compra") and _cell(
        dados.get("percentual_honorarios")
    ):
        pct_raw = re.sub(r"[^\d,.-]", "", _cell(dados.get("percentual_honorarios"))).replace(
            ",", "."
        )
        try:
            honor = float(pct_raw or 0)
            if 1 <= honor <= 100:
                compra = 100 - honor
                dados["percentual_de_compra"] = (
                    f"{compra:.2f}".rstrip("0").rstrip(".").replace(".", ",") + "%"
                )
        except ValueError:
            pass


def _normalize_herdeiros_payload(raw: Any) -> list[dict[str, Any]]:
    if not isinstance(raw, list):
        return []
    out: list[dict[str, Any]] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        dados = item.get("dados") if isinstance(item.get("dados"), dict) else item
        if not isinstance(dados, dict):
            dados = {}
        out.append({"dados": dict(dados), "ordem": item.get("ordem")})
    return out


def _legacy_required_keys(dados: dict[str, Any]) -> list[str]:
    keys = list(_SAVE_MINIMUM | _ALWAYS_REQUIRED)
    return keys


def _herdeiro_dados(h: dict[str, Any]) -> dict[str, Any]:
    dados = dict(h.get("dados") or {})
    if h.get("nome_herdeiro") and _field_missing(dados, "nome_herdeiro"):
        dados["nome_herdeiro"] = h.get("nome_herdeiro")
    if h.get("cpf_herdeiro") and _field_missing(dados, "cpf_herdeiro"):
        dados["cpf_herdeiro"] = h.get("cpf_herdeiro")
    return dados


def _parse_percent_num(value: Any) -> float:
    text = _cell(value).replace(",", ".")
    try:
        return float(re.sub(r"[^\d.-]", "", text) or 0)
    except ValueError:
        return 0.0


def _normalize_cpf(value: Any) -> str:
    return re.sub(r"\D", "", _cell(value))


def validar_herdeiros_save(herdeiros: list[dict[str, Any]]) -> list[str]:
    """Validação completa de herdeiros (paridade legado). Lista vazia = OK."""
    return _validar_herdeiros(herdeiros)


def _validar_herdeiros(herdeiros: list[dict[str, Any]]) -> list[str]:
    issues: list[str] = []
    if not herdeiros:
        return issues

    total = 0.0
    cpfs_vistos: dict[str, str] = {}
    for idx, h in enumerate(herdeiros, start=1):
        d = _herdeiro_dados(h)
        nome = _cell(d.get("nome_herdeiro")) or f"Herdeiro {idx}"
        required = [
            "herdeiro_habilitado",
            "nome_herdeiro",
            "cpf_herdeiro",
            "parentesco_herdeiro",
            "nacionalidade_herdeiro",
            "estado_civil_herdeiro",
            "percentual_detido",
            "herdeiro_cedente",
            "percentual_honorarios_herdeiro",
        ]
        missing = [field_label(k) for k in required if _field_missing(d, k)]
        if missing:
            issues.append(f"{nome}: faltam {', '.join(missing)}.")

        cpf_norm = _normalize_cpf(d.get("cpf_herdeiro"))
        if cpf_norm:
            if cpf_norm in cpfs_vistos:
                issues.append(
                    f"{nome}: CPF duplicado (mesmo de {cpfs_vistos[cpf_norm]})."
                )
            else:
                cpfs_vistos[cpf_norm] = nome

        detido = _parse_percent_num(d.get("percentual_detido"))
        if _field_missing(d, "percentual_detido"):
            pass
        elif detido <= 0:
            issues.append(f"{nome}: percentual detido inválido.")
        else:
            total += detido

        estado = _cell(d.get("estado_civil_herdeiro")).lower()
        regime = _cell(d.get("regime_herdeiro_conjuge"))
        if estado == "casado(a)":
            if _field_missing(d, "regime_herdeiro_conjuge"):
                issues.append(f"{nome}: regime do cônjuge obrigatório (casado).")
        if estado == "casado(a)" and regime in (
            "Comunhão Universal de Bens",
            "Comunhão Parcial de Bens",
        ):
            if _field_missing(d, "nome_conjuge_herdeiro") or _field_missing(
                d, "cpf_conjuge_herdeiro"
            ):
                issues.append(f"{nome}: cônjuge obrigatório (comunhão de bens).")

        if _cell(d.get("herdeiro_cedente")).lower() == "sim":
            cedido = _parse_percent_num(d.get("percentual_cedido"))
            if _field_missing(d, "percentual_cedido") or cedido <= 0:
                issues.append(f"{nome}: cedente exige percentual cedido.")
            elif cedido > detido:
                issues.append(
                    f"{nome}: percentual cedido não pode exceder o percentual detido."
                )
            proposta = _cell(d.get("valor_da_proposta_herdeiro"))
            if not proposta:
                issues.append(f"{nome}: cedente exige valor da proposta.")
            for bank_key in (
                "banco_herdeiro",
                "agencia_herdeiro",
                "conta_herdeiro",
                "tipo_conta_herdeiro",
                "tem_chave_pix_herdeiro",
            ):
                if _field_missing(d, bank_key):
                    issues.append(
                        f"{nome}: cedente exige {field_label(bank_key)}."
                    )
                    break
            if _cell(d.get("tem_chave_pix_herdeiro")).lower() == "sim" and _field_missing(
                d, "chave_pix_herdeiro"
            ):
                issues.append(f"{nome}: cedente com PIX exige chave PIX.")

    if total and abs(total - 100.0) > 0.01:
        issues.append(
            f"Soma dos percentuais detidos pelos herdeiros: {total:.2f}% (esperado 100%)."
        )
    return issues


def _credor_condicional(dados: dict[str, Any]) -> list[str]:
    issues: list[str] = []
    estado = _cell(dados.get("estado_civil_credor")).lower()
    regime = _cell(dados.get("regime_conjuge_credor"))
    if estado == "casado(a)":
        if _field_missing(dados, "regime_conjuge_credor"):
            issues.append("Credor casado: informe o regime do cônjuge.")
    if estado == "casado(a)" and regime in (
        "Comunhão Universal de Bens",
        "Comunhão Parcial de Bens",
    ):
        if _field_missing(dados, "nome_conjuge_credor") or _field_missing(
            dados, "cpf_conjuge_credor"
        ):
            issues.append("Credor casado: preencha nome e CPF do cônjuge.")
    if _cell(dados.get("anuente_credor")).lower() == "sim":
        for key in ("nome_anuente_credor", "cpf_anuente_credor"):
            if _field_missing(dados, key):
                issues.append(f"Anuente indicado: falta {field_label(key)}.")
                break
    if _cell(dados.get("credor_falecido")).lower() == "sim" and _field_missing(
        dados, "data_do_obito"
    ):
        issues.append("Credor falecido: informe a data do óbito.")
    return issues


def _resolve_contrato_labels(
    *, tipo: str, contrato_id: str, recupere: bool
) -> list[str]:
    tipos = listar_tipos(tipo=tipo, recupere=recupere)
    by_id = {t["id"]: t["label"] for t in tipos}
    cid = str(contrato_id or "0").strip()
    if cid != "0":
        label = by_id.get(cid)
        return [label] if label else []
    return [t["label"] for t in tipos if t["id"] != "0"]


def validar_contratos_geracao(
    *,
    cumprimento_de_sentenca: str,
    incidente: str,
    nome_credor: str,
    tipo: str,
    contrato_id: str,
    recupere: int = 0,
    caso_id: str | None = None,
    id_externo: str | None = None,
    cessionaria: str | None = None,
    dados_override: dict[str, Any] | None = None,
    herdeiros_override: list[dict[str, Any]] | None = None,
    herdeiros_abertos_override: bool | None = None,
    herdeiros_validado_override: bool | None = None,
    depre_hint: str | None = None,
) -> tuple[dict[str, Any], int]:
    cumprimento = _cell(cumprimento_de_sentenca)
    inc = _cell(incidente)
    credor = _cell(nome_credor)
    if not cumprimento or not inc:
        return (
            {"ok": False, "valido": False, "error": "Processo e incidente são obrigatórios."},
            400,
        )
    if not credor:
        return (
            {
                "ok": False,
                "valido": False,
                "error": "nome_credor é obrigatório para gerar contratos.",
            },
            400,
        )

    pack, code = carregar_ficha(
        cumprimento_de_sentenca=cumprimento,
        incidente=inc,
        caso_id=caso_id,
        id_externo=id_externo,
        nome_credor_hint=credor,
        depre_hint=depre_hint,
    )
    if code != 200 or not pack.get("ok"):
        return pack, code

    dados = _merge_dados_overlay(dict(pack.get("dados") or {}), dados_override)
    dados["cumprimento_de_sentenca"] = dados.get("cumprimento_de_sentenca") or cumprimento
    dados["incidente"] = dados.get("incidente") or inc
    dados["nome_credor"] = dados.get("nome_credor") or credor
    if _cell(cessionaria):
        dados["cessionaria"] = _cell(cessionaria)
    _apply_derived_fields(dados)

    herdeiros_abertos = (
        bool(herdeiros_abertos_override)
        if herdeiros_abertos_override is not None
        else bool(pack.get("herdeiros_abertos"))
    )
    herdeiros_validado = (
        bool(herdeiros_validado_override)
        if herdeiros_validado_override is not None
        else bool(pack.get("herdeiros_validado"))
    )
    herdeiros = (
        _normalize_herdeiros_payload(herdeiros_override)
        if herdeiros_override is not None
        else list(pack.get("herdeiros") or [])
    )

    campos_faltantes = _missing_fields(dados, _legacy_required_keys(dados))
    avisos: list[str] = list(_credor_condicional(dados))

    bloqueios: list[str] = []
    if not _cell(dados.get("cessionaria")):
        bloqueios.append("Cessionária não preenchida.")
    if herdeiros_abertos and not herdeiros_validado:
        bloqueios.append(
            "Herdeiros em rascunho — valide os herdeiros na ficha antes de gerar contratos."
        )
    if herdeiros_abertos:
        herdeiro_issues = _validar_herdeiros(herdeiros)
        avisos.extend(herdeiro_issues)
        bloqueios.extend(herdeiro_issues)

    tipo_norm = (tipo or "preca").strip().lower()
    labels = _resolve_contrato_labels(
        tipo=tipo_norm, contrato_id=contrato_id, recupere=bool(recupere)
    )
    if not labels:
        return (
            {"ok": False, "valido": False, "error": "Contrato selecionado inválido."},
            400,
        )

    todos = str(contrato_id or "0").strip() == "0"
    ficha_ok = not campos_faltantes and not bloqueios
    contratos = [
        {
            "label": label,
            "ok": ficha_ok,
            "campos_faltantes": campos_faltantes,
            "motivos": avisos,
        }
        for label in labels
    ]

    if bloqueios:
        valido = False
        bloqueado = True
    else:
        valido = ficha_ok
        bloqueado = not ficha_ok

    campos_faltantes_agregados = list(campos_faltantes)

    resumo: str | None = None
    if bloqueios:
        resumo = " ".join(bloqueios)
    elif not ficha_ok:
        parts = [m["label"] for m in campos_faltantes_agregados[:8]]
        if avisos:
            parts.extend(avisos[:3])
        if todos:
            resumo = "Preencha os campos obrigatórios da ficha (borda amarela no legado)."
            if parts:
                resumo += " Pendentes: " + ", ".join(parts) + "."
        else:
            resumo = "Preencha os campos obrigatórios da ficha."
            if parts:
                resumo += " Pendentes: " + ", ".join(parts) + "."

    return (
        {
            "ok": True,
            "valido": valido,
            "bloqueado": bloqueado,
            "todos": todos,
            "bloqueios": bloqueios,
            "contratos": contratos,
            "campos_faltantes_agregados": campos_faltantes_agregados,
            "motivos": avisos,
            "invalidos": 0 if ficha_ok else len(contratos),
            "total": len(contratos),
            "resumo": resumo,
        },
        200,
    )
