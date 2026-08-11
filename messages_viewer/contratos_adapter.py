# -*- coding: utf-8 -*-
"""Adaptador: ficha PRÉ Análise → autoPrecaInfos + Herdeiros (new_autocontratos)."""
from __future__ import annotations

import math
import re
from datetime import datetime
from typing import Any

from messages_viewer.contratos_db import connect, fetch_all, is_configured
from messages_viewer.contratos_tipos import template_from_cessionaria
from messages_viewer.contratos_validacao import (
    _apply_derived_fields,
    _merge_dados_overlay,
    _normalize_herdeiros_payload,
)
from messages_viewer.pre_analise_ficha import carregar_ficha

_MONEY_FIELDS = frozenset(
    {
        "principal_liquido",
        "juros_moratorio",
        "juros_compensatorio",
        "descontos_previdenciarios",
        "descontos_de_assistencia_medica",
        "total_da_requisicao",
        "preco_de_compra",
        "valor_liquido_do_oficio",
        "custas",
    }
)

_PERCENT_FIELDS = frozenset({"percentual_honorarios", "percentual_de_compra"})

_ANUENTE_ADDR_MAP = {
    "cep_anuente_credor": "cep_anuente",
    "logradouro_anuente_credor": "logradouro_anuente",
    "numero_logradouro_anuente_credor": "numero_logradouro_anuente",
    "bairro_anuente_credor": "bairro_anuente",
    "cidade_anuente_credor": "cidade_anuente",
    "estado_anuente_credor": "estado_anuente",
}

_AUTO_PRECA_COLUMNS = [
    "cumprimento_de_sentenca",
    "incidente",
    "entidade_devedora",
    "vara",
    "foro",
    "ordem_cronologica",
    "numero_processo_principal",
    "controle",
    "depre",
    "ep",
    "expedicao_oficio_requisitorio",
    "procuradoria",
    "principal_liquido",
    "juros_moratorio",
    "juros_compensatorio",
    "descontos_previdenciarios",
    "descontos_de_assistencia_medica",
    "total_da_requisicao",
    "percentual_honorarios",
    "percentual_de_compra",
    "data_base",
    "preco_de_compra",
    "valor_liquido_do_oficio",
    "nome_credor",
    "cpf_credor",
    "rg_credor",
    "uf_credor",
    "data_de_nascimento_credor",
    "estado_civil_credor",
    "regime_conjuge_credor",
    "email_credor",
    "telefone_credor",
    "nacionalidade_credor",
    "ocupacao_credor",
    "credor_falecido",
    "data_do_obito",
    "anuente_credor",
    "cep_credor",
    "logradouro_credor",
    "numero_logradouro_credor",
    "bairro_credor",
    "cidade_credor",
    "estado_credor",
    "codigo_banco_credor",
    "banco_credor",
    "agencia_credor",
    "conta_credor",
    "tipo_conta_credor",
    "tem_chave_pix_credor",
    "chave_pix_credor",
    "nome_conjuge_credor",
    "cpf_conjuge_credor",
    "rg_conjuge_credor",
    "uf_conjuge_credor",
    "data_de_nascimento_conjuge_credor",
    "email_conjuge_credor",
    "telefone_conjuge_credor",
    "nacionalidade_conjuge_credor",
    "ocupacao_conjuge_credor",
    "codigo_banco_conjuge_credor",
    "banco_conjuge_credor",
    "agencia_conjuge_credor",
    "conta_conjuge_credor",
    "tipo_conta_conjuge_credor",
    "tem_chave_pix_conjuge_credor",
    "chave_pix_conjuge_credor",
    "nome_anuente_credor",
    "cpf_anuente_credor",
    "rg_anuente_credor",
    "uf_anuente_credor",
    "estado_civil_anuente_credor",
    "data_de_nascimento_anuente_credor",
    "email_anuente_credor",
    "telefone_anuente_credor",
    "nacionalidade_anuente_credor",
    "ocupacao_anuente_credor",
    "cep_anuente",
    "logradouro_anuente",
    "numero_logradouro_anuente",
    "bairro_anuente",
    "cidade_anuente",
    "estado_anuente",
    "cessionaria",
    "template",
    "qtd_herdeiros",
    "percentual_detido_herdeiros",
    "sobra",
    "nome_advogado",
    "cpf_advogado",
    "cep_advogado",
    "logradouro_advogado",
    "numero_logradouro_advogado",
    "bairro_advogado",
    "cidade_advogado",
    "estado_advogado",
    "exequente",
    "complemento_credor",
    "custas",
    "recupere",
    "interveniente_anuente",
]

_HERDEIRO_COLUMNS = [
    "herdeiro_habilitado",
    "nome_herdeiro",
    "cpf_herdeiro",
    "rg_herdeiro",
    "uf_herdeiro",
    "email_herdeiro",
    "telefone_herdeiro",
    "parentesco_herdeiro",
    "nacionalidade_herdeiro",
    "ocupacao_herdeiro",
    "data_de_nascimento_herdeiro",
    "estado_civil_herdeiro",
    "regime_herdeiro_conjuge",
    "cep_herdeiro",
    "logradouro_herdeiro",
    "numero_logradouro_herdeiro",
    "bairro_herdeiro",
    "cidade_herdeiro",
    "estado_herdeiro",
    "herdeiro_cedente",
    "percentual_honorarios",
    "percentual_detido",
    "percentual_cedido",
    "honorarios_reservados",
    "percentual_nao_cedido",
    "valor_da_proposta_herdeiro",
    "banco_herdeiro",
    "agencia_herdeiro",
    "conta_herdeiro",
    "tipo_conta_herdeiro",
    "tem_chave_pix_herdeiro",
    "chave_pix_herdeiro",
    "nome_conjuge_herdeiro",
    "cpf_conjuge_herdeiro",
    "rg_conjuge_herdeiro",
    "uf_conjuge_herdeiro",
    "data_de_nascimento_conjuge_herdeiro",
    "email_conjuge_herdeiro",
    "telefone_conjuge_herdeiro",
    "nacionalidade_conjuge_herdeiro",
    "ocupacao_conjuge_herdeiro",
    "banco_conjuge_herdeiro",
    "agencia_conjuge_herdeiro",
    "conta_conjuge_herdeiro",
    "tipo_conta_conjuge_herdeiro",
    "tem_chave_pix_conjuge_herdeiro",
    "chave_pix_conjuge_herdeiro",
    "mencionar",
]

_UPPER_NAME_FIELDS = frozenset(
    {
        "nome_credor",
        "nome_conjuge_credor",
        "nome_anuente_credor",
        "nome_herdeiro",
        "nome_conjuge_herdeiro",
    }
)


def _cell(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() in ("none", "null", "nan"):
        return ""
    return text


def _format_money(value: Any) -> str:
    text = _cell(value)
    if not text:
        return "R$ 0,00"
    if text.upper().startswith("R$"):
        return text
    cleaned = re.sub(r"[^\d,.-]", "", text).replace(".", "").replace(",", ".")
    try:
        num = float(cleaned)
    except ValueError:
        return text
    formatted = f"{num:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return f"R${formatted}"


def _format_percent(value: Any) -> str:
    text = _cell(value)
    if not text:
        return "0%"
    if text.endswith("%"):
        return text
    num = re.sub(r"[^\d,.-]", "", text).replace(",", ".")
    if not num:
        return text
    return f"{num}%"


def _interveniente_flag(value: Any) -> int:
    text = _cell(value).lower()
    return 1 if text == "sim" else 0


def ficha_to_autopreca_row(
    dados: dict[str, Any],
    *,
    user_name: str | None = None,
    recupere: int = 0,
) -> dict[str, Any]:
    row: dict[str, Any] = {}
    for col in _AUTO_PRECA_COLUMNS:
        row[col] = ""

    for key, val in (dados or {}).items():
        if key in row:
            row[key] = _cell(val)

    for src, dst in _ANUENTE_ADDR_MAP.items():
        if _cell(dados.get(src)):
            row[dst] = _cell(dados.get(src))

    cessionaria = _cell(dados.get("cessionaria"))
    row["cessionaria"] = cessionaria
    row["template"] = template_from_cessionaria(cessionaria) or _cell(dados.get("template"))

    for field in _MONEY_FIELDS:
        row[field] = _format_money(row.get(field))

    for field in _PERCENT_FIELDS:
        row[field] = _format_percent(row.get(field))

    for field in _UPPER_NAME_FIELDS:
        if row.get(field):
            row[field] = row[field].upper()

    row["interveniente_anuente"] = _interveniente_flag(dados.get("interveniente_anuente"))
    row["recupere"] = int(recupere or 0)
    row["qtd_herdeiros"] = _cell(dados.get("qtd_herdeiros")) or "0"

    now = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    row["alterado_por"] = user_name or row.get("alterado_por") or ""
    row["alterado_em"] = now
    if not row.get("incluido_por"):
        row["incluido_por"] = user_name or "Centro Organizacional"
    if not row.get("incluido_em"):
        row["incluido_em"] = now

    return row


_HERDEIRO_PERCENT_FIELDS = (
    "percentual_honorarios",
    "percentual_detido",
    "percentual_cedido",
    "honorarios_reservados",
    "percentual_nao_cedido",
)


def _parse_percent_num(value: Any) -> float:
    text = _cell(value).replace(",", ".")
    try:
        return float(re.sub(r"[^\d.-]", "", text) or 0)
    except ValueError:
        return 0.0


def _normalize_mencionar(value: Any) -> str:
    text = _cell(value).lower()
    if text in ("sim", "s", "1", "true", "yes"):
        return "sim"
    if not text or text in ("nao", "não", "n", "0", "false", "no"):
        return "não"
    if re.search(r"\bsim\b", text):
        return "sim"
    return "não"


def _recalc_herdeiro_derived(dados: dict[str, Any]) -> dict[str, Any]:
    d = dict(dados or {})
    detido = _parse_percent_num(d.get("percentual_detido"))
    honor = _parse_percent_num(
        d.get("percentual_honorarios_herdeiro") or d.get("percentual_honorarios")
    )
    cedente = _cell(d.get("herdeiro_cedente")).lower() == "sim"
    if not cedente:
        d["percentual_cedido"] = "0"
        cedido = 0.0
    else:
        cedido = _parse_percent_num(d.get("percentual_cedido"))
    reservados = math.ceil((detido / 100) * (honor / 100) * 100)
    nao_cedido = detido - cedido - reservados
    d["honorarios_reservados"] = str(int(reservados))
    d["percentual_nao_cedido"] = str(nao_cedido)
    return d


def _herdeiro_to_row(dados: dict[str, Any]) -> dict[str, Any]:
    dados = _recalc_herdeiro_derived(dados)
    row: dict[str, Any] = {c: "" for c in _HERDEIRO_COLUMNS}
    alias = {"percentual_honorarios_herdeiro": "percentual_honorarios"}
    for key, val in (dados or {}).items():
        target = alias.get(key, key)
        if target in row:
            row[target] = _cell(val)

    for field in ("nome_herdeiro", "nome_conjuge_herdeiro"):
        if row.get(field):
            row[field] = row[field].upper()

    if row.get("valor_da_proposta_herdeiro"):
        row["valor_da_proposta_herdeiro"] = _format_money(row["valor_da_proposta_herdeiro"])

    for field in _HERDEIRO_PERCENT_FIELDS:
        if row.get(field):
            row[field] = _format_percent(row[field])

    if row.get("mencionar"):
        row["mencionar"] = _normalize_mencionar(row["mencionar"])

    return row


_CESSIONARIAS_SQL = (
    "SELECT nome FROM Cessionarias WHERE nome IS NOT NULL AND TRIM(nome) <> '' ORDER BY nome"
)


def get_cessionarias_names() -> list[str]:
    """Lista de cessionárias para cache no render da página (poucos registros)."""
    if not is_configured():
        return []
    try:
        rows = fetch_all(_CESSIONARIAS_SQL)
    except Exception:
        return []
    return [_cell(r.get("nome")) for r in rows if _cell(r.get("nome"))]


def listar_cessionarias() -> tuple[dict[str, Any], int]:
    if not is_configured():
        return (
            {"ok": False, "error": "CONTRATOS_MYSQL_* não configurado no .env."},
            503,
        )
    names = get_cessionarias_names()
    if not names:
        return (
            {"ok": False, "error": "Nenhuma cessionária encontrada ou falha ao consultar o banco."},
            500,
        )
    return {"ok": True, "cessionarias": names}, 200


def sync_ficha_to_autocontratos(
    *,
    cumprimento_de_sentenca: str,
    incidente: str,
    nome_credor: str,
    caso_id: str | None = None,
    id_externo: str | None = None,
    user_name: str | None = None,
    recupere: int = 0,
    cessionaria: str | None = None,
    dados_override: dict[str, Any] | None = None,
    herdeiros_override: list[dict[str, Any]] | None = None,
    herdeiros_abertos_override: bool | None = None,
    herdeiros_validado_override: bool | None = None,
    depre_hint: str | None = None,
) -> tuple[dict[str, Any], int]:
    if not is_configured():
        return (
            {"ok": False, "error": "CONTRATOS_MYSQL_* não configurado no .env."},
            503,
        )

    cumprimento = (cumprimento_de_sentenca or "").strip()
    inc = (incidente or "").strip()
    credor = (nome_credor or "").strip()
    if not cumprimento or not inc:
        return (
            {"ok": False, "error": "Processo e incidente são obrigatórios."},
            400,
        )
    if not credor:
        return (
            {"ok": False, "error": "nome_credor é obrigatório para gerar contratos."},
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
        dados["template"] = template_from_cessionaria(dados["cessionaria"])
    _apply_derived_fields(dados)

    if not _cell(dados.get("cessionaria")):
        return (
            {"ok": False, "error": "Cessionária não preenchida na ficha."},
            400,
        )

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

    if herdeiros_abertos and not herdeiros_validado:
        return (
            {
                "ok": False,
                "error": "Herdeiros em rascunho — valide os herdeiros na ficha antes de gerar contratos.",
            },
            400,
        )

    row = ficha_to_autopreca_row(dados, user_name=user_name, recupere=recupere)
    row["nome_credor"] = (row.get("nome_credor") or credor).upper()

    conn = connect()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            """
            SELECT id FROM autoPrecaInfos
            WHERE cumprimento_de_sentenca = %s AND incidente = %s AND nome_credor = %s
            LIMIT 1
            """,
            (cumprimento, inc, row["nome_credor"]),
        )
        existing = cur.fetchone()

        if existing:
            id_processo = existing["id"]
            sets = ", ".join(f"`{c}` = %s" for c in _AUTO_PRECA_COLUMNS)
            vals = [row[c] for c in _AUTO_PRECA_COLUMNS] + [id_processo]
            cur.execute(
                f"UPDATE autoPrecaInfos SET {sets} WHERE id = %s",
                vals,
            )
        else:
            cols = ", ".join(f"`{c}`" for c in _AUTO_PRECA_COLUMNS)
            placeholders = ", ".join(["%s"] * len(_AUTO_PRECA_COLUMNS))
            cur.execute(
                f"INSERT INTO autoPrecaInfos ({cols}) VALUES ({placeholders})",
                [row[c] for c in _AUTO_PRECA_COLUMNS],
            )
            id_processo = cur.lastrowid

        cur.execute("DELETE FROM Herdeiros WHERE id_processo = %s", (id_processo,))

        if herdeiros_abertos and herdeiros:
            hcols = ["id_processo"] + _HERDEIRO_COLUMNS
            hplace = ", ".join(["%s"] * len(hcols))
            hnames = ", ".join(f"`{c}`" for c in hcols)
            for item in herdeiros:
                hd = item.get("dados") if isinstance(item, dict) else {}
                if not isinstance(hd, dict):
                    hd = {}
                hrow = _herdeiro_to_row(hd)
                cur.execute(
                    f"INSERT INTO Herdeiros ({hnames}) VALUES ({hplace})",
                    [id_processo] + [hrow[c] for c in _HERDEIRO_COLUMNS],
                )

        conn.commit()
        cur.close()

        total_herdeiros = len(herdeiros) if herdeiros_abertos else 0
        return (
            {
                "ok": True,
                "id_processo": id_processo,
                "nome_credor": row["nome_credor"],
                "cessionaria": row["cessionaria"],
                "template": row["template"],
                "total_herdeiros": total_herdeiros,
            },
            200,
        )
    except Exception as e:
        conn.rollback()
        return (
            {"ok": False, "error": f"Falha ao sincronizar ficha: {e}"},
            500,
        )
    finally:
        conn.close()
