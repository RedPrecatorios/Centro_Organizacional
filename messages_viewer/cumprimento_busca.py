# -*- coding: utf-8 -*-
"""Pesquisa de casos em ``cumprimento.coleta_cumprimento``.

Entra só na busca por nome, ou por processo com incidente vazio ou zero.
Cada linha precisa de processo e de nome ou CPF. Os campos de valor
preenchem o formulário de cálculo manual. Com data-base, entidade,
principal, juros e meses preenchidos, o caso pode ir para a atualização.
A data da decisão é o dia em que o cálculo é disparado.
"""
from __future__ import annotations

import calendar
import os
import re
from datetime import date
from decimal import Decimal
from typing import Any

import mysql.connector
from dotenv import load_dotenv

from messages_viewer.calculo_manual import _format_money_br, _parse_money

_LIMITE = 40


def incidente_lista_ampla(incidente: object) -> bool:
    """Incidente vazio ou só zeros: a busca lista todos os casos do processo."""
    raw = str(incidente or "").strip()
    if not raw:
        return True
    return raw.strip("0") == ""


def normalizar_incidente(incidente: object) -> str:
    """``01`` / ``001`` → ``1``; vazio ou só zeros → ``""``."""
    raw = str(incidente or "").strip()
    if not raw or raw.strip("0") == "":
        return ""
    if raw.isdigit():
        return str(int(raw))
    return raw


def incidente_match_variants(incidente: object) -> list[str]:
    """Formas equivalentes para igualdade em BD (``1`` casa com ``01``, ``001``…)."""
    norm = normalizar_incidente(incidente)
    if not norm:
        return []
    variants = [norm]
    raw = str(incidente or "").strip()
    if raw and raw not in variants:
        variants.append(raw)
    if norm.isdigit():
        for width in (2, 3, 4):
            padded = norm.zfill(width)
            if padded not in variants:
                variants.append(padded)
    return variants


def normalizar_processo_incidente_chave(processo: object, incidente: object) -> str:
    """Chave ``PROCESSO|INCIDENTE`` com incidente canónico (``01`` → ``1``)."""
    proc = " ".join(str(processo or "").strip().upper().split())
    if not proc or proc.lower() == "nan":
        return ""
    raw_inc = str(incidente or "").strip()
    if raw_inc.lower() == "nan":
        raw_inc = ""
    inc = normalizar_incidente(raw_inc)
    return f"{proc}|{inc}"


def cumprimento_mysql_config() -> dict | None:
    load_dotenv()
    name = (os.getenv("CUMPRIMENTO_MYSQL_DATABASE") or "").strip().strip("'\"")
    user = (os.getenv("CUMPRIMENTO_MYSQL_USER") or "").strip().strip("'\"")
    if not name or not user:
        return None
    host = (os.getenv("CUMPRIMENTO_MYSQL_HOST") or "127.0.0.1").strip().strip("'\"")
    password = (os.getenv("CUMPRIMENTO_MYSQL_PASSWORD") or "").strip().strip("'\"")
    try:
        port = int(str(os.getenv("CUMPRIMENTO_MYSQL_PORT") or "3306").strip())
    except ValueError:
        port = 3306
    return {
        "host": host,
        "port": port,
        "database": name,
        "user": user,
        "password": password,
        "connection_timeout": 12,
    }


def _clean(value: object) -> str:
    return " ".join(str(value or "").replace("\t", " ").split())


def _digits(value: object) -> str:
    return re.sub(r"\D", "", str(value or ""))


def _esc_like(value: str) -> str:
    return value.replace("\\", "\\\\").replace("%", r"\%").replace("_", r"\_")


def _tem_nome_ou_cpf(row: dict) -> bool:
    return bool(_clean(row.get("nome")) or _clean(row.get("cpf")))


def _iso_data(value: object) -> str:
    """Data completa, ou mês/ano completado com o último dia do mês."""
    text = _clean(value)
    if not text or text.casefold() in {"null", "none", "nan"}:
        return ""
    full = re.fullmatch(r"(\d{1,2})/(\d{1,2})/(\d{4})", text)
    if full:
        day, month, year = int(full.group(1)), int(full.group(2)), int(full.group(3))
        try:
            return date(year, month, day).isoformat()
        except ValueError:
            return ""
    month_year = re.fullmatch(r"(\d{1,2})/(\d{4})", text)
    if not month_year:
        return ""
    month, year = int(month_year.group(1)), int(month_year.group(2))
    if not 1 <= month <= 12:
        return ""
    last = calendar.monthrange(year, month)[1]
    return date(year, month, last).isoformat()


def _money_br(value: object) -> str:
    parsed = _parse_money(value)
    if parsed is None:
        return ""
    return _format_money_br(parsed)


def formulario_completo(form: dict) -> bool:
    """True quando a linha tem o que a planilha exige para atualizar o cálculo.

    Meses podem faltar: se o total for ≤ R$ 200 mil o robô grava 1000;
    se for maior, a memória fica com «Verificar Meses» para correção na UI.
    """
    if not _clean(form.get("requerente")):
        return False
    entidade = _clean(form.get("entidade_devedora"))
    if not entidade:
        return False
    if not re.search(r"\d", _clean(form.get("numero_de_processo"))):
        return False
    if not form.get("data_base"):
        return False
    if not form.get("principal_liquido") or not form.get("juros_moratorio"):
        return False
    raw_meses = str(form.get("numero_de_meses") or "").strip()
    if raw_meses:
        try:
            meses = int(raw_meses)
        except (TypeError, ValueError):
            return False
        if meses < 0:
            return False
    return True


def _descontos_br(previdencia: object, assistencia: object) -> str:
    total = (_parse_money(previdencia) or Decimal("0")) + (
        _parse_money(assistencia) or Decimal("0")
    )
    return _format_money_br(total)


def _chave(nome: object, processo: object) -> tuple[str, str]:
    return (_clean(nome).casefold(), _digits(processo) or _clean(processo))


def linha_cumprimento(row: dict) -> dict[str, Any] | None:
    """Monta o resultado da pesquisa. Sem nome e sem CPF, a linha fica de fora."""
    if not _tem_nome_ou_cpf(row):
        return None
    processo = _clean(row.get("numero_do_cumprimento"))
    if not processo:
        return None
    nome = _clean(row.get("nome"))
    requerente = _clean(row.get("requerente"))
    cpf = _clean(row.get("cpf"))
    titulo = nome or requerente or cpf
    if not titulo:
        return None
    meses_raw = row.get("meses")
    try:
        meses = int(meses_raw) if meses_raw is not None and str(meses_raw).strip() != "" else 0
    except (TypeError, ValueError):
        meses = 0
    principal = _money_br(row.get("principal_liquido"))
    juros = _money_br(row.get("juros"))
    form = {
        "id_precainfosnew": "",
        "requerente": titulo,
        "entidade_devedora": _clean(row.get("entidade_devedora")),
        "numero_de_processo": processo,
        "numero_do_incidente": "",
        "cumprimento": _clean(row.get("processo_principal")),
        "oc": "",
        "data_base": _iso_data(row.get("data_base")),
        "data_decisao": "",
        "principal_liquido": principal,
        "juros_moratorio": juros,
        "numero_de_meses": str(meses) if meses > 0 else "",
        "descontos": _descontos_br(
            row.get("desconto_previdenciario"),
            row.get("desconto_assistencia_medica"),
        ),
    }
    return {
        "id": None,
        "id_precainfosnew": None,
        "id_coleta_cumprimento": row.get("id"),
        "requerente": titulo,
        "nome_complementar": nome,
        "requerente_coleta": requerente if requerente and requerente.casefold() != titulo.casefold() else "",
        "cpf": cpf,
        "numero_de_processo": processo,
        "numero_do_incidente": "",
        "principal_bruto": 0,
        "juros": 0,
        "desc_saude_prev": 0,
        "desc_ir": 0,
        "percentual_honorarios": 30,
        "total_bruto": 0,
        "reserva_honorarios": 0,
        "total_liquido": 0,
        "ultima_atualizacao": None,
        "source": "coleta_cumprimento",
        "calculo_completo": formulario_completo(form),
        "form": form,
    }


def mesclar_sem_duplicar(existentes: list[dict], novos: list[dict]) -> list[dict]:
    """Não repete cumprimento já listado pelo mesmo nome e o mesmo processo."""
    known: set[tuple[str, str]] = set()
    known_ids: set[int] = set()
    for row in existentes:
        proc = row.get("numero_de_processo")
        for nome in (row.get("requerente"), row.get("nome_complementar")):
            if _clean(nome) and _clean(proc):
                known.add(_chave(nome, proc))
        coleta_id = row.get("id_coleta_cumprimento")
        try:
            if coleta_id is not None:
                known_ids.add(int(coleta_id))
        except (TypeError, ValueError):
            pass
    out = list(existentes)
    for row in novos:
        coleta_id = row.get("id_coleta_cumprimento")
        try:
            coleta_int = int(coleta_id) if coleta_id is not None else None
        except (TypeError, ValueError):
            coleta_int = None
        if coleta_int is not None and coleta_int in known_ids:
            continue
        chaves = [
            _chave(nome, row.get("numero_de_processo"))
            for nome in (
                row.get("requerente"),
                row.get("nome_complementar"),
                row.get("requerente_coleta"),
            )
            if _clean(nome)
        ]
        if chaves and all(chave in known for chave in chaves):
            continue
        if coleta_int is not None:
            known_ids.add(coleta_int)
        known.update(chaves)
        out.append(row)
    return out


def buscar_cumprimentos(*, nome: str = "", processo: str = "") -> list[dict]:
    """Nome ou processo. Processo sozinho não devolve linha sem nome ou CPF."""
    cfg = cumprimento_mysql_config()
    if not cfg:
        return []
    nome = _clean(nome)
    processo = _clean(processo)
    if nome and processo:
        return []
    if not nome and not processo:
        return []
    where = [
        "TRIM(COALESCE(numero_do_cumprimento, '')) <> ''",
        "(TRIM(COALESCE(nome, '')) <> '' OR TRIM(COALESCE(cpf, '')) <> '')",
    ]
    params: list[Any] = []
    if nome:
        like = f"%{_esc_like(nome)}%"
        pessoa = ["nome LIKE %s", "requerente LIKE %s"]
        params.extend([like, like])
        cpf_digits = _digits(nome)
        if len(cpf_digits) >= 11:
            pessoa.append(
                "REPLACE(REPLACE(REPLACE(REPLACE(COALESCE(cpf, ''), '.', ''), '-', ''), '/', ''), ' ', '') LIKE %s"
            )
            params.append(f"%{cpf_digits}%")
        where.append("(" + " OR ".join(pessoa) + ")")
    else:
        alvos = [processo]
        digits = _digits(processo)
        pessoa_proc = [
            "TRIM(numero_do_cumprimento) = %s",
            "TRIM(processo_principal) = %s",
        ]
        params.extend([processo, processo])
        if len(digits) >= 15:
            pessoa_proc.append(
                "REPLACE(REPLACE(REPLACE(TRIM(COALESCE(numero_do_cumprimento, '')), '.', ''), '-', ''), ' ', '') = %s"
            )
            pessoa_proc.append(
                "REPLACE(REPLACE(REPLACE(TRIM(COALESCE(processo_principal, '')), '.', ''), '-', ''), ' ', '') = %s"
            )
            params.extend([digits, digits])
        where.append("(" + " OR ".join(pessoa_proc) + ")")
        del alvos
    sql = f"""
        SELECT id, requerente, nome, cpf, numero_do_cumprimento, processo_principal,
               entidade_devedora, principal_liquido, juros,
               desconto_previdenciario, desconto_assistencia_medica,
               meses, data_base
        FROM coleta_cumprimento
        WHERE {' AND '.join(where)}
        ORDER BY COALESCE(nome, requerente), id
        LIMIT {_LIMITE}
    """
    conn = mysql.connector.connect(**cfg, charset="utf8mb4", collation="utf8mb4_unicode_ci")
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(sql, tuple(params))
        rows = cur.fetchall() or []
        cur.close()
    finally:
        conn.close()
    out: list[dict] = []
    for row in rows:
        item = linha_cumprimento(row)
        if item is not None:
            out.append(item)
    return out
