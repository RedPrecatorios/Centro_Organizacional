# -*- coding: utf-8 -*-
"""Pesquisa cruzada: emails, telefones e processos (MySQL EDA)."""
from __future__ import annotations

import os
import re
from datetime import date, datetime
from typing import Any

import mysql.connector

_NON_DIGITS_RE = re.compile(r"\D+")


def localize_mysql_connect_kwargs() -> dict[str, Any]:
    host = (os.getenv("EDA_MYSQL_HOST") or "localhost").strip()
    port = int(os.getenv("EDA_MYSQL_PORT", "3306") or "3306")
    user = (os.getenv("EDA_MYSQL_USER") or "root").strip()
    password = os.getenv("EDA_MYSQL_PASSWORD", "") or ""
    database = (os.getenv("EDA_MYSQL_DATABASE") or "plataforma_central").strip()
    timeout = int(os.getenv("EDA_MYSQL_CONNECT_TIMEOUT", "15") or "15")
    return {
        "host": host,
        "port": port,
        "user": user,
        "password": password,
        "database": database,
        "connection_timeout": timeout,
        "charset": "utf8mb4",
        "collation": "utf8mb4_unicode_ci",
    }


def _only_digits(s: str, max_len: int = 32) -> str:
    return _NON_DIGITS_RE.sub("", s or "")[:max_len]


def _serialize_value(v: Any) -> Any:
    if isinstance(v, datetime):
        return v.isoformat(sep=" ", timespec="seconds")
    if isinstance(v, date):
        return v.isoformat()
    if isinstance(v, (int, float, str)) or v is None:
        return v
    return str(v)


def _serialize_row(row: dict[str, Any]) -> dict[str, Any]:
    return {k: _serialize_value(v) for k, v in row.items()}


def _phone_sql_expr(col: str) -> str:
    """Normaliza telefone na query (só dígitos)."""
    return (
        f"REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(TRIM({col}), ' ', ''), '-', ''), "
        f"'(', ''), ')', ''), '+', '')"
    )


def _find_process_ids_by_email(
    cur: Any, email_q: str, *, limit: int, offset: int, include_total: bool
) -> tuple[list[int], int | None]:
    q = email_q.strip().lower()
    if len(q) < 3:
        raise ValueError("Informe ao menos 3 caracteres do e-mail.")
    if "@" in q:
        cond = "LOWER(TRIM(e.email)) = %s"
        params: list[Any] = [q]
    else:
        cond = "LOWER(TRIM(e.email)) LIKE %s"
        params = [f"%{q}%"]

    base = f"""
        FROM emails e
        INNER JOIN processos_juridicos pj ON pj.id = e.id_processo_juridico
        INNER JOIN pessoas p ON p.id = pj.id_pessoa
        WHERE {cond}
    """
    total: int | None = None
    if include_total:
        cur.execute(f"SELECT COUNT(DISTINCT pj.id) AS c {base}", params)
        total = int((cur.fetchone() or {}).get("c", 0))

    cur.execute(
        f"""
        SELECT DISTINCT pj.id AS pid
        {base}
        ORDER BY pj.ultimo_processamento DESC, pj.id DESC
        LIMIT %s OFFSET %s
        """,
        params + [limit, offset],
    )
    ids = [int(r["pid"]) for r in cur.fetchall() or []]
    return ids, total


def _find_process_ids_by_phone(
    cur: Any, phone_q: str, *, limit: int, offset: int, include_total: bool
) -> tuple[list[int], int | None]:
    digits = _only_digits(phone_q)
    if len(digits) < 4:
        raise ValueError("Informe ao menos 4 dígitos do telefone.")
    like = f"%{digits}%"
    tel_sms = _phone_sql_expr("s.telefone")
    tel_hsm = _phone_sql_expr("h.telefone_hsm")

    union = f"""
        SELECT DISTINCT pj.id AS pid, pj.ultimo_processamento AS ord
        FROM sms s
        INNER JOIN processos_juridicos pj ON pj.id = s.id_processo_juridico
        WHERE {tel_sms} LIKE %s
        UNION
        SELECT DISTINCT pj.id AS pid, pj.ultimo_processamento AS ord
        FROM disparo_hsm h
        INNER JOIN processos_juridicos pj ON pj.id = h.id_processo_juridico
        WHERE {tel_hsm} LIKE %s
    """
    params = [like, like]

    total: int | None = None
    if include_total:
        cur.execute(
            f"SELECT COUNT(*) AS c FROM ({union}) AS u",
            params,
        )
        total = int((cur.fetchone() or {}).get("c", 0))

    cur.execute(
        f"""
        SELECT pid FROM (
            {union}
        ) AS u
        ORDER BY u.ord DESC, u.pid DESC
        LIMIT %s OFFSET %s
        """,
        params + [limit, offset],
    )
    ids = [int(r["pid"]) for r in cur.fetchall() or []]
    return ids, total


def _fetch_bundles(cur: Any, process_ids: list[int]) -> list[dict[str, Any]]:
    if not process_ids:
        return []

    ph = ",".join(["%s"] * len(process_ids))

    cur.execute(
        f"""
        SELECT
            pj.id AS processo_id,
            pj.numero_processo,
            pj.numero_incidente,
            pj.natureza,
            pj.assunto,
            pj.foro,
            pj.requerente,
            pj.entidade_devedora,
            pj.advogado,
            pj.principal_liquido,
            pj.valor_requisitado,
            pj.calculo_atualizado,
            pj.data_entrada,
            pj.ultimo_processamento,
            p.id AS pessoa_id,
            p.nome AS pessoa_nome,
            p.cpf AS pessoa_cpf
        FROM processos_juridicos pj
        INNER JOIN pessoas p ON p.id = pj.id_pessoa
        WHERE pj.id IN ({ph})
        """,
        process_ids,
    )
    proc_rows = cur.fetchall() or []
    by_id: dict[int, dict[str, Any]] = {}
    order: list[int] = []
    for r in proc_rows:
        pid = int(r["processo_id"])
        order.append(pid)
        by_id[pid] = {
            "processo": _serialize_row(dict(r)),
            "telefones": [],
            "emails": [],
        }

    cur.execute(
        f"""
        SELECT id_processo_juridico, telefone, fornecedor, primeira_aparicao
        FROM sms
        WHERE id_processo_juridico IN ({ph})
        ORDER BY telefone
        """,
        process_ids,
    )
    for r in cur.fetchall() or []:
        pid = int(r["id_processo_juridico"])
        if pid in by_id:
            row = _serialize_row(dict(r))
            row["origem"] = "sms"
            by_id[pid]["telefones"].append(row)

    cur.execute(
        f"""
        SELECT id_processo_juridico, telefone_hsm AS telefone, fornecedor,
               nome, primeira_aparicao
        FROM disparo_hsm
        WHERE id_processo_juridico IN ({ph})
        ORDER BY telefone
        """,
        process_ids,
    )
    for r in cur.fetchall() or []:
        pid = int(r["id_processo_juridico"])
        if pid in by_id:
            row = _serialize_row(dict(r))
            row["origem"] = "disparo_hsm"
            by_id[pid]["telefones"].append(row)

    cur.execute(
        f"""
        SELECT id_processo_juridico, email, fornecedor, primeira_aparicao,
               campanha_disparo_status, campanha_disparo_ultimo
        FROM emails
        WHERE id_processo_juridico IN ({ph})
        ORDER BY email
        """,
        process_ids,
    )
    for r in cur.fetchall() or []:
        pid = int(r["id_processo_juridico"])
        if pid in by_id:
            by_id[pid]["emails"].append(_serialize_row(dict(r)))

    out: list[dict[str, Any]] = []
    for pid in process_ids:
        if pid in by_id:
            out.append(by_id[pid])
    return out


def search_localize(
    *,
    tipo: str,
    q: str,
    limit: int = 15,
    offset: int = 0,
    include_total: bool = True,
) -> tuple[list[dict[str, Any]], int | None]:
    """
    tipo: 'email' | 'telefone'
    Retorna lista de { processo, telefones[], emails[] } e total (se include_total).
    """
    tipo = (tipo or "").strip().lower()
    if tipo not in ("email", "telefone"):
        raise ValueError("Tipo de pesquisa inválido (email ou telefone).")

    limit = max(1, min(int(limit), 30))
    offset = max(0, int(offset))

    kw = localize_mysql_connect_kwargs()
    conn = mysql.connector.connect(**kw)
    try:
        cur = conn.cursor(dictionary=True)
        if tipo == "email":
            pids, total = _find_process_ids_by_email(
                cur, q, limit=limit, offset=offset, include_total=include_total
            )
        else:
            pids, total = _find_process_ids_by_phone(
                cur, q, limit=limit, offset=offset, include_total=include_total
            )
        bundles = _fetch_bundles(cur, pids)
        cur.close()
    finally:
        conn.close()

    return bundles, total
