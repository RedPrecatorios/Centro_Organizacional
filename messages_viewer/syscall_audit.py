# -*- coding: utf-8 -*-
"""
Leitura da tabela de auditoria de requisições (syscall) no MySQL EDA.

Configuração (mesmo host/base que a campanha, tipicamente EDA_MYSQL_*):
  EDA_MYSQL_HOST, EDA_MYSQL_PORT, EDA_MYSQL_DATABASE, EDA_MYSQL_USER, EDA_MYSQL_PASSWORD,
  EDA_MYSQL_CONNECT_TIMEOUT (opcional)

Nome da tabela (padrão request_audit):
  MYSQL_AUDIT_TABLE
"""
from __future__ import annotations

import json
import os
import re
from datetime import datetime
from typing import Any

import mysql.connector

_TABLE_RE = re.compile(r"^[a-zA-Z0-9_]{1,64}$")


def audit_mysql_connect_kwargs() -> dict[str, Any]:
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


def audit_table_name() -> str:
    raw = (os.getenv("MYSQL_AUDIT_TABLE") or "request_audit").strip()
    if not _TABLE_RE.match(raw):
        raise ValueError("MYSQL_AUDIT_TABLE inválido (use apenas letras, números e _).")
    return raw


def _json_cell(val: Any) -> Any:
    if val is None:
        return None
    if isinstance(val, (dict, list)):
        return val
    if isinstance(val, (bytes, bytearray)):
        try:
            val = val.decode("utf-8")
        except Exception:
            return str(val)
    if isinstance(val, str):
        s = val.strip()
        if not s:
            return None
        try:
            return json.loads(s)
        except json.JSONDecodeError:
            return s
    return val


def _serialize_row(row: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for k, v in row.items():
        if isinstance(v, datetime):
            out[k] = v.isoformat(sep=" ", timespec="microseconds")
        elif isinstance(v, date):
            out[k] = v.isoformat()
        elif k in ("payload_json", "headers_json"):
            out[k] = _json_cell(v)
        else:
            out[k] = v
    return out


def _only_digits(s: str, max_len: int = 64) -> str:
    d = "".join(c for c in s if c.isdigit())
    return d[:max_len]


def list_audit_rows(
    *,
    limit: int = 50,
    offset: int = 0,
    ligacao_id: int | None = None,
    request_id: str | None = None,
    user_usuario: str | None = None,
    user_nome: str | None = None,
    credor_nome: str | None = None,
    credor_cpf: str | None = None,
    credor_telefone: str | None = None,
    desde: str | None = None,
    ate: str | None = None,
) -> tuple[list[dict[str, Any]], int]:
    """
    Lista linhas (sem payload completo na listagem) e total que bate com os filtros.
    """
    table = audit_table_name()
    limit = max(1, min(int(limit), 200))
    offset = max(0, int(offset))

    where: list[str] = ["1=1"]
    params: list[Any] = []

    if ligacao_id is not None:
        where.append("ligacao_id = %s")
        params.append(int(ligacao_id))
    if request_id:
        rid = request_id.strip()
        if len(rid) <= 40:
            where.append("request_id = %s")
            params.append(rid)
    if user_usuario:
        where.append("user_usuario LIKE %s")
        params.append("%" + user_usuario.strip()[:180] + "%")
    if user_nome:
        where.append("user_nome LIKE %s")
        params.append("%" + user_nome.strip()[:250] + "%")
    if credor_nome:
        where.append("credor_nome LIKE %s")
        params.append("%" + credor_nome.strip()[:500] + "%")
    if credor_cpf:
        cpf = _only_digits(credor_cpf, 20)
        if cpf:
            where.append("credor_cpf LIKE %s")
            params.append(cpf + "%")
    if credor_telefone:
        tel = _only_digits(credor_telefone, 32)
        if tel:
            where.append("credor_telefone LIKE %s")
            params.append("%" + tel + "%")
    if desde:
        where.append("ultima_ligacao_at >= %s")
        params.append(desde.strip()[:32])
    if ate:
        where.append("ultima_ligacao_at <= %s")
        params.append(ate.strip()[:32])

    wsql = " AND ".join(where)
    cols = (
        "id, dedup_key, request_id, user_usuario, user_nome, credor_nome, credor_cpf, "
        "credor_telefone, ligacao_id, ligacao_acionamento, mailing_nome, "
        "primeira_requisicao_at, ultima_ligacao_at, total_ligacoes, client_ip"
    )

    kw = audit_mysql_connect_kwargs()
    conn = mysql.connector.connect(**kw)
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            f"SELECT COUNT(*) AS c FROM `{table}` WHERE {wsql}",
            params,
        )
        total = int((cur.fetchone() or {}).get("c", 0))
        cur.execute(
            f"SELECT {cols} FROM `{table}` WHERE {wsql} "
            "ORDER BY ultima_ligacao_at DESC LIMIT %s OFFSET %s",
            params + [limit, offset],
        )
        rows = cur.fetchall() or []
        cur.close()
    finally:
        conn.close()

    return [_serialize_row(dict(r)) for r in rows], total


def get_audit_row(row_id: int) -> dict[str, Any] | None:
    table = audit_table_name()
    kw = audit_mysql_connect_kwargs()
    conn = mysql.connector.connect(**kw)
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(f"SELECT * FROM `{table}` WHERE id = %s", (int(row_id),))
        row = cur.fetchone()
        cur.close()
    finally:
        conn.close()
    if not row:
        return None
    return _serialize_row(dict(row))
