# -*- coding: utf-8 -*-
"""Conexão MySQL compartilhada com o motor legado (new_autocontratos)."""
from __future__ import annotations

import os
from typing import Any

import mysql.connector
from mysql.connector import MySQLConnection


def is_configured() -> bool:
    return bool(
        (os.getenv("CONTRATOS_MYSQL_HOST") or "").strip()
        and (os.getenv("CONTRATOS_MYSQL_DATABASE") or "").strip()
        and (os.getenv("CONTRATOS_MYSQL_USER") or "").strip()
        and (os.getenv("CONTRATOS_MYSQL_PASSWORD") or "") != ""
    )


def connect() -> MySQLConnection:
    if not is_configured():
        raise RuntimeError("CONTRATOS_MYSQL_* não configurado no .env.")
    port_raw = (os.getenv("CONTRATOS_MYSQL_PORT") or "3306").strip()
    try:
        port = int(port_raw)
    except ValueError:
        port = 3306
    return mysql.connector.connect(
        host=(os.getenv("CONTRATOS_MYSQL_HOST") or "").strip(),
        port=port,
        database=(os.getenv("CONTRATOS_MYSQL_DATABASE") or "").strip(),
        user=(os.getenv("CONTRATOS_MYSQL_USER") or "").strip(),
        password=os.getenv("CONTRATOS_MYSQL_PASSWORD") or "",
        charset="utf8mb4",
        use_unicode=True,
    )


def fetch_all(sql: str, params: tuple[Any, ...] | None = None) -> list[dict[str, Any]]:
    conn = connect()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(sql, params or ())
        rows = cur.fetchall()
        cur.close()
        return list(rows or [])
    finally:
        conn.close()
