# -*- coding: utf-8 -*-
"""
Manutenção por módulo/página da plataforma.

- Persistência em SQLite (tabela page_maintenance), editável em Utilizadores (admin).
- Override por .env: PAGE_MAINTENANCE=memoria_calculo,campanha
  e/ou PAGE_MAINTENANCE_memoria_calculo=1
  Mensagem opcional: PAGE_MAINTENANCE_MSG ou PAGE_MAINTENANCE_MSG_<tab_id>
"""
from __future__ import annotations

import os
import sqlite3
from typing import Any

from messages_viewer.plataforma_auth import TAB_PANELS, _connect, init_db

MAINTAINABLE_TAB_IDS = {p[0] for p in TAB_PANELS}

_TAB_LABELS: dict[str, str] = {p[0]: p[1] for p in TAB_PANELS}

_DEFAULT_MESSAGE = (
    "Este módulo está temporariamente indisponível para manutenção. "
    "Tente novamente mais tarde ou contacte o administrador."
)


def _ensure_table() -> None:
    init_db()
    with _connect() as c:
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS page_maintenance (
                tab_id TEXT PRIMARY KEY,
                enabled INTEGER NOT NULL DEFAULT 0,
                message TEXT,
                updated_at TEXT NOT NULL DEFAULT (datetime('now'))
            )
            """
        )
        c.commit()


def _env_enabled_tabs() -> set[str]:
    out: set[str] = set()
    bulk = (os.getenv("PAGE_MAINTENANCE") or "").strip()
    if bulk:
        for part in bulk.replace(";", ",").split(","):
            t = part.strip().lower()
            if t in MAINTAINABLE_TAB_IDS:
                out.add(t)
    for tab_id in MAINTAINABLE_TAB_IDS:
        raw = (os.getenv(f"PAGE_MAINTENANCE_{tab_id}") or "").strip().lower()
        if raw in ("1", "true", "yes", "on", "sim", "enabled"):
            out.add(tab_id)
    return out


def _env_message_for(tab_id: str) -> str | None:
    specific = (os.getenv(f"PAGE_MAINTENANCE_MSG_{tab_id}") or "").strip()
    if specific:
        return specific
    general = (os.getenv("PAGE_MAINTENANCE_MSG") or "").strip()
    return general or None


def _db_row(tab_id: str) -> sqlite3.Row | None:
    _ensure_table()
    with _connect() as c:
        return c.execute(
            "SELECT tab_id, enabled, message FROM page_maintenance WHERE tab_id = ?",
            (tab_id,),
        ).fetchone()


def is_tab_in_maintenance(tab_id: str) -> bool:
    if tab_id not in MAINTAINABLE_TAB_IDS:
        return False
    if tab_id in _env_enabled_tabs():
        return True
    row = _db_row(tab_id)
    return bool(row and int(row["enabled"] or 0))


def maintenance_message(tab_id: str) -> str:
    msg = _env_message_for(tab_id)
    if msg:
        return msg
    row = _db_row(tab_id)
    if row and row["message"]:
        return str(row["message"]).strip()
    return _DEFAULT_MESSAGE


def tab_display_name(tab_id: str) -> str:
    return _TAB_LABELS.get(tab_id, tab_id.replace("_", " ").title())


def list_maintenance_states() -> list[dict[str, Any]]:
    """Estado de cada módulo do menu (para admin e templates)."""
    _ensure_table()
    env_tabs = _env_enabled_tabs()
    rows_by_id: dict[str, sqlite3.Row] = {}
    with _connect() as c:
        for r in c.execute(
            "SELECT tab_id, enabled, message FROM page_maintenance"
        ).fetchall():
            rows_by_id[r["tab_id"]] = r

    result: list[dict[str, Any]] = []
    for tab_id, title, desc in TAB_PANELS:
        row = rows_by_id.get(tab_id)
        db_on = bool(row and int(row["enabled"] or 0))
        env_on = tab_id in env_tabs
        enabled = env_on or db_on
        msg = None
        if row and row["message"]:
            msg = str(row["message"]).strip() or None
        result.append(
            {
                "tab_id": tab_id,
                "title": title,
                "description": desc,
                "enabled": enabled,
                "db_enabled": db_on,
                "env_enabled": env_on,
                "message": msg,
                "effective_message": maintenance_message(tab_id) if enabled else None,
            }
        )
    return result


def save_maintenance_from_form(form) -> None:
    """Grava flags e mensagens enviadas pelo formulário de admin."""
    _ensure_table()
    with _connect() as c:
        for tab_id, _, _ in TAB_PANELS:
            enabled = 1 if form.get(f"maint_{tab_id}") else 0
            msg = (form.get(f"maint_msg_{tab_id}") or "").strip() or None
            c.execute(
                """
                INSERT INTO page_maintenance (tab_id, enabled, message, updated_at)
                VALUES (?, ?, ?, datetime('now'))
                ON CONFLICT(tab_id) DO UPDATE SET
                    enabled = excluded.enabled,
                    message = excluded.message,
                    updated_at = excluded.updated_at
                """,
                (tab_id, enabled, msg),
            )
        c.commit()


def handle_maintenance_response(tab_id: str) -> Any:
    from flask import jsonify, render_template, request

    title = tab_display_name(tab_id)
    message = maintenance_message(tab_id)
    if request.path.startswith("/api/"):
        return (
            jsonify(
                {
                    "ok": False,
                    "maintenance": True,
                    "tab": tab_id,
                    "module": title,
                    "error": message,
                }
            ),
            503,
        )
    return (
        render_template(
            "em_manutencao.html",
            module_title=title,
            maintenance_message=message,
            tab_id=tab_id,
        ),
        503,
    )


def maintenance_block_for_tab(tab_id: str, user: dict | None) -> Any | None:
    """None = seguir; caso contrário, resposta Flask (página ou JSON)."""
    if not tab_id or tab_id not in MAINTAINABLE_TAB_IDS:
        return None
    if not is_tab_in_maintenance(tab_id):
        return None
    return handle_maintenance_response(tab_id)
