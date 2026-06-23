# -*- coding: utf-8 -*-
"""Consulta de casos para geração de proposta (precainfosnew + memoria_calculo)."""
from __future__ import annotations

import os
from decimal import Decimal
from typing import Any

import mysql.connector


def _flask_mysql_config() -> dict | None:
    name = (os.getenv("FLASK_MYSQL_DATABASE") or "").strip().strip("'\"")
    if not name:
        return None
    return {
        "host": (os.getenv("FLASK_MYSQL_HOST") or "127.0.0.1").strip().strip("'\""),
        "port": int(str(os.getenv("FLASK_MYSQL_PORT") or "3306").strip()),
        "database": name,
        "user": (os.getenv("FLASK_MYSQL_USER") or "root").strip().strip("'\""),
        "password": (os.getenv("FLASK_MYSQL_PASSWORD") or "").strip().strip("'\""),
        "connection_timeout": 15,
    }


def _memoria_mysql_config() -> dict | None:
    name = (os.getenv("MEMORIA_MYSQL_DATABASE") or "").strip()
    if not name:
        return None
    return {
        "host": (os.getenv("MEMORIA_MYSQL_HOST") or "127.0.0.1").strip(),
        "port": int(os.getenv("MEMORIA_MYSQL_PORT", "3306")),
        "database": name,
        "user": (os.getenv("MEMORIA_MYSQL_USER") or "root").strip(),
        "password": os.getenv("MEMORIA_MYSQL_PASSWORD", "") or "",
        "connection_timeout": 15,
    }


def _pick_field(fields: set[str], *candidates: str) -> str | None:
    if not fields:
        return None
    lc = {f.lower(): f for f in fields}
    for c in candidates:
        if c in fields:
            return c
        k = c.lower()
        if k in lc:
            return lc[k]
    return None


def _cell(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except UnicodeDecodeError:
            return value.decode("latin-1", errors="replace")
    return value


def _str_val(value: Any) -> str:
    v = _cell(value)
    if v is None:
        return ""
    return str(v).strip()


def _liquido_memoria(
    cur,
    *,
    id_precainfosnew: int | None,
    processo: str,
    incidente: str,
) -> float | None:
    cur.execute("SHOW COLUMNS FROM `memoria_calculo` LIKE 'total_liquido'")
    if not cur.fetchone():
        return None
    if id_precainfosnew:
        cur.execute(
            """
            SELECT total_liquido
            FROM memoria_calculo
            WHERE id_precainfosnew = %s
            ORDER BY id DESC
            LIMIT 1
            """,
            (int(id_precainfosnew),),
        )
        row = cur.fetchone()
        if row and row.get("total_liquido") is not None:
            try:
                return float(row["total_liquido"])
            except (TypeError, ValueError):
                pass
    cur.execute(
        """
        SELECT total_liquido
        FROM memoria_calculo
        WHERE TRIM(COALESCE(numero_de_processo, '')) = %s
          AND TRIM(COALESCE(numero_do_incidente, '')) = %s
        ORDER BY id DESC
        LIMIT 1
        """,
        (processo, incidente),
    )
    row = cur.fetchone()
    if not row or row.get("total_liquido") is None:
        return None
    try:
        return float(row["total_liquido"])
    except (TypeError, ValueError):
        return None


def buscar_por_processo_incidente(processo: str, incidente: str = "") -> dict[str, Any]:
    """
    Pesquisa exclusivamente por nº de processo + incidente (incidente vazio = sem incidente).
  """
    proc = (processo or "").strip()
    inc = (incidente or "").strip()
    if not proc:
        return {"ok": False, "error": "Informe o nº do processo."}

    fcfg = _flask_mysql_config()
    if not fcfg:
        return {
            "ok": False,
            "error": "MySQL do flaskdb não configurado (FLASK_MYSQL_*).",
        }

    try:
        conn = mysql.connector.connect(**fcfg, charset="utf8mb4", collation="utf8mb4_unicode_ci")
        cur = conn.cursor(dictionary=True)
        cur.execute("SHOW TABLES LIKE 'precainfosnew'")
        if not cur.fetchone():
            return {"ok": False, "error": "Tabela precainfosnew não encontrada."}

        cur.execute("SHOW COLUMNS FROM `precainfosnew`")
        raw_cols = cur.fetchall() or []
        fields: set[str] = set()
        for r in raw_cols:
            if isinstance(r, dict):
                nm = r.get("Field") or r.get("field")
                if nm:
                    fields.add(str(nm))
            else:
                fields.add(str(r[0]))

        f_ordem = _pick_field(fields, "Ordem", "ordem")
        f_proc = _pick_field(
            fields,
            "numero_de_processo",
            "Numero_de_processo",
            "Numero_de_Processo",
            "processo",
            "Processo",
        )
        f_inc = _pick_field(
            fields,
            "numero_do_incidente",
            "Numero_do_incidente",
            "Numero_do_Incidente",
            "numero_de_incidente",
            "incidente",
            "Incidente",
        )
        f_req = _pick_field(fields, "requerente", "Requerente")
        f_calc = _pick_field(
            fields,
            "Calculo_Atualizado",
            "calculo_atualizado",
            "Calculo_atualizado",
        )
        f_ent = _pick_field(fields, "Entidade_Devedora", "entidade_devedora")
        f_adv = _pick_field(fields, "Advogado", "advogado")

        if not f_proc:
            return {"ok": False, "error": "Tabela precainfosnew sem coluna de processo."}

        sel = ["id"]
        if f_ordem:
            sel.append(f"`{f_ordem}` AS ordem")
        else:
            sel.append("NULL AS ordem")
        sel.append(f"`{f_proc}` AS numero_de_processo")
        if f_inc:
            sel.append(f"`{f_inc}` AS numero_do_incidente")
        else:
            sel.append("NULL AS numero_do_incidente")
        if f_req:
            sel.append(f"`{f_req}` AS requerente")
        else:
            sel.append("NULL AS requerente")
        if f_calc:
            sel.append(f"`{f_calc}` AS calculo_atualizado")
        else:
            sel.append("NULL AS calculo_atualizado")
        if f_ent:
            sel.append(f"`{f_ent}` AS entidade_devedora")
        else:
            sel.append("NULL AS entidade_devedora")
        if f_adv:
            sel.append(f"`{f_adv}` AS advogado")
        else:
            sel.append("NULL AS advogado")

        if inc and f_inc:
            where = (
                f"TRIM(COALESCE(`{f_proc}`, '')) = %s "
                f"AND TRIM(COALESCE(`{f_inc}`, '')) = %s"
            )
            params = (proc, inc)
        else:
            where = f"TRIM(COALESCE(`{f_proc}`, '')) = %s"
            params = (proc,)

        cur.execute(
            f"""
            SELECT {", ".join(sel)}
            FROM precainfosnew
            WHERE {where}
            ORDER BY id DESC
            LIMIT 15
            """,
            params,
        )
        rows = cur.fetchall() or []
        cur.close()
        conn.close()
    except mysql.connector.Error as e:
        return {"ok": False, "error": f"Erro ao consultar precainfosnew: {e}"}

    if not rows:
        return {"ok": True, "results": []}

    mcfg = _memoria_mysql_config()
    mem_cur = None
    mem_conn = None
    try:
        if mcfg:
            mem_conn = mysql.connector.connect(
                **mcfg, charset="utf8mb4", collation="utf8mb4_unicode_ci"
            )
            mem_cur = mem_conn.cursor(dictionary=True)
    except mysql.connector.Error:
        mem_cur = None

    out: list[dict[str, Any]] = []
    for r in rows:
        try:
            pid = int(r.get("id")) if r.get("id") is not None else None
        except (TypeError, ValueError):
            pid = None
        p_proc = _str_val(r.get("numero_de_processo"))
        p_inc = _str_val(r.get("numero_do_incidente"))
        liq = None
        if mem_cur:
            try:
                liq = _liquido_memoria(
                    mem_cur,
                    id_precainfosnew=pid,
                    processo=p_proc or proc,
                    incidente=p_inc,
                )
            except mysql.connector.Error:
                liq = None
        out.append(
            {
                "id_precainfosnew": pid,
                "ordem": _str_val(r.get("ordem")) or None,
                "numero_de_processo": p_proc or proc,
                "numero_do_incidente": p_inc,
                "requerente": _str_val(r.get("requerente")) or None,
                "valor_liquido_atualizado": liq,
                "calculo_atualizado": _str_val(r.get("calculo_atualizado")) or None,
                "entidade_devedora": _str_val(r.get("entidade_devedora")) or None,
                "advogado": _str_val(r.get("advogado")) or None,
            }
        )

    if mem_cur:
        mem_cur.close()
    if mem_conn:
        mem_conn.close()

    return {"ok": True, "results": out}
