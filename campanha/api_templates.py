"""CRUD de templates de e-mail da Campanha (MySQL)."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import mysql.connector

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib

from campanha.core import extrair_variaveis_template

# Nome fixo do template espelhado dos ficheiros default.html / default.txt (script/CLI).
TEMPLATE_SCRIPT_ANTECIP_NOME = "antecipacao_credito_juridico"


def _conn(db_config: dict, db_name: str):
    return mysql.connector.connect(**db_config, database=db_name)


def listar_templates(db_config: dict, db_name: str) -> list[dict]:
    conn = _conn(db_config, db_name)
    cur = conn.cursor(dictionary=True)
    cur.execute(
        """
        SELECT id, nome, assunto, LEFT(corpo_html, 120) AS html_preview,
               LENGTH(corpo_html) AS html_len, LENGTH(corpo_texto) AS texto_len,
               mapeamento_json, ativo, criado_em, atualizado_em
        FROM campanha_templates WHERE ativo = 1 ORDER BY nome
        """
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    for r in rows:
        for k in ("criado_em", "atualizado_em"):
            if r.get(k) and hasattr(r[k], "isoformat"):
                r[k] = r[k].isoformat()
    return rows


def obter_template(template_id: int, db_config: dict, db_name: str) -> dict | None:
    conn = _conn(db_config, db_name)
    cur = conn.cursor(dictionary=True)
    cur.execute(
        "SELECT * FROM campanha_templates WHERE id = %s AND ativo = 1",
        (template_id,),
    )
    row = cur.fetchone()
    cur.close()
    conn.close()
    if not row:
        return None
    for k in ("criado_em", "atualizado_em"):
        if row.get(k) and hasattr(row[k], "isoformat"):
            row[k] = row[k].isoformat()
    m: dict[str, str] = {}
    if row.get("mapeamento_json"):
        try:
            raw = json.loads(row["mapeamento_json"])
            if isinstance(raw, dict):
                m = {str(k): str(v) for k, v in raw.items()}
        except (json.JSONDecodeError, TypeError):
            pass
    row["mapeamento"] = m
    row["variaveis"] = extrair_variaveis_template(
        row.get("assunto") or "",
        row.get("corpo_html") or "",
        row.get("corpo_texto") or "",
    )
    return row


def criar_template(
    nome: str,
    assunto: str,
    corpo_html: str,
    corpo_texto: str,
    mapeamento: dict[str, str] | None,
    db_config: dict,
    db_name: str,
) -> dict:
    conn = _conn(db_config, db_name)
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO campanha_templates (nome, assunto, corpo_html, corpo_texto, mapeamento_json)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (
            nome.strip(),
            assunto or "",
            corpo_html or "",
            corpo_texto or "",
            json.dumps(mapeamento, ensure_ascii=False) if mapeamento else None,
        ),
    )
    conn.commit()
    tid = cur.lastrowid
    cur.close()
    conn.close()
    return {"ok": True, "id": tid}


def atualizar_template(
    template_id: int,
    nome: str,
    assunto: str,
    corpo_html: str,
    corpo_texto: str,
    mapeamento: dict[str, str] | None,
    db_config: dict,
    db_name: str,
) -> dict:
    conn = _conn(db_config, db_name)
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE campanha_templates SET
            nome = %s, assunto = %s, corpo_html = %s, corpo_texto = %s, mapeamento_json = %s
        WHERE id = %s AND ativo = 1
        """,
        (
            nome.strip(),
            assunto or "",
            corpo_html or "",
            corpo_texto or "",
            json.dumps(mapeamento, ensure_ascii=False) if mapeamento else None,
            template_id,
        ),
    )
    n = cur.rowcount
    conn.commit()
    cur.close()
    conn.close()
    return {"ok": n > 0, "updated": n}


def remover_template(template_id: int, db_config: dict, db_name: str) -> dict:
    conn = _conn(db_config, db_name)
    cur = conn.cursor()
    cur.execute("UPDATE campanha_templates SET ativo = 0 WHERE id = %s", (template_id,))
    n = cur.rowcount
    conn.commit()
    cur.close()
    conn.close()
    return {"ok": n > 0}


def garantir_template_padrao_script(db_config: dict, db_name: str) -> dict:
    """
    Carrega campanha/templates/default.html e default.txt (+ [content].subject em config.toml)
    e grava em campanha_templates com mapeamento credor->nome, processo->processo (CSV README).

    Idempotente: se ja existir linha ativa com o mesmo `nome`, nao altera.
    """
    conn = _conn(db_config, db_name)
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT id FROM campanha_templates WHERE nome = %s AND ativo = 1",
            (TEMPLATE_SCRIPT_ANTECIP_NOME,),
        )
        if cur.fetchone():
            return {"ok": True, "skipped": True, "nome": TEMPLATE_SCRIPT_ANTECIP_NOME}
    finally:
        cur.close()
        conn.close()

    campanha_dir = Path(__file__).resolve().parent
    html_path = campanha_dir / "templates" / "default.html"
    txt_path = campanha_dir / "templates" / "default.txt"
    html = html_path.read_text(encoding="utf-8")
    txt = txt_path.read_text(encoding="utf-8")
    subject = "Antecipação do Crédito Jurídico"
    cfg_path = campanha_dir / "config.toml"
    if cfg_path.exists():
        try:
            data = tomllib.loads(cfg_path.read_text(encoding="utf-8"))
            cr = data.get("content") or {}
            subject = str(cr.get("subject") or subject).strip() or subject
        except (OSError, tomllib.TOMLDecodeError, TypeError):
            pass

    mapeamento = {"credor": "nome", "processo": "processo"}
    try:
        out = criar_template(
            TEMPLATE_SCRIPT_ANTECIP_NOME,
            subject,
            html,
            txt,
            mapeamento,
            db_config,
            db_name,
        )
        return {**out, "skipped": False, "nome": TEMPLATE_SCRIPT_ANTECIP_NOME}
    except mysql.connector.IntegrityError:
        return {"ok": True, "skipped": True, "nome": TEMPLATE_SCRIPT_ANTECIP_NOME}
