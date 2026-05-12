"""
Motor de disparo web -- reutiliza campanha/core.py, adaptado para execucao
via thread de background com progresso em tempo real.
"""
from __future__ import annotations

import json
import os
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import mysql.connector

from campanha.core import (
    BlacklistConfig,
    ContentConfig,
    DomainSender,
    MailgunConfig,
    MysqlConfig,
    Recipient,
    SendingConfig,
    _build_message,
    _effective_reply_to,
    _idempotency_key,
    _mailgun_send,
    _norm_email,
    _read_text,
    _render_template,
    _round_robin,
    _smtp_send,
    load_blacklist_emails,
)

_disparo_lock = threading.Lock()
_disparo_atual: dict[str, Any] | None = None
_cancelar_flag = threading.Event()


def _db_config() -> tuple[dict, str]:
    host = (os.getenv("EDA_MYSQL_HOST") or "localhost").strip()
    port = int(os.getenv("EDA_MYSQL_PORT", "3306") or "3306")
    user = (os.getenv("EDA_MYSQL_USER") or "root").strip()
    password = os.getenv("EDA_MYSQL_PASSWORD", "") or ""
    db_name = (os.getenv("EDA_MYSQL_DATABASE") or "plataforma_central").strip()
    timeout = int(os.getenv("EDA_MYSQL_CONNECT_TIMEOUT", "15") or "15")
    cfg = {
        "host": host, "port": port, "user": user,
        "password": password, "connection_timeout": timeout,
    }
    return cfg, db_name


def _conectar():
    cfg, db_name = _db_config()
    return mysql.connector.connect(**cfg, database=db_name)


def _atualizar_progresso(campaign_id: str, **fields):
    global _disparo_atual
    if _disparo_atual and _disparo_atual.get("campaign_id") == campaign_id:
        _disparo_atual.update(fields)

    sets = []
    params = []
    field_map = {
        "enviados": "enviados", "falhos": "falhos",
        "blacklist_skip": "blacklist_skip", "duplicados": "duplicados",
        "progresso_pct": "progresso_pct", "status": "status",
        "concluido_em": "concluido_em",
    }
    for py_key, col in field_map.items():
        if py_key in fields:
            sets.append(f"`{col}` = %s")
            params.append(fields[py_key])
    if "log_line" in fields and _disparo_atual:
        log = _disparo_atual.get("log_lines", [])
        log.append(fields["log_line"])
        if len(log) > 200:
            log = log[-200:]
        _disparo_atual["log_lines"] = log
        sets.append("`log_json` = %s")
        params.append(json.dumps(log[-50:], ensure_ascii=False))

    if not sets:
        return
    params.append(campaign_id)
    try:
        conn = _conectar()
        cur = conn.cursor()
        cur.execute(
            f"UPDATE campanha_disparos SET {', '.join(sets)} WHERE campaign_id = %s",
            params,
        )
        conn.commit()
        cur.close()
        conn.close()
    except Exception:
        pass


def _ts() -> str:
    return datetime.now().strftime("%H:%M:%S")


def _thread_disparo(
    campaign_id: str,
    recipients: list[Recipient],
    domains: list[DomainSender],
    mysql_cfg: MysqlConfig,
    sending: SendingConfig,
    mailgun: MailgunConfig | None,
    content: ContentConfig,
    blacklist_cfg: BlacklistConfig,
    criado_por: str,
):
    global _disparo_atual
    _cancelar_flag.clear()

    try:
        conn = _conectar()
        cur = conn.cursor()
        cur.execute(
            "UPDATE campanha_disparos SET status='rodando', iniciado_em=NOW() WHERE campaign_id = %s",
            (campaign_id,),
        )
        conn.commit()
        cur.close()
        conn.close()
    except Exception:
        pass

    if _disparo_atual:
        _disparo_atual.update({"status": "rodando", "iniciado_em": datetime.now(timezone.utc).isoformat()})
    _atualizar_progresso(campaign_id, log_line=f"[{_ts()}] Iniciando disparo -- {len(recipients)} destinatario(s)")

    try:
        blocked = load_blacklist_emails(mysql_cfg, blacklist_cfg.use_db, blacklist_cfg.extra_email_file)
    except Exception as e:
        _atualizar_progresso(
            campaign_id, status="erro",
            log_line=f"[{_ts()}] ERRO blacklist: {e}",
            concluido_em=datetime.now(timezone.utc).isoformat(),
        )
        if _disparo_atual:
            _disparo_atual["status"] = "erro"
        return

    html_t = _read_text(content.html_template)
    text_t = _read_text(content.text_template)

    per_min = max(1, sending.per_domain_per_minute)
    min_interval = 60.0 / per_min
    last_sent: dict[str, float] = {d.name: 0.0 for d in domains}
    rr = _round_robin(domains)

    sent_keys: set[str] = set()
    totals = {"enviados": 0, "falhos": 0, "blacklist_skip": 0, "duplicados": 0}
    total = len(recipients)

    for i, r in enumerate(recipients):
        if _cancelar_flag.is_set():
            _atualizar_progresso(
                campaign_id, status="cancelado",
                log_line=f"[{_ts()}] Cancelado pelo utilizador.",
                concluido_em=datetime.now(timezone.utc).isoformat(),
            )
            if _disparo_atual:
                _disparo_atual["status"] = "cancelado"
            return

        to_norm = _norm_email(r.email)
        if not to_norm:
            continue

        if to_norm in blocked:
            totals["blacklist_skip"] += 1
            pct = round(((i + 1) / total) * 100, 2)
            _atualizar_progresso(
                campaign_id, blacklist_skip=totals["blacklist_skip"],
                progresso_pct=pct, log_line=f"[{_ts()}] Blacklist: {r.email}",
            )
            continue

        key = _idempotency_key(campaign_id, r.email, content.subject)
        if key in sent_keys:
            totals["duplicados"] += 1
            pct = round(((i + 1) / total) * 100, 2)
            _atualizar_progresso(
                campaign_id, duplicados=totals["duplicados"],
                progresso_pct=pct, log_line=f"[{_ts()}] Duplicado: {r.email}",
            )
            continue
        sent_keys.add(key)

        domain = next(rr)

        now = time.time()
        delta = now - last_sent[domain.name]
        if delta < min_interval:
            time.sleep(min_interval - delta)

        vars_ = dict(content.vars)
        vars_.update({
            "subject": content.subject,
            "name": r.name,
            "email": r.email,
            "credor": r.name,
        })
        vars_.update(r.fields)
        html = _render_template(html_t, vars_)
        text = _render_template(text_t, vars_)

        msg = _build_message(
            domain=domain, to_email=r.email, subject=content.subject,
            html=html, text=text,
            headers={"X-Campaign-Id": campaign_id, "X-Idempotency-Key": key},
            reply_to=_effective_reply_to(domain, sending),
        )

        ok = False
        error = None
        for attempt in range(1, sending.max_retries + 1):
            if _cancelar_flag.is_set():
                break
            try:
                if sending.method == "mailgun" and mailgun:
                    _mailgun_send(domain, sending, mailgun, msg)
                else:
                    _smtp_send(domain, sending, msg)
                ok = True
                break
            except Exception as e:
                error = f"{type(e).__name__}: {e}"
                time.sleep(min(10, 2 * attempt))

        last_sent[domain.name] = time.time()
        pct = round(((i + 1) / total) * 100, 2)

        if ok:
            totals["enviados"] += 1
            _atualizar_progresso(
                campaign_id, enviados=totals["enviados"],
                progresso_pct=pct, log_line=f"[{_ts()}] Enviado: {r.email} via {domain.name}",
            )
        else:
            totals["falhos"] += 1
            _atualizar_progresso(
                campaign_id, falhos=totals["falhos"],
                progresso_pct=pct, log_line=f"[{_ts()}] FALHOU: {r.email} -- {error}",
            )

    final_status = "cancelado" if _cancelar_flag.is_set() else "concluido"
    _atualizar_progresso(
        campaign_id, status=final_status, progresso_pct=100.0,
        concluido_em=datetime.now(timezone.utc).isoformat(),
        log_line=(
            f"[{_ts()}] Concluido -- Env:{totals['enviados']} "
            f"Falha:{totals['falhos']} BL:{totals['blacklist_skip']} Dup:{totals['duplicados']}"
        ),
    )
    if _disparo_atual:
        _disparo_atual["status"] = final_status


def _run_and_release(*args):
    try:
        _thread_disparo(*args)
    finally:
        _disparo_lock.release()


def iniciar_disparo(
    recipients: list[Recipient],
    campaign_id: str,
    assunto: str,
    origem: str,
    filtros: dict | None,
    criado_por: str,
) -> dict:
    global _disparo_atual

    if not _disparo_lock.acquire(blocking=False):
        return {"ok": False, "error": "Ja existe um disparo em andamento."}

    try:
        if _disparo_atual and _disparo_atual.get("status") == "rodando":
            _disparo_lock.release()
            return {"ok": False, "error": "Ja existe um disparo em andamento."}

        cfg_dict, db_name = _db_config()
        conn = mysql.connector.connect(**cfg_dict, database=db_name)
        cur = conn.cursor(dictionary=True)

        cur.execute("SELECT * FROM campanha_dominios WHERE ativo = 1 AND mailgun_state = 'active' ORDER BY nome")
        dom_rows = cur.fetchall()
        if not dom_rows:
            cur.execute("SELECT * FROM campanha_dominios WHERE ativo = 1 ORDER BY nome")
            dom_rows = cur.fetchall()

        if not dom_rows:
            cur.close()
            conn.close()
            _disparo_lock.release()
            return {"ok": False, "error": "Nenhum dominio cadastrado. Adicione dominios antes de disparar."}

        domains = [
            DomainSender(
                name=r["nome"], from_name=r["from_name"], from_email=r["from_email"],
                reply_to=r.get("reply_to") or "",
            )
            for r in dom_rows
        ]

        cur.execute(
            """INSERT INTO campanha_disparos
               (campaign_id, assunto, total_destinatarios, origem, filtros_json, criado_por, status)
               VALUES (%s, %s, %s, %s, %s, %s, 'preparando')""",
            (campaign_id, assunto, len(recipients), origem,
             json.dumps(filtros, ensure_ascii=False) if filtros else None,
             criado_por),
        )
        conn.commit()
        cur.close()
        conn.close()

        mysql_cfg = MysqlConfig(
            host=cfg_dict["host"], port=cfg_dict["port"], user=cfg_dict["user"],
            password=cfg_dict["password"], database=db_name,
            connection_timeout=cfg_dict.get("connection_timeout", 15),
        )
        mg_key = (os.getenv("MAILGUN_API_KEY") or "").strip()
        mailgun = MailgunConfig(api_key=mg_key, region="us") if mg_key else None
        sending = SendingConfig(
            dry_run=False, per_domain_per_minute=60,
            smtp_timeout_seconds=30, max_retries=3,
            method="mailgun" if mailgun else "smtp",
            reply_to="contato@redprecatorios.com.br",
        )
        blacklist_cfg = BlacklistConfig(use_db=True, extra_email_file=None)

        project_root = Path(__file__).resolve().parent.parent
        html_p = project_root / "campanha" / "templates" / "default.html"
        text_p = project_root / "campanha" / "templates" / "default.txt"

        content = ContentConfig(
            subject=assunto,
            html_template=str(html_p),
            text_template=str(text_p),
            vars={"company_name": "RED PRECATORIOS", "support_email": "contato@redprecatorios.com.br"},
        )

        _disparo_atual = {
            "campaign_id": campaign_id,
            "status": "preparando",
            "total": len(recipients),
            "enviados": 0, "falhos": 0,
            "blacklist_skip": 0, "duplicados": 0,
            "progresso_pct": 0, "log_lines": [],
            "iniciado_em": None,
        }

        t = threading.Thread(
            target=_run_and_release,
            args=(campaign_id, recipients, domains, mysql_cfg, sending, mailgun,
                  content, blacklist_cfg, criado_por),
            daemon=True,
        )
        t.start()

        return {"ok": True, "campaign_id": campaign_id, "total": len(recipients)}

    except Exception as e:
        _disparo_lock.release()
        return {"ok": False, "error": str(e)}


def cancelar_disparo() -> dict:
    if not _disparo_atual or _disparo_atual.get("status") != "rodando":
        return {"ok": False, "error": "Nenhum disparo em andamento."}
    _cancelar_flag.set()
    return {"ok": True}


def obter_status() -> dict:
    if not _disparo_atual:
        return {"ok": True, "ativo": False}
    return {
        "ok": True,
        "ativo": _disparo_atual.get("status") == "rodando",
        "campaign_id": _disparo_atual.get("campaign_id"),
        "status": _disparo_atual.get("status"),
        "total": _disparo_atual.get("total", 0),
        "enviados": _disparo_atual.get("enviados", 0),
        "falhos": _disparo_atual.get("falhos", 0),
        "blacklist_skip": _disparo_atual.get("blacklist_skip", 0),
        "duplicados": _disparo_atual.get("duplicados", 0),
        "progresso_pct": _disparo_atual.get("progresso_pct", 0),
        "log_lines": (_disparo_atual.get("log_lines") or [])[-30:],
    }


def obter_historico(limit: int = 50) -> list[dict]:
    conn = _conectar()
    cur = conn.cursor(dictionary=True)
    cur.execute(
        """SELECT campaign_id, assunto, total_destinatarios, enviados, falhos,
                  blacklist_skip, duplicados, status, progresso_pct, origem,
                  iniciado_em, concluido_em, criado_por
           FROM campanha_disparos ORDER BY id DESC LIMIT %s""",
        (limit,),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    for r in rows:
        for k in ("iniciado_em", "concluido_em"):
            if r.get(k) and hasattr(r[k], "isoformat"):
                r[k] = r[k].isoformat()
    return rows


def obter_historico_detalhe(campaign_id: str) -> dict | None:
    conn = _conectar()
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM campanha_disparos WHERE campaign_id = %s", (campaign_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    if not row:
        return None
    for k in ("iniciado_em", "concluido_em"):
        if row.get(k) and hasattr(row[k], "isoformat"):
            row[k] = row[k].isoformat()
    if row.get("log_json"):
        try:
            row["log_lines"] = json.loads(row["log_json"])
        except (json.JSONDecodeError, TypeError):
            row["log_lines"] = []
    else:
        row["log_lines"] = []
    return row


def buscar_destinatarios_base(
    periodo_entrada_inicio: str | None = None,
    periodo_entrada_fim: str | None = None,
    periodo_disparo_inicio: str | None = None,
    periodo_disparo_fim: str | None = None,
    status_disparo: str | None = None,
    fornecedor: str | None = None,
) -> list[dict]:
    """Busca emails da tabela `emails` com filtros."""
    conn = _conectar()
    cur = conn.cursor(dictionary=True)

    where = ["1=1"]
    params: list = []

    if periodo_entrada_inicio:
        where.append("e.primeira_aparicao >= %s")
        params.append(periodo_entrada_inicio)
    if periodo_entrada_fim:
        where.append("e.primeira_aparicao <= %s")
        params.append(periodo_entrada_fim + " 23:59:59")
    if periodo_disparo_inicio:
        where.append("e.campanha_disparo_ultimo >= %s")
        params.append(periodo_disparo_inicio)
    if periodo_disparo_fim:
        where.append("e.campanha_disparo_ultimo <= %s")
        params.append(periodo_disparo_fim + " 23:59:59")
    if status_disparo == "nao_enviado":
        where.append("e.campanha_disparo_status IS NULL")
    elif status_disparo == "enviado":
        where.append("e.campanha_disparo_status = 'sent'")
    elif status_disparo == "falhou":
        where.append("e.campanha_disparo_status = 'failed'")
    if fornecedor:
        where.append("e.fornecedor = %s")
        params.append(fornecedor)

    sql = f"""
        SELECT DISTINCT e.email, p.nome, pj.numero_processo AS processo
        FROM emails e
        JOIN processos_juridicos pj ON pj.id = e.id_processo_juridico
        JOIN pessoas p ON p.id = pj.id_pessoa
        WHERE {' AND '.join(where)}
        ORDER BY e.email
        LIMIT 10000
    """
    cur.execute(sql, params)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows
