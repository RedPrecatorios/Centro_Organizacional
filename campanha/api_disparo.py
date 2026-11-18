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

from campanha.api_templates import obter_template
from concurrent.futures import ALL_COMPLETED, FIRST_COMPLETED, ThreadPoolExecutor, wait

from campanha.entregabilidade import (
    aplicar_rodape,
    emails_recebidos_na_janela,
    garantir_esquema_campanha,
    headers_descadastro,
    paralelismo_para_prazo,
    prazo_envio_segundos,
    selecionar_par_da_semana,
    url_descadastro,
)
from campanha.core import (
    BlacklistConfig,
    ContentConfig,
    DomainSender,
    ElasticEmailConfig,
    MailgunConfig,
    MysqlConfig,
    Recipient,
    SendingConfig,
    _build_message,
    _effective_reply_to,
    _elasticemail_send,
    _idempotency_key,
    _mailgun_send,
    _norm_email,
    _read_text,
    _render_template,
    _round_robin,
    _smtp_send,
    _update_emails_campanha_log,
    build_recipient_template_vars,
    load_blacklist_for_campanha,
    recipient_blocked_by_blacklist,
    recipient_processo_incidente,
)

_disparo_lock = threading.Lock()
_disparo_atual: dict[str, Any] | None = None
_cancelar_flag = threading.Event()
_ultimo_flush_db = 0.0


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


def _atualizar_progresso(campaign_id: str, forcar: bool = False, **fields):
    global _disparo_atual, _ultimo_flush_db
    if _disparo_atual and _disparo_atual.get("campaign_id") == campaign_id:
        _disparo_atual.update({k: v for k, v in fields.items() if k != "log_line"})

    sets = []
    params = []
    field_map = {
        "enviados": "enviados", "falhos": "falhos",
        "blacklist_skip": "blacklist_skip", "duplicados": "duplicados",
        "reenvio_skip": "reenvio_skip",
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
    if "status" in fields or "concluido_em" in fields:
        forcar = True
    agora = time.monotonic()
    if not forcar and (agora - _ultimo_flush_db) < 1.0:
        return
    _ultimo_flush_db = agora
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


def _enviar_mensagem(domain, sending, mailgun, elasticemail, msg, meta: dict) -> tuple[bool, str | None, float]:
    inicio = time.monotonic()
    error = None
    ok = False
    for attempt in range(1, sending.max_retries + 1):
        if _cancelar_flag.is_set():
            return False, "cancelado", time.monotonic() - inicio
        try:
            if sending.method == "elasticemail" and elasticemail:
                _elasticemail_send(domain, sending, elasticemail, msg)
            elif sending.method == "mailgun" and mailgun:
                _mailgun_send(domain, sending, mailgun, msg)
            else:
                _smtp_send(domain, sending, msg)
            ok = True
            error = None
            break
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"
            time.sleep(min(10, 2 * attempt))
    if error != "cancelado":
        _registrar_resultado_email(
            meta["to_norm"],
            "sent" if ok else "failed",
            None if ok else error,
            meta["campaign_id"],
            meta["dominio"],
            meta["remetente"],
        )
    return ok, error, time.monotonic() - inicio


def _data_curta(iso: str) -> str:
    partes = (iso or "").split("-")
    if len(partes) != 3:
        return iso
    return f"{partes[2]}/{partes[1]}/{partes[0]}"


def _registrar_resultado_email(
    email_norm: str,
    status: str,
    erro: str | None,
    campaign_id: str,
    dominio: str | None,
    remetente: str | None,
) -> None:
    try:
        conn = _conectar()
        _update_emails_campanha_log(
            conn,
            email_norm_upper=email_norm,
            status=status,
            erro=erro,
            campaign_id=campaign_id,
            dry_run=False,
            dominio=dominio,
            remetente=remetente,
        )
        conn.close()
    except Exception as exc:
        _atualizar_progresso(
            campaign_id,
            log_line=f"[{_ts()}] Aviso: não gravei o resultado de {email_norm}: {exc}",
        )


def _thread_disparo(
    campaign_id: str,
    recipients: list[Recipient],
    domains: list[DomainSender],
    mysql_cfg: MysqlConfig,
    sending: SendingConfig,
    mailgun: MailgunConfig | None,
    elasticemail: ElasticEmailConfig | None,
    content: ContentConfig,
    blacklist_cfg: BlacklistConfig,
    criado_por: str,
    mapeamento: dict[str, str] | None,
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
        bl = load_blacklist_for_campanha(
            mysql_cfg, blacklist_cfg.use_db, blacklist_cfg.extra_email_file
        )
    except Exception as e:
        _atualizar_progresso(
            campaign_id, status="erro",
            log_line=f"[{_ts()}] ERRO blacklist: {e}",
            concluido_em=datetime.now(timezone.utc).isoformat(),
        )
        if _disparo_atual:
            _disparo_atual["status"] = "erro"
        return

    try:
        conn_janela = _conectar()
        garantir_esquema_campanha(conn_janela)
        recentes = emails_recebidos_na_janela(conn_janela, 30)
        conn_janela.close()
    except Exception as e:
        _atualizar_progresso(
            campaign_id, status="erro",
            log_line=f"[{_ts()}] ERRO ao ler o prazo de 30 dias: {e}",
            concluido_em=datetime.now(timezone.utc).isoformat(),
        )
        if _disparo_atual:
            _disparo_atual["status"] = "erro"
        return

    html_t = (
        content.html_inline
        if content.html_inline is not None
        else _read_text(content.html_template)
    )
    text_t = (
        content.text_inline
        if content.text_inline is not None
        else _read_text(content.text_template)
    )

    rr = _round_robin(domains)
    _, info_par = selecionar_par_da_semana(list(domains), lambda d: d.from_email.split("@")[-1])
    nomes = ", ".join(d.from_email.split("@")[-1] for d in domains)
    prazo_s = float(prazo_envio_segundos())
    prazo_min = int(prazo_s // 60)
    inicio_disparo = time.monotonic()
    latencias: list[float] = []
    limite_em_voo = paralelismo_para_prazo(len(recipients), 1.0, prazo_s)
    teto_em_voo = max(limite_em_voo, paralelismo_para_prazo(len(recipients), 4.0, prazo_s))
    _atualizar_progresso(
        campaign_id,
        forcar=True,
        log_line=(
            f"[{_ts()}] Par da semana: {nomes} (até {_data_curta(info_par.get('fim') or '')}). "
            f"Prazo: {prazo_min} min para {len(recipients)} destinatário(s). "
            f"O ritmo se ajusta ao que falta."
        ),
    )

    sent_keys: set[str] = set()
    totals = {"enviados": 0, "falhos": 0, "blacklist_skip": 0, "duplicados": 0, "reenvio_skip": 0}
    total = len(recipients)
    concluidos = 0
    em_voo: dict = {}
    limite_atual = limite_em_voo

    def _recalibrar(faltam: int) -> None:
        nonlocal limite_atual
        sobra = prazo_s - (time.monotonic() - inicio_disparo)
        media = (sum(latencias) / len(latencias)) if latencias else 1.0
        preciso = paralelismo_para_prazo(max(1, faltam), media, sobra)
        limite_atual = max(1, min(teto_em_voo, preciso))

    def _anotar_envio(meta: dict, ok: bool, error: str | None, duracao: float) -> None:
        nonlocal concluidos
        concluidos += 1
        if duracao > 0 and error != "cancelado":
            latencias.append(duracao)
        pct = round((concluidos / total) * 100, 2) if total else 100.0
        if ok:
            totals["enviados"] += 1
            recentes.add(meta["to_norm"])
            _atualizar_progresso(
                campaign_id, enviados=totals["enviados"],
                progresso_pct=pct,
                log_line=f"[{_ts()}] Enviado: {meta['email']} via {meta['dominio']}",
            )
            return
        if error == "cancelado":
            return
        totals["falhos"] += 1
        _atualizar_progresso(
            campaign_id, falhos=totals["falhos"],
            progresso_pct=pct, log_line=f"[{_ts()}] FALHOU: {meta['email']} -- {error}",
        )

    def _colher(esperar_todos: bool) -> None:
        if not em_voo:
            return
        feitos, _pendentes = wait(
            set(em_voo),
            return_when=ALL_COMPLETED if esperar_todos else FIRST_COMPLETED,
        )
        for fut in feitos:
            meta = em_voo.pop(fut)
            try:
                ok, error, duracao = fut.result()
            except Exception as exc:
                ok, error, duracao = False, f"{type(exc).__name__}: {exc}", 0.0
            _anotar_envio(meta, ok, error, duracao)

    pool = ThreadPoolExecutor(max_workers=teto_em_voo)
    try:
        for i, r in enumerate(recipients):
            if _cancelar_flag.is_set():
                break

            to_norm = _norm_email(r.email)
            if not to_norm:
                continue

            bloqueado, motivo_bl = recipient_blocked_by_blacklist(r, bl)
            if bloqueado:
                totals["blacklist_skip"] += 1
                pct = round(((i + 1) / total) * 100, 2)
                proc, inc = recipient_processo_incidente(r)
                bl_msg = f"{r.email} [{motivo_bl}]"
                if proc:
                    bl_msg += f" (proc={proc}" + (f" inc={inc})" if inc else ")")
                _atualizar_progresso(
                    campaign_id, blacklist_skip=totals["blacklist_skip"],
                    progresso_pct=pct, log_line=f"[{_ts()}] Blacklist: {bl_msg}",
                )
                _registrar_resultado_email(
                    to_norm, "skipped_blacklist", motivo_bl, campaign_id, None, None,
                )
                continue

            if to_norm in recentes:
                totals["reenvio_skip"] += 1
                pct = round(((i + 1) / total) * 100, 2)
                _atualizar_progresso(
                    campaign_id, reenvio_skip=totals["reenvio_skip"],
                    progresso_pct=pct,
                    log_line=f"[{_ts()}] Prazo 30 dias: {r.email}",
                )
                continue

            vars_ = build_recipient_template_vars(
                r, content.subject, dict(content.vars), mapeamento,
            )
            subj_final = vars_["subject"]
            key = _idempotency_key(campaign_id, r.email, subj_final)
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
            link = url_descadastro(r.email)
            vars_["unsubscribe_url"] = link
            html = _render_template(html_t, vars_)
            text = _render_template(text_t, vars_)
            html, text = aplicar_rodape(html, text, link)
            msg = _build_message(
                domain=domain, to_email=r.email, subject=subj_final,
                html=html, text=text,
                headers=headers_descadastro(link, campaign_id, key),
                reply_to=_effective_reply_to(domain, sending),
            )
            meta = {
                "to_norm": to_norm,
                "email": r.email,
                "dominio": domain.name,
                "remetente": domain.from_email,
                "campaign_id": campaign_id,
            }
            fut = pool.submit(_enviar_mensagem, domain, sending, mailgun, elasticemail, msg, meta)
            em_voo[fut] = meta
            faltam = max(1, (total - i - 1) + len(em_voo))
            _recalibrar(faltam)
            while len(em_voo) >= limite_atual and not _cancelar_flag.is_set():
                _colher(False)
                _recalibrar(max(1, (total - i - 1) + len(em_voo)))
        _colher(True)
    finally:
        pool.shutdown(wait=True)

    final_status = "cancelado" if _cancelar_flag.is_set() else "concluido"
    _atualizar_progresso(
        campaign_id, forcar=True, status=final_status, progresso_pct=100.0,
        concluido_em=datetime.now(timezone.utc).isoformat(),
        log_line=(
            f"[{_ts()}] Concluido -- Env:{totals['enviados']} "
            f"Falha:{totals['falhos']} BL:{totals['blacklist_skip']} "
            f"Prazo30:{totals['reenvio_skip']} Dup:{totals['duplicados']}"
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
    *,
    template_id: int | None = None,
    mapeamento: dict[str, str] | None = None,
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

        # mailgun_state guarda o estado no provedor atual (Elastic Email): active = SPF+DKIM ok.
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

        par, _info_par = selecionar_par_da_semana(dom_rows, lambda r: r.get("dominio") or "")
        dom_rows = [r for r in par if r.get("mailgun_state") == "active"] or par

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
        ee_key = (os.getenv("ELASTICEMAIL_API_KEY") or "").strip()
        mailgun = MailgunConfig(api_key=mg_key, region="us") if mg_key else None
        elasticemail = ElasticEmailConfig(api_key=ee_key) if ee_key else None
        if elasticemail:
            send_method = "elasticemail"
        elif mailgun:
            send_method = "mailgun"
        else:
            send_method = "smtp"
        sending = SendingConfig(
            dry_run=False, per_domain_per_minute=60,
            smtp_timeout_seconds=30, max_retries=3,
            method=send_method,
            reply_to="contato@redprecatorios.com.br",
        )
        project_root = Path(__file__).resolve().parent.parent
        blacklist_cfg = BlacklistConfig(use_db=True, extra_email_file=None)
        raw_extra = (os.getenv("CAMPANHA_BLACKLIST_EXTRA_EMAIL_FILE") or "").strip()
        if raw_extra:
            xp = Path(raw_extra)
            if not xp.is_absolute():
                xp = project_root / raw_extra
            if xp.is_file():
                blacklist_cfg = BlacklistConfig(use_db=True, extra_email_file=str(xp.resolve()))

        html_p = project_root / "campanha" / "templates" / "default.html"
        text_p = project_root / "campanha" / "templates" / "default.txt"

        html_inline: str | None = None
        text_inline: str | None = None
        map_use: dict[str, str] | None = mapeamento

        if template_id is not None:
            trow = obter_template(int(template_id), cfg_dict, db_name)
            if not trow:
                _disparo_lock.release()
                return {"ok": False, "error": "Template nao encontrado ou inativo."}
            html_inline = (trow.get("corpo_html") or "").strip() or None
            text_inline = (trow.get("corpo_texto") or "").strip() or None
            if mapeamento is None and trow.get("mapeamento"):
                map_use = trow["mapeamento"]

        content = ContentConfig(
            subject=assunto,
            html_template=str(html_p),
            text_template=str(text_p),
            vars={"company_name": "RED PRECATORIOS", "support_email": "contato@redprecatorios.com.br"},
            html_inline=html_inline,
            text_inline=text_inline,
        )

        _disparo_atual = {
            "campaign_id": campaign_id,
            "status": "preparando",
            "total": len(recipients),
            "enviados": 0, "falhos": 0,
            "blacklist_skip": 0, "duplicados": 0, "reenvio_skip": 0,
            "progresso_pct": 0, "log_lines": [],
            "iniciado_em": None,
        }

        t = threading.Thread(
            target=_run_and_release,
            args=(
                campaign_id, recipients, domains, mysql_cfg, sending, mailgun,
                elasticemail, content, blacklist_cfg, criado_por, map_use,
            ),
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
        "reenvio_skip": _disparo_atual.get("reenvio_skip", 0),
        "progresso_pct": _disparo_atual.get("progresso_pct", 0),
        "log_lines": (_disparo_atual.get("log_lines") or [])[-30:],
    }


def obter_historico(limit: int = 50) -> list[dict]:
    conn = _conectar()
    garantir_esquema_campanha(conn)
    cur = conn.cursor(dictionary=True)
    cur.execute(
        """SELECT campaign_id, assunto, total_destinatarios, enviados, falhos,
                  blacklist_skip, duplicados, reenvio_skip, aberturas, recusas, bounces,
                  status, progresso_pct, origem, iniciado_em, concluido_em, criado_por
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


def _where_destinatarios(
    periodo_entrada_inicio: str | None = None,
    periodo_entrada_fim: str | None = None,
    periodo_disparo_inicio: str | None = None,
    periodo_disparo_fim: str | None = None,
    status_disparo: str | None = None,
    fornecedor: str | None = None,
) -> tuple[str, list]:
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
    return " AND ".join(where), params


def contar_destinatarios_base(**filtros) -> int:
    where, params = _where_destinatarios(**filtros)
    conn = _conectar()
    cur = conn.cursor()
    cur.execute(
        f"""
        SELECT COUNT(*) FROM (
            SELECT DISTINCT e.email, p.nome, p.cpf,
                   pj.numero_processo, pj.numero_incidente
            FROM emails e
            JOIN processos_juridicos pj ON pj.id = e.id_processo_juridico
            JOIN pessoas p ON p.id = pj.id_pessoa
            WHERE {where}
        ) t
        """,
        params,
    )
    total = int(cur.fetchone()[0] or 0)
    cur.close()
    conn.close()
    return total


def buscar_destinatarios_base(
    periodo_entrada_inicio: str | None = None,
    periodo_entrada_fim: str | None = None,
    periodo_disparo_inicio: str | None = None,
    periodo_disparo_fim: str | None = None,
    status_disparo: str | None = None,
    fornecedor: str | None = None,
    limite: int | None = None,
) -> list[dict]:
    """Busca emails da tabela `emails` com filtros. Sem limite, devolve a lista inteira."""
    where, params = _where_destinatarios(
        periodo_entrada_inicio, periodo_entrada_fim,
        periodo_disparo_inicio, periodo_disparo_fim,
        status_disparo, fornecedor,
    )
    sql = f"""
        SELECT DISTINCT e.email, p.nome, p.cpf,
               pj.numero_processo AS processo,
               pj.numero_incidente AS incidente
        FROM emails e
        JOIN processos_juridicos pj ON pj.id = e.id_processo_juridico
        JOIN pessoas p ON p.id = pj.id_pessoa
        WHERE {where}
        ORDER BY e.email
    """
    if limite is not None:
        sql += " LIMIT %s"
        params.append(int(limite))
    conn = _conectar()
    cur = conn.cursor(dictionary=True)
    cur.execute(sql, params)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows
