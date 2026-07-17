# -*- coding: utf-8 -*-
"""
Autenticação da plataforma: MySQL (plataforma_central), sessão Flask, admin e permissões por aba.
"""
from __future__ import annotations

import os
import ipaddress
import re
import traceback
from typing import Any

import mysql.connector.errors
from flask import (
    Blueprint,
    abort,
    g,
    redirect,
    render_template,
    request,
    session,
    url_for,
)
from werkzeug.security import check_password_hash, generate_password_hash

from messages_viewer.plataforma_auth_store import (
    auth_connection,
    auth_cursor,
    init_auth_schema,
    migrate_sqlite_to_mysql_if_needed,
    platform_meta_get,
    platform_meta_set,
)

auth_bp = Blueprint("auth", __name__, template_folder="templates")

# Permissões por painel (colaborador): admin ignora a tabela e acede a tudo.
# (id, nome no menu, descrição curta)
TAB_PANELS: tuple[tuple[str, str, str], ...] = (
    ("index", "Início", "Página inicial e resumo do painel"),
    ("conversas", "Conversas", "WhatsApp, instâncias e histórico de mensagens"),
    ("outro_modulo", "2.º módulo", "Iframe extra na página Conversas (/embedded/)"),
    ("memoria_calculo", "Memória de cálculo", "Consulta e actualização de memórias"),
    (
        "pre_analise_processual",
        "PRÉ Análise Processual",
        "Formulário inicial para triagem de processo/incidente",
    ),
    (
        "levantamento_processual",
        "Levantamento Processual",
        "Pesquisa por nome, CPF ou processo e listagem de aptos/inaptos (API TJSP)",
    ),
    (
        "tabela_juros",
        "Tabela de Juros (OC x Rendimento)",
        "Comparativo de rendimento: manter OC vs venda antecipada",
    ),
    ("proposta", "Gerar Proposta", "PDF comercial para formalização com o cliente"),
    ("campanha", "Campanha", "E-mail: domínios, templates e disparos"),
    ("auditoria_syscall", "Auditoria syscall", "Ligações auditadas (request_audit)"),
    ("localize", "Localize", "Pesquisa de e-mails e telefones na base EDA"),
    ("eda", "EDA Diário", "Processamento e relatórios EDA (/eda/)"),
)

# Permissões extra (não aparecem no menu lateral; configuráveis em Utilizadores).
FEATURE_PERMISSIONS: tuple[tuple[str, str, str], ...] = (
    (
        "analise_processual",
        "Análise processual",
        "Botão «Análise Processual» na Memória de cálculo (validação e-SAJ)",
    ),
    (
        "salvar_numero_meses",
        "Salvar número de meses",
        "Campo e botão «Salvar Número de Meses» na Memória de cálculo (precainfosnew)",
    ),
    (
        "forcar_atualizar_calculo",
        "Forçar atualizar cálculo",
        "Ignora o bloqueio mensal de «Atualizar Cálculo» quando BLOQUEAR_CALCULO=1",
    ),
)

# Painéis do menu + funcionalidades extra (checkboxes em Utilizadores).
PERMISSION_PANELS: tuple[tuple[str, str, str], ...] = TAB_PANELS + FEATURE_PERMISSIONS

# Compat: (id, label) para loops antigos
TAB_KEYS: tuple[tuple[str, str], ...] = tuple((p[0], p[1]) for p in TAB_PANELS)

TAB_IDS = {p[0] for p in TAB_PANELS}
PERMISSION_IDS = {p[0] for p in PERMISSION_PANELS}
SESSION_USER_ID = "plataforma_uid"
SESSION_VERSION = "plataforma_ver"
COLLAB_ALLOWED_IPS_META_KEY = "collaborator_allowed_ips"


def _platform_meta_get(key: str, default: str = "") -> str:
    return platform_meta_get(key, default)


def _platform_meta_set(key: str, value: str) -> None:
    platform_meta_set(key, value)


def _split_ip_entries(raw: str) -> list[str]:
    entries: list[str] = []
    for part in re.split(r"[\s,;]+", raw or ""):
        part = part.strip()
        if part:
            entries.append(part)
    return entries


def _normalize_ip_entries(raw: str) -> str:
    normalized: list[str] = []
    seen: set[str] = set()
    invalid: list[str] = []
    for entry in _split_ip_entries(raw):
        try:
            if "/" in entry:
                parsed = ipaddress.ip_network(entry, strict=False)
            else:
                parsed = ipaddress.ip_address(entry)
        except ValueError:
            invalid.append(entry)
            continue
        text = str(parsed)
        if text not in seen:
            normalized.append(text)
            seen.add(text)
    if invalid:
        raise ValueError(
            "IP/faixa inválido em acesso de colaboradores: " + ", ".join(invalid[:5])
        )
    return "\n".join(normalized)


def collaborator_allowed_ips_raw() -> str:
    return _platform_meta_get(COLLAB_ALLOWED_IPS_META_KEY, "")


def save_collaborator_allowed_ips(raw: str) -> None:
    _platform_meta_set(COLLAB_ALLOWED_IPS_META_KEY, _normalize_ip_entries(raw))


def _ip_matches_entry(ip: ipaddress._BaseAddress, entry: str) -> bool:
    try:
        if "/" in entry:
            return ip in ipaddress.ip_network(entry, strict=False)
        return ip == ipaddress.ip_address(entry)
    except ValueError:
        return False


def collaborator_ip_allowed(remote_addr: str | None) -> bool:
    entries = _split_ip_entries(collaborator_allowed_ips_raw())
    if not entries:
        return False
    try:
        client_ip = ipaddress.ip_address((remote_addr or "").strip())
    except ValueError:
        return False
    return any(_ip_matches_entry(client_ip, entry) for entry in entries)


def _current_remote_ip() -> str:
    return str(request.remote_addr or "").strip()


def _sync_auditoria_syscall_permission_for_campanha_users() -> None:
    """Quem já tem Campanha passa a ver Auditoria syscall sem reconfigurar cada conta."""
    with auth_connection() as conn:
        cur = auth_cursor(conn)
        cur.execute(
            """
            INSERT IGNORE INTO plataforma_user_permissions (user_id, tab_id)
            SELECT user_id, 'auditoria_syscall'
            FROM plataforma_user_permissions
            WHERE tab_id = 'campanha'
            """
        )
        conn.commit()


def _migrate_analise_processual_once() -> None:
    """
    Migração única: quem já tinha Memória de cálculo recebe Análise processual uma vez.
    Não repetir em cada pedido — senão anula remoções feitas em Utilizadores.
    """
    with auth_connection() as conn:
        cur = auth_cursor(conn)
        cur.execute(
            "SELECT 1 FROM plataforma_meta WHERE meta_key = 'migrated_analise_processual_v1'"
        )
        if cur.fetchone():
            return
        cur.execute(
            "SELECT 1 FROM plataforma_user_permissions WHERE tab_id = 'analise_processual' LIMIT 1"
        )
        had_any = cur.fetchone()
        if not had_any:
            cur.execute(
                """
                INSERT IGNORE INTO plataforma_user_permissions (user_id, tab_id)
                SELECT user_id, 'analise_processual'
                FROM plataforma_user_permissions
                WHERE tab_id = 'memoria_calculo'
                """
            )
        cur.execute(
            """
            INSERT INTO plataforma_meta (meta_key, meta_value)
            VALUES ('migrated_analise_processual_v1', '1')
            ON DUPLICATE KEY UPDATE meta_value = VALUES(meta_value)
            """
        )
        conn.commit()


def _migrate_tabela_juros_once() -> None:
    """Quem já tem Memória de cálculo recebe a nova aba Tabela de Juros (uma vez)."""
    with auth_connection() as conn:
        cur = auth_cursor(conn)
        cur.execute(
            "SELECT 1 FROM plataforma_meta WHERE meta_key = 'migrated_tabela_juros_v1'"
        )
        if cur.fetchone():
            return
        cur.execute(
            "SELECT 1 FROM plataforma_user_permissions WHERE tab_id = 'tabela_juros' LIMIT 1"
        )
        had_any = cur.fetchone()
        if not had_any:
            cur.execute(
                """
                INSERT IGNORE INTO plataforma_user_permissions (user_id, tab_id)
                SELECT user_id, 'tabela_juros'
                FROM plataforma_user_permissions
                WHERE tab_id = 'memoria_calculo'
                """
            )
        cur.execute(
            """
            INSERT INTO plataforma_meta (meta_key, meta_value)
            VALUES ('migrated_tabela_juros_v1', '1')
            ON DUPLICATE KEY UPDATE meta_value = VALUES(meta_value)
            """
        )
        conn.commit()


def _migrate_proposta_once() -> None:
    """Quem já tem Memória de cálculo recebe Gerar Proposta (uma vez)."""
    with auth_connection() as conn:
        cur = auth_cursor(conn)
        cur.execute(
            "SELECT 1 FROM plataforma_meta WHERE meta_key = 'migrated_proposta_v1'"
        )
        if cur.fetchone():
            return
        cur.execute(
            "SELECT 1 FROM plataforma_user_permissions WHERE tab_id = 'proposta' LIMIT 1"
        )
        had_any = cur.fetchone()
        if not had_any:
            cur.execute(
                """
                INSERT IGNORE INTO plataforma_user_permissions (user_id, tab_id)
                SELECT user_id, 'proposta'
                FROM plataforma_user_permissions
                WHERE tab_id = 'memoria_calculo'
                """
            )
        cur.execute(
            """
            INSERT INTO plataforma_meta (meta_key, meta_value)
            VALUES ('migrated_proposta_v1', '1')
            ON DUPLICATE KEY UPDATE meta_value = VALUES(meta_value)
            """
        )
        conn.commit()


def _migrate_levantamento_processual_once() -> None:
    """Quem já tem PRÉ Análise ou Memória recebe Levantamento Processual (uma vez)."""
    with auth_connection() as conn:
        cur = auth_cursor(conn)
        cur.execute(
            "SELECT 1 FROM plataforma_meta WHERE meta_key = 'migrated_levantamento_processual_v1'"
        )
        if cur.fetchone():
            return
        cur.execute(
            "SELECT 1 FROM plataforma_user_permissions WHERE tab_id = 'levantamento_processual' LIMIT 1"
        )
        had_any = cur.fetchone()
        if not had_any:
            cur.execute(
                """
                INSERT IGNORE INTO plataforma_user_permissions (user_id, tab_id)
                SELECT DISTINCT user_id, 'levantamento_processual'
                FROM plataforma_user_permissions
                WHERE tab_id IN ('pre_analise_processual', 'memoria_calculo')
                """
            )
        cur.execute(
            """
            INSERT INTO plataforma_meta (meta_key, meta_value)
            VALUES ('migrated_levantamento_processual_v1', '1')
            ON DUPLICATE KEY UPDATE meta_value = VALUES(meta_value)
            """
        )
        conn.commit()


def init_db() -> None:
    with auth_connection() as conn:
        init_auth_schema(conn)
    migrate_sqlite_to_mysql_if_needed()
    _sync_auditoria_syscall_permission_for_campanha_users()
    _migrate_analise_processual_once()
    _migrate_tabela_juros_once()
    _migrate_proposta_once()
    _migrate_levantamento_processual_once()


def _bootstrap_admin_if_empty() -> None:
    u = (os.getenv("AUTH_BOOTSTRAP_USER") or "").strip()
    p = (os.getenv("AUTH_BOOTSTRAP_PASSWORD") or "").strip()
    if not u or not p:
        return
    with auth_connection() as conn:
        cur = auth_cursor(conn)
        cur.execute("SELECT COUNT(*) AS n FROM plataforma_users")
        n = int((cur.fetchone() or {}).get("n") or 0)
        if n:
            return
        cur.execute(
            "INSERT INTO plataforma_users (username, password_hash, role) VALUES (%s, %s, 'admin')",
            (u, generate_password_hash(p)),
        )
        conn.commit()
        print(f"[plataforma_auth] Utilizador admin inicial criado: {u!r} (defina outro e remova a variável do .env se quiser).")


def get_user_by_id(uid: int) -> dict | None:
    with auth_connection() as conn:
        cur = auth_cursor(conn)
        cur.execute("SELECT * FROM plataforma_users WHERE id = %s", (uid,))
        r = cur.fetchone()
    return dict(r) if r else None


def get_user_tabs(uid: int) -> set[str]:
    with auth_connection() as conn:
        cur = auth_cursor(conn)
        cur.execute(
            "SELECT tab_id FROM plataforma_user_permissions WHERE user_id = %s",
            (uid,),
        )
        rows = cur.fetchall()
    return {r["tab_id"] for r in rows if r["tab_id"] in PERMISSION_IDS}


def _session_user() -> dict | None:
    uid = session.get(SESSION_USER_ID)
    if not uid:
        return None
    u = get_user_by_id(int(uid))
    if not u or not u.get("active"):
        session.pop(SESSION_USER_ID, None)
        session.pop(SESSION_VERSION, None)
        return None
    if session.get(SESSION_VERSION) != u.get("perms_version", 0):
        session[SESSION_VERSION] = u.get("perms_version", 0)
    return u


def current_user() -> dict | None:
    if not hasattr(g, "_plataforma_user"):
        g._plataforma_user = _session_user()
    return g._plataforma_user


def user_can_tab(tab: str) -> bool:
    u = current_user()
    if not u:
        return False
    if u.get("role") == "admin":
        return True
    if tab not in PERMISSION_IDS:
        return False
    return tab in get_user_tabs(int(u["id"]))


def _first_accessible_url_for_user(u: dict) -> str | None:
    """Primeira página que o colaborador pode abrir, por ordem do menu. Admin → início."""
    if u.get("role") == "admin":
        return url_for("index")
    if u.get("role") != "colaborador":
        return None
    uid = int(u["id"])
    tabs = get_user_tabs(uid)
    if not tabs:
        return None
    order: list[tuple[str, str]] = [
        ("index", "index"),
        ("conversas", "conversas"),
        ("memoria_calculo", "memoria_calculo"),
        ("pre_analise_processual", "pre_analise_processual_page"),
        ("levantamento_processual", "levantamento_processual_page"),
        ("tabela_juros", "tabela_juros_page"),
        ("proposta", "proposta_page"),
        ("campanha", "campanha_page"),
        ("auditoria_syscall", "auditoria_syscall_page"),
        ("localize", "localize_page"),
        ("outro_modulo", "embedded.index"),
    ]
    for tab, endpoint in order:
        if tab in tabs:
            return url_for(endpoint)
    if "eda" in tabs:
        return "/eda/"
    return None


def _tab_for_login_path(path_with_query: str) -> str | None:
    """Que permissão (aba) corresponde a um `next` após o login; None = não forçar troca (ex. /auth)."""
    s = (path_with_query or "").strip()
    if not s or not s.startswith("/") or s.startswith("//"):
        return "index"
    path = s.split("?", 1)[0].rstrip("/") or "/"
    if path == "/":
        return "index"
    if path.startswith("/conversas"):
        return "conversas"
    if path.startswith("/memoria-calculo"):
        return "memoria_calculo"
    if path.startswith("/pre-analise-processual"):
        return "pre_analise_processual"
    if path.startswith("/levantamento-processual"):
        return "levantamento_processual"
    if path.startswith("/tabela-juros"):
        return "tabela_juros"
    if path.startswith("/proposta"):
        return "proposta"
    if path.startswith("/embedded"):
        return "outro_modulo"
    if path.startswith("/campanha"):
        return "campanha"
    if path.startswith("/auditoria-syscall"):
        return "auditoria_syscall"
    if path.startswith("/localize"):
        return "localize"
    if path.startswith("/eda"):
        return "eda"
    if path.startswith("/auth"):
        return None
    return "index"


def _safe_post_login_url(nxt: str) -> str:
    """
    Garante que, após login, o colaborador não cai em `/` sem permissão Início
    (por exemplo, só com Memória de cálculo → envia para /memoria-calculo).
    """
    u = _session_user()
    if not u or u.get("role") == "admin":
        return nxt
    need = _tab_for_login_path(nxt)
    if need is not None and user_can_tab(need):
        return nxt
    if need is not None and not user_can_tab(need):
        alt = _first_accessible_url_for_user(u)
        if alt:
            return alt
    return nxt


def login_user(user_id: int) -> None:
    u = get_user_by_id(user_id)
    if not u or not u.get("active"):
        return
    session[SESSION_USER_ID] = int(u["id"])
    session[SESSION_VERSION] = int(u.get("perms_version", 0))
    session.permanent = True


def logout_user() -> None:
    session.pop(SESSION_USER_ID, None)
    session.pop(SESSION_VERSION, None)


def _endpoint_to_tab() -> str | None:
    ep = request.endpoint
    if not ep or ep == "static":
        return None
    if ep.startswith("auth."):
        return None
    m = {
        "index": "index",
        "conversas": "conversas",
        "memoria_calculo": "memoria_calculo",
        "pre_analise_processual_page": "pre_analise_processual",
        "api_pre_analise_iniciar": "pre_analise_processual",
        "api_pre_analise_casos": "pre_analise_processual",
        "api_pre_analise_status": "pre_analise_processual",
        "api_pre_analise_sincronizar": "pre_analise_processual",
        "api_pre_analise_health": "pre_analise_processual",
        "api_pre_analise_reconciliar": "pre_analise_processual",
        "api_pre_analise_por_externo": "pre_analise_processual",
        "api_pre_analise_cancelar": "pre_analise_processual",
        "api_pre_analise_excluir": "pre_analise_processual",
        "api_pre_analise_anexos": "pre_analise_processual",
        "api_pre_analise_anexo_download": "pre_analise_processual",
        "api_pre_analise_ficha_get": "pre_analise_processual",
        "api_pre_analise_ficha_save": "pre_analise_processual",
        "levantamento_processual_page": "levantamento_processual",
        "api_levantamento_health": "levantamento_processual",
        "api_levantamento_iniciar": "levantamento_processual",
        "api_levantamento_status": "levantamento_processual",
        "tabela_juros_page": "tabela_juros",
        "api_tabela_juros_calcular": "tabela_juros",
        "proposta_page": "proposta",
        "api_proposta_buscar": "proposta",
        "api_proposta_gerar_pdf": "proposta",
        "embedded.index": "outro_modulo",
        "get_summary": "index",
        "get_instances": "conversas",
        "get_conversations": "conversas",
        "get_messages": "conversas",
        "api_memoria_buscar": "memoria_calculo",
        "api_memoria_atualizar_calculo": "memoria_calculo",
        "api_memoria_atualizar_calculo_fila": "memoria_calculo",
        "api_memoria_salvar_numero_meses": "salvar_numero_meses",
        "api_memoria_analise_processual_start": "analise_processual",
        "api_memoria_analise_processual_status": "analise_processual",
        "api_memoria_precainfos_detalhes": "memoria_calculo",
        "api_memoria_controle_coleta_status": "memoria_calculo",
        "campanha_page": "campanha",
        "api_campanha_dominios": "campanha",
        "api_campanha_dominios_add": "campanha",
        "api_campanha_dominios_delete": "campanha",
        "api_campanha_dominios_verify": "campanha",
        "api_campanha_disparar": "campanha",
        "api_campanha_disparar_unico": "campanha",
        "api_campanha_status": "campanha",
        "api_campanha_cancelar": "campanha",
        "api_campanha_historico": "campanha",
        "api_campanha_historico_detalhe": "campanha",
        "api_campanha_destinatarios_preview": "campanha",
        "api_campanha_migrar_toml": "campanha",
        "api_campanha_sincronizar_mailgun": "campanha",
        "api_campanha_templates_list": "campanha",
        "api_campanha_templates_create": "campanha",
        "api_campanha_templates_get": "campanha",
        "api_campanha_templates_update": "campanha",
        "api_campanha_templates_delete": "campanha",
        "auditoria_syscall_page": "auditoria_syscall",
        "api_auditoria_syscall_linhas": "auditoria_syscall",
        "api_auditoria_syscall_detalhe": "auditoria_syscall",
        "localize_page": "localize",
        "api_localize_pesquisar": "localize",
    }
    t = m.get(ep)
    if t is not None:
        return t
    return "deny"


def is_public_request() -> bool:
    p = request.path
    if p.startswith("/static/"):
        return True
    ep = request.endpoint
    if ep == "auth.login":
        return True
    return False


def handle_access_denied(needs: str) -> Any:
    from flask import jsonify

    if request.path.startswith("/api/"):
        if needs == "ip_forbidden":
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": "Acesso negado para este IP. Contacte um administrador.",
                    }
                ),
                403,
            )
        if needs == "unauth":
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": "Não autenticado. Inicie sessão na plataforma.",
                    }
                ),
                401,
            )
        return (
            jsonify({"ok": False, "error": "Acesso negado a este recurso."}),
            403,
        )
    if needs in ("unauth", "forbidden", "login"):
        qs = request.query_string.decode() if request.query_string else ""
        nxt = request.path + (("?" + qs) if qs else "")
        return redirect(url_for("auth.login", next=nxt))
    if needs == "ip_forbidden":
        return (
            render_template(
                "acesso_negado.html",
                motivo="Acesso negado para este IP. Contacte um administrador.",
            ),
            403,
        )
    if needs in PERMISSION_IDS or needs in ("deny", "forbidden", "admin"):
        return (
            render_template("acesso_negado.html", motivo=needs),
            403,
        )
    return redirect(url_for("index"))


def plataforma_before_request() -> Any | None:
    init_db()
    _bootstrap_admin_if_empty()

    if is_public_request():
        return None

    u = _session_user()
    if not u:
        return handle_access_denied("unauth")
    if not u.get("active"):
        logout_user()
        return handle_access_denied("unauth")

    ep = request.endpoint
    if ep and ep.startswith("auth.") and ep != "auth.admin_usuarios":
        return None

    if u.get("role") == "colaborador" and not collaborator_ip_allowed(_current_remote_ip()):
        return handle_access_denied("ip_forbidden")

    if ep == "auth.admin_usuarios":
        if u.get("role") != "admin":
            return handle_access_denied("admin")
        return None

    needs = _endpoint_to_tab()
    if needs is None:
        return None
    if needs == "deny":
        return handle_access_denied("deny")

    from messages_viewer.page_maintenance import maintenance_block_for_tab

    blocked = maintenance_block_for_tab(needs, u)
    if blocked is not None:
        return blocked

    if u.get("role") == "admin":
        return None
    if not user_can_tab(needs):
        if needs == "auditoria_syscall" and user_can_tab("campanha"):
            return None
        # Só "Início" sem permissão: manda para a primeira aba que o colaborador tenha
        if request.endpoint == "index" and needs == "index":
            alt = _first_accessible_url_for_user(u)
            if alt:
                return redirect(alt)
        return handle_access_denied(needs)
    return None


def wsgi_eda_session_guard(app, inner_wsgi):
    from urllib.parse import quote
    from werkzeug.wrappers import Request, Response

    has_eda = app.config.get("HAS_EDIARIO", False)

    def application(environ, start_response):
        path = environ.get("PATH_INFO", "")
        if not (path == "/eda" or path.startswith("/eda/")):
            return inner_wsgi(environ, start_response)
        if not has_eda:
            return inner_wsgi(environ, start_response)
        u = None
        allowed = False
        ip_forbidden = False
        maint_html: str | None = None
        maint_status = 503
        with app.request_context(environ):
            u = _session_user()
            if u and u.get("active"):
                if u.get("role") == "colaborador" and not collaborator_ip_allowed(_current_remote_ip()):
                    ip_forbidden = True
                elif u.get("role") == "admin" or user_can_tab("eda"):
                    allowed = True
            if allowed:
                from messages_viewer.page_maintenance import maintenance_block_for_tab

                maint = maintenance_block_for_tab("eda", u)
                if maint is not None:
                    if isinstance(maint, tuple):
                        maint_html, maint_status = maint[0], int(maint[1])
                    else:
                        maint_html = maint.get_data(as_text=True)
                        maint_status = int(maint.status_code)
        if not u or not u.get("active"):
            req = Request(environ)
            nxt = req.path
            if req.query_string:
                nxt += "?" + req.query_string.decode("latin-1", "replace")
            loc = f"/auth/login?next={quote(nxt, safe='')}"
            r = Response(status=302, headers=[("Location", loc)])
            return r(environ, start_response)
        if ip_forbidden:
            r = Response(
                "Acesso negado para este IP. Contacte um administrador.",
                status=403,
                mimetype="text/html; charset=utf-8",
            )
            return r(environ, start_response)
        if maint_html is not None:
            r = Response(
                maint_html,
                status=maint_status,
                mimetype="text/html; charset=utf-8",
            )
            return r(environ, start_response)
        if allowed:
            return inner_wsgi(environ, start_response)
        r = Response(
            "Acesso negado ao EDA Diário. Peça a um administrador a permissão «EDA Diário».",
            status=403,
            mimetype="text/html; charset=utf-8",
        )
        return r(environ, start_response)

    return application


# --- Rotas de autenticação / admin


@auth_bp.route("/login", methods=["GET", "POST"])
def login():
    nxt0 = (request.args.get("next") or request.form.get("next") or "").strip()
    if not nxt0 or not nxt0.startswith("/") or nxt0.startswith("//"):
        nxt0 = url_for("index")
    if current_user() and request.method == "GET":
        return redirect(_safe_post_login_url(nxt0))
    err = None
    if request.method == "POST":
        name = (request.form.get("username") or "").strip()
        password = request.form.get("password") or ""
        nxt = (request.form.get("next") or "").strip() or nxt0
        if not nxt or not nxt.startswith("/") or nxt.startswith("//"):
            nxt = url_for("index")
        with auth_connection() as conn:
            cur = auth_cursor(conn)
            cur.execute(
                "SELECT * FROM plataforma_users WHERE username = %s AND active = 1",
                (name,),
            )
            r = cur.fetchone()
        if r and check_password_hash(r["password_hash"], password):
            login_user(int(r["id"]))
            return redirect(_safe_post_login_url(nxt))
        err = "Utilizador ou palavra-passe incorretos."
    n_users = 0
    with auth_connection() as conn:
        cur = auth_cursor(conn)
        cur.execute("SELECT COUNT(*) AS n FROM plataforma_users")
        n_users = int((cur.fetchone() or {}).get("n") or 0)
    return render_template("login.html", error=err, n_users=n_users, next_url=nxt0)


@auth_bp.route("/logout", methods=["GET", "POST"])
def logout():
    logout_user()
    return redirect(url_for("auth.login"))


@auth_bp.route("/admin/usuarios", methods=["GET", "POST"])
def admin_usuarios():
    u0 = _session_user()
    if not u0 or u0.get("role") != "admin":
        abort(403)
    err = None
    ok = None
    if request.method == "POST":
        act = (request.form.get("action") or "").strip()
        try:
            if act == "create":
                new_u = (request.form.get("new_username") or "").strip()
                new_p = request.form.get("new_password") or ""
                role = (request.form.get("new_role") or "colaborador").strip()
                if role not in ("admin", "colaborador"):
                    role = "colaborador"
                if len(new_u) < 2 or len(new_p) < 4:
                    err = "Utilizador (≥2 caracteres) e palavra-passe (≥4) obrigatórios."
                else:
                    with auth_connection() as conn:
                        cur = auth_cursor(conn)
                        try:
                            cur.execute(
                                "INSERT INTO plataforma_users (username, password_hash, role) VALUES (%s, %s, %s)",
                                (new_u, generate_password_hash(new_p), role),
                            )
                            new_id = int(cur.lastrowid or 0)
                            if role == "colaborador" and new_id:
                                for k, _, _ in PERMISSION_PANELS:
                                    if request.form.get(f"new_tab_{k}"):
                                        cur.execute(
                                            "INSERT INTO plataforma_user_permissions (user_id, tab_id) VALUES (%s, %s)",
                                            (new_id, k),
                                        )
                            conn.commit()
                            ok = f"Utilizador {new_u!r} criado."
                        except mysql.connector.errors.IntegrityError:
                            err = "Esse nome de utilizador já existe."
            elif act == "delete":
                del_id = int(request.form.get("user_id", 0))
                if del_id and del_id != int(u0["id"]):
                    with auth_connection() as conn:
                        cur = auth_cursor(conn)
                        cur.execute("DELETE FROM plataforma_users WHERE id = %s", (del_id,))
                        conn.commit()
                    ok = "Utilizador removido."
            elif act == "toggle":
                t_id = int(request.form.get("user_id", 0))
                if t_id and t_id != int(u0["id"]):
                    with auth_connection() as conn:
                        cur = auth_cursor(conn)
                        cur.execute(
                            "UPDATE plataforma_users SET active = 1 - active WHERE id = %s",
                            (t_id,),
                        )
                        conn.commit()
                    ok = "Estado actualizado."
            elif act == "set_password":
                sp_id = int(request.form.get("user_id", 0))
                sp = request.form.get("new_pass") or ""
                if sp_id and len(sp) >= 4:
                    with auth_connection() as conn:
                        cur = auth_cursor(conn)
                        cur.execute(
                            """
                            UPDATE plataforma_users
                            SET password_hash = %s, perms_version = perms_version + 1
                            WHERE id = %s
                            """,
                            (generate_password_hash(sp), sp_id),
                        )
                        conn.commit()
                    ok = "Palavra-passe actualizada."
            elif act == "save_perms":
                puid = int(request.form.get("user_id", 0))
                if puid and puid != int(u0["id"]):
                    with auth_connection() as conn:
                        cur = auth_cursor(conn)
                        cur.execute(
                            "DELETE FROM plataforma_user_permissions WHERE user_id = %s",
                            (puid,),
                        )
                        cur.execute(
                            "SELECT role FROM plataforma_users WHERE id = %s",
                            (puid,),
                        )
                        ro = cur.fetchone()
                        if ro and ro["role"] == "colaborador":
                            for k, _, _ in PERMISSION_PANELS:
                                if request.form.get(f"tab_{k}"):
                                    cur.execute(
                                        "INSERT INTO plataforma_user_permissions (user_id, tab_id) VALUES (%s, %s)",
                                        (puid, k),
                                    )
                        cur.execute(
                            "UPDATE plataforma_users SET perms_version = perms_version + 1 WHERE id = %s",
                            (puid,),
                        )
                        conn.commit()
                    ok = "Permissões guardadas."
            elif act == "save_maintenance":
                from messages_viewer.page_maintenance import save_maintenance_from_form

                save_maintenance_from_form(request.form)
                ok = "Estado de manutenção dos módulos guardado."
            elif act == "save_ip_restriction":
                save_collaborator_allowed_ips(
                    request.form.get("collaborator_allowed_ips") or ""
                )
                ok = "IPs permitidos para colaboradores guardados."
        except Exception as e:
            err = str(e)
            traceback.print_exc()

    users: list[dict] = []
    with auth_connection() as conn:
        cur = auth_cursor(conn)
        cur.execute("SELECT * FROM plataforma_users ORDER BY username")
        for r in cur.fetchall():
            d = dict(r)
            d["tabs"] = get_user_tabs(int(d["id"]))
            users.append(d)
    from messages_viewer.page_maintenance import list_maintenance_states

    return render_template(
        "admin_usuarios.html",
        users=users,
        panel_defs=PERMISSION_PANELS,
        maintenance_states=list_maintenance_states(),
        collaborator_allowed_ips=collaborator_allowed_ips_raw(),
        current_remote_ip=_current_remote_ip(),
        error=err,
        ok_message=ok,
    )


def inject_plataforma_template_globals():
    from messages_viewer.page_maintenance import is_tab_in_maintenance

    u = _session_user()
    return {
        "plataforma_user": u,
        "user_can": user_can_tab,
        "is_plataforma_admin": bool(u and u.get("role") == "admin"),
        "page_in_maintenance": is_tab_in_maintenance,
    }


def init_plataforma_auth(app) -> None:
    app.config.setdefault("SESSION_COOKIE_HTTPONLY", True)
    app.config.setdefault("SESSION_COOKIE_SAMESITE", "Lax")
    _sec = (os.getenv("SESSION_COOKIE_SECURE") or "").strip().lower()
    if _sec in ("1", "true", "yes", "on"):
        app.config["SESSION_COOKIE_SECURE"] = True
    init_db()
    _bootstrap_admin_if_empty()
    app.context_processor(inject_plataforma_template_globals)
