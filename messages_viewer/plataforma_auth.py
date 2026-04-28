# -*- coding: utf-8 -*-
"""
Autenticação da plataforma: SQLite, sessão Flask, admin e permissões por aba.
"""
from __future__ import annotations

import os
import sqlite3
import traceback
from pathlib import Path
from typing import Any

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

auth_bp = Blueprint("auth", __name__, template_folder="templates")

# Permissões por aba (colaborador): admin ignora a tabela e acede a tudo.
# Cada chave = uma aba/entrada no painel; o admin controla o que o colaborador vê.
TAB_KEYS: tuple[tuple[str, str], ...] = (
    ("index", "Início (resumo no menu lateral)"),
    ("conversas", "Conversas (WhatsApp no menu e APIs)"),
    (
        "outro_modulo",
        "2.º módulo (aba extra na página Conversas + rota /embedded/)",
    ),
    ("memoria_calculo", "Memória de cálculo (menu lateral)"),
    ("eda", "EDA Diário (link no menu, se o módulo estiver activo)"),
)

TAB_IDS = {k for k, _ in TAB_KEYS}
SESSION_USER_ID = "plataforma_uid"
SESSION_VERSION = "plataforma_ver"


def _project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _db_path() -> Path:
    raw = (os.getenv("AUTH_SQLITE_PATH") or "").strip()
    if raw:
        return Path(raw).expanduser().resolve()
    d = _project_root() / "instance"
    d.mkdir(exist_ok=True)
    return d / "plataforma_auth.db"


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(str(_db_path()), timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def init_db() -> None:
    p = _db_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    with _connect() as c:
        c.executescript(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL COLLATE NOCASE UNIQUE,
                password_hash TEXT NOT NULL,
                role TEXT NOT NULL CHECK (role IN ('admin', 'colaborador')),
                active INTEGER NOT NULL DEFAULT 1,
                perms_version INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );
            CREATE TABLE IF NOT EXISTS user_permissions (
                user_id INTEGER NOT NULL,
                tab_id TEXT NOT NULL,
                PRIMARY KEY (user_id, tab_id),
                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
            );
            """
        )
        c.commit()


def _bootstrap_admin_if_empty() -> None:
    u = (os.getenv("AUTH_BOOTSTRAP_USER") or "").strip()
    p = (os.getenv("AUTH_BOOTSTRAP_PASSWORD") or "").strip()
    if not u or not p:
        return
    with _connect() as c:
        n = c.execute("SELECT COUNT(*) AS n FROM users").fetchone()["n"]
        if n:
            return
        c.execute(
            "INSERT INTO users (username, password_hash, role) VALUES (?, ?, 'admin')",
            (u, generate_password_hash(p)),
        )
        c.commit()
        print(f"[plataforma_auth] Utilizador admin inicial criado: {u!r} (defina outro e remova a variável do .env se quiser).")


def get_user_by_id(uid: int) -> dict | None:
    with _connect() as c:
        r = c.execute("SELECT * FROM users WHERE id = ?", (uid,)).fetchone()
    return dict(r) if r else None


def get_user_tabs(uid: int) -> set[str]:
    with _connect() as c:
        rows = c.execute(
            "SELECT tab_id FROM user_permissions WHERE user_id = ?", (uid,)
        ).fetchall()
    return {r["tab_id"] for r in rows if r["tab_id"] in TAB_IDS}


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
    if tab not in TAB_IDS:
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
    if path.startswith("/embedded"):
        return "outro_modulo"
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
        "embedded.index": "outro_modulo",
        "get_summary": "index",
        "get_instances": "conversas",
        "get_conversations": "conversas",
        "get_messages": "conversas",
        "api_memoria_buscar": "memoria_calculo",
        "api_memoria_atualizar_calculo": "memoria_calculo",
        "api_memoria_controle_coleta_status": "memoria_calculo",
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
    if needs in TAB_IDS or needs in ("deny", "forbidden", "admin"):
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
    if ep == "auth.admin_usuarios":
        if u.get("role") != "admin":
            return handle_access_denied("admin")
        return None

    needs = _endpoint_to_tab()
    if needs is None:
        return None
    if needs == "deny":
        return handle_access_denied("deny")
    if u.get("role") == "admin":
        return None
    if not user_can_tab(needs):
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
        with app.request_context(environ):
            u = _session_user()
            if u and u.get("active") and (
                u.get("role") == "admin" or user_can_tab("eda")
            ):
                allowed = True
        if not u or not u.get("active"):
            req = Request(environ)
            nxt = req.path
            if req.query_string:
                nxt += "?" + req.query_string.decode("latin-1", "replace")
            loc = f"/auth/login?next={quote(nxt, safe='')}"
            r = Response(status=302, headers=[("Location", loc)])
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
        with _connect() as c:
            r = c.execute(
                "SELECT * FROM users WHERE username = ? AND active = 1", (name,)
            ).fetchone()
        if r and check_password_hash(r["password_hash"], password):
            login_user(int(r["id"]))
            return redirect(_safe_post_login_url(nxt))
        err = "Utilizador ou palavra-passe incorretos."
    n_users = 0
    with _connect() as c:
        n_users = c.execute("SELECT COUNT(*) AS n FROM users").fetchone()["n"]
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
                    with _connect() as c:
                        try:
                            ins = c.execute(
                                "INSERT INTO users (username, password_hash, role) VALUES (?, ?, ?)",
                                (new_u, generate_password_hash(new_p), role),
                            )
                            new_id = ins.lastrowid
                            if role == "colaborador" and new_id:
                                for k, _ in TAB_KEYS:
                                    if request.form.get(f"new_tab_{k}"):
                                        c.execute(
                                            "INSERT INTO user_permissions (user_id, tab_id) VALUES (?, ?)",
                                            (new_id, k),
                                        )
                            c.commit()
                            ok = f"Utilizador {new_u!r} criado."
                        except sqlite3.IntegrityError:
                            err = "Esse nome de utilizador já existe."
            elif act == "delete":
                del_id = int(request.form.get("user_id", 0))
                if del_id and del_id != int(u0["id"]):
                    with _connect() as c:
                        c.execute("DELETE FROM users WHERE id = ?", (del_id,))
                        c.commit()
                    ok = "Utilizador removido."
            elif act == "toggle":
                t_id = int(request.form.get("user_id", 0))
                if t_id and t_id != int(u0["id"]):
                    with _connect() as c:
                        c.execute(
                            "UPDATE users SET active = 1 - active WHERE id = ?", (t_id,)
                        )
                        c.commit()
                    ok = "Estado actualizado."
            elif act == "set_password":
                sp_id = int(request.form.get("user_id", 0))
                sp = request.form.get("new_pass") or ""
                if sp_id and len(sp) >= 4:
                    with _connect() as c:
                        c.execute(
                            "UPDATE users SET password_hash = ?, perms_version = perms_version + 1 WHERE id = ?",
                            (generate_password_hash(sp), sp_id),
                        )
                        c.commit()
                    ok = "Palavra-passe actualizada."
            elif act == "save_perms":
                puid = int(request.form.get("user_id", 0))
                if puid and puid != int(u0["id"]):
                    with _connect() as c:
                        c.execute("DELETE FROM user_permissions WHERE user_id = ?", (puid,))
                        ro = c.execute(
                            "SELECT role FROM users WHERE id = ?", (puid,)
                        ).fetchone()
                        if ro and ro["role"] == "colaborador":
                            for k, _ in TAB_KEYS:
                                if request.form.get(f"tab_{k}"):
                                    c.execute(
                                        "INSERT INTO user_permissions (user_id, tab_id) VALUES (?, ?)",
                                        (puid, k),
                                    )
                        c.execute(
                            "UPDATE users SET perms_version = perms_version + 1 WHERE id = ?",
                            (puid,),
                        )
                        c.commit()
                    ok = "Permissões guardadas."
        except Exception as e:
            err = str(e)
            traceback.print_exc()

    users: list[dict] = []
    with _connect() as c:
        rows = c.execute("SELECT * FROM users ORDER BY username").fetchall()
        for r in rows:
            d = dict(r)
            d["tabs"] = get_user_tabs(int(d["id"]))
            users.append(d)
    return render_template(
        "admin_usuarios.html",
        users=users,
        tab_defs=TAB_KEYS,
        error=err,
        ok_message=ok,
    )


def inject_plataforma_template_globals():
    u = _session_user()
    return {
        "plataforma_user": u,
        "user_can": user_can_tab,
        "is_plataforma_admin": bool(u and u.get("role") == "admin"),
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
