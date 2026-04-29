import json
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from time import perf_counter
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import mysql.connector
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from flask import Flask, jsonify, render_template, request, url_for

# cd "c:\Users\justi\OneDrive\Documentos\Python Projects\PycharmProjects\View_Message"
# python app.py

load_dotenv()

_PROJECT_ROOT = Path(__file__).resolve().parent
_MV = _PROJECT_ROOT / "messages_viewer"

app = Flask(
    __name__,
    template_folder=str(_MV / "templates"),
    static_folder=str(_MV / "static"),
    static_url_path="/static",
)
app.secret_key = (os.getenv("FLASK_SECRET_KEY") or "").strip() or "dev-unsafe-defina-FLASK_SECRET_KEY-no-.env"
app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(days=14)


def _safe_embed_url(raw: str) -> str | None:
    """Permite só http(s) para iframe; evita javascript: e dados malformados."""
    url = (raw or "").strip()
    if not url:
        return None
    if not re.match(r"^https?://", url, re.IGNORECASE):
        return None
    if re.search(r'[\s<>"\']', url):
        return None
    return url


_HAS_EMBEDDED = False
try:
    from messages_viewer.embedded.blueprint import embedded_bp

    app.register_blueprint(embedded_bp)
    _HAS_EMBEDDED = True
except ImportError:
    pass

from messages_viewer.plataforma_auth import (
    auth_bp,
    init_plataforma_auth,
    plataforma_before_request,
    wsgi_eda_session_guard,
)

app.register_blueprint(auth_bp, url_prefix="/auth")
init_plataforma_auth(app)


@app.before_request
def _plataforma_auth_guard():
    return plataforma_before_request()


@app.context_processor
def inject_platform():
    env_url = _safe_embed_url(os.getenv("SECOND_APP_URL", ""))
    has_edi = bool(app.config.get("HAS_EDIARIO"))
    internal = None
    if _HAS_EMBEDDED:
        try:
            internal = url_for("embedded.index")
        except Exception:
            internal = None
    if env_url:
        second_src = env_url
    else:
        second_src = internal
    custom_label = (os.getenv("SECOND_TAB_LABEL") or "").strip()
    if custom_label:
        second_label = custom_label
    else:
        second_label = "Outro módulo"
    return {
        "app_title": (os.getenv("APP_TITLE") or "Plataforma").strip() or "Plataforma",
        "second_tab_label": second_label,
        "second_iframe_src": second_src,
        "has_ediario": has_edi,
    }


DB_CONFIG = {
    "host":     os.getenv("DB_HOST",     "209.38.154.187"),
    "port":     int(os.getenv("DB_PORT", 5432)),
    "database": os.getenv("DB_NAME",     "evolution"),
    "user":     os.getenv("DB_USER",     "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD") or os.getenv("DB_PASSWORD") or "",
}

NORM_JID = """
    CASE
        WHEN key->>'remoteJid'    LIKE '%%@s.whatsapp.net' THEN key->>'remoteJid'
        WHEN key->>'remoteJidAlt' LIKE '%%@s.whatsapp.net' THEN key->>'remoteJidAlt'
        ELSE key->>'remoteJid'
    END
"""


def get_db():
    return psycopg2.connect(**DB_CONFIG)


def _memoria_mysql_config() -> dict | None:
    """
    Conexão MySQL só para a tabela `memoria_calculo`.
    Sem MEMORIA_MYSQL_DATABASE = desativado.

    Variáveis de ambiente: MEMORIA_MYSQL_HOST, MEMORIA_MYSQL_PORT, MEMORIA_MYSQL_DATABASE,
    MEMORIA_MYSQL_USER, MEMORIA_MYSQL_PASSWORD, MEMORIA_MYSQL_CONNECT_TIMEOUT (segundos; padrão 1200),
    MEMORIA_MYSQL_ULTIMA_ATUALIZACAO_COL (opcional: nome da coluna de data da última actualização; senão autodetecta).
    Credenciais no .env (nunca no código; use aspas se a password tiver # ou !).
    """
    name = (os.getenv("MEMORIA_MYSQL_DATABASE") or "").strip()
    if not name:
        return None
    # Timeout de conexão: evita "loading infinito" quando o MySQL está inacessível.
    # Pode ajustar via env, mas limitamos para não prender requests por minutos.
    raw_to = (os.getenv("MEMORIA_MYSQL_CONNECT_TIMEOUT") or "10").strip()
    try:
        connect_timeout = int(raw_to)
    except ValueError:
        connect_timeout = 10
    if connect_timeout < 1:
        connect_timeout = 10
    if connect_timeout > 30:
        connect_timeout = 30
    # mysql-connector-python: use `connection_timeout` (não `connect_timeout`).
    return {
        "host": (os.getenv("MEMORIA_MYSQL_HOST") or "127.0.0.1").strip(),
        "port": int(os.getenv("MEMORIA_MYSQL_PORT", "3306")),
        "database": name,
        "user": (os.getenv("MEMORIA_MYSQL_USER") or "root").strip(),
        "password": os.getenv("MEMORIA_MYSQL_PASSWORD", "") or "",
        "connection_timeout": connect_timeout,
    }


def _memoria_calculo_ultima_atualizacao_field(cur) -> str | None:
    """
    Nome da coluna de data/hora da última actualização em `memoria_calculo`.
    Pode forçar com MEMORIA_MYSQL_ULTIMA_ATUALIZACAO_COL; senão tenta nomes comuns
    (data_ultima_atualizacao, ultima_atualizacao, data_atualizacao, updated_at, …).
    """
    cur.execute("SHOW COLUMNS FROM `memoria_calculo`")
    raw = cur.fetchall()
    # Com cursor em modo dicionário, SHOW COLUMNS devolve {'Field': 'nome', 'Type': ...},
    # não tuplas indexadas por 0.
    fields = set()
    for row in raw:
        if isinstance(row, dict):
            name = row.get("Field") or row.get("field")
            if name is not None:
                fields.add(str(name))
        else:
            fields.add(str(row[0]))
    env = (os.getenv("MEMORIA_MYSQL_ULTIMA_ATUALIZACAO_COL") or "").strip()
    if env and re.match(r"^[a-zA-Z0-9_]+$", env) and env in fields:
        return env
    prefer = (
        "data_ultima_atualizacao",
        "ultima_atualizacao",
        "data_atualizacao",
        "updated_at",
        "atualizado_em",
        "data_modificacao",
    )
    for p in prefer:
        if p in fields:
            return p
    return None


def _memoria_row_to_api(row: dict) -> dict:
    from datetime import date, datetime
    from decimal import Decimal

    out: dict = {}
    for k, v in row.items():
        if v is None:
            out[k] = None
        elif isinstance(v, Decimal):
            out[k] = float(v)
        elif isinstance(v, (datetime, date)):
            out[k] = v.isoformat()
        else:
            out[k] = v
    return out


def _flask_mysql_config() -> dict | None:
    """
    Conexão MySQL do banco da plataforma (ex.: flaskdb), onde existem tabelas como
    `precainfosnew` e `controle_coleta_TJSP`.

    Env:
    - FLASK_MYSQL_HOST / FLASK_MYSQL_PORT / FLASK_MYSQL_DATABASE / FLASK_MYSQL_USER / FLASK_MYSQL_PASSWORD
    - (opcional) FLASK_MYSQL_CONNECT_TIMEOUT
    """
    name = (os.getenv("FLASK_MYSQL_DATABASE") or "").strip().strip("'\"")
    if not name:
        return None
    host = (os.getenv("FLASK_MYSQL_HOST") or "127.0.0.1").strip().strip("'\"")
    user = (os.getenv("FLASK_MYSQL_USER") or "root").strip().strip("'\"")
    password = (os.getenv("FLASK_MYSQL_PASSWORD") or "").strip().strip("'\"")
    try:
        port = int(str(os.getenv("FLASK_MYSQL_PORT") or "3306").strip())
    except ValueError:
        port = 3306
    raw_to = (os.getenv("FLASK_MYSQL_CONNECT_TIMEOUT") or "10").strip()
    try:
        connection_timeout = int(raw_to)
    except ValueError:
        connection_timeout = 10
    if connection_timeout < 1:
        connection_timeout = 10
    if connection_timeout > 30:
        connection_timeout = 30
    return {
        "host": host,
        "port": port,
        "database": name,
        "user": user,
        "password": password,
        "connection_timeout": connection_timeout,
    }


def _pick_field(fields: set[str], *candidates: str) -> str | None:
    """Escolhe o 1º campo existente (case-insensitive)."""
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


def _bloquear_calculo_mes_atual_enabled() -> bool:
    """
    Flag simples para testes.

    - BLOQUEAR_CALCULO=1/true/on/sim  -> activa bloqueio do mês actual
    - BLOQUEAR_CALCULO=0/false/off/nao -> desactiva
    Padrão: activo (1).
    """
    raw = (os.getenv("BLOQUEAR_CALCULO") or "1").strip().lower()
    return raw not in ("0", "false", "off", "no", "nao", "não", "disabled")


def _memoria_calculo_bloqueado_mes_atual(prec_id: int) -> tuple[bool, str | None]:
    """
    Bloqueio de segurança: se ``memoria_calculo`` já tiver sido actualizada no mês actual
    para este ``id_precainfosnew``, evita rodar a automação novamente (cliques repetidos).

    Returns
    -------
    (blocked, ultima_iso)
        ``blocked=True`` quando a data de última actualização existe e é do mesmo mês/ano
        do relógio do servidor; ``ultima_iso`` é string ISO para mensagem.
    """
    from datetime import datetime

    if not _bloquear_calculo_mes_atual_enabled():
        return False, None

    cfg = _memoria_mysql_config()
    if not cfg:
        return False, None
    try:
        conn = mysql.connector.connect(**cfg)
        cur = conn.cursor(dictionary=True)
        ultima_f = _memoria_calculo_ultima_atualizacao_field(cur)
        if not ultima_f:
            return False, None
        cur.execute(
            f"SELECT `{ultima_f}` AS ultima_atualizacao FROM memoria_calculo WHERE id_precainfosnew = %s LIMIT 1",
            (int(prec_id),),
        )
        row = cur.fetchone() or {}
        ultima = row.get("ultima_atualizacao")
        if not ultima:
            return False, None
        # mysql-connector pode devolver datetime ou string
        if isinstance(ultima, str):
            try:
                ultima_dt = datetime.fromisoformat(ultima.replace("Z", "+00:00"))
            except ValueError:
                return False, ultima
        else:
            ultima_dt = ultima
        now = datetime.now()
        blocked = (ultima_dt.year == now.year) and (ultima_dt.month == now.month)
        try:
            ultima_iso = ultima_dt.isoformat()
        except Exception:
            ultima_iso = str(ultima_dt)
        return blocked, ultima_iso
    except Exception:
        return False, None
    finally:
        try:
            cur.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass


@app.route("/")
def index():
    return render_template("capa.html")


@app.route("/conversas")
def conversas():
    return render_template("conversas.html")


@app.route("/memoria-calculo")
def memoria_calculo():
    return render_template(
        "memoria_calculo.html",
        memoria_mysql_configured=bool((os.getenv("MEMORIA_MYSQL_DATABASE") or "").strip()),
        calculo_atualizacao_configured=bool(
            (os.getenv("CALCULO_ATUALIZACAO_API_URL") or "").strip()
        ),
        bloquear_calculo_mes_atual=_bloquear_calculo_mes_atual_enabled(),
    )


@app.route("/api/memoria-calculo/buscar")
def api_memoria_buscar():
    """
    Dois modos (exclusivos):
    - Nome: LIKE no requerente (mín. 2 caracteres; bind + escape de % e _).
    - Processo + incidente: igualdade em TRIM de numero_de_processo e
      TRIM de numero_do_incidente (incidente vazio = sem incidente na BD).
    Não combinar parâmetros dos dois modos.
    """
    nome = (request.args.get("nome") or request.args.get("q") or "").strip()
    proc = (request.args.get("numero_de_processo") or request.args.get("processo") or "").strip()
    inc = (request.args.get("numero_do_incidente") or request.args.get("incidente") or "").strip()

    if (proc or inc) and nome:
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "Use ou a pesquisa por nome, ou por processo e incidente — não ambas.",
                }
            ),
            400,
        )
    if inc and not proc:
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "Para pesquisar por incidente, indique também o nº do processo.",
                }
            ),
            400,
        )
    if proc:
        use_process = True
    elif len(nome) >= 2:
        use_process = False
    else:
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "Digite ao menos 2 caracteres no nome, ou o nº do processo (e o incidente, se houver).",
                }
            ),
            400,
        )

    cfg = _memoria_mysql_config()
    if not cfg:
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "Banco de memória não configurado. Defina MEMORIA_MYSQL_DATABASE (e, se necessário, MEMORIA_MYSQL_HOST, etc.) no .env.",
                }
            ),
            503,
        )

    def esc_like(s: str) -> str:
        return s.replace("\\", "\\\\").replace("%", r"\%").replace("_", r"\_")

    if use_process:
        where_sql = """
        TRIM(COALESCE(numero_de_processo, '')) = %s
        AND TRIM(COALESCE(numero_do_incidente, '')) = %s
        """
        params_exec: list = [proc, inc]
        order_by = """
            requerente ASC,
            COALESCE(numero_de_processo, '') ASC,
            COALESCE(numero_do_incidente, '') ASC,
            id ASC
        """
    else:
        like_pat = f"%{esc_like(nome)}%"
        where_sql = """
        requerente IS NOT NULL
        AND TRIM(requerente) <> ''
        AND requerente LIKE %s
        """
        params_exec = [like_pat]
        order_by = """
            requerente ASC,
            COALESCE(numero_de_processo, '') ASC,
            COALESCE(numero_do_incidente, '') ASC,
            id ASC
        """

    t0 = perf_counter()
    try:
        conn = mysql.connector.connect(
            **cfg, charset="utf8mb4", collation="utf8mb4_unicode_ci"
        )
        cur = conn.cursor(dictionary=True)
        ultima_f = _memoria_calculo_ultima_atualizacao_field(cur)
        if ultima_f:
            ultima_sql = f", `{ultima_f}` AS ultima_atualizacao"
        else:
            ultima_sql = ", NULL AS ultima_atualizacao"
        sql = f"""
        SELECT
            id,
            id_precainfosnew,
            requerente,
            numero_de_processo,
            numero_do_incidente,
            principal_bruto,
            juros,
            desc_saude_prev,
            desc_ir,
            percentual_honorarios,
            total_bruto,
            reserva_honorarios,
            total_liquido
            {ultima_sql}
        FROM memoria_calculo
        WHERE {where_sql}
        ORDER BY
            {order_by}
    """
        cur.execute(sql, tuple(params_exec))
        raw = cur.fetchall()
        cur.close()
        conn.close()
    except mysql.connector.Error as e:
        dt_ms = int((perf_counter() - t0) * 1000)
        host = cfg.get("host")
        port = cfg.get("port")
        print(f"[memoria-calculo] erro em {dt_ms}ms ({host}:{port}): {e}")
        return (
            jsonify(
                {
                    "ok": False,
                    "error": f"Erro ao consultar o MySQL ({host}:{port}): {e}",
                }
            ),
            500,
        )
    dt_ms = int((perf_counter() - t0) * 1000)
    print(
        f"[memoria-calculo] ok em {dt_ms}ms; results={len(raw)}; modo={'processo' if use_process else 'nome'}"
    )
    results = [_memoria_row_to_api(r) for r in raw]
    if results:
        return jsonify(
            {
                "ok": True,
                "modo": "processo" if use_process else "nome",
                "source": "memoria_calculo",
                "results": results,
            }
        )

    # Fallback: se não houver memória, procura no precainfosnew (flaskdb).
    # Se achou (principalmente por processo/incidente), o front pode auto-rodar o cálculo.
    fcfg = _flask_mysql_config()
    if not fcfg:
        return jsonify(
            {
                "ok": True,
                "modo": "processo" if use_process else "nome",
                "source": "memoria_calculo",
                "results": [],
            }
        )

    try:
        conn2 = mysql.connector.connect(
            **fcfg, charset="utf8mb4", collation="utf8mb4_unicode_ci"
        )
        cur2 = conn2.cursor(dictionary=True)
        cur2.execute("SHOW TABLES LIKE 'precainfosnew'")
        if not cur2.fetchone():
            return jsonify(
                {
                    "ok": True,
                    "modo": "processo" if use_process else "nome",
                    "source": "precainfosnew",
                    "results": [],
                }
            )

        cur2.execute("SHOW COLUMNS FROM `precainfosnew`")
        raw_cols = cur2.fetchall() or []
        fields: set[str] = set()
        for r in raw_cols:
            if isinstance(r, dict):
                nm = r.get("Field") or r.get("field")
                if nm:
                    fields.add(str(nm))
            else:
                fields.add(str(r[0]))

        f_req = _pick_field(fields, "requerente", "Requerente")
        f_proc = _pick_field(fields, "numero_de_processo", "Numero_de_processo", "processo", "Processo")
        f_inc = _pick_field(
            fields,
            "numero_do_incidente",
            "Numero_do_incidente",
            "numero_de_incidente",
            "Numero_de_incidente",
            "incidente",
            "Incidente",
        )
        if not f_proc:
            return jsonify(
                {
                    "ok": True,
                    "modo": "processo" if use_process else "nome",
                    "source": "precainfosnew",
                    "results": [],
                }
            )

        if use_process:
            if f_inc:
                cur2.execute(
                    f"""
                    SELECT id,
                           {f"`{f_req}` AS requerente" if f_req else "NULL AS requerente"},
                           `{f_proc}` AS numero_de_processo,
                           `{f_inc}` AS numero_do_incidente
                    FROM precainfosnew
                    WHERE TRIM(COALESCE(`{f_proc}`, '')) = %s
                      AND TRIM(COALESCE(`{f_inc}`, '')) = %s
                    ORDER BY id DESC
                    LIMIT 10
                    """,
                    (proc, inc),
                )
            else:
                cur2.execute(
                    f"""
                    SELECT id,
                           {f"`{f_req}` AS requerente" if f_req else "NULL AS requerente"},
                           `{f_proc}` AS numero_de_processo,
                           NULL AS numero_do_incidente
                    FROM precainfosnew
                    WHERE TRIM(COALESCE(`{f_proc}`, '')) = %s
                    ORDER BY id DESC
                    LIMIT 10
                    """,
                    (proc,),
                )
        else:
            # Para pesquisa por nome, devolvemos resultados mas NÃO auto-rodamos cálculo
            # (pode haver muitos casos).
            if not f_req:
                return jsonify(
                    {
                        "ok": True,
                        "modo": "nome",
                        "source": "precainfosnew",
                        "results": [],
                    }
                )
            like_pat = f"%{esc_like(nome)}%"
            cur2.execute(
                f"""
                SELECT id,
                       `{f_req}` AS requerente,
                       {f"`{f_proc}` AS numero_de_processo" if f_proc else "NULL AS numero_de_processo"},
                       {f"`{f_inc}` AS numero_do_incidente" if f_inc else "NULL AS numero_do_incidente"}
                FROM precainfosnew
                WHERE `{f_req}` IS NOT NULL
                  AND TRIM(`{f_req}`) <> ''
                  AND `{f_req}` LIKE %s
                ORDER BY `{f_req}` ASC, id DESC
                LIMIT 10
                """,
                (like_pat,),
            )

        rows = cur2.fetchall() or []
        out_rows: list[dict] = []
        for r in rows:
            try:
                pid = int(r.get("id")) if r.get("id") is not None else None
            except (TypeError, ValueError):
                pid = None
            out_rows.append(
                {
                    "id": None,
                    "id_precainfosnew": pid,
                    "requerente": r.get("requerente"),
                    "numero_de_processo": r.get("numero_de_processo"),
                    "numero_do_incidente": r.get("numero_do_incidente"),
                    "principal_bruto": 0,
                    "juros": 0,
                    "desc_saude_prev": 0,
                    "desc_ir": 0,
                    "percentual_honorarios": 30,
                    "total_bruto": 0,
                    "reserva_honorarios": 0,
                    "total_liquido": 0,
                    "ultima_atualizacao": None,
                    "source": "precainfosnew",
                    # auto-run apenas quando der match único e houver id válido
                    "auto_update": bool((len(rows) == 1) and (pid is not None)),
                }
            )
        return jsonify(
            {
                "ok": True,
                "modo": "processo" if use_process else "nome",
                "source": "precainfosnew",
                "results": out_rows,
            }
        )
    except mysql.connector.Error as e:
        print(f"[memoria-calculo] fallback precainfosnew falhou: {e}")
        return jsonify(
            {
                "ok": True,
                "modo": "processo" if use_process else "nome",
                "source": "memoria_calculo",
                "results": [],
            }
        )
    finally:
        try:
            cur2.close()
        except Exception:
            pass
        try:
            conn2.close()
        except Exception:
            pass


@app.route("/api/memoria-calculo/atualizar-calculo", methods=["POST"])
def api_memoria_atualizar_calculo():
    """
    Encaminha para a API interna (systemd) que executa a automação de cálculo
    (planilha, precainfosnew, memoria_calculo) — ver ``api_atualizacao_calculo.py`` e
    ``CALCULO_ATUALIZACAO_API_URL`` no ``.env``.
    """
    data = request.get_json(silent=True) or {}
    pid = data.get("id_precainfosnew")
    if pid is None and data.get("id") is not None:
        pid = data.get("id")
    if pid is None:
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "Parâmetro obrigatório: id_precainfosnew (ou id) — id em precainfosnew.",
                }
            ),
            400,
        )
    try:
        prec_id = int(pid)
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "id_precainfosnew inválido."}), 400

    blocked, ultima_iso = _memoria_calculo_bloqueado_mes_atual(prec_id)
    if blocked:
        return (
            jsonify(
                {
                    "ok": False,
                    "error": (
                        "Este cálculo já foi actualizado no mês actual. "
                        + (f"Última actualização: {ultima_iso}." if ultima_iso else "")
                    ),
                }
            ),
            409,
        )

    base = (os.getenv("CALCULO_ATUALIZACAO_API_URL") or "").strip()
    if not base:
        return (
            jsonify(
                {
                    "ok": False,
                    "error": (
                        "API de cálculo não configurada. Defina CALCULO_ATUALIZACAO_API_URL no .env da plataforma "
                        "(ex.: http://127.0.0.1:5099) e o serviço interno (systemd)."
                    ),
                }
            ),
            503,
        )
    try:
        timeout = int((os.getenv("CALCULO_ATUALIZACAO_API_TIMEOUT") or "600").strip())
    except ValueError:
        timeout = 600
    if timeout < 1:
        timeout = 600

    url = base.rstrip("/") + "/atualizar"
    body = json.dumps({"id_precainfosnew": prec_id}).encode("utf-8")
    headers = {
        "Content-Type": "application/json; charset=utf-8",
        "Accept": "application/json",
    }
    key = (os.getenv("CALCULO_ATUALIZACAO_API_KEY") or "").strip()
    if key:
        headers["X-API-Key"] = key
    req = Request(url, data=body, method="POST", headers=headers)
    try:
        with urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
            code = resp.getcode()
    except HTTPError as e:
        raw = e.read()
        code = e.code
    except URLError as e:
        return (
            jsonify(
                {
                    "ok": False,
                    "error": f"Não foi possível contactar a API de cálculo ({url}): {e}",
                }
            ),
            502,
        )
    try:
        out = json.loads(raw.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "Resposta inválida da API de cálculo (JSON esperado).",
                }
            ),
            502,
        )
    if not isinstance(out, dict):
        return (
            jsonify({"ok": False, "error": "Resposta inválida da API de cálculo."}),
            502,
        )
    if code in (200, 400, 403, 409, 500):
        return jsonify(out), code
    return jsonify(out), 502


@app.route("/api/memoria-calculo/controle-coleta-status")
def api_memoria_controle_coleta_status():
    """
    Consulta `controle_coleta_TJSP` no MySQL do flaskdb (FLASK_MYSQL_*),
    por `numero_de_processo` e `numero_do_incidente`, retornando o `status`
    (quando existir).
    """
    proc = (request.args.get("numero_de_processo") or request.args.get("processo") or "").strip()
    inc = (request.args.get("numero_do_incidente") or request.args.get("incidente") or "").strip()
    if not proc:
        return jsonify({"ok": False, "error": "Obrigatório: numero_de_processo."}), 400

    cfg = _flask_mysql_config()
    if not cfg:
        return (
            jsonify({"ok": False, "error": "MySQL do flaskdb não configurado (FLASK_MYSQL_*)."}),
            503,
        )
    try:
        conn = mysql.connector.connect(**cfg, charset="utf8mb4", collation="utf8mb4_unicode_ci")
        cur = conn.cursor(dictionary=True)
        cur.execute("SHOW TABLES LIKE 'controle_coleta_TJSP'")
        if not cur.fetchone():
            return jsonify({"ok": True, "found": False, "status": None, "error": None})

        cur.execute("SHOW COLUMNS FROM `controle_coleta_TJSP`")
        raw_cols = cur.fetchall() or []
        fields: set[str] = set()
        for r in raw_cols:
            if isinstance(r, dict):
                nm = r.get("Field") or r.get("field")
                if nm:
                    fields.add(str(nm))
            else:
                fields.add(str(r[0]))

        f_proc = _pick_field(fields, "numero_de_processo", "Numero_de_processo", "processo", "Processo")
        f_inc = _pick_field(
            fields,
            "numero_do_incidente",
            "Numero_do_incidente",
            "numero_de_incidente",
            "Numero_de_incidente",
            "incidente",
            "Incidente",
        )
        f_status = _pick_field(fields, "status", "Status", "situacao", "Situacao")
        if not f_proc or not f_status:
            return jsonify({"ok": True, "found": False, "status": None, "error": None})

        if f_inc:
            cur.execute(
                f"""
                SELECT `{f_status}` AS status
                FROM controle_coleta_TJSP
                WHERE TRIM(COALESCE(`{f_proc}`, '')) = %s
                  AND TRIM(COALESCE(`{f_inc}`, '')) = %s
                ORDER BY 1 DESC
                LIMIT 1
                """,
                (proc, inc),
            )
        else:
            cur.execute(
                f"""
                SELECT `{f_status}` AS status
                FROM controle_coleta_TJSP
                WHERE TRIM(COALESCE(`{f_proc}`, '')) = %s
                ORDER BY 1 DESC
                LIMIT 1
                """,
                (proc,),
            )
        row = cur.fetchone() or {}
        status = row.get("status")
        return jsonify({"ok": True, "found": bool(status is not None), "status": status})
    except mysql.connector.Error as e:
        return jsonify({"ok": False, "error": str(e)}), 500
    finally:
        try:
            cur.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass


# ── dashboard ────────────────────────────────────────────────────────────────
@app.route("/api/summary")
def get_summary():
    """Métricas agregadas para a capa (um pedido, inclui lista de instâncias)."""
    try:
        conn = get_db()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            f"""
            WITH norm AS (
                SELECT
                    m."instanceId" AS iid,
                    {NORM_JID} AS contact_jid
                FROM "Message" m
            )
            SELECT
                (SELECT COALESCE(COUNT(*)::bigint, 0) FROM "Message") AS total_messages,
                (SELECT COALESCE(COUNT(*)::bigint, 0) FROM "Instance") AS total_instances,
                (SELECT COALESCE(COUNT(*)::bigint, 0) FROM "Instance" WHERE "connectionStatus" = 'open') AS online_instances,
                (SELECT COALESCE(COUNT(*)::bigint, 0) FROM (SELECT DISTINCT iid, contact_jid FROM norm WHERE contact_jid IS NOT NULL) d) AS total_threads,
                (SELECT COALESCE(COUNT(*)::bigint, 0) FROM "Message" m2
                 WHERE (timezone('America/Sao_Paulo', to_timestamp(m2."messageTimestamp")))::date
                     = (timezone('America/Sao_Paulo', now()))::date
                ) AS messages_today
        """
        )
        agg = cur.fetchone()
        cur.execute(
            """
            SELECT i.id, i.name, i."connectionStatus" AS status,
                   i."profileName",
                   COALESCE(COUNT(m.id), 0)::bigint AS message_count
            FROM "Instance" i
            LEFT JOIN "Message" m ON m."instanceId" = i.id
            GROUP BY i.id
            ORDER BY i.name
        """
        )
        inst_rows = [dict(r) for r in cur.fetchall()]
        cur.close()
        conn.close()
        d = dict(agg) if agg else {}
        d["instances"] = inst_rows
        return jsonify(d)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── instances ────────────────────────────────────────────────────────────────
@app.route("/api/instances")
def get_instances():
    try:
        conn = get_db()
        cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT i.id, i.name, i."connectionStatus" AS status,
                   i."profileName",
                   COUNT(m.id) AS message_count
            FROM "Instance" i
            LEFT JOIN "Message" m ON m."instanceId" = i.id
            GROUP BY i.id
            ORDER BY i.name
        """)
        rows = cur.fetchall()
        cur.close(); conn.close()
        return jsonify([dict(r) for r in rows])
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/conversations")
def get_conversations():
    instance_id = request.args.get("instanceId", "")
    try:
        conn = get_db()
        cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(f"""
            WITH norm AS (
                SELECT
                    {NORM_JID} AS contact_jid,
                    "pushName",
                    "messageTimestamp",
                    COALESCE(
                        message->>'conversation',
                        message->'extendedTextMessage'->>'text'
                    )                        AS text,
                    (key->>'fromMe')::boolean AS from_me,
                    "messageType"
                FROM "Message"
                WHERE "instanceId" = %s
            ),
            ranked AS (
                SELECT *,
                    ROW_NUMBER() OVER (PARTITION BY contact_jid ORDER BY "messageTimestamp" DESC) AS rn
                FROM norm
            )
            SELECT
                n.contact_jid,
                REPLACE(n.contact_jid, '@s.whatsapp.net', '') AS phone_number,
                COUNT(*)                     AS message_count,
                MAX(n."messageTimestamp")    AS last_ts,
                MIN(n."messageTimestamp")    AS first_ts,
                r.text                       AS last_text,
                r.from_me                    AS last_from_me,
                r."pushName"                 AS last_push_name,
                r."messageType"              AS last_message_type
            FROM norm n
            JOIN ranked r ON r.contact_jid = n.contact_jid AND r.rn = 1
            GROUP BY n.contact_jid, r.text, r.from_me, r."pushName", r."messageType"
            ORDER BY last_ts DESC
        """, (instance_id,))
        rows = cur.fetchall()
        cur.close(); conn.close()
        return jsonify([dict(r) for r in rows])
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/messages")
def get_messages():
    instance_id = request.args.get("instanceId", "")
    contact_jid = request.args.get("contactJid", "")
    date_from   = request.args.get("dateFrom", "")
    date_to     = request.args.get("dateTo", "")

    date_clauses = ""
    params = [instance_id, contact_jid]
    if date_from:
        date_clauses += ' AND "messageTimestamp" >= EXTRACT(EPOCH FROM %s::date)::bigint'
        params.append(date_from)
    if date_to:
        date_clauses += ' AND "messageTimestamp" < EXTRACT(EPOCH FROM (%s::date + INTERVAL \'1 day\'))::bigint'
        params.append(date_to)

    try:
        conn = get_db()
        cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(f"""
            SELECT
                id,
                "pushName",
                "messageType",
                "messageTimestamp",
                (key->>'fromMe')::boolean AS from_me,
                COALESCE(
                    message->>'conversation',
                    message->'extendedTextMessage'->>'text'
                ) AS text
            FROM "Message"
            WHERE "instanceId" = %s
              AND ({NORM_JID}) = %s
              {date_clauses}
            ORDER BY "messageTimestamp" ASC
        """, params)
        rows = cur.fetchall()
        cur.close(); conn.close()

        result = []
        for r in rows:
            d = dict(r)
            ts = d.get("messageTimestamp")
            d["sent_at"] = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat() if ts else None
            result.append(d)

        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# EDA Diário: montado em /eda/ (ver eda_integracao.py e variável plataforma_central_PATH)
try:
    from eda_integracao import tentar_montar_eda

    tentar_montar_eda(app)
except Exception as _e:
    _log_msg = f"[EDA Diário] integração: {_e}"
    import sys
    print(_log_msg, file=sys.stderr)
    app.config["HAS_EDIARIO"] = False

app.wsgi_app = wsgi_eda_session_guard(app, app.wsgi_app)

# Atrás de Nginx / balanceador: cabeçalhos X-Forwarded-* passam a ser respeitados
if (os.getenv("TRUSTED_PROXY") or "").strip().lower() in ("1", "true", "yes", "on"):
    from werkzeug.middleware.proxy_fix import ProxyFix

    app.wsgi_app = ProxyFix(
        app.wsgi_app,
        x_for=1,
        x_proto=1,
        x_host=1,
        x_port=0,
        x_prefix=1,
    )

# Em produção (REQUIRE_STRONG_SECRETS=1), exige FLASK_SECRET_KEY definido
if (os.getenv("REQUIRE_STRONG_SECRETS") or "").strip().lower() in ("1", "true", "yes", "on"):
    if not (os.getenv("FLASK_SECRET_KEY") or "").strip():
        import sys

        print(
            "ERRO: defina FLASK_SECRET_KEY no ambiente (e REQUIRE_STRONG_SECRETS).",
            file=sys.stderr,
        )
        raise SystemExit(1)

if __name__ == "__main__":
    _dbg = (os.getenv("FLASK_DEBUG") or "").strip().lower() in ("1", "true", "yes", "on")
    app.run(debug=_dbg, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
