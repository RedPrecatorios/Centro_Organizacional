import os
import re
from datetime import datetime, timezone
from pathlib import Path

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
    elif has_edi:
        second_src = "/eda/"
    else:
        second_src = internal
    custom_label = (os.getenv("SECOND_TAB_LABEL") or "").strip()
    if custom_label:
        second_label = custom_label
    elif has_edi:
        second_label = "EDA Diário"
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


@app.route("/")
def index():
    return render_template("capa.html")


@app.route("/conversas")
def conversas():
    return render_template("conversas.html")


@app.route("/memoria-calculo")
def memoria_calculo():
    return render_template("memoria_calculo.html")


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


# EDA Diário: montado em /eda/ (ver eda_integracao.py e variável EDA_DIARIO_PATH)
try:
    from eda_integracao import tentar_montar_eda

    tentar_montar_eda(app)
except Exception as _e:
    _log_msg = f"[EDA Diário] integração: {_e}"
    import sys
    print(_log_msg, file=sys.stderr)
    app.config["HAS_EDIARIO"] = False

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
