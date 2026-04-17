import os
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from flask import Flask, jsonify, render_template, request

# cd "c:\Users\justi\OneDrive\Documentos\Python Projects\PycharmProjects\View_Message"
# python app.py

load_dotenv()

app = Flask(__name__)

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
    return render_template("index.html")


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


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
