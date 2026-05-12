"""
Integração Mailgun (domínios) + GoDaddy (DNS) para a aba Campanha.

Mailgun API v4: criar, verificar e remover domínios.
GoDaddy API: configurar registros DNS (SPF, DKIM, MX, CNAME) automaticamente.
"""
from __future__ import annotations

import base64
import json
import os
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

import mysql.connector

_MAILGUN_API_BASE = "https://api.mailgun.net"


def _mailgun_key() -> str:
    return (os.getenv("MAILGUN_API_KEY") or "").strip()


def _godaddy_headers() -> dict[str, str] | None:
    key = (os.getenv("GODADDY_API_KEY") or "").strip()
    secret = (os.getenv("GODADDY_API_SECRET") or "").strip()
    if not key or not secret:
        return None
    return {
        "Authorization": f"sso-key {key}:{secret}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def _mg_request(method: str, path: str, data: dict | None = None, timeout: int = 30) -> dict:
    url = f"{_MAILGUN_API_BASE}{path}"
    body = None
    headers: dict[str, str] = {}
    auth = base64.b64encode(f"api:{_mailgun_key()}".encode()).decode("ascii")
    headers["Authorization"] = f"Basic {auth}"

    if data is not None:
        body = urllib.parse.urlencode(data).encode("utf-8")
        headers["Content-Type"] = "application/x-www-form-urlencoded"

    req = urllib.request.Request(url, data=body, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
            return json.loads(raw.decode("utf-8")) if raw else {}
    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8", errors="replace") if hasattr(e, "read") else ""
        try:
            detail = json.loads(raw)
        except (json.JSONDecodeError, ValueError):
            detail = {"message": raw}
        raise MailgunError(e.code, detail) from e


class MailgunError(Exception):
    def __init__(self, status: int, detail: dict):
        self.status = status
        self.detail = detail
        super().__init__(f"Mailgun HTTP {status}: {detail}")


# ── Mailgun domain operations ────────────────────────────────────────────────

def mailgun_create_domain(domain: str) -> dict:
    return _mg_request("POST", "/v4/domains", {"name": domain})


def mailgun_get_domain(domain: str) -> dict:
    return _mg_request("GET", f"/v4/domains/{urllib.parse.quote(domain, safe='')}")


def mailgun_verify_domain(domain: str) -> dict:
    return _mg_request("PUT", f"/v4/domains/{urllib.parse.quote(domain, safe='')}/verify")


def mailgun_delete_domain(domain: str) -> dict:
    return _mg_request("DELETE", f"/v4/domains/{urllib.parse.quote(domain, safe='')}")


def mailgun_list_domains() -> list[dict]:
    result = _mg_request("GET", "/v4/domains")
    return result.get("items", [])


def _extract_dns_records(mg_response: dict) -> list[dict]:
    """Extrai registros DNS necessários da resposta do Mailgun."""
    records = []
    dns = mg_response.get("dns", mg_response.get("receiving_dns_records", []))

    for section_key in ("sending_dns_records", "receiving_dns_records"):
        for rec in mg_response.get(section_key, []):
            records.append({
                "type": rec.get("record_type", "").upper(),
                "name": rec.get("name", ""),
                "value": rec.get("value", ""),
                "priority": rec.get("priority"),
                "valid": rec.get("valid", "unknown"),
            })

    if not records and isinstance(dns, list):
        for rec in dns:
            records.append({
                "type": rec.get("record_type", "").upper(),
                "name": rec.get("name", ""),
                "value": rec.get("value", ""),
                "priority": rec.get("priority"),
                "valid": rec.get("valid", "unknown"),
            })

    return records


# ── GoDaddy DNS operations ───────────────────────────────────────────────────

def _gd_request(method: str, url: str, data: Any = None, timeout: int = 30) -> Any:
    headers = _godaddy_headers()
    if not headers:
        raise GoDaddyError(0, "Credenciais GoDaddy não configuradas (GODADDY_API_KEY / GODADDY_API_SECRET).")

    body = json.dumps(data).encode("utf-8") if data is not None else None
    req = urllib.request.Request(url, data=body, method=method, headers=headers)

    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
            return json.loads(raw.decode("utf-8")) if raw else {}
    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8", errors="replace") if hasattr(e, "read") else ""
        raise GoDaddyError(e.code, raw) from e


class GoDaddyError(Exception):
    def __init__(self, status: int, detail: str):
        self.status = status
        self.detail = detail
        super().__init__(f"GoDaddy HTTP {status}: {detail}")


def godaddy_add_dns_records(domain: str, records: list[dict]) -> None:
    """Adiciona registros DNS no GoDaddy para um domínio."""
    if not records:
        return
    headers = _godaddy_headers()
    if not headers:
        raise GoDaddyError(0, "Credenciais GoDaddy não configuradas.")

    for rec in records:
        rec_type = rec["type"].upper()
        rec_name = rec["name"]
        rec_value = rec["value"]
        priority = rec.get("priority")

        # GoDaddy expects the record name relative to the domain
        if rec_name.endswith(f".{domain}"):
            rel_name = rec_name[: -(len(domain) + 1)]
        elif rec_name == domain:
            rel_name = "@"
        else:
            rel_name = rec_name

        if not rel_name:
            rel_name = "@"

        payload = [{
            "data": rec_value,
            "name": rel_name,
            "ttl": 3600,
            "type": rec_type,
        }]
        if priority is not None and rec_type == "MX":
            payload[0]["priority"] = int(priority)

        url = f"https://api.godaddy.com/v1/domains/{domain}/records/{rec_type}/{rel_name}"
        _gd_request("PUT", url, payload)


def godaddy_get_dns_records(domain: str) -> list[dict]:
    url = f"https://api.godaddy.com/v1/domains/{domain}/records"
    return _gd_request("GET", url)


# ── Combined flow ─────────────────────────────────────────────────────────────

def adicionar_dominio_completo(
    dominio: str,
    nome: str,
    from_name: str,
    from_email: str,
    reply_to: str | None,
    db_config: dict,
    db_name: str,
) -> dict:
    """
    Fluxo completo: Mailgun create → GoDaddy DNS → MySQL insert.
    Se o dominio ja existir no Mailgun, busca os dados existentes.
    Retorna dict com status e dns_records.
    """
    already_existed = False
    try:
        mg_result = mailgun_create_domain(dominio)
    except MailgunError as e:
        msg = (e.detail.get("message") or "") if isinstance(e.detail, dict) else str(e.detail)
        if "already exists" in msg.lower():
            already_existed = True
            mg_result = mailgun_get_domain(dominio)
        else:
            raise

    dns_records = _extract_dns_records(mg_result)

    # Detect Mailgun state from response (useful when domain already existed)
    mg_state = "pending"
    dom_info = mg_result.get("domain", {})
    if isinstance(dom_info, dict) and dom_info.get("state") == "active":
        mg_state = "active"

    dns_ok = False
    dns_error = None
    if _godaddy_headers():
        try:
            godaddy_add_dns_records(dominio, dns_records)
            dns_ok = True
        except GoDaddyError as e:
            dns_error = str(e)
    else:
        dns_error = "GoDaddy nao configurado — configure DNS manualmente."

    if already_existed and mg_state == "active":
        dns_ok = True

    conn = mysql.connector.connect(**db_config, database=db_name)
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO campanha_dominios (nome, dominio, from_name, from_email, reply_to, mailgun_state, dns_configured)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            from_name = VALUES(from_name),
            from_email = VALUES(from_email),
            reply_to = VALUES(reply_to),
            mailgun_state = VALUES(mailgun_state),
            dns_configured = VALUES(dns_configured),
            ativo = 1
        """,
        (nome, dominio, from_name, from_email, reply_to, mg_state, 1 if dns_ok else 0),
    )
    conn.commit()
    inserted_id = cur.lastrowid
    cur.close()
    conn.close()

    return {
        "ok": True,
        "id": inserted_id,
        "dominio": dominio,
        "mailgun_state": mg_state,
        "dns_configured": dns_ok,
        "dns_error": dns_error,
        "dns_records": dns_records,
        "already_existed_in_mailgun": already_existed,
    }


def verificar_dominio(dominio: str, db_config: dict, db_name: str) -> dict:
    """Chama Mailgun verify e atualiza estado no MySQL."""
    mg_result = mailgun_verify_domain(dominio)

    state = mg_result.get("domain", {}).get("state", "unknown")
    mg_state = "active" if state == "active" else "pending"

    dns_records = _extract_dns_records(mg_result)

    conn = mysql.connector.connect(**db_config, database=db_name)
    cur = conn.cursor()
    cur.execute(
        "UPDATE campanha_dominios SET mailgun_state = %s WHERE dominio = %s AND ativo = 1",
        (mg_state, dominio),
    )
    conn.commit()
    cur.close()
    conn.close()

    return {
        "ok": True,
        "dominio": dominio,
        "mailgun_state": mg_state,
        "dns_records": dns_records,
    }


def remover_dominio(dominio_id: int, db_config: dict, db_name: str) -> dict:
    """Remove do Mailgun e desativa no MySQL."""
    conn = mysql.connector.connect(**db_config, database=db_name)
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT dominio FROM campanha_dominios WHERE id = %s", (dominio_id,))
    row = cur.fetchone()
    if not row:
        cur.close()
        conn.close()
        return {"ok": False, "error": "Domínio não encontrado."}

    dominio = row["dominio"]

    mg_error = None
    try:
        mailgun_delete_domain(dominio)
    except MailgunError as e:
        if e.status != 404:
            mg_error = str(e)

    cur.execute(
        "UPDATE campanha_dominios SET ativo = 0, mailgun_state = 'deleted' WHERE id = %s",
        (dominio_id,),
    )
    conn.commit()
    cur.close()
    conn.close()

    return {"ok": True, "dominio": dominio, "mailgun_error": mg_error}


def listar_dominios(db_config: dict, db_name: str) -> list[dict]:
    conn = mysql.connector.connect(**db_config, database=db_name)
    cur = conn.cursor(dictionary=True)
    cur.execute(
        """
        SELECT id, nome, dominio, from_name, from_email, reply_to,
               mailgun_state, dns_configured, ativo, criado_em, atualizado_em
        FROM campanha_dominios
        WHERE ativo = 1
        ORDER BY nome
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
