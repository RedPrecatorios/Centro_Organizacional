"""
Cliente Elastic Email (https://app.elasticemail.com, API v4).

Substitui o Mailgun no cadastro de domínios e no envio HTTP.
Auth: header X-ElasticEmail-ApiKey (ELASTICEMAIL_API_KEY no .env).
"""
from __future__ import annotations

import json
import os
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

import mysql.connector

_EE_API_BASE = "https://api.elasticemail.com/v4"

# DNS padrão de envio (DKIM específico sai do painel / verify).
_STANDARD_DNS = [
    {
        "type": "TXT",
        "name": "@",
        "value": "v=spf1 include:_spf.elasticemail.com ~all",
        "priority": None,
    },
    {
        "type": "CNAME",
        "name": "tracking",
        "value": "api.elasticemail.com",
        "priority": None,
    },
]


class ElasticEmailError(Exception):
    def __init__(self, status: int, detail: Any):
        self.status = status
        self.detail = detail
        super().__init__(f"Elastic Email HTTP {status}: {detail}")


def elasticemail_configured() -> bool:
    return bool((os.getenv("ELASTICEMAIL_API_KEY") or "").strip())


def _ee_key() -> str:
    return (os.getenv("ELASTICEMAIL_API_KEY") or "").strip()


def _ee_request(method: str, path: str, body: Any = None, timeout: int = 30) -> Any:
    url = f"{_EE_API_BASE}{path}"
    headers = {
        "X-ElasticEmail-ApiKey": _ee_key(),
        "Accept": "application/json",
    }
    data = None
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
            if not raw:
                return {}
            return json.loads(raw.decode("utf-8"))
    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8", errors="replace") if hasattr(e, "read") else ""
        try:
            detail = json.loads(raw) if raw else {"message": str(e)}
        except (json.JSONDecodeError, ValueError):
            detail = {"message": raw or str(e)}
        raise ElasticEmailError(e.code, detail) from e


def elasticemail_list_domains() -> list[dict]:
    result = _ee_request("GET", "/domains")
    if isinstance(result, list):
        return result
    if isinstance(result, dict):
        return result.get("items") or result.get("Data") or []
    return []


def elasticemail_get_domain(domain: str) -> dict:
    return _ee_request("GET", f"/domains/{urllib.parse.quote(domain, safe='')}")


def elasticemail_create_domain(domain: str, *, set_as_default: bool = False) -> dict:
    try:
        return _ee_request("POST", "/domains", {"Domain": domain, "SetAsDefault": set_as_default})
    except ElasticEmailError as e:
        msg = json.dumps(e.detail).lower() if e.detail else ""
        if e.status in (400, 409) and ("already" in msg or "exist" in msg):
            return elasticemail_get_domain(domain)
        raise


def elasticemail_verify_domain(domain: str) -> dict:
    # "None" = só autenticação de envio (SPF/DKIM), sem tracking HTTPS.
    return _ee_request(
        "PUT",
        f"/domains/{urllib.parse.quote(domain, safe='')}/verification",
        "None",
    )


def elasticemail_delete_domain(domain: str) -> dict:
    return _ee_request("DELETE", f"/domains/{urllib.parse.quote(domain, safe='')}")


def _dns_from_domain_payload(payload: dict, dominio: str) -> list[dict]:
    recs = list(_STANDARD_DNS)
    dkim = payload.get("DkimValue") or payload.get("Dkim") or payload.get("DKIM")
    selector = payload.get("DkimSelector") or payload.get("Selector") or "api"
    if isinstance(dkim, str) and dkim and dkim.lower() not in ("true", "false"):
        recs.insert(
            1,
            {
                "type": "TXT",
                "name": f"{selector}._domainkey",
                "value": dkim,
                "priority": None,
            },
        )
    return recs


def _mg_state_from_payload(payload: dict) -> str:
    if payload.get("Verify") is True or str(payload.get("Verify")).lower() == "true":
        return "active"
    if payload.get("Spf") is True and payload.get("Dkim") is True:
        return "active"
    return "pending"


def adicionar_dominio_elasticemail(
    dominio: str,
    nome: str,
    from_name: str,
    from_email: str,
    reply_to: str | None,
    db_config: dict,
    db_name: str,
) -> dict:
    already = False
    try:
        ee = elasticemail_create_domain(dominio)
    except ElasticEmailError:
        raise
    dns_records = _dns_from_domain_payload(ee if isinstance(ee, dict) else {}, dominio)
    mg_state = _mg_state_from_payload(ee if isinstance(ee, dict) else {})

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
        (nome, dominio, from_name, from_email, reply_to, mg_state, 0),
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
        "dns_configured": False,
        "dns_error": "Configure SPF/DKIM no DNS (Elastic Email). DKIM: copie no painel app.elasticemail.com → Settings → Domains.",
        "dns_records": dns_records,
        "already_existed_in_mailgun": already,
        "provider": "elasticemail",
        "elasticemail": ee,
    }


def verificar_dominio_elasticemail(dominio: str, db_config: dict, db_name: str) -> dict:
    payload = elasticemail_verify_domain(dominio)
    mg_state = _mg_state_from_payload(payload if isinstance(payload, dict) else {})
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
        "dns_records": _dns_from_domain_payload(payload if isinstance(payload, dict) else {}, dominio),
        "provider": "elasticemail",
        "elasticemail": payload,
    }


def elasticemail_send(
    *,
    from_addr: str,
    to_addr: str,
    subject: str,
    html: str,
    text: str,
    reply_to: str | None,
    timeout: int = 30,
) -> dict:
    body: list[dict] = []
    if html:
        body.append({"ContentType": "HTML", "Content": html})
    if text:
        body.append({"ContentType": "PlainText", "Content": text})
    if not body:
        body.append({"ContentType": "PlainText", "Content": " "})
    payload: dict[str, Any] = {
        "Recipients": {"To": [to_addr]},
        "Content": {
            "From": from_addr,
            "Subject": subject,
            "Body": body,
        },
    }
    if reply_to:
        payload["Content"]["ReplyTo"] = reply_to
    return _ee_request("POST", "/emails/transactional", payload, timeout=timeout)
