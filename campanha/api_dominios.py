"""
Integração Mailgun (domínios) + GoDaddy (DNS) para a aba Campanha.

Mailgun API v4: criar, verificar e remover domínios.
GoDaddy API: configurar registros DNS (SPF, DKIM, MX, CNAME) automaticamente.
"""
from __future__ import annotations

import base64
import json
import os
import re
import time
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
    items: list[dict] = []
    skip = 0
    limit = 100
    while True:
        result = _mg_request("GET", f"/v4/domains?limit={limit}&skip={skip}")
        chunk = result.get("items") or []
        items.extend(chunk)
        if len(chunk) < limit:
            break
        skip += limit
    return items


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
    ultimo_erro: GoDaddyError | None = None
    for tentativa in range(4):
        req = urllib.request.Request(url, data=body, method=method, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read()
                return json.loads(raw.decode("utf-8")) if raw else {}
        except urllib.error.HTTPError as e:
            raw = e.read().decode("utf-8", errors="replace") if hasattr(e, "read") else ""
            ultimo_erro = GoDaddyError(e.code, raw)
            if e.code not in (403, 429, 500, 502, 503) or tentativa == 3:
                raise ultimo_erro from e
            time.sleep(1.5 * (tentativa + 1))
    raise ultimo_erro or GoDaddyError(0, "Falha GoDaddy")


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


def godaddy_rel_name(domain: str, name: str) -> str:
    domain = (domain or "").strip().lower().rstrip(".")
    rel = (name or "@").strip().lower().rstrip(".")
    if rel in ("", "@", domain):
        return "@"
    suffix = f".{domain}"
    if domain and rel.endswith(suffix):
        rel = rel[: -len(suffix)]
    return rel or "@"


def _dns_unquote(value: str) -> str:
    text = str(value or "").strip()
    if len(text) >= 2 and text[0] == text[-1] and text[0] in ('"', "'"):
        text = text[1:-1]
    return text.strip()


def merge_spf(current: str | None, include: str = "include:_spf.elasticemail.com") -> str:
    """Inclui o SPF do Elastic Email sem descartar os mecanismos que já existem."""
    default = f"v=spf1 {include} ~all"
    text = _dns_unquote(current or "")
    if not text.lower().startswith("v=spf1"):
        return default
    if "_spf.elasticemail.com" in text.lower():
        return text
    matched = re.search(r"\s([~\-+?]?all)\s*$", text, re.I)
    if matched:
        return text[: matched.start()] + f" {include} " + matched.group(1)
    return text + f" {include} ~all"


def _dkim_p(value: str) -> str:
    text = re.sub(r"\s+", "", _dns_unquote(value))
    matched = re.search(r"p=([A-Za-z0-9+/=]+)", text)
    return matched.group(1) if matched else ""


def _looks_dkim(value: str) -> bool:
    text = _dns_unquote(value).lower().replace(" ", "")
    return "p=" in text and ("k=rsa" in text or text.startswith("v=dkim1"))


def _put_items(records: list[dict]) -> list[dict]:
    out = []
    for rec in records:
        item = {"data": rec["data"], "ttl": int(rec.get("ttl") or 3600)}
        if rec.get("priority") is not None:
            item["priority"] = int(rec["priority"])
        out.append(item)
    return out


def godaddy_ensure_records(domain: str, records: list[dict]) -> dict:
    """Publica SPF, DKIM, tracking e DMARC sem apagar os TXT que já estão no ápice.

    Cada item: type, name, value, mode = spf | dkim | cname | if_absent.
    Domínio já correto não é reescrito.
    """
    existing = godaddy_get_dns_records(domain)
    if not isinstance(existing, list):
        existing = []
    changed: list[str] = []
    skipped: list[str] = []

    def group_for(rtype: str, rel: str) -> list[dict]:
        found = []
        for rec in existing:
            if str(rec.get("type") or "").upper() != rtype:
                continue
            if godaddy_rel_name(domain, str(rec.get("name") or "@")) == rel:
                found.append(rec)
        return found

    for rec in records:
        rtype = str(rec.get("type") or "").upper()
        rel = godaddy_rel_name(domain, str(rec.get("name") or "@"))
        mode = str(rec.get("mode") or "set")
        value = str(rec.get("value") or "").strip()
        if not rtype or not value:
            continue
        group = group_for(rtype, rel)
        new_group: list[dict] | None = None

        if mode == "spf":
            idx = next((i for i, item in enumerate(group) if _dns_unquote(item.get("data") or "").lower().startswith("v=spf1")), None)
            if idx is None:
                new_group = list(group) + [{"data": value, "ttl": 3600}]
            else:
                merged = merge_spf(_dns_unquote(group[idx].get("data") or ""))
                if merged == _dns_unquote(group[idx].get("data") or ""):
                    skipped.append(f"{rtype} {rel}")
                    continue
                new_group = list(group)
                updated = dict(group[idx])
                updated["data"] = merged
                new_group[idx] = updated
        elif mode == "dkim":
            wanted = _dkim_p(value)
            if not wanted:
                skipped.append(f"{rtype} {rel}")
                continue
            if any(_dkim_p(item.get("data") or "") == wanted for item in group):
                skipped.append(f"{rtype} {rel}")
                continue
            others = [item for item in group if not _looks_dkim(item.get("data") or "")]
            new_group = others + [{"data": value, "ttl": 3600}]
        elif mode == "dmarc":
            desejado = _dns_unquote(value)
            rua = ""
            achou_rua = re.search(r"rua=mailto:([^;\s]+)", desejado, re.I)
            if achou_rua:
                rua = achou_rua.group(1).lower()

            def _eh_dmarc(item: dict) -> bool:
                return _dns_unquote(item.get("data") or "").lower().startswith("v=dmarc1")

            atual = next((item for item in group if _eh_dmarc(item)), None)
            if atual is None:
                new_group = list(group) + [{"data": desejado, "ttl": 3600}]
            else:
                texto = _dns_unquote(atual.get("data") or "")
                politica = re.search(r"\bp\s*=\s*(none|quarantine|reject)", texto, re.I)
                if politica and politica.group(1).lower() != "none":
                    skipped.append(f"{rtype} {rel}")
                    continue
                if rua and rua in texto.lower():
                    skipped.append(f"{rtype} {rel}")
                    continue
                new_group = []
                trocou = False
                for item in group:
                    if not trocou and _eh_dmarc(item):
                        atualizado = dict(item)
                        atualizado["data"] = desejado
                        new_group.append(atualizado)
                        trocou = True
                    else:
                        new_group.append(item)
        elif mode == "cname":
            target = value.rstrip(".").lower()
            if any(str(item.get("data") or "").rstrip(".").lower() == target for item in group):
                skipped.append(f"{rtype} {rel}")
                continue
            new_group = [{"data": value.rstrip("."), "ttl": group[0].get("ttl") if group else 3600}]
        else:
            if group:
                skipped.append(f"{rtype} {rel}")
                continue
            new_group = [{"data": value, "ttl": 3600}]

        if not new_group:
            continue
        quoted = urllib.parse.quote(rel, safe="@._-")
        url = f"https://api.godaddy.com/v1/domains/{domain}/records/{rtype}/{quoted}"
        _gd_request("PUT", url, _put_items(new_group))
        changed.append(f"{rtype} {rel}")
        existing = [item for item in existing if not (
            str(item.get("type") or "").upper() == rtype
            and godaddy_rel_name(domain, str(item.get("name") or "@")) == rel
        )]
        for item in new_group:
            existing.append({"type": rtype, "name": rel, "data": item["data"], "ttl": item.get("ttl") or 3600})

    return {"changed": changed, "skipped": skipped}


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


def desativar_dominios_exceto(
    keep_domains: set[str],
    db_config: dict,
    db_name: str,
    *,
    delete_mailgun: bool = False,
) -> list[dict]:
    """
    Desativa no MySQL os remetentes ativos cujo domínio não está em keep_domains.
    Só apaga no Mailgun se delete_mailgun=True, e apenas esses (não a conta inteira).
    """
    keep = {str(d).strip().lower() for d in keep_domains if str(d).strip()}
    conn = mysql.connector.connect(**db_config, database=db_name)
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT id, nome, dominio, mailgun_state FROM campanha_dominios WHERE ativo = 1")
    rows = cur.fetchall() or []
    out: list[dict] = []
    for row in rows:
        dominio = (row.get("dominio") or "").strip()
        if dominio.lower() in keep:
            continue
        mg_error = None
        mg_deleted = False
        if delete_mailgun and dominio:
            from campanha.api_elasticemail import (
                ElasticEmailError,
                elasticemail_configured,
                elasticemail_delete_domain,
            )

            if elasticemail_configured():
                try:
                    elasticemail_delete_domain(dominio)
                    mg_deleted = True
                except ElasticEmailError as e:
                    if e.status != 404:
                        mg_error = str(e)
                    else:
                        mg_deleted = True
            else:
                try:
                    mailgun_delete_domain(dominio)
                    mg_deleted = True
                except MailgunError as e:
                    if e.status != 404:
                        mg_error = str(e)
                    else:
                        mg_deleted = True
        if mg_deleted:
            cur.execute(
                "UPDATE campanha_dominios SET ativo = 0, mailgun_state = 'deleted' WHERE id = %s",
                (row["id"],),
            )
        else:
            cur.execute("UPDATE campanha_dominios SET ativo = 0 WHERE id = %s", (row["id"],))
        out.append(
            {
                "id": row["id"],
                "nome": row.get("nome"),
                "dominio": dominio,
                "mailgun_deleted": mg_deleted,
                "mailgun_error": mg_error,
            }
        )
    conn.commit()
    cur.close()
    conn.close()
    return out


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
    from campanha.entregabilidade import selecionar_par_da_semana

    _par, info = selecionar_par_da_semana(rows, lambda r: r.get("dominio") or "")
    em_uso = {chave.lower() for chave in info.get("chaves") or []}
    for r in rows:
        for k in ("criado_em", "atualizado_em"):
            if r.get(k) and hasattr(r[k], "isoformat"):
                r[k] = r[k].isoformat()
        dominio = (r.get("dominio") or "").lower()
        if r.get("mailgun_state") == "active" and dominio in em_uso:
            r["rodizio"] = "em_uso"
        elif r.get("mailgun_state") == "active":
            r["rodizio"] = "aguardando"
        else:
            r["rodizio"] = ""
        r["rodizio_ate"] = info.get("fim") or ""
    return rows
