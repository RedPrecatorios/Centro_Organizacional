"""
Cliente Elastic Email (https://app.elasticemail.com, API v4).

Substitui o Mailgun no cadastro de domínios e no envio HTTP.
Auth: header X-ElasticEmail-ApiKey (ELASTICEMAIL_API_KEY no .env).
"""
from __future__ import annotations

import json
import os
import re
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

import mysql.connector

_EE_API_BASE = "https://api.elasticemail.com/v4"
_DOMAIN_RE = re.compile(r"^(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,}$")

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


_TRACKING_TYPES = {"None", "Http", "ExternalHttps", "InternalCertHttps", "LetsEncryptCert"}


def elasticemail_verify_domain(domain: str, tracking: str = "None") -> dict:
    """Revalida DNS. O corpo é o TrackingType atual — não troca LetsEncrypt por None."""
    if tracking not in _TRACKING_TYPES:
        tracking = "None"
    return _ee_request(
        "PUT",
        f"/domains/{urllib.parse.quote(domain, safe='')}/verification",
        tracking,
    )


def elasticemail_delete_domain(domain: str) -> dict:
    return _ee_request("DELETE", f"/domains/{urllib.parse.quote(domain, safe='')}")


def elasticemail_domain_name(item: dict) -> str:
    """Nome de host. A listagem às vezes devolve ``dominio (contato@dominio)`` no default."""
    raw = str(item.get("Domain") or item.get("name") or item.get("domain") or "").strip()
    raw = raw.split("(", 1)[0].strip().split()[0].strip().lower().rstrip(".")
    return raw


def _dominio_valido(dominio: str) -> bool:
    return bool(dominio) and bool(_DOMAIN_RE.match(dominio))


def _dns_from_domain_payload(payload: dict, dominio: str) -> list[dict]:
    recs = list(_STANDARD_DNS)
    dkim_rec = payload.get("DKIMRecord") if isinstance(payload, dict) else None
    selector = "api"
    pubkey = ""
    if isinstance(dkim_rec, dict):
        selector = str(dkim_rec.get("Selector") or "api").strip() or "api"
        pubkey = str(dkim_rec.get("PublicKey") or "").strip()
    if not pubkey:
        dkim = payload.get("DkimValue") or payload.get("DKIM")
        selector = str(payload.get("DkimSelector") or payload.get("Selector") or selector)
        if isinstance(dkim, str) and dkim and dkim.lower() not in ("true", "false"):
            pubkey = dkim
    if pubkey and not pubkey.lower().startswith("k="):
        pubkey = f"k=rsa;t=s;p={pubkey}"
    if pubkey:
        recs.insert(
            1,
            {
                "type": "TXT",
                "name": f"{selector}._domainkey",
                "value": pubkey,
                "priority": None,
            },
        )
    return recs


def _mg_state_from_payload(payload: dict) -> str:
    """Estado usado pela fila de disparo.

    Na API v4, ``Verify`` pode ficar false mesmo com SPF e DKIM válidos.
    O envio depende de SPF + DKIM (ou de ``Verify`` true).
    """
    if payload.get("IsMarkedForDeletion") is True:
        return "deleted"
    if payload.get("Verify") is True or str(payload.get("Verify")).lower() == "true":
        return "active"
    if payload.get("Spf") is True and payload.get("Dkim") is True:
        return "active"
    return "pending"


def _dns_ok_from_payload(payload: dict) -> bool:
    return payload.get("Spf") is True and payload.get("Dkim") is True


def _godaddy_ready() -> bool:
    return bool((os.getenv("GODADDY_API_KEY") or "").strip() and (os.getenv("GODADDY_API_SECRET") or "").strip())


def registros_dns_elasticemail(payload: dict) -> list[dict]:
    """Registros que o Elastic Email precisa. O DKIM só entra quando a API devolve a chave."""
    records = [
        {
            "type": "TXT",
            "name": "@",
            "value": "v=spf1 include:_spf.elasticemail.com ~all",
            "mode": "spf",
        },
        {
            "type": "CNAME",
            "name": "tracking",
            "value": "api.elasticemail.com",
            "mode": "cname",
        },
        {
            "type": "TXT",
            "name": "_dmarc",
            "value": "v=DMARC1; p=none; rua=mailto:ti@redprecatorios.com.br",
            "mode": "dmarc",
        },
    ]
    dkim_rec = payload.get("DKIMRecord") if isinstance(payload, dict) else None
    if isinstance(dkim_rec, dict):
        selector = str(dkim_rec.get("Selector") or "api").strip() or "api"
        pubkey = re.sub(r"\s+", "", str(dkim_rec.get("PublicKey") or ""))
        if pubkey:
            records.insert(
                1,
                {
                    "type": "TXT",
                    "name": f"{selector}._domainkey",
                    "value": f"v=DKIM1; k=rsa; p={pubkey}",
                    "mode": "dkim",
                },
            )
    return records


def garantir_dns_elasticemail(dominio: str) -> dict:
    """Cria o que faltar no DNS do GoDaddy e pede ao Elastic Email para revalidar.

    Domínio que já tem SPF e DKIM confirmados não é alterado.
    """
    payload = elasticemail_get_domain(dominio)
    if not isinstance(payload, dict):
        payload = {}
    if _dns_ok_from_payload(payload):
        return {"dominio": dominio, "acao": "ja_pronto", "payload": payload}

    if not _godaddy_ready():
        return {
            "dominio": dominio,
            "acao": "pendente",
            "erro": "GoDaddy nao configurado (GODADDY_API_KEY / GODADDY_API_SECRET).",
            "payload": payload,
        }

    from campanha.api_dominios import GoDaddyError, godaddy_ensure_records

    try:
        published = godaddy_ensure_records(dominio, registros_dns_elasticemail(payload))
    except GoDaddyError as e:
        return {"dominio": dominio, "acao": "falha", "erro": str(e), "payload": payload}

    tracking = "LetsEncryptCert"
    if payload.get("Type") in _TRACKING_TYPES:
        tracking = str(payload.get("Type"))
    try:
        elasticemail_verify_domain(dominio, tracking)
        refreshed = elasticemail_get_domain(dominio)
        if isinstance(refreshed, dict):
            payload = refreshed
    except ElasticEmailError as e:
        return {
            "dominio": dominio,
            "acao": "publicado",
            "erro": f"DNS publicado, verificacao falhou: {e}",
            "payload": payload,
            "changed": published.get("changed") or [],
        }

    if _dns_ok_from_payload(payload):
        acao = "pronto"
    elif published.get("changed"):
        acao = "publicado"
    else:
        acao = "pendente"
    return {
        "dominio": dominio,
        "acao": acao,
        "payload": payload,
        "changed": published.get("changed") or [],
        "erro": None if acao != "pendente" else "SPF/DKIM ainda nao confirmados pelo Elastic Email.",
    }


def adicionar_dominio_elasticemail(
    dominio: str,
    nome: str,
    from_name: str,
    from_email: str,
    reply_to: str | None,
    db_config: dict,
    db_name: str,
) -> dict:
    try:
        ee = elasticemail_create_domain(dominio)
    except ElasticEmailError:
        raise
    if not isinstance(ee, dict) or not ee.get("Spf"):
        try:
            fetched = elasticemail_get_domain(dominio)
            if isinstance(fetched, dict):
                ee = fetched
        except ElasticEmailError:
            pass
    if not isinstance(ee, dict):
        ee = {}
    garantia = garantir_dns_elasticemail(dominio)
    if isinstance(garantia.get("payload"), dict):
        ee = garantia["payload"]
    dns_records = registros_dns_elasticemail(ee)
    mg_state = _mg_state_from_payload(ee)
    dns_ok = _dns_ok_from_payload(ee)
    dns_error = garantia.get("erro")
    if garantia.get("acao") == "publicado" and not dns_error:
        dns_error = "Registros publicados no GoDaddy. Aguarde a propagacao para o Elastic Email confirmar."

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
        "provider": "elasticemail",
        "elasticemail": ee,
    }


def verificar_dominio_elasticemail(dominio: str, db_config: dict, db_name: str) -> dict:
    current = elasticemail_get_domain(dominio)
    tracking = "None"
    if isinstance(current, dict):
        tracking = str(current.get("Type") or current.get("TrackingTypeUserRequest") or "None")
    elasticemail_verify_domain(dominio, tracking)
    payload = elasticemail_get_domain(dominio)
    if not isinstance(payload, dict):
        payload = {}
    mg_state = _mg_state_from_payload(payload)
    dns_ok = 1 if _dns_ok_from_payload(payload) else 0
    conn = mysql.connector.connect(**db_config, database=db_name)
    cur = conn.cursor()
    cur.execute(
        """UPDATE campanha_dominios
           SET mailgun_state = %s, dns_configured = %s
           WHERE dominio = %s AND ativo = 1""",
        (mg_state, dns_ok, dominio),
    )
    conn.commit()
    cur.close()
    conn.close()
    return {
        "ok": True,
        "dominio": dominio,
        "mailgun_state": mg_state,
        "dns_configured": bool(dns_ok),
        "dns_records": _dns_from_domain_payload(payload, dominio),
        "provider": "elasticemail",
        "elasticemail": payload,
    }


def remover_dominio_elasticemail(dominio_id: int, db_config: dict, db_name: str) -> dict:
    """Remove o domínio na conta Elastic Email e desativa a linha no MySQL."""
    conn = mysql.connector.connect(**db_config, database=db_name)
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT dominio FROM campanha_dominios WHERE id = %s", (dominio_id,))
    row = cur.fetchone()
    if not row:
        cur.close()
        conn.close()
        return {"ok": False, "error": "Domínio não encontrado."}

    dominio = row["dominio"]
    provider_error = None
    try:
        elasticemail_delete_domain(dominio)
    except ElasticEmailError as e:
        if e.status != 404:
            provider_error = str(e)

    cur.execute(
        "UPDATE campanha_dominios SET ativo = 0, mailgun_state = 'deleted' WHERE id = %s",
        (dominio_id,),
    )
    conn.commit()
    cur.close()
    conn.close()
    return {"ok": True, "dominio": dominio, "provider": "elasticemail", "provider_error": provider_error}


def sincronizar_dominios_elasticemail(db_config: dict, db_name: str) -> dict:
    """Importa domínios da conta Elastic Email (GET /v4/domains) para campanha_dominios.

    Atualiza o estado dos que já estão ativos. Não reativa linhas desligadas de propósito.
    Domínios novos entram como contato@dominio.
    """
    if not elasticemail_configured():
        return {"ok": False, "error": "ELASTICEMAIL_API_KEY nao configurada.", "status": 500}

    items = elasticemail_list_domains()
    by_domain: dict[str, dict] = {}
    ignorados_invalidos = 0
    for item in items:
        if not isinstance(item, dict):
            ignorados_invalidos += 1
            continue
        dominio = elasticemail_domain_name(item)
        if not _dominio_valido(dominio):
            ignorados_invalidos += 1
            continue
        by_domain[dominio] = item

    if not by_domain:
        return {"ok": False, "error": "Nenhum dominio valido encontrado na conta Elastic Email.", "status": 404}

    conn = mysql.connector.connect(**db_config, database=db_name)
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT id, nome, dominio, ativo FROM campanha_dominios")
    rows = cur.fetchall() or []
    por_dominio: dict[str, list[dict]] = {}
    nomes = set()
    for row in rows:
        key = (row.get("dominio") or "").strip().lower()
        por_dominio.setdefault(key, []).append(row)
        if row.get("nome"):
            nomes.add(row["nome"])

    inseridos = 0
    atualizados = 0
    mantidos_inativos = 0
    dns_ja_prontos = 0
    dns_publicados = 0
    dns_pendentes = 0
    dns_falhas: list[dict] = []

    def _aplicar_dns(dominio: str, payload: dict) -> dict:
        nonlocal dns_ja_prontos, dns_publicados, dns_pendentes
        if _dns_ok_from_payload(payload):
            dns_ja_prontos += 1
            return payload
        try:
            garantia = garantir_dns_elasticemail(dominio)
        except ElasticEmailError as e:
            dns_pendentes += 1
            dns_falhas.append({"dominio": dominio, "erro": str(e)})
            return payload
        refreshed = garantia.get("payload") if isinstance(garantia.get("payload"), dict) else payload
        acao = garantia.get("acao")
        if acao == "ja_pronto":
            dns_ja_prontos += 1
        elif acao in ("pronto", "publicado"):
            dns_publicados += 1
            if not _dns_ok_from_payload(refreshed):
                dns_pendentes += 1
        else:
            dns_pendentes += 1
        erro = garantia.get("erro")
        if erro and acao != "ja_pronto":
            if "GoDaddy nao configurado" in erro and any("GoDaddy nao configurado" in f["erro"] for f in dns_falhas):
                pass
            else:
                dns_falhas.append({"dominio": dominio, "erro": erro})
        return refreshed

    for dominio in list(by_domain):
        by_domain[dominio] = _aplicar_dns(dominio, by_domain[dominio])

    for key, group in por_dominio.items():
        if key in by_domain or not _dominio_valido(key):
            continue
        if not any(row.get("ativo") for row in group):
            continue
        try:
            elasticemail_create_domain(key)
            created = elasticemail_get_domain(key)
        except ElasticEmailError as e:
            dns_pendentes += 1
            dns_falhas.append({"dominio": key, "erro": str(e)})
            continue
        if not isinstance(created, dict):
            created = {}
        by_domain[key] = _aplicar_dns(key, created)

    for dominio, payload in by_domain.items():
        state = _mg_state_from_payload(payload)
        dns_ok = 1 if _dns_ok_from_payload(payload) else 0
        matches = por_dominio.get(dominio, [])
        ativos = [r for r in matches if r.get("ativo")]
        if ativos:
            cur.execute(
                """UPDATE campanha_dominios
                   SET mailgun_state = %s, dns_configured = %s
                   WHERE dominio = %s AND ativo = 1""",
                (state, dns_ok, dominio),
            )
            atualizados += 1
            continue
        if matches:
            mantidos_inativos += 1
            continue
        nome = dominio.replace(".", "_").replace("-", "_")
        if nome in nomes:
            nome = nome + "_ee"
        nomes.add(nome)
        cur.execute(
            """INSERT INTO campanha_dominios
               (nome, dominio, from_name, from_email, mailgun_state, dns_configured, ativo)
               VALUES (%s, %s, %s, %s, %s, %s, 1)""",
            (nome, dominio, "RED PRECATÓRIOS", f"contato@{dominio}", state, dns_ok),
        )
        inseridos += 1

    conn.commit()
    cur.close()
    conn.close()
    return {
        "ok": True,
        "provider": "elasticemail",
        "total_elasticemail": len(by_domain),
        "inseridos": inseridos,
        "atualizados": atualizados,
        "mantidos_inativos": mantidos_inativos,
        "ignorados_invalidos": ignorados_invalidos,
        "godaddy_configurado": _godaddy_ready(),
        "dns_ja_prontos": dns_ja_prontos,
        "dns_publicados": dns_publicados,
        "dns_pendentes": dns_pendentes,
        "dns_falhas": dns_falhas[:8],
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
    headers: dict | None = None,
    rastrear: bool = False,
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
    if headers:
        payload["Content"]["Headers"] = {str(k): str(v) for k, v in headers.items() if v}
    if rastrear:
        payload["Options"] = {"TrackOpens": True, "TrackClicks": True}
    return _ee_request("POST", "/emails/transactional", payload, timeout=timeout)
