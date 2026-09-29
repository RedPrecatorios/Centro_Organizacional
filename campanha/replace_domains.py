"""
Troca em lote os remetentes da campanha: Mailgun + DNS (GoDaddy se houver chave)
+ MySQL + [[domains]] no config.toml.

Uso (na raiz do projeto):

  python -m campanha.cli domains-status
  python -m campanha.cli replace-domains --file campanha/novos_dominios.txt --dry-run
  python -m campanha.cli replace-domains --file campanha/novos_dominios.txt
  python -m campanha.cli verify-domains
"""
from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path
from typing import Any

from campanha.api_dominios import (
    GoDaddyError,
    MailgunError,
    adicionar_dominio_completo,
    desativar_dominios_exceto,
    listar_dominios,
    mailgun_list_domains,
    verificar_dominio,
)
from campanha.api_elasticemail import (
    ElasticEmailError,
    adicionar_dominio_elasticemail,
    elasticemail_configured,
    elasticemail_list_domains,
    verificar_dominio_elasticemail,
)


def _godaddy_configured() -> bool:
    return bool((os.getenv("GODADDY_API_KEY") or "").strip() and (os.getenv("GODADDY_API_SECRET") or "").strip())


if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib

try:
    from dotenv import load_dotenv

    _REPO_ROOT = Path(__file__).resolve().parents[1]
    load_dotenv(_REPO_ROOT / ".env")
    load_dotenv()
except ImportError:
    pass

_DOMAIN_RE = re.compile(
    r"^(?=.{1,253}$)([a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,}$"
)


def _db() -> tuple[dict, str]:
    cfg = {
        "host": (os.getenv("EDA_MYSQL_HOST") or "localhost").strip(),
        "port": int(os.getenv("EDA_MYSQL_PORT", "3306") or "3306"),
        "user": (os.getenv("EDA_MYSQL_USER") or "root").strip(),
        "password": os.getenv("EDA_MYSQL_PASSWORD", "") or "",
        "connection_timeout": int(os.getenv("EDA_MYSQL_CONNECT_TIMEOUT", "15") or "15"),
    }
    db_name = (os.getenv("EDA_MYSQL_DATABASE") or "plataforma_central").strip()
    return cfg, db_name


def parse_domain_list(raw: str) -> list[str]:
    """Aceita um domínio por linha, vírgulas, ou espaços. Ignora # e vazio."""
    out: list[str] = []
    seen: set[str] = set()
    for line in (raw or "").replace(",", "\n").splitlines():
        s = line.strip().lstrip("\ufeff")
        if not s or s.startswith("#"):
            continue
        for tok in s.split():
            d = tok.strip().lower()
            if d.startswith("http://") or d.startswith("https://"):
                d = d.split("://", 1)[1]
            d = d.split("/", 1)[0].strip().rstrip(".")
            if d.startswith("www."):
                d = d[4:]
            if d and d not in seen:
                seen.add(d)
                out.append(d)
    return out


def _validate_domains(domains: list[str]) -> None:
    if not domains:
        raise ValueError("Nenhum domínio informado.")
    bad = [d for d in domains if not _DOMAIN_RE.match(d)]
    if bad:
        raise ValueError("Domínio(s) inválido(s): " + ", ".join(bad))


def _nome_chave(dominio: str) -> str:
    return dominio.split(".", 1)[0]


def _toml_escape(s: str) -> str:
    return s.replace("\\", "\\\\").replace('"', '\\"')


def _toml_domain_block(name: str, from_name: str, from_email: str) -> str:
    return (
        "[[domains]]\n"
        f'name = "{_toml_escape(name)}"\n'
        f'from_name = "{_toml_escape(from_name)}"\n'
        f'from_email = "{_toml_escape(from_email)}"\n'
        '# Com method = "mailgun" não precisa preencher SMTP abaixo.\n'
        'smtp_host = ""\n'
        "smtp_port = 587\n"
        'smtp_user = ""\n'
        'smtp_password = ""\n'
        "smtp_starttls = true\n"
    )


def rewrite_config_toml_domains(
    config_path: str,
    domains: list[dict[str, str]],
) -> None:
    """Substitui todos os [[domains]] pelos novos, preservando o resto do TOML."""
    p = Path(config_path)
    text = p.read_text(encoding="utf-8") if p.exists() else ""
    idx = text.find("[[domains]]")
    prefix = text[:idx].rstrip() + "\n\n" if idx >= 0 else (text.rstrip() + "\n\n" if text.strip() else "")
    blocks = "\n".join(
        _toml_domain_block(d["name"], d["from_name"], d["from_email"]) for d in domains
    )
    p.write_text(prefix + blocks, encoding="utf-8")


def _toml_domains(config_path: str) -> list[str]:
    p = Path(config_path)
    if not p.exists():
        return []
    data = tomllib.loads(p.read_text(encoding="utf-8"))
    out: list[str] = []
    for d in data.get("domains") or []:
        fe = str(d.get("from_email") or "")
        if "@" in fe:
            out.append(fe.split("@", 1)[1].strip().lower())
    return out


def domains_status(config_path: str = "campanha/config.toml") -> dict[str, Any]:
    cfg, db = _db()
    mysql_rows: list[dict] = []
    mysql_error = None
    try:
        mysql_rows = listar_dominios(cfg, db)
    except Exception as e:
        mysql_error = str(e)

    mg_items: list[dict] = []
    mg_error = None
    try:
        if os.getenv("MAILGUN_API_KEY", "").strip():
            mg_items = mailgun_list_domains()
        else:
            mg_error = "MAILGUN_API_KEY vazia"
    except MailgunError as e:
        mg_error = str(e)

    ee_items: list[dict] = []
    ee_error = None
    try:
        if elasticemail_configured():
            ee_items = elasticemail_list_domains()
        else:
            ee_error = "ELASTICEMAIL_API_KEY vazia"
    except ElasticEmailError as e:
        ee_error = str(e)

    return {
        "toml": _toml_domains(config_path),
        "mysql": [
            {
                "nome": r.get("nome"),
                "dominio": r.get("dominio"),
                "from_email": r.get("from_email"),
                "mailgun_state": r.get("mailgun_state"),
                "dns_configured": r.get("dns_configured"),
            }
            for r in mysql_rows
        ],
        "mysql_error": mysql_error,
        "mailgun": [
            {"name": i.get("name"), "state": i.get("state")} for i in mg_items
        ],
        "mailgun_error": mg_error,
        "elasticemail": [
            {
                "name": (i.get("Domain") or i.get("name") or i.get("domain")),
                "verify": i.get("Verify"),
                "spf": i.get("Spf"),
                "dkim": i.get("Dkim"),
            }
            for i in ee_items
            if isinstance(i, dict)
        ],
        "elasticemail_error": ee_error,
        "provider": "elasticemail" if elasticemail_configured() else "mailgun",
        "godaddy_configured": _godaddy_configured(),
        "database": db,
    }


def replace_domains(
    domains: list[str],
    *,
    config_path: str = "campanha/config.toml",
    from_name: str = "RED PRECATÓRIOS",
    from_local: str = "contato",
    reply_to: str = "contato@redprecatorios.com.br",
    deactivate_old: bool = True,
    delete_old_mailgun: bool = False,
    update_toml: bool = True,
    verify: bool = True,
    dry_run: bool = False,
) -> dict[str, Any]:
    domains = parse_domain_list("\n".join(domains)) if domains else []
    _validate_domains(domains)

    cfg, db = _db()
    specs = []
    for dominio in domains:
        specs.append(
            {
                "nome": _nome_chave(dominio),
                "dominio": dominio,
                "from_name": from_name,
                "from_email": f"{from_local}@{dominio}",
                "reply_to": reply_to or None,
            }
        )

    current_mysql = []
    try:
        current_mysql = [r.get("dominio") for r in listar_dominios(cfg, db)]
    except Exception as e:
        current_mysql = [f"(erro ao ler MySQL: {e})"]

    plan = {
        "novos": [s["dominio"] for s in specs],
        "mysql_ativos_hoje": current_mysql,
        "toml_hoje": _toml_domains(config_path),
        "desativar_antigos": deactivate_old,
        "apagar_antigos_no_mailgun": delete_old_mailgun,
        "atualizar_toml": update_toml,
        "verificar_mailgun": verify,
        "provider": "elasticemail" if elasticemail_configured() else "mailgun",
        "godaddy_configurado": _godaddy_configured(),
        "database": db,
        "dry_run": dry_run,
    }
    if dry_run:
        return {"ok": True, "dry_run": True, "plan": plan}

    use_ee = elasticemail_configured()
    added: list[dict] = []
    errors: list[dict] = []
    for s in specs:
        try:
            if use_ee:
                result = adicionar_dominio_elasticemail(
                    s["dominio"],
                    s["nome"],
                    s["from_name"],
                    s["from_email"],
                    s["reply_to"],
                    cfg,
                    db,
                )
                if verify:
                    try:
                        v = verificar_dominio_elasticemail(s["dominio"], cfg, db)
                        result["mailgun_state"] = v.get("mailgun_state", result.get("mailgun_state"))
                        result["dns_records"] = v.get("dns_records") or result.get("dns_records")
                    except ElasticEmailError as e:
                        result["verify_error"] = str(e)
            else:
                result = adicionar_dominio_completo(
                    s["dominio"],
                    s["nome"],
                    s["from_name"],
                    s["from_email"],
                    s["reply_to"],
                    cfg,
                    db,
                )
                if verify:
                    try:
                        v = verificar_dominio(s["dominio"], cfg, db)
                        result["mailgun_state"] = v.get("mailgun_state", result.get("mailgun_state"))
                        result["dns_records"] = v.get("dns_records") or result.get("dns_records")
                    except MailgunError as e:
                        result["verify_error"] = str(e)
            added.append(result)
        except (MailgunError, GoDaddyError, ElasticEmailError, Exception) as e:
            errors.append({"dominio": s["dominio"], "error": str(e)})

    keep = {s["dominio"] for s in specs}
    removed: list[dict] = []
    skipped_remove = False
    if deactivate_old:
        if errors:
            skipped_remove = True
        else:
            removed = desativar_dominios_exceto(
                keep, cfg, db, delete_mailgun=delete_old_mailgun
            )

    toml_written = False
    if update_toml and not errors:
        rewrite_config_toml_domains(
            config_path,
            [
                {
                    "name": s["nome"],
                    "from_name": s["from_name"],
                    "from_email": s["from_email"],
                }
                for s in specs
            ],
        )
        toml_written = True

    return {
        "ok": not errors,
        "plan": plan,
        "added": added,
        "errors": errors,
        "removed": removed,
        "skipped_remove_because_errors": skipped_remove,
        "toml_written": toml_written,
    }


def verify_all_active() -> dict[str, Any]:
    cfg, db = _db()
    rows = listar_dominios(cfg, db)
    out: list[dict] = []
    use_ee = elasticemail_configured()
    for r in rows:
        dominio = r.get("dominio") or ""
        try:
            if use_ee:
                out.append(verificar_dominio_elasticemail(dominio, cfg, db))
            else:
                out.append(verificar_dominio(dominio, cfg, db))
        except (MailgunError, ElasticEmailError) as e:
            out.append({"ok": False, "dominio": dominio, "error": str(e)})
    return {"ok": True, "verificados": out}


def _print_status(st: dict) -> None:
    gd = "sim" if st.get("godaddy_configured") else "nao (DNS tera de ser manual)"
    print(f"database={st.get('database')}  godaddy={gd}  provider={st.get('provider')}")
    print("\nTOML:")
    for d in st.get("toml") or []:
        print(f"  - {d}")
    if not st.get("toml"):
        print("  (vazio)")
    print("\nMySQL (ativos):")
    if st.get("mysql_error"):
        print(f"  erro: {st['mysql_error']}")
    elif not st.get("mysql"):
        print("  (vazio)")
    else:
        for r in st["mysql"]:
            print(
                f"  - {r.get('dominio')}  from={r.get('from_email')}  "
                f"mailgun={r.get('mailgun_state')}  dns={r.get('dns_configured')}"
            )
    print("\nMailgun:")
    if st.get("mailgun_error"):
        print(f"  erro: {st['mailgun_error']}")
    elif not st.get("mailgun"):
        print("  (vazio)")
    else:
        for r in st["mailgun"]:
            print(f"  - {r.get('name')}  state={r.get('state')}")
    print("\nElastic Email:")
    if st.get("elasticemail_error"):
        print(f"  erro: {st['elasticemail_error']}")
    elif not st.get("elasticemail"):
        print("  (vazio)")
    else:
        for r in st["elasticemail"]:
            print(
                f"  - {r.get('name')}  verify={r.get('verify')}  "
                f"spf={r.get('spf')}  dkim={r.get('dkim')}"
            )


def _print_replace(result: dict) -> None:
    print(json.dumps(result.get("plan"), ensure_ascii=False, indent=2))
    if result.get("dry_run"):
        print("\n[dry-run] Nada foi alterado.")
        return
    print("\nAdicionados:")
    for a in result.get("added") or []:
        recs = a.get("dns_records") or []
        print(
            f"  + {a.get('dominio')}  mailgun={a.get('mailgun_state')}  "
            f"dns_ok={a.get('dns_configured')}  dns_error={a.get('dns_error')}"
        )
        if a.get("dns_error") and recs:
            print("    registros DNS do Mailgun (configure no provedor):")
            for rec in recs:
                print(
                    f"      {rec.get('type')}  {rec.get('name')}  "
                    f"{rec.get('value')}  prio={rec.get('priority')}"
                )
    if result.get("errors"):
        print("\nFalhas (antigos NAO foram desativados, TOML NAO foi reescrito):")
        for e in result["errors"]:
            print(f"  ! {e.get('dominio')}: {e.get('error')}")
    print("\nDesativados:")
    if result.get("skipped_remove_because_errors"):
        print("  (adiado por falha nos novos)")
    elif not result.get("removed"):
        print("  (nenhum)")
    else:
        for r in result["removed"]:
            print(
                f"  - {r.get('dominio')}  mailgun_deleted={r.get('mailgun_deleted')}  "
                f"err={r.get('mailgun_error')}"
            )
    print(f"\ntoml_written={result.get('toml_written')}  ok={result.get('ok')}")
