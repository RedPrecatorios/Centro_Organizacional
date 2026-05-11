from __future__ import annotations

import csv
import hashlib
import json
import os
import smtplib
import sys
import ssl
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from email.message import EmailMessage
from pathlib import Path
from typing import Iterable

import mysql.connector

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


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _norm_email(email: str) -> str:
    return (email or "").strip().upper()


def _read_text(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")


def _render_template(template_text: str, vars: dict[str, str]) -> str:
    # Template mínimo, deliberadamente simples: {{var}}
    out = template_text
    for k, v in vars.items():
        out = out.replace("{{" + k + "}}", str(v))
    return out


@dataclass(frozen=True)
class Recipient:
    name: str
    email: str
    fields: dict[str, str]


@dataclass(frozen=True)
class DomainSender:
    name: str
    from_name: str
    from_email: str
    # Se preenchido, coloca Reply-To (respostas vão para este endereço). Se vazio, usa [sending].reply_to.
    reply_to: str = ""
    # SMTP (usado quando sending.method = "smtp")
    smtp_host: str = ""
    smtp_port: int = 587
    smtp_user: str = ""
    smtp_password: str = ""
    smtp_starttls: bool = True


@dataclass(frozen=True)
class MysqlConfig:
    host: str
    port: int
    user: str
    password: str
    database: str
    connection_timeout: int = 15


@dataclass(frozen=True)
class SendingConfig:
    dry_run: bool
    per_domain_per_minute: int
    smtp_timeout_seconds: int
    max_retries: int
    method: str  # "smtp" | "mailgun"
    # Respostas do cliente: endereço que recebe ao clicar "Responder" (cabeçalho Reply-To).
    reply_to: str | None


@dataclass(frozen=True)
class MailgunConfig:
    api_key: str
    region: str  # "us" | "eu"

    @property
    def api_base(self) -> str:
        return "https://api.eu.mailgun.net" if self.region.lower() == "eu" else "https://api.mailgun.net"


@dataclass(frozen=True)
class ContentConfig:
    subject: str
    html_template: str
    text_template: str
    vars: dict[str, str]


@dataclass(frozen=True)
class BlacklistConfig:
    use_db: bool
    extra_email_file: str | None


@dataclass(frozen=True)
class CampaignEmailsLogConfig:
    """Se habilitado, atualiza linhas na tabela `emails` cujo campo `email` bate com o destinatário (mesmo DB)."""

    enabled: bool


def _ensure_emails_campanha_columns_mysql(conn) -> None:
    cur = conn.cursor()
    try:
        cols: list[tuple[str, str]] = [
            (
                "campanha_disparo_status",
                "VARCHAR(40) DEFAULT NULL COMMENT 'sent,failed,skipped_blacklist,skipped_duplicate'",
            ),
            ("campanha_disparo_erro", "TEXT DEFAULT NULL COMMENT 'Erro quando status=failed'"),
            (
                "campanha_disparo_data_entrada",
                "DATETIME(3) DEFAULT NULL COMMENT 'Primeiro disparo registrado pela campanha'",
            ),
            ("campanha_disparo_ultimo", "DATETIME(3) DEFAULT NULL COMMENT 'Ultimo disparo registrado pela campanha'"),
            ("campanha_disparo_campaign_id", "VARCHAR(191) DEFAULT NULL"),
            ("campanha_disparo_dry_run", "TINYINT(1) DEFAULT NULL COMMENT '1=simulacao'"),
            ("campanha_disparo_dominio", "VARCHAR(128) DEFAULT NULL COMMENT 'Chave domains do disparo'"),
            ("campanha_disparo_remetente", "VARCHAR(320) DEFAULT NULL COMMENT 'Remetente from'"),
        ]
        for col_name, col_type in cols:
            cur.execute(
                """
                SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE()
                  AND TABLE_NAME = 'emails'
                  AND COLUMN_NAME = %s
                """,
                (col_name,),
            )
            if cur.fetchone()[0] == 0:
                cur.execute(f"ALTER TABLE emails ADD COLUMN `{col_name}` {col_type}")
        conn.commit()
    finally:
        cur.close()


def _update_emails_campanha_log(
    conn,
    *,
    email_norm_upper: str,
    status: str,
    erro: str | None,
    campaign_id: str,
    dry_run: bool,
    dominio: str | None,
    remetente: str | None,
) -> None:
    cur = conn.cursor()
    sql = """
    UPDATE emails SET
      campanha_disparo_status = %s,
      campanha_disparo_erro = %s,
      campanha_disparo_ultimo = CURRENT_TIMESTAMP(3),
      campanha_disparo_campaign_id = %s,
      campanha_disparo_dry_run = %s,
      campanha_disparo_dominio = %s,
      campanha_disparo_remetente = %s,
      campanha_disparo_data_entrada = COALESCE(
        campanha_disparo_data_entrada, CURRENT_TIMESTAMP(3)
      )
    WHERE email IS NOT NULL AND UPPER(TRIM(email)) = %s
    """
    err = erro[:65535] if erro else None
    cur.execute(
        sql,
        (
            status[:40],
            err,
            campaign_id[:191],
            1 if dry_run else 0,
            (dominio[:128] if dominio else None),
            (remetente[:320] if remetente else None),
            email_norm_upper[:300],
        ),
    )
    conn.commit()
    cur.close()


@dataclass(frozen=True)
class CampaignConfig:
    project_name: str
    mysql: MysqlConfig
    blacklist: BlacklistConfig
    campaign_emails_log: CampaignEmailsLogConfig
    sending: SendingConfig
    mailgun: MailgunConfig | None
    content: ContentConfig
    domains: list[DomainSender]
    # Caminho absoluto do TOML; usado para resolver caminhos relativos mesmo fora da raiz do repo.
    source_config_path: str = ""
    # Se true, grava `campanha/logs/<campaign_id>.jsonl`. Se false, não cria arquivo de log.
    jsonl_log_enabled: bool = True


def _env_strip(key: str) -> str | None:
    raw = os.environ.get(key)
    if raw is None:
        return None
    v = raw.strip().strip('"').strip("'")
    return v or None


def _mysql_from_toml_with_env_overlay(db_section: dict) -> MysqlConfig:
    """Mescla [db.mysql] com CAMPANHA_MYSQL_* (prioridade) e EDA_MYSQL_* (Mesmo projeto EDA Diário)."""
    host = str(db_section.get("host", "localhost"))
    port = int(db_section.get("port", 3306))
    user = str(db_section.get("user", "root"))
    password = str(db_section.get("password", "") or "")
    database = str(db_section.get("database", "eda_diario"))

    eh = _env_strip("CAMPANHA_MYSQL_HOST") or _env_strip("EDA_MYSQL_HOST")
    if eh:
        host = eh

    ep = _env_strip("CAMPANHA_MYSQL_PORT") or _env_strip("EDA_MYSQL_PORT")
    if ep:
        try:
            port = int(ep)
        except ValueError:
            pass

    eu = _env_strip("CAMPANHA_MYSQL_USER") or _env_strip("EDA_MYSQL_USER")
    if eu:
        user = eu

    if "CAMPANHA_MYSQL_PASSWORD" in os.environ:
        password = os.environ["CAMPANHA_MYSQL_PASSWORD"] or ""
    elif "EDA_MYSQL_PASSWORD" in os.environ:
        password = os.environ["EDA_MYSQL_PASSWORD"] or ""

    edb = _env_strip("CAMPANHA_MYSQL_DATABASE") or _env_strip("EDA_MYSQL_DATABASE")
    if edb:
        database = edb

    timeout = 15
    for key in ("CAMPANHA_MYSQL_CONNECT_TIMEOUT", "EDA_MYSQL_CONNECT_TIMEOUT"):
        ts = _env_strip(key)
        if ts:
            try:
                timeout = max(1, int(ts))
            except ValueError:
                pass
            break

    return MysqlConfig(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        connection_timeout=timeout,
    )


def _mysql_connect_kwargs(mc: MysqlConfig) -> dict:
    return {
        "host": mc.host,
        "port": mc.port,
        "user": mc.user,
        "password": mc.password,
        "database": mc.database,
        "connection_timeout": mc.connection_timeout,
    }


def _workspace_root_from_config(abs_config_file: Path) -> Path:
    """Se o TOML estiver em `<repo>/campanha/config.toml`, retorna `<repo>`; caso contrário, pasta do arquivo."""
    p = abs_config_file.resolve()
    if p.parent.name == "campanha":
        return p.parent.parent
    return p.parent


def _resolve_file_path_behind_config(abs_config_file: Path, relative: str) -> str:
    """Resolve caminho relativo (templates, blacklist) a partir da raiz do projeto ou cwd."""
    p = Path(relative)
    if p.is_absolute():
        return str(p)
    ws = _workspace_root_from_config(abs_config_file)
    searches = [(ws / relative).resolve(), (abs_config_file.parent / relative).resolve(), (Path.cwd() / relative).resolve()]
    for c in searches:
        if c.is_file():
            return str(c)
    # fallback: raiz esperada para mensagem FileNotFound clara ao ler template
    return str((ws / relative).resolve())


def _resolve_writable_relative_path_behind_config(abs_config_file: Path, relative: str) -> str:
    """Pastas estado/logs relativas a `campanha/`: preferimos a cópia sob a raiz do workspace."""
    p = Path(relative)
    if p.is_absolute():
        return str(p.resolve())
    ws = _workspace_root_from_config(abs_config_file)
    c_ws = (ws / relative).resolve()
    return str(c_ws)


def load_config_toml(path: str) -> CampaignConfig:
    abs_cfg = Path(path).resolve()

    data = tomllib.loads(abs_cfg.read_text(encoding="utf-8"))
    project_name = str(data.get("project", {}).get("name", "campanha"))

    db = data.get("db", {}).get("mysql", {})
    mysql = _mysql_from_toml_with_env_overlay(db)

    bl = data.get("blacklist", {})
    extra_raw = str(bl["extra_email_file"]).strip() if bl.get("extra_email_file") else None
    extra_resolved = _resolve_file_path_behind_config(abs_cfg, extra_raw) if extra_raw else None
    blacklist = BlacklistConfig(use_db=bool(bl.get("use_db", True)), extra_email_file=extra_resolved)

    log_em = data.get("campaign_emails_log", {}) or {}
    campaign_emails_log = CampaignEmailsLogConfig(enabled=bool(log_em.get("enabled", False)))

    logging_raw = data.get("logging", {}) or {}
    jsonl_log_enabled = logging_raw.get("jsonl_log_enabled")
    if jsonl_log_enabled is None:
        # Padrão: se você já grava no banco, evita duplicar em arquivo.
        jsonl_log_enabled = not campaign_emails_log.enabled
    jsonl_log_enabled = bool(jsonl_log_enabled)

    sending_raw = data.get("sending", {})
    rt = sending_raw.get("reply_to")
    reply_to_global = str(rt).strip() if rt is not None and str(rt).strip() else None

    sending = SendingConfig(
        dry_run=bool(sending_raw.get("dry_run", True)),
        per_domain_per_minute=int(sending_raw.get("per_domain_per_minute", 60)),
        smtp_timeout_seconds=int(sending_raw.get("smtp_timeout_seconds", 30)),
        max_retries=int(sending_raw.get("max_retries", 3)),
        method=str(sending_raw.get("method", "smtp")).strip().lower() or "smtp",
        reply_to=reply_to_global,
    )

    mailgun: MailgunConfig | None = None
    mailgun_raw = data.get("mailgun", {}) or {}
    if sending.method == "mailgun":
        # Preferência: variável de ambiente (não commitar chave no TOML).
        env_key = os.environ.get("MAILGUN_API_KEY", "").strip()
        api_key = env_key or str(mailgun_raw.get("api_key", "")).strip()
        region = str(mailgun_raw.get("region", "us")).strip().lower() or "us"
        if not api_key:
            raise ValueError(
                "Config inválida: com sending.method = 'mailgun' defina MAILGUN_API_KEY no .env "
                "(recomendado) ou preencha [mailgun].api_key no config.toml."
            )
        if region not in ("us", "eu"):
            raise ValueError("Config inválida: [mailgun].region deve ser 'us' ou 'eu'.")
        mailgun = MailgunConfig(api_key=api_key, region=region)

    content_raw = data.get("content", {})
    vars_raw = content_raw.get("vars", {}) or {}
    html_rel = str(content_raw.get("html_template", "campanha/templates/default.html"))
    text_rel = str(content_raw.get("text_template", "campanha/templates/default.txt"))
    content = ContentConfig(
        subject=str(content_raw.get("subject", "")),
        html_template=_resolve_file_path_behind_config(abs_cfg, html_rel),
        text_template=_resolve_file_path_behind_config(abs_cfg, text_rel),
        vars={str(k): str(v) for k, v in vars_raw.items()},
    )

    domains_raw = data.get("domains", [])
    domains: list[DomainSender] = []
    for d in domains_raw:
        domains.append(
            DomainSender(
                name=str(d["name"]),
                from_name=str(d["from_name"]),
                from_email=str(d["from_email"]),
                reply_to=str(d.get("reply_to", "") or "").strip(),
                smtp_host=str(d.get("smtp_host", "")),
                smtp_port=int(d.get("smtp_port", 587)),
                smtp_user=str(d.get("smtp_user", "")),
                smtp_password=str(d.get("smtp_password", "")),
                smtp_starttls=bool(d.get("smtp_starttls", True)),
            )
        )

    if not domains:
        raise ValueError("Config inválida: você precisa declarar ao menos 1 item em [[domains]].")

    if not content.subject:
        raise ValueError("Config inválida: defina [content].subject.")

    return CampaignConfig(
        project_name=project_name,
        mysql=mysql,
        blacklist=blacklist,
        campaign_emails_log=campaign_emails_log,
        sending=sending,
        mailgun=mailgun,
        content=content,
        domains=domains,
        source_config_path=str(abs_cfg),
        jsonl_log_enabled=jsonl_log_enabled,
    )


def load_recipients_csv(path: str) -> list[Recipient]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Arquivo de recipients não encontrado: {path}")
    out: list[Recipient] = []
    with p.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Aceita cabeçalhos no padrão atual da operação: Email, nome, processo
            # e também compatibilidade retro: name,email.
            row_norm: dict[str, str] = {}
            for k, v in row.items():
                if not k:
                    continue
                kk = str(k).strip().lower()
                vv = "" if v is None else str(v).strip()
                row_norm[kk] = vv

            email = (row_norm.get("email") or row_norm.get("e-mail") or "").strip()
            name = (row_norm.get("nome") or row_norm.get("name") or "").strip()
            if not email:
                continue
            fields: dict[str, str] = {}
            for k, v in row.items():
                if not k:
                    continue
                kk_raw = str(k).strip()
                kk = kk_raw.lower()
                if kk in ("name", "nome", "email", "e-mail"):
                    continue
                vv = "" if v is None else str(v).strip()
                if vv:
                    # Mantém a chave original para usar no template como {{processo}}, {{credor}}, etc.
                    fields[kk_raw] = vv
            out.append(Recipient(name=name or email, email=email, fields=fields))
    return out


def load_blacklist_emails(mysql_cfg: MysqlConfig, use_db: bool, extra_email_file: str | None) -> set[str]:
    blocked: set[str] = set()

    if use_db:
        try:
            conn = mysql.connector.connect(**_mysql_connect_kwargs(mysql_cfg))
        except mysql.connector.Error as e:
            raise RuntimeError(
                "Campanha: não foi possível conectar ao MySQL para carregar blacklist. "
                "Ajuste [db.mysql] no config ou defina variáveis CAMPANHA_MYSQL_* ou EDA_MYSQL_* no .env "
                "(as variáveis de ambiente sobrescrevem o TOML). "
                f"Ou defina blacklist.use_db = false para ignorar blacklist do banco. Detalhes: {e}"
            ) from e
        cur = conn.cursor(buffered=True)
        cur.execute("SELECT valor FROM blacklist WHERE ativo = 1 AND tipo = 'EMAIL'")
        for (valor,) in cur.fetchall():
            n = _norm_email(str(valor))
            if n:
                blocked.add(n)
        cur.close()
        conn.close()

    if extra_email_file:
        p = Path(extra_email_file)
        if p.exists():
            for line in p.read_text(encoding="utf-8").splitlines():
                s = line.strip()
                if not s or s.startswith("#"):
                    continue
                n = _norm_email(s)
                if n:
                    blocked.add(n)

    return blocked


class JsonlLogger:
    def __init__(self, path: str) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def write(self, event: dict) -> None:
        event = dict(event)
        event.setdefault("ts", _utc_now_iso())
        self.path.open("a", encoding="utf-8").write(json.dumps(event, ensure_ascii=False) + "\n")


class NullLogger:
    def write(self, event: dict) -> None:
        return


def _idempotency_key(campaign_id: str, recipient_email: str, subject: str) -> str:
    s = f"{campaign_id}|{_norm_email(recipient_email)}|{subject}"
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:24]


def _load_sent_keys(path: str) -> set[str]:
    p = Path(path)
    if not p.exists():
        return set()
    out: set[str] = set()
    for line in p.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if s:
            out.add(s)
    return out


def _append_sent_key(path: str, key: str) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.open("a", encoding="utf-8").write(key + "\n")


def _smtp_send(domain: DomainSender, cfg: SendingConfig, msg: EmailMessage) -> None:
    if cfg.dry_run:
        return

    if not domain.smtp_host:
        raise ValueError(f"SMTP não configurado para o domínio '{domain.name}' (smtp_host vazio).")

    context = ssl.create_default_context()
    with smtplib.SMTP(domain.smtp_host, domain.smtp_port, timeout=cfg.smtp_timeout_seconds) as server:
        server.ehlo()
        if domain.smtp_starttls:
            server.starttls(context=context)
            server.ehlo()
        if domain.smtp_user:
            server.login(domain.smtp_user, domain.smtp_password)
        server.send_message(msg)


def _mailgun_send(domain: DomainSender, cfg: SendingConfig, mg: MailgunConfig, msg: EmailMessage) -> None:
    if cfg.dry_run:
        return

    # O domínio usado na URL do Mailgun precisa existir/verificado na conta.
    # Por padrão, usamos o domínio do from_email.
    try:
        sending_domain = domain.from_email.split("@", 1)[1].strip().lower()
    except Exception:
        raise ValueError(f"from_email inválido para domínio '{domain.name}': {domain.from_email!r}")
    if not sending_domain:
        raise ValueError(f"from_email inválido para domínio '{domain.name}': {domain.from_email!r}")

    url = f"{mg.api_base}/v3/{sending_domain}/messages"

    # Extrai partes relevantes do EmailMessage
    to_addr = str(msg["To"])
    subject = str(msg["Subject"])
    from_addr = str(msg["From"])

    text = msg.get_body(preferencelist=("plain",))
    html = msg.get_body(preferencelist=("html",))
    text_content = text.get_content() if text else ""
    html_content = html.get_content() if html else ""

    payload = {
        "from": from_addr,
        "to": to_addr,
        "subject": subject,
        "text": text_content,
        "html": html_content,
    }

    # Headers: X-* e Reply-To (respostas) via h: no Mailgun
    for k, v in msg.items():
        lk = str(k).lower()
        if lk.startswith("x-") or lk == "reply-to":
            payload[f"h:{k}"] = str(v)

    data = urllib.parse.urlencode(payload).encode("utf-8")
    req = urllib.request.Request(url=url, data=data, method="POST")
    auth = (("api:" + mg.api_key).encode("utf-8"))
    req.add_header("Authorization", "Basic " + __import__("base64").b64encode(auth).decode("ascii"))
    req.add_header("Content-Type", "application/x-www-form-urlencoded")

    try:
        with urllib.request.urlopen(req, timeout=cfg.smtp_timeout_seconds) as resp:
            _ = resp.read()
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace") if hasattr(e, "read") else str(e)
        raise RuntimeError(f"Mailgun HTTP {e.code}: {body}") from e


def _build_message(
    domain: DomainSender,
    to_email: str,
    subject: str,
    html: str,
    text: str,
    headers: dict[str, str] | None = None,
    reply_to: str | None = None,
) -> EmailMessage:
    msg = EmailMessage()
    msg["From"] = f"{domain.from_name} <{domain.from_email}>"
    msg["To"] = to_email
    msg["Subject"] = subject
    msg["Date"] = datetime.now().strftime("%a, %d %b %Y %H:%M:%S %z")
    if reply_to:
        msg["Reply-To"] = reply_to
    if headers:
        for k, v in headers.items():
            msg[k] = v

    msg.set_content(text)
    msg.add_alternative(html, subtype="html")
    return msg


def _effective_reply_to(domain: DomainSender, sending: SendingConfig) -> str | None:
    d = (domain.reply_to or "").strip()
    if d:
        return d
    g = (sending.reply_to or "").strip() if sending.reply_to else ""
    return g or None


def _round_robin(items: list[DomainSender]) -> Iterable[DomainSender]:
    i = 0
    while True:
        yield items[i % len(items)]
        i += 1


def run_campaign(
    cfg: CampaignConfig,
    recipients: list[Recipient],
    *,
    campaign_id: str,
    log_dir: str = "campanha/logs",
    sent_keys_file: str = "campanha/state/sent_keys.txt",
) -> dict[str, int]:
    cfg_path = Path(cfg.source_config_path) if (cfg.source_config_path and Path(cfg.source_config_path).is_file()) else None
    if cfg_path:
        log_dir = _resolve_writable_relative_path_behind_config(cfg_path, log_dir)
        sent_keys_file = _resolve_writable_relative_path_behind_config(cfg_path, sent_keys_file)

    logger = JsonlLogger(str(Path(log_dir) / f"{campaign_id}.jsonl")) if cfg.jsonl_log_enabled else NullLogger()

    blocked = load_blacklist_emails(cfg.mysql, cfg.blacklist.use_db, cfg.blacklist.extra_email_file)
    sent_keys = _load_sent_keys(sent_keys_file)

    html_t = _read_text(cfg.content.html_template)
    text_t = _read_text(cfg.content.text_template)

    # rate limit simples (por domínio): espaçamento mínimo entre envios
    per_min = max(1, int(cfg.sending.per_domain_per_minute))
    min_interval = 60.0 / per_min
    last_sent_at: dict[str, float] = {d.name: 0.0 for d in cfg.domains}

    rr = _round_robin(cfg.domains)

    emails_log_conn = None
    try:
        if cfg.campaign_emails_log.enabled:
            emails_log_conn = mysql.connector.connect(**_mysql_connect_kwargs(cfg.mysql))
            _ensure_emails_campanha_columns_mysql(emails_log_conn)

        totals = {
            "input": len(recipients),
            "skipped_blacklist": 0,
            "skipped_duplicate": 0,
            "sent": 0,
            "failed": 0,
        }

        for r in recipients:
            to_norm = _norm_email(r.email)
            if not to_norm:
                continue
            if to_norm in blocked:
                totals["skipped_blacklist"] += 1
                logger.write(
                    {
                        "event": "skip_blacklist",
                        "campaign_id": campaign_id,
                        "to": r.email,
                        "name": r.name,
                    }
                )
                if emails_log_conn:
                    try:
                        _update_emails_campanha_log(
                            emails_log_conn,
                            email_norm_upper=to_norm,
                            status="skipped_blacklist",
                            erro=None,
                            campaign_id=campaign_id,
                            dry_run=cfg.sending.dry_run,
                            dominio=None,
                            remetente=None,
                        )
                    except mysql.connector.Error as e:
                        logger.write(
                            {
                                "event": "emails_table_log_failed",
                                "campaign_id": campaign_id,
                                "error": str(e),
                            }
                        )
                continue

            key = _idempotency_key(campaign_id, r.email, cfg.content.subject)
            if key in sent_keys:
                totals["skipped_duplicate"] += 1
                logger.write(
                    {
                        "event": "skip_duplicate",
                        "campaign_id": campaign_id,
                        "to": r.email,
                        "name": r.name,
                        "key": key,
                    }
                )
                if emails_log_conn:
                    try:
                        _update_emails_campanha_log(
                            emails_log_conn,
                            email_norm_upper=to_norm,
                            status="skipped_duplicate",
                            erro=None,
                            campaign_id=campaign_id,
                            dry_run=cfg.sending.dry_run,
                            dominio=None,
                            remetente=None,
                        )
                    except mysql.connector.Error as e:
                        logger.write(
                            {
                                "event": "emails_table_log_failed",
                                "campaign_id": campaign_id,
                                "error": str(e),
                            }
                        )
                continue

            domain = next(rr)

            # rate limit por domínio (espere o suficiente desde o último envio)
            now = time.time()
            delta = now - last_sent_at[domain.name]
            if delta < min_interval:
                time.sleep(min_interval - delta)

            vars_ = dict(cfg.content.vars)
            vars_.update(
                {
                    "project_name": cfg.project_name,
                    "subject": cfg.content.subject,
                    "name": r.name,
                    "email": r.email,
                    # Compatibilidade com templates antigos que usam {{credor}} no saudação.
                    # No CSV atual, o equivalente é a coluna "nome".
                    "credor": r.name,
                }
            )
            vars_.update(r.fields)
            html = _render_template(html_t, vars_)
            text = _render_template(text_t, vars_)

            msg = _build_message(
                domain=domain,
                to_email=r.email,
                subject=cfg.content.subject,
                html=html,
                text=text,
                headers={"X-Campaign-Id": campaign_id, "X-Idempotency-Key": key},
                reply_to=_effective_reply_to(domain, cfg.sending),
            )

            ok = False
            error: str | None = None
            for attempt in range(1, cfg.sending.max_retries + 1):
                try:
                    if cfg.sending.method == "mailgun":
                        if not cfg.mailgun:
                            raise ValueError("Mailgun não configurado (cfg.mailgun=None).")
                        _mailgun_send(domain, cfg.sending, cfg.mailgun, msg)
                    else:
                        _smtp_send(domain, cfg.sending, msg)
                    ok = True
                    break
                except (smtplib.SMTPServerDisconnected, smtplib.SMTPConnectError, TimeoutError) as e:
                    error = f"{type(e).__name__}: {e}"
                    time.sleep(min(10, 2 * attempt))
                except smtplib.SMTPException as e:
                    error = f"{type(e).__name__}: {e}"
                    # erros SMTP genéricos: ainda pode ser transitório, então respeita retries
                    time.sleep(min(10, 2 * attempt))
                except Exception as e:  # não esperado
                    error = f"{type(e).__name__}: {e}"
                    break

            last_sent_at[domain.name] = time.time()

            if ok:
                totals["sent"] += 1
                sent_keys.add(key)
                _append_sent_key(sent_keys_file, key)
                logger.write(
                    {
                        "event": "sent",
                        "campaign_id": campaign_id,
                        "domain": domain.name,
                        "from_email": domain.from_email,
                        "to": r.email,
                        "name": r.name,
                        "key": key,
                        "dry_run": cfg.sending.dry_run,
                    }
                )
                if emails_log_conn:
                    try:
                        _update_emails_campanha_log(
                            emails_log_conn,
                            email_norm_upper=to_norm,
                            status="sent",
                            erro=None,
                            campaign_id=campaign_id,
                            dry_run=cfg.sending.dry_run,
                            dominio=domain.name,
                            remetente=domain.from_email,
                        )
                    except mysql.connector.Error as e:
                        logger.write(
                            {
                                "event": "emails_table_log_failed",
                                "campaign_id": campaign_id,
                                "error": str(e),
                            }
                        )
            else:
                totals["failed"] += 1
                logger.write(
                    {
                        "event": "failed",
                        "campaign_id": campaign_id,
                        "domain": domain.name,
                        "from_email": domain.from_email,
                        "to": r.email,
                        "name": r.name,
                        "key": key,
                        "error": error,
                        "dry_run": cfg.sending.dry_run,
                    }
                )
                if emails_log_conn:
                    try:
                        _update_emails_campanha_log(
                            emails_log_conn,
                            email_norm_upper=to_norm,
                            status="failed",
                            erro=str(error) if error else None,
                            campaign_id=campaign_id,
                            dry_run=cfg.sending.dry_run,
                            dominio=domain.name,
                            remetente=domain.from_email,
                        )
                    except mysql.connector.Error as e:
                        logger.write(
                            {
                                "event": "emails_table_log_failed",
                                "campaign_id": campaign_id,
                                "error": str(e),
                            }
                        )

        logger.write({"event": "summary", "campaign_id": campaign_id, **totals})
        return totals
    finally:
        if emails_log_conn is not None:
            emails_log_conn.close()


def run_single_email(
    cfg: CampaignConfig,
    *,
    to_name: str,
    to_email: str,
    campaign_id: str,
    domain_name: str | None = None,
    log_dir: str = "campanha/logs",
) -> dict[str, int]:
    recipients = [Recipient(name=to_name or to_email, email=to_email, fields={})]
    if domain_name:
        domains = [d for d in cfg.domains if d.name == domain_name]
        if not domains:
            raise ValueError(f"Domínio não encontrado em config: {domain_name}")
        cfg = CampaignConfig(
            project_name=cfg.project_name,
            mysql=cfg.mysql,
            blacklist=cfg.blacklist,
            campaign_emails_log=cfg.campaign_emails_log,
            sending=cfg.sending,
            mailgun=cfg.mailgun,
            content=cfg.content,
            domains=domains,
            source_config_path=cfg.source_config_path,
        )
    return run_campaign(cfg, recipients, campaign_id=campaign_id, log_dir=log_dir)

