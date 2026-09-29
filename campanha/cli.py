from __future__ import annotations

import argparse
import os
from datetime import datetime
from pathlib import Path

from campanha.core import load_config_toml, load_recipients_csv, run_campaign, run_single_email
from campanha.replace_domains import (
    _print_replace,
    _print_status,
    domains_status,
    parse_domain_list,
    replace_domains,
    verify_all_active,
)


def _resolve_recipients_path(recipients_arg: str, config_path: str) -> Path:
    """
    Resolve CSV path: primeiro no diretório atual; depois na pasta do config (ex.: campanha/).
    Evita FileNotFoundError quando o terminal não está na raiz do projeto.
    """
    rel = Path(recipients_arg)
    cfg_dir = Path(config_path).resolve().parent

    candidates: list[Path] = []
    if rel.is_absolute():
        candidates.append(rel)
    else:
        candidates.append(Path.cwd() / rel)
        candidates.append(cfg_dir / rel)
        candidates.append(cfg_dir / rel.name)

    seen: set[str] = set()
    ordered: list[Path] = []
    for c in candidates:
        key = str(c.resolve())
        if key not in seen:
            seen.add(key)
            ordered.append(c)

    for p in ordered:
        if p.is_file():
            return p.resolve()

    raise FileNotFoundError(
        "Arquivo de recipients não encontrado.\n"
        f"  Pedido: {recipients_arg!r}\n"
        f"  Pasta do config: {cfg_dir}\n"
        f"  Diretório atual (cwd): {os.getcwd()}\n"
        "  Dica: use caminho absoluto ou rode o comando a partir da raiz do projeto "
        f'({cfg_dir.parent}) ou coloque o CSV em {cfg_dir / rel.name}.'
    )


def _default_campaign_id() -> str:
    return datetime.now().strftime("campanha-%Y%m%d-%H%M%S")


def cmd_init(args: argparse.Namespace) -> int:
    target = Path(args.config_path)
    if target.exists() and not args.force:
        raise SystemExit(f"Arquivo já existe: {target} (use --force para sobrescrever)")

    example = Path("campanha/config.example.toml")
    if not example.exists():
        raise SystemExit("Não achei `campanha/config.example.toml` no projeto.")

    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(example.read_text(encoding="utf-8"), encoding="utf-8")
    print(f"[OK] Config criado em: {target}")
    return 0


def cmd_send_single(args: argparse.Namespace) -> int:
    cfg = load_config_toml(args.config)
    totals = run_single_email(
        cfg,
        to_name=args.name or args.to,
        to_email=args.to,
        campaign_id=args.campaign_id or _default_campaign_id(),
        domain_name=args.domain,
    )
    print(totals)
    return 0


def cmd_send_bulk(args: argparse.Namespace) -> int:
    cfg = load_config_toml(args.config)
    csv_path = _resolve_recipients_path(args.recipients, args.config)
    recipients = load_recipients_csv(str(csv_path))
    totals = run_campaign(
        cfg,
        recipients,
        campaign_id=args.campaign_id or _default_campaign_id(),
    )
    print(totals)
    return 0


def _load_replace_domain_list(args: argparse.Namespace) -> list[str]:
    chunks: list[str] = []
    if args.file:
        p = Path(args.file)
        if not p.is_file():
            raise SystemExit(f"Arquivo de domínios não encontrado: {p}")
        chunks.append(p.read_text(encoding="utf-8"))
    if args.domains:
        chunks.append(args.domains)
    return parse_domain_list("\n".join(chunks))


def cmd_domains_status(args: argparse.Namespace) -> int:
    _print_status(domains_status(args.config))
    return 0


def cmd_replace_domains(args: argparse.Namespace) -> int:
    domains = _load_replace_domain_list(args)
    if not domains:
        raise SystemExit("Informe --file e/ou --domains (um domínio por linha, ou separados por vírgula).")
    result = replace_domains(
        domains,
        config_path=args.config,
        from_name=args.from_name,
        from_local=args.from_local,
        reply_to=args.reply_to,
        deactivate_old=not args.keep_old,
        delete_old_mailgun=args.delete_old_mailgun,
        update_toml=not args.skip_toml,
        verify=not args.skip_verify,
        dry_run=args.dry_run,
    )
    _print_replace(result)
    return 0 if result.get("ok") else 1


def cmd_verify_domains(args: argparse.Namespace) -> int:
    import json

    print(json.dumps(verify_all_active(), ensure_ascii=False, indent=2))
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="campanha", description="Disparador de campanhas por e-mail")
    sub = p.add_subparsers(dest="cmd", required=True)

    p_init = sub.add_parser("init", help="Cria um config TOML a partir do exemplo")
    p_init.add_argument("--config-path", default="campanha/config.toml")
    p_init.add_argument("--force", action="store_true")
    p_init.set_defaults(func=cmd_init)

    p_single = sub.add_parser("send-single", help="Dispara 1 email (teste/avisos pontuais)")
    p_single.add_argument("--config", default="campanha/config.toml")
    p_single.add_argument("--campaign-id", default="")
    p_single.add_argument("--to", required=True, help="Email do destinatário")
    p_single.add_argument("--name", default="", help="Nome do destinatário")
    p_single.add_argument("--domain", default="", help="Força usar um domínio específico (name do [[domains]])")
    p_single.set_defaults(func=cmd_send_single)

    p_bulk = sub.add_parser("send-bulk", help="Dispara para base CSV (Email,nome,processo) alternando domínios")
    p_bulk.add_argument("--config", default="campanha/config.toml")
    p_bulk.add_argument("--campaign-id", default="")
    p_bulk.add_argument("--recipients", required=True, help="CSV com colunas Email,nome,processo (aceita também name,email)")
    p_bulk.set_defaults(func=cmd_send_bulk)

    p_st = sub.add_parser("domains-status", help="Lista remetentes no TOML, MySQL e Mailgun")
    p_st.add_argument("--config", default="campanha/config.toml")
    p_st.set_defaults(func=cmd_domains_status)

    p_rep = sub.add_parser(
        "replace-domains",
        help="Cria os novos remetentes (Mailgun/DNS/MySQL/TOML) e desativa os antigos",
    )
    p_rep.add_argument("--config", default="campanha/config.toml")
    p_rep.add_argument("--file", default="", help="Arquivo texto: 1 domínio por linha")
    p_rep.add_argument("--domains", default="", help="Lista separada por vírgula ou espaços")
    p_rep.add_argument("--from-name", default="RED PRECATÓRIOS")
    p_rep.add_argument("--from-local", default="contato", help="Parte local do From: contato@DOMINIO")
    p_rep.add_argument("--reply-to", default="contato@redprecatorios.com.br")
    p_rep.add_argument("--dry-run", action="store_true", help="Só mostra o plano, não altera nada")
    p_rep.add_argument("--keep-old", action="store_true", help="Não desativa os remetentes atuais")
    p_rep.add_argument(
        "--delete-old-mailgun",
        action="store_true",
        help="Além de desativar no MySQL, apaga os antigos no Mailgun (irreversível)",
    )
    p_rep.add_argument("--skip-toml", action="store_true", help="Não reescreve [[domains]] no config.toml")
    p_rep.add_argument("--skip-verify", action="store_true", help="Não chama Mailgun verify após criar")
    p_rep.set_defaults(func=cmd_replace_domains)

    p_ver = sub.add_parser("verify-domains", help="Revalida DNS/Mailgun de todos os remetentes ativos")
    p_ver.set_defaults(func=cmd_verify_domains)

    return p


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())

