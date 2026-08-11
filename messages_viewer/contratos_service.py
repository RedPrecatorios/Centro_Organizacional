# -*- coding: utf-8 -*-
"""Orquestração: sync ficha + invocação do motor PHP legado."""
from __future__ import annotations

import os
import re
import subprocess
from pathlib import Path
from typing import Any

from messages_viewer.contratos_adapter import sync_ficha_to_autocontratos
from messages_viewer.contratos_db import is_configured
from messages_viewer.contratos_tipos import tipos_payload
from messages_viewer.contratos_validacao import validar_contratos_geracao


def _engine_root() -> Path:
    raw = (os.getenv("CONTRATOS_ENGINE_PATH") or "").strip()
    if raw:
        return Path(raw)
    return Path(__file__).resolve().parents[1] / "contratos_engine"


def _output_roots(engine: Path) -> list[Path]:
    """Diretórios onde o PHP legado grava DOCX/ZIP (paths hardcoded no motor)."""
    candidates: list[Path] = []
    for raw in (
        (os.getenv("CONTRATOS_OUTPUT_PATH") or "").strip(),
        "/var/www/html/RedPrecatorios_Contratos",
        str(engine),
    ):
        if not raw:
            continue
        p = Path(raw)
        try:
            resolved = p.resolve()
        except OSError:
            continue
        if resolved.is_dir() and resolved not in candidates:
            candidates.append(resolved)
    return candidates or [engine.resolve()]


def _find_zip(engine: Path, nome_credor: str, suffix: str) -> Path | None:
    stem = (nome_credor or "").strip()
    if not stem:
        return None
    pattern = re.compile(re.escape(stem), re.I)
    for root in _output_roots(engine):
        direct = root / f"{stem}{suffix}"
        if direct.is_file() and direct.stat().st_size > 0:
            return direct
        matches: list[Path] = []
        for p in root.glob(f"*{suffix}"):
            if pattern.search(p.stem) and p.is_file() and p.stat().st_size > 0:
                matches.append(p)
        if matches:
            matches.sort(key=lambda x: x.stat().st_mtime, reverse=True)
            return matches[0]
    return None


def _php_bin() -> str:
    return (os.getenv("PHP_BIN") or "php").strip() or "php"


def _api_token() -> str:
    return (os.getenv("CONTRATOS_API_TOKEN") or "").strip()


def validar_contratos(
    *,
    cumprimento_de_sentenca: str,
    incidente: str,
    nome_credor: str,
    tipo: str,
    contrato_id: str,
    recupere: int = 0,
    caso_id: str | None = None,
    id_externo: str | None = None,
    cessionaria: str | None = None,
    dados_override: dict[str, Any] | None = None,
    herdeiros_override: list[dict[str, Any]] | None = None,
    herdeiros_abertos_override: bool | None = None,
    herdeiros_validado_override: bool | None = None,
    depre_hint: str | None = None,
) -> tuple[dict[str, Any], int]:
    return validar_contratos_geracao(
        cumprimento_de_sentenca=cumprimento_de_sentenca,
        incidente=incidente,
        nome_credor=nome_credor,
        tipo=tipo,
        contrato_id=contrato_id,
        recupere=recupere,
        caso_id=caso_id,
        id_externo=id_externo,
        cessionaria=cessionaria,
        dados_override=dados_override,
        herdeiros_override=herdeiros_override,
        herdeiros_abertos_override=herdeiros_abertos_override,
        herdeiros_validado_override=herdeiros_validado_override,
        depre_hint=depre_hint,
    )


def gerar_contratos(
    *,
    cumprimento_de_sentenca: str,
    incidente: str,
    nome_credor: str,
    tipo: str,
    contrato_id: str,
    recupere: int = 0,
    caso_id: str | None = None,
    id_externo: str | None = None,
    user_name: str | None = None,
    user_email: str | None = None,
    cessionaria: str | None = None,
    force: bool = False,
    dados_override: dict[str, Any] | None = None,
    herdeiros_override: list[dict[str, Any]] | None = None,
    herdeiros_abertos_override: bool | None = None,
    herdeiros_validado_override: bool | None = None,
    depre_hint: str | None = None,
) -> tuple[dict[str, Any] | bytes, int, dict[str, str] | None]:
    if not is_configured():
        return (
            {"ok": False, "error": "CONTRATOS_MYSQL_* não configurado no .env."},
            503,
            None,
        )

    token = _api_token()
    if not token:
        return (
            {"ok": False, "error": "CONTRATOS_API_TOKEN não configurado no .env."},
            503,
            None,
        )

    engine = _engine_root()
    if not engine.is_dir():
        return (
            {
                "ok": False,
                "error": f"Motor de contratos não encontrado em {engine}.",
            },
            503,
            None,
        )

    val_out, val_code = validar_contratos_geracao(
        cumprimento_de_sentenca=cumprimento_de_sentenca,
        incidente=incidente,
        nome_credor=nome_credor,
        tipo=tipo,
        contrato_id=contrato_id,
        recupere=recupere,
        caso_id=caso_id,
        id_externo=id_externo,
        cessionaria=cessionaria,
        dados_override=dados_override,
        herdeiros_override=herdeiros_override,
        herdeiros_abertos_override=herdeiros_abertos_override,
        herdeiros_validado_override=herdeiros_validado_override,
        depre_hint=depre_hint,
    )
    if val_code != 200:
        return val_out, val_code, None
    if val_out.get("bloqueado") and not force:
        return (
            {
                **val_out,
                "ok": False,
                "error": val_out.get("resumo") or "Campos obrigatórios não preenchidos.",
            },
            400,
            None,
        )

    sync_out, sync_code = sync_ficha_to_autocontratos(
        cumprimento_de_sentenca=cumprimento_de_sentenca,
        incidente=incidente,
        nome_credor=nome_credor,
        caso_id=caso_id,
        id_externo=id_externo,
        user_name=user_name,
        recupere=recupere,
        cessionaria=cessionaria,
        dados_override=dados_override,
        herdeiros_override=herdeiros_override,
        herdeiros_abertos_override=herdeiros_abertos_override,
        herdeiros_validado_override=herdeiros_validado_override,
        depre_hint=depre_hint,
    )
    if sync_code != 200 or not sync_out.get("ok"):
        return sync_out, sync_code, None

    cessionaria = str(sync_out.get("cessionaria") or "")
    requerente = str(sync_out.get("nome_credor") or nome_credor)
    qtd_herdeiros = str(sync_out.get("total_herdeiros") or 0)

    tipo_norm = (tipo or "preca").strip().lower()
    if tipo_norm not in ("preca", "rpv"):
        return (
            {"ok": False, "error": "tipo deve ser 'preca' ou 'rpv'."},
            400,
            None,
        )

    script = "gerar_preca.php" if tipo_norm == "preca" else "gerar_rpv.php"
    script_path = engine / script
    if not script_path.is_file():
        return (
            {"ok": False, "error": f"Script não encontrado: {script_path}"},
            503,
            None,
        )

    args = [
        _php_bin(),
        str(script_path),
        f"--api-token={token}",
        f"numero_do_processo={cumprimento_de_sentenca}",
        f"incidente={incidente}",
        f"requerente={requerente}",
        f"quantidade_de_herdeiros={qtd_herdeiros}",
        f"template_preca={cessionaria}",
        f"contrato={contrato_id}",
        f"recupere={1 if recupere else 0}",
        f"user_name={user_name or 'Centro Organizacional'}",
    ]
    if user_email:
        args.append(f"user_email={user_email}")

    try:
        proc = subprocess.run(
            args,
            cwd=str(engine),
            capture_output=True,
            text=True,
            timeout=int(os.getenv("CONTRATOS_PHP_TIMEOUT", "120")),
        )
    except subprocess.TimeoutExpired:
        return (
            {"ok": False, "error": "Timeout ao gerar contratos (motor PHP)."},
            504,
            None,
        )
    except OSError as e:
        return (
            {"ok": False, "error": f"Falha ao executar PHP: {e}"},
            503,
            None,
        )

    stderr = (proc.stderr or "").strip()
    stdout = (proc.stdout or "").strip()
    if proc.returncode != 0:
        return (
            {
                "ok": False,
                "error": "Erro no motor PHP ao gerar contratos.",
                "stdout": stdout[:2000],
                "stderr": stderr[:2000],
            },
            500,
            None,
        )

    zip_suffix = "-Precatorio.zip" if tipo_norm == "preca" else "-RPV.zip"
    zip_path = _find_zip(engine, requerente, zip_suffix)
    if not zip_path or not zip_path.is_file():
        log_hint = engine / "logs" / "geracao_contratos.log"
        searched = [str(p) for p in _output_roots(engine)]
        err_detail = stderr or stdout
        if "Unsupported operand types" in err_detail or "PHP_ERROR" in err_detail:
            php_err = "Erro no motor PHP (valores numéricos ou campos vazios)."
        else:
            php_err = "Contratos processados, mas ZIP não encontrado."
        return (
            {
                "ok": False,
                "error": php_err,
                "stdout": stdout[:2000],
                "stderr": stderr[:2000],
                "searched_dirs": searched,
                "expected_zip": f"{requerente}{zip_suffix}",
                "log": str(log_hint) if log_hint.is_file() else "",
            },
            500,
            None,
        )

    data = zip_path.read_bytes()
    filename = zip_path.name
    headers = {
        "Content-Disposition": f'attachment; filename="{filename}"',
        "Cache-Control": "no-store",
    }
    return data, 200, headers
