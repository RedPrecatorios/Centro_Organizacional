# -*- coding: utf-8 -*-
"""Atualização De Imposto — upload de demonstrativo + comprovante e cálculo IR.

Adaptação do pacote ``pacote_calculo_pos_comprovante``:
- extrai IR do comprovante
- monta payload single-case
- cálculo IA → RRA → Selic
- persiste no MySQL (auto_recupere)
- Monday desligado
"""
from __future__ import annotations

import json
import os
import re
import shutil
import sys
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from werkzeug.datastructures import FileStorage
from werkzeug.utils import secure_filename

_PDF_MAGIC = b"%PDF"
_MAX_PDF_BYTES = 40 * 1024 * 1024
_SLOT_KEYS = ("demonstrativo", "comprovante")
_SLOT_ALIASES = {
    "demonstrativo": ("demonstrativo", "pdf_1"),
    "comprovante": ("comprovante", "pdf_2"),
}
_SLOT_LABELS = {
    "demonstrativo": "Demonstrativo de Pagamento",
    "comprovante": "Comprovante de levantamento",
}

_lock = threading.Lock()
_running_jobs: set[str] = set()


def _project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _package_root() -> Path:
    raw = (os.getenv("PACOTE_CALCULO_PATH") or "").strip()
    if raw:
        return Path(raw).expanduser().resolve()
    return (_project_root().parent / "pacote_calculo_pos_comprovante").resolve()


def jobs_root() -> Path:
    root = _project_root() / "instance" / "atualizacao_imposto_jobs"
    root.mkdir(parents=True, exist_ok=True)
    try:
        os.chmod(root, 0o2775)
    except OSError:
        pass
    return root


def _job_dir(job_id: str) -> Path:
    safe = re.sub(r"[^a-zA-Z0-9_-]", "", job_id or "")
    if not safe or safe != job_id:
        raise ValueError("job_id inválido.")
    return jobs_root() / safe


def _ensure_job_workdir(job_id: str) -> Path:
    job_path = _job_dir(job_id)
    job_path.mkdir(parents=True, exist_ok=False)
    try:
        os.chmod(job_path, 0o2775)
    except OSError:
        pass
    return job_path


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _is_pdf_bytes(raw: bytes) -> bool:
    return bool(raw) and raw[:4] == _PDF_MAGIC


def _write_meta(job_path: Path, meta: dict[str, Any]) -> None:
    meta = dict(meta)
    meta["updated_at"] = _utc_now_iso()
    (job_path / "meta.json").write_text(
        json.dumps(meta, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _read_meta(job_path: Path) -> dict[str, Any]:
    meta_path = job_path / "meta.json"
    if not meta_path.is_file():
        raise FileNotFoundError("Job não encontrado.")
    data = json.loads(meta_path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError("meta.json inválido.")
    return data


def _update_meta(job_path: Path, **patch: Any) -> dict[str, Any]:
    with _lock:
        meta = _read_meta(job_path)
        meta.update(patch)
        _write_meta(job_path, meta)
        return meta


def _validate_pdf_file(fs: FileStorage | None, slot: str) -> tuple[bytes, str]:
    label = _SLOT_LABELS.get(slot, slot)
    if fs is None or not getattr(fs, "filename", None):
        raise ValueError(f"{label} é obrigatório.")
    filename = secure_filename(fs.filename or "") or f"{slot}.pdf"
    if not filename.lower().endswith(".pdf"):
        raise ValueError(f"{label} deve ser um ficheiro PDF.")
    raw = fs.read()
    if not raw:
        raise ValueError(f"{label} está vazio.")
    if len(raw) > _MAX_PDF_BYTES:
        raise ValueError(
            f"{label} excede o limite de {_MAX_PDF_BYTES // (1024 * 1024)} MB."
        )
    if not _is_pdf_bytes(raw):
        raise ValueError(f"{label} não parece ser um PDF válido.")
    return raw, filename


def _pick_file(files: Any, slot: str) -> FileStorage | None:
    for name in _SLOT_ALIASES.get(slot, (slot,)):
        fs = files.get(name)
        if fs is not None and getattr(fs, "filename", None):
            return fs
    return None


def _digits_only(value: str | None) -> str:
    return re.sub(r"\D+", "", str(value or ""))


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _ensure_package_on_path() -> Path:
    root = _package_root()
    src = root / "src"
    if not (src / "recupere_monday").is_dir():
        raise RuntimeError(
            f"Pacote de cálculo não encontrado em {root}. "
            "Defina PACOTE_CALCULO_PATH no .env."
        )
    src_s = str(src)
    if src_s not in sys.path:
        sys.path.insert(0, src_s)
    return root


def _load_calculo_settings():
    """Carrega Settings do pacote (cwd temporário = raiz do pacote)."""
    root = _ensure_package_on_path()
    # Centro tem prioridade; o .env do pacote só preenche o que faltar.
    load_dotenv(root / ".env", override=False)

    needed = (
        "LLM_PROVIDER",
        "OPENAI_API_KEY",
        "GEMINI_API_KEY",
        "AUTO_RECUPERE_DB_HOST",
        "AUTO_RECUPERE_DB_USER",
        "MONDAY_API_TOKEN",
        "MONDAY_BOARD_ID",
        "GOOGLE_DRIVE_FOLDER_ID",
        "GOOGLE_SERVICE_ACCOUNT_FILE",
    )
    if any(not (os.getenv(k) or "").strip() for k in needed):
        # Preenche só chaves ainda vazias (dotenv override=False já fez isso;
        # aqui forçamos re-leitura caso o processo tenha .env parcial).
        from dotenv import dotenv_values

        for key, value in (dotenv_values(root / ".env") or {}).items():
            if key and value is not None and not (os.getenv(key) or "").strip():
                os.environ[key] = str(value)

    ws = (os.getenv("WEBSHARE_PROXIES_LIST") or "").strip()
    if not ws or not Path(ws).expanduser().exists():
        pkg_ws = root / "Webshare 500 proxies.txt"
        if pkg_ws.is_file():
            os.environ["WEBSHARE_PROXIES_LIST"] = str(pkg_ws)

    from recupere_monday.config import get_settings
    from recupere_monday.http_proxy import configure_proxy_from_settings
    from recupere_monday.logging_config import setup_logging

    prev = Path.cwd()
    try:
        os.chdir(root)
        settings = get_settings()
    finally:
        os.chdir(prev)
    setup_logging(settings.log_level, style=getattr(settings, "log_style", "simple"))
    configure_proxy_from_settings(settings)
    return settings


def _detect_banco(comprovante_pdf: Path, banco_hint: str | None) -> str:
    """Detecta o banco pelo conteúdo do comprovante (fonte do IR); hint só como fallback."""
    try:
        import fitz

        text = ""
        doc = fitz.open(comprovante_pdf)
        try:
            if doc.page_count:
                text = doc[0].get_text() or ""
        finally:
            doc.close()
        upper = text.upper()
        # Comprovante BB de resgate de precatório federal
        if (
            "BANCO DO BRASIL" in upper
            or "BB.COM" in upper
            or "COMPROVANTE DE RESGATE" in upper
            or "VALOR DO IR" in upper
        ):
            return "BB"
        if (
            "CAIXA" in upper
            or "CEF" in upper
            or "DEPÓSITO JUDICIAL" in upper
            or "DEPOSITO JUDICIAL" in upper
            or "IMPOSTO RETIDO NA FONTE" in upper
        ):
            return "CEF"
    except Exception:
        pass

    hint = str(banco_hint or "").strip().upper()
    if hint in {"CEF", "BB", "CAIXA"}:
        return "CEF" if hint == "CAIXA" else hint
    return "CEF"


def _build_single_case_payload(
    *,
    demonstrativo_pdf: Path,
    comprovante_pdf: Path,
) -> tuple[dict[str, Any], dict[str, Any]]:
    _ensure_package_on_path()
    from recupere_monday.extraction.comprovante_ir import extract_ir_from_comprovante
    from recupere_monday.extraction.demonstrativo_fields import extract_campos_from_pdf

    campos = extract_campos_from_pdf(demonstrativo_pdf)
    banco = _detect_banco(comprovante_pdf, campos.banco)
    ir = extract_ir_from_comprovante(comprovante_pdf, banco)

    nome = _clean_text(campos.beneficiario_nome) or _clean_text(ir.requerente_nome)
    cpf = _clean_text(campos.beneficiario_cpf_cnpj) or _clean_text(ir.requerente_cpf_cnpj)
    processo = (
        _clean_text(campos.processo_requisicao)
        or demonstrativo_pdf.stem
        or "sem_processo"
    )
    digits = _digits_only(cpf)
    is_cnpj = len(digits) == 14
    tem_ir = bool(ir.tem_ir_retido)
    tem_meses = bool(_clean_text(ir.numero_meses_rra))
    pronto = tem_ir and tem_meses and not is_cnpj

    record_id = f"{digits or 'sem_doc'}_{_digits_only(processo) or 'sem_proc'}"
    requerente_key = f"{digits or 'sem_doc'} - {processo}"

    ir_dict = ir.to_dict()
    registro = {
        "id": record_id,
        "status": {
            "comprovante_encontrado": True,
            "tem_ir_retido": tem_ir,
            "tem_numero_meses_rra": tem_meses,
            "falha_comprovante": False,
            "ignorado": is_cnpj,
            "motivo_ignorado": "cnpj_ignorado" if is_cnpj else None,
            "pronto_para_calculo": pronto,
        },
        "demonstrativo": {
            "arquivo": demonstrativo_pdf.name,
            "processo_pasta": processo,
            "processo_requisicao": processo,
            "beneficiario_nome": nome,
            "beneficiario_cpf_cnpj": cpf,
            "requerido": campos.requerido,
            "advogado": campos.advogado,
            "banco": banco,
            "agencia": campos.agencia,
            "conta_deposito": campos.conta_deposito,
            "mes_pagamento": campos.mes_pagamento,
            "data_disponibilidade_saque": campos.data_disponibilidade_saque,
            "juros_mora_selic": campos.juros_mora_selic,
            "json_path": None,
            "pdf_extraido_path": str(demonstrativo_pdf),
        },
        "comprovante": {
            "pdf_path": str(comprovante_pdf),
            "success": True,
            "ir_retido": ir_dict,
        },
    }
    payload = {
        "tipo_payload": "caso_manual_plataforma",
        "requerente_key": requerente_key,
        "requerente": {
            "nome": nome,
            "cpf_cnpj": cpf,
            "documento_digits": digits or None,
        },
        "registros": [registro],
        "origem": {
            "demonstrativo_pdf": str(demonstrativo_pdf),
            "comprovante_pdf": str(comprovante_pdf),
            "banco_detectado": banco,
        },
    }
    precheck = {
        "nome": nome,
        "cpf_cnpj": cpf,
        "processo": processo,
        "banco": banco,
        "tem_ir_retido": tem_ir,
        "valor_ir_retido": ir_dict.get("valor_ir_retido"),
        "numero_meses_rra": ir_dict.get("numero_meses_rra"),
        "pronto_para_calculo": pronto,
        "motivo_bloqueio": (
            "CNPJ ignorado"
            if is_cnpj
            else (
                None
                if pronto
                else (
                    "Sem IR retido no comprovante"
                    if not tem_ir
                    else "Número de meses RRA ausente no comprovante"
                )
            )
        ),
    }
    return payload, precheck


def _summarize_resultado(
    output_payload: dict[str, Any],
    *,
    precheck: dict[str, Any],
) -> dict[str, Any]:
    resultado = output_payload.get("resultado") or {}
    case = {}
    casos = resultado.get("casos") if isinstance(resultado, dict) else None
    if isinstance(casos, list) and casos:
        case = casos[0] if isinstance(casos[0], dict) else {}

    dem = case.get("dados_demonstrativo") if isinstance(case.get("dados_demonstrativo"), dict) else {}
    comp = (
        case.get("dados_comprovante_bancario")
        if isinstance(case.get("dados_comprovante_bancario"), dict)
        else {}
    )
    pessoa_resultado = case.get("requerente") if isinstance(case.get("requerente"), dict) else {}

    imposto = output_payload.get("resultado_imposto_retido_indevidamente_atualizado") or {}
    rra = output_payload.get("resultado_receita_rra") or {}
    rra_res = rra.get("resultado") if isinstance(rra.get("resultado"), dict) else {}

    nome = (
        _clean_text(pessoa_resultado.get("nome"))
        or _clean_text(dem.get("beneficiario_nome"))
        or _clean_text(precheck.get("nome"))
    )
    cpf = (
        _clean_text(pessoa_resultado.get("cpf_cnpj"))
        or _clean_text(dem.get("beneficiario_cpf_cnpj"))
        or _clean_text(comp.get("cpf_cnpj"))
        or _clean_text(precheck.get("cpf_cnpj"))
    )
    processo = (
        _clean_text(dem.get("processo_requisicao"))
        or _clean_text(precheck.get("processo"))
    )

    return {
        "pessoa": {
            "nome": nome,
            "cpf_cnpj": cpf,
            "processo": processo,
            "banco": _clean_text(precheck.get("banco")),
            "advogado": _clean_text(dem.get("advogado")) or _clean_text(
                (output_payload.get("requerente") or {}).get("advogado")
                if isinstance(output_payload.get("requerente"), dict)
                else None
            ),
            "requerido": _clean_text(dem.get("requerido")),
        },
        "ir_comprovante": {
            "valor_ir_retido": precheck.get("valor_ir_retido")
            or comp.get("imposto_retido_na_fonte"),
            "numero_meses_rra": precheck.get("numero_meses_rra")
            or comp.get("numero_meses_rra"),
        },
        "receita_rra": {
            "status": rra.get("status"),
            "motivo": rra.get("motivo"),
            "irrf": rra_res.get("irrf"),
        },
        "imposto_atualizado": {
            "status": imposto.get("status"),
            "motivo": imposto.get("motivo"),
            "imposto_retido": imposto.get("imposto_retido"),
            "irrf_calculado": imposto.get("irrf_calculado"),
            "imposto_indevido": imposto.get("imposto_indevido"),
            "selic_percentual": (imposto.get("selic") or {}).get("percentual")
            if isinstance(imposto.get("selic"), dict)
            else None,
            "selic_acumulado": imposto.get("selic_acumulado"),
            "imposto_retido_indevidamente_atualizado": imposto.get(
                "imposto_retido_indevidamente_atualizado"
            ),
        },
        "fluxo": (output_payload.get("decisao_fluxo") or {}).get("tipo_fluxo"),
        "mysql_persistido": bool(output_payload.get("_mysql_persistido")),
    }


class _CalculosIASemMonday:
    """Wrapper: reutiliza o orquestrador do pacote sem encaminhar à Monday."""

    def __init__(self, settings: Any) -> None:
        from recupere_monday.pipeline.process_calculos_ia import CalculosIAOrchestrator

        class _Orchestrator(CalculosIAOrchestrator):
            def _encaminhar_para_monday(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
                return {
                    "status": "desabilitado",
                    "motivo": "Monday desligado na plataforma Centro Organizacional",
                }

        self._orch = _Orchestrator(settings)

    def process_registro(
        self,
        payload: dict[str, Any],
        registro: dict[str, Any],
        output_path: Path,
    ) -> dict[str, Any]:
        # `_process_record` grava o JSON completo em disco e devolve só um resumo.
        summary = self._orch._process_record(payload, registro, output_path)
        if output_path.is_file():
            try:
                full = json.loads(output_path.read_text(encoding="utf-8"))
                if isinstance(full, dict) and (
                    "resultado_imposto_retido_indevidamente_atualizado" in full
                    or "resultado_receita_rra" in full
                    or "resultado" in full
                ):
                    return full
            except (OSError, json.JSONDecodeError):
                pass
        return summary if isinstance(summary, dict) else {}


def _run_job(job_id: str) -> None:
    job_path = _job_dir(job_id)
    try:
        _update_meta(
            job_path,
            status="extraindo",
            message="A extrair dados do demonstrativo e do comprovante…",
            error=None,
        )
        dem_pdf = job_path / "demonstrativo.pdf"
        comp_pdf = job_path / "comprovante.pdf"
        if not dem_pdf.is_file() or not comp_pdf.is_file():
            raise RuntimeError("PDFs do job não encontrados.")

        payload, precheck = _build_single_case_payload(
            demonstrativo_pdf=dem_pdf,
            comprovante_pdf=comp_pdf,
        )
        (job_path / "payload_calculo.json").write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        _update_meta(
            job_path,
            status="validado",
            message="Dados extraídos. A iniciar cálculo…",
            precheck=precheck,
            resultado=None,
        )

        if not precheck.get("pronto_para_calculo"):
            _update_meta(
                job_path,
                status="inapto",
                message=precheck.get("motivo_bloqueio") or "Caso inapto para cálculo.",
                precheck=precheck,
                resultado={
                    "pessoa": {
                        "nome": precheck.get("nome"),
                        "cpf_cnpj": precheck.get("cpf_cnpj"),
                        "processo": precheck.get("processo"),
                        "banco": precheck.get("banco"),
                    },
                    "ir_comprovante": {
                        "valor_ir_retido": precheck.get("valor_ir_retido"),
                        "numero_meses_rra": precheck.get("numero_meses_rra"),
                    },
                    "imposto_atualizado": {
                        "status": "nao_calculado",
                        "motivo": precheck.get("motivo_bloqueio"),
                        "imposto_retido_indevidamente_atualizado": None,
                    },
                },
            )
            return

        _update_meta(
            job_path,
            status="calculando",
            message="Aguarde alguns instantes pois estamos atualizando os cálculos.....",
        )
        settings = _load_calculo_settings()
        registro = payload["registros"][0]
        out_path = job_path / "calculos_ia" / f"{registro['id']}.json"
        orch = _CalculosIASemMonday(settings)
        output_payload = orch.process_registro(payload, registro, out_path)

        mysql_ok = bool(getattr(settings, "auto_recupere_db_enabled", False))
        if isinstance(output_payload, dict):
            output_payload = dict(output_payload)
            output_payload["_mysql_persistido"] = mysql_ok
            # Não sobrescrever o JSON completo do orquestrador com um resumo.
            # Só anexa a flag MySQL se o ficheiro ainda for o payload completo.
            try:
                if out_path.is_file():
                    on_disk = json.loads(out_path.read_text(encoding="utf-8"))
                    if isinstance(on_disk, dict) and (
                        "resultado" in on_disk
                        or "resultado_receita_rra" in on_disk
                        or "resultado_imposto_retido_indevidamente_atualizado" in on_disk
                    ):
                        on_disk["_mysql_persistido"] = mysql_ok
                        out_path.write_text(
                            json.dumps(on_disk, ensure_ascii=False, indent=2),
                            encoding="utf-8",
                        )
                        output_payload = on_disk
            except (OSError, json.JSONDecodeError):
                pass

        resumo = _summarize_resultado(output_payload, precheck=precheck)
        # Fallback: se o resumo veio do return enxuto do orquestrador
        if not (resumo.get("imposto_atualizado") or {}).get("status"):
            if output_payload.get("imposto_atualizado_status"):
                resumo.setdefault("imposto_atualizado", {})
                resumo["imposto_atualizado"]["status"] = output_payload.get(
                    "imposto_atualizado_status"
                )
                resumo["imposto_atualizado"]["motivo"] = output_payload.get(
                    "imposto_atualizado_motivo"
                )
            if output_payload.get("receita_rra_status"):
                resumo.setdefault("receita_rra", {})
                resumo["receita_rra"]["status"] = output_payload.get("receita_rra_status")
                resumo["receita_rra"]["motivo"] = output_payload.get("receita_rra_motivo")
            if output_payload.get("fluxo"):
                resumo["fluxo"] = output_payload.get("fluxo")

        imposto = resumo.get("imposto_atualizado") or {}
        if imposto.get("status") == "calculado" and imposto.get(
            "imposto_retido_indevidamente_atualizado"
        ) not in (None, ""):
            msg = (
                "Cálculo concluído. Valor retido indevidamente atualizado: "
                f"R$ {imposto['imposto_retido_indevidamente_atualizado']}"
            )
            status = "concluido"
        elif (resumo.get("receita_rra") or {}).get("status") == "calculado":
            msg = (
                "Cálculo parcialmente concluído (RRA ok; imposto atualizado: "
                f"{imposto.get('status') or 'n/d'})."
            )
            status = "concluido_parcial"
        else:
            msg = (
                "Cálculo finalizado com restrições: "
                f"{imposto.get('motivo') or (resumo.get('receita_rra') or {}).get('motivo') or 'ver detalhes'}."
            )
            status = "concluido_parcial"

        _update_meta(
            job_path,
            status=status,
            message=msg,
            precheck=precheck,
            resultado=resumo,
            output_json=str(out_path),
            mysql_persistido=mysql_ok,
        )
    except Exception as exc:  # noqa: BLE001
        try:
            _update_meta(
                job_path,
                status="erro",
                message=f"Falha no processamento: {exc}",
                error=str(exc),
            )
        except Exception:
            pass
    finally:
        with _lock:
            _running_jobs.discard(job_id)


def _start_job_thread(job_id: str) -> None:
    with _lock:
        if job_id in _running_jobs:
            return
        _running_jobs.add(job_id)
    t = threading.Thread(
        target=_run_job,
        args=(job_id,),
        name=f"atualizacao-imposto-{job_id}",
        daemon=True,
    )
    t.start()


def create_job_from_uploads(
    *,
    files: Any = None,
    pdf_1: FileStorage | None = None,
    pdf_2: FileStorage | None = None,
    demonstrativo: FileStorage | None = None,
    comprovante: FileStorage | None = None,
    user_id: int | None = None,
    username: str | None = None,
) -> dict[str, Any]:
    """Valida, guarda os 2 PDFs e inicia o cálculo em background."""
    if files is not None:
        demonstrativo = demonstrativo or _pick_file(files, "demonstrativo")
        comprovante = comprovante or _pick_file(files, "comprovante")
    demonstrativo = demonstrativo or pdf_1
    comprovante = comprovante or pdf_2

    dem_raw, dem_name = _validate_pdf_file(demonstrativo, "demonstrativo")
    comp_raw, comp_name = _validate_pdf_file(comprovante, "comprovante")

    job_id = uuid.uuid4().hex[:16]
    try:
        job_path = _ensure_job_workdir(job_id)
    except PermissionError as exc:
        raise OSError(
            "Sem permissão para gravar em instance/atualizacao_imposto_jobs. "
            "Ajuste o dono/permissões da pasta (www-data)."
        ) from exc
    (job_path / "demonstrativo.pdf").write_bytes(dem_raw)
    (job_path / "comprovante.pdf").write_bytes(comp_raw)

    meta: dict[str, Any] = {
        "job_id": job_id,
        "status": "na_fila",
        "message": "PDFs recebidos. Cálculo na fila…",
        "created_at": _utc_now_iso(),
        "updated_at": _utc_now_iso(),
        "user_id": user_id,
        "username": username,
        "files": {
            "demonstrativo": {
                "original_name": dem_name,
                "stored_name": "demonstrativo.pdf",
                "size_bytes": len(dem_raw),
            },
            "comprovante": {
                "original_name": comp_name,
                "stored_name": "comprovante.pdf",
                "size_bytes": len(comp_raw),
            },
            # Compat com UI antiga
            "pdf_1": {
                "original_name": dem_name,
                "stored_name": "demonstrativo.pdf",
                "size_bytes": len(dem_raw),
            },
            "pdf_2": {
                "original_name": comp_name,
                "stored_name": "comprovante.pdf",
                "size_bytes": len(comp_raw),
            },
        },
        "precheck": None,
        "resultado": None,
        "automation": {
            "ready": True,
            "monday": False,
            "mysql": True,
            "note": "Fluxo: IR → payload → IA/RRA/Selic → MySQL (sem Monday).",
        },
    }
    _write_meta(job_path, meta)
    _start_job_thread(job_id)
    return {
        "ok": True,
        "job_id": job_id,
        "status": meta["status"],
        "message": meta["message"],
        "files": {
            "demonstrativo": meta["files"]["demonstrativo"],
            "comprovante": meta["files"]["comprovante"],
        },
    }


def get_job_status(job_id: str) -> dict[str, Any]:
    try:
        job_path = _job_dir(job_id)
    except ValueError as e:
        return {"ok": False, "error": str(e)}
    try:
        meta = _read_meta(job_path)
    except FileNotFoundError:
        return {"ok": False, "error": "Job não encontrado."}
    except (OSError, json.JSONDecodeError, ValueError):
        return {"ok": False, "error": "Não foi possível ler o estado do job."}

    # Retoma jobs órfãos (ex.: restart a meio do processo)
    status = str(meta.get("status") or "")
    if status in {"na_fila", "extraindo", "validado", "calculando"}:
        with _lock:
            running = job_id in _running_jobs
        if not running:
            _start_job_thread(job_id)

    return {
        "ok": True,
        "job_id": meta.get("job_id") or job_id,
        "status": meta.get("status") or "desconhecido",
        "message": meta.get("message") or "",
        "created_at": meta.get("created_at"),
        "updated_at": meta.get("updated_at"),
        "files": meta.get("files") or {},
        "precheck": meta.get("precheck"),
        "resultado": meta.get("resultado"),
        "mysql_persistido": meta.get("mysql_persistido"),
        "error": meta.get("error"),
        "automation": meta.get("automation") or {},
    }


def list_completed_cases(*, limit: int = 100) -> dict[str, Any]:
    """Lista casos do histórico local (concluídos, parciais, inaptos e erros)."""
    try:
        limit_n = max(1, min(int(limit or 100), 500))
    except (TypeError, ValueError):
        limit_n = 100

    rows: list[dict[str, Any]] = []
    root = jobs_root()
    for meta_path in root.glob("*/meta.json"):
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(meta, dict):
            continue
        status = str(meta.get("status") or "")
        if status not in {"concluido", "concluido_parcial", "inapto", "erro"}:
            continue
        res = meta.get("resultado") if isinstance(meta.get("resultado"), dict) else {}
        pessoa = res.get("pessoa") if isinstance(res.get("pessoa"), dict) else {}
        ir = res.get("ir_comprovante") if isinstance(res.get("ir_comprovante"), dict) else {}
        imposto = (
            res.get("imposto_atualizado")
            if isinstance(res.get("imposto_atualizado"), dict)
            else {}
        )
        pre = meta.get("precheck") if isinstance(meta.get("precheck"), dict) else {}
        valor_atualizado = imposto.get("imposto_retido_indevidamente_atualizado")
        rows.append(
            {
                "job_id": meta.get("job_id") or meta_path.parent.name,
                "status": status,
                "created_at": meta.get("created_at"),
                "updated_at": meta.get("updated_at"),
                "nome": pessoa.get("nome") or pre.get("nome"),
                "cpf_cnpj": pessoa.get("cpf_cnpj") or pre.get("cpf_cnpj"),
                "processo": pessoa.get("processo") or pre.get("processo"),
                "ir_retido_comprovante": ir.get("valor_ir_retido")
                or pre.get("valor_ir_retido"),
                "imposto_retido_indevidamente_atualizado": valor_atualizado,
                "message": meta.get("message"),
            }
        )

    def _sort_key(item: dict[str, Any]) -> str:
        return str(item.get("updated_at") or item.get("created_at") or "")

    rows.sort(key=_sort_key, reverse=True)
    return {"ok": True, "total": len(rows), "items": rows[:limit_n]}


def delete_job(job_id: str) -> dict[str, Any]:
    """Remove um job do histórico (pasta local completa)."""
    jid = str(job_id or "").strip()
    if not jid or not re.fullmatch(r"[0-9a-fA-F-]{8,64}", jid):
        raise ValueError("job_id inválido.")
    job_path = _job_dir(jid)
    if not job_path.is_dir():
        raise FileNotFoundError("Cálculo não encontrado.")
    with _lock:
        if jid in _running_jobs:
            raise RuntimeError("Não é possível remover um cálculo em andamento.")
    # Garante permissões de escrita para limpeza (ficheiros criados por root em testes).
    try:
        for path in job_path.rglob("*"):
            try:
                if path.is_dir():
                    path.chmod(path.stat().st_mode | 0o770)
                else:
                    path.chmod(path.stat().st_mode | 0o660)
            except OSError:
                pass
        job_path.chmod(job_path.stat().st_mode | 0o770)
    except OSError:
        pass
    shutil.rmtree(job_path)
    return {"ok": True, "job_id": jid, "deleted": True}


def is_package_configured() -> bool:
    try:
        root = _package_root()
        return (root / "src" / "recupere_monday").is_dir()
    except Exception:
        return False
