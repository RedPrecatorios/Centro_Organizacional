"""Search orchestration: scrape → reprocessamento+cálculo → APT / INAPTO lists."""

from __future__ import annotations

import json
import logging
import re
import time
from pathlib import Path
from typing import Any, Callable

from api.adapters.pipeline_adapter import (
    PrecatorioRecord,
    enrich_for_search,
    is_rpv_text,
    record_is_rpv,
    resolve_search_query,
    scrape_by_query,
)
from api.schemas import AptoProcess, InaptoProcess, SearchJobArtifacts, SkippedProcess
from api.services.monday_lookup import lookup_incident
from api.services.precainfos_reader import PrecainfosReader
from tjsp_pipeline.config import Settings
from tjsp_pipeline.integration.refactor_log import parse_refactor_log_line
from tjsp_pipeline.scraper.search_url import SearchKind
from tjsp_pipeline.scraper.show_page import names_match

logger = logging.getLogger(__name__)

ProgressCb = Callable[..., None]


def _slug(value: str) -> str:
    return re.sub(r"[^\w.-]+", "_", value).strip("_")[:60] or "record"


def _normalize_inc(incidente: str) -> str:
    try:
        return str(int(str(incidente).strip()))
    except (TypeError, ValueError):
        return str(incidente).strip()


def _record_key(processo: str, incidente: str) -> str:
    return f"{str(processo).strip()}|{_normalize_inc(incidente)}"


def _stringify_record(row: dict[str, Any] | None) -> dict[str, Any]:
    if not row:
        return {}
    return {
        k: (None if v is None else str(v) if not isinstance(v, (dict, list)) else v)
        for k, v in row.items()
    }


def _blacklist_doc_titles(docs: Any) -> list[str]:
    if not isinstance(docs, list) or not docs:
        return []
    parts: list[str] = []
    for item in docs[:8]:
        title = ""
        if isinstance(item, str) and item.strip():
            title = item.strip()
        elif isinstance(item, dict):
            title = str(
                item.get("title")
                or item.get("titulo")
                or item.get("documento")
                or item.get("doc")
                or item.get("tipo")
                or ""
            ).strip()
        title = re.sub(r"\s*\(\s*BLACKLISTED\s*\)\s*", "", title, flags=re.I).strip()
        title = re.sub(r"^blacklist:\s*", "", title, flags=re.I).strip()
        if title:
            parts.append(title)
    seen: set[str] = set()
    uniq: list[str] = []
    for part in parts:
        key = part.casefold()
        if key in seen:
            continue
        seen.add(key)
        uniq.append(part)
    return uniq


def _format_blacklist_docs(docs: Any) -> str | None:
    titles = _blacklist_doc_titles(docs)
    if not titles:
        return None
    joined = ", ".join(titles)
    suffix = " encontrado" if len(titles) == 1 else " encontrado(s)"
    return f"{joined}{suffix}"


def _cpf_digits(value: Any) -> str:
    return re.sub(r"\D+", "", str(value or ""))


def _monday_ja_atribuido(result: Any) -> bool:
    return isinstance(result, dict) and result.get("outcome") == "ja_atribuido"


def _mark_ja_atribuido(
    live: dict[str, dict[str, Any]],
    rec: PrecatorioRecord,
    result: dict[str, Any],
    *,
    requerente: str | None,
    cpf: str | None,
) -> None:
    key = _record_key(rec.numero_de_processo, rec.numero_do_incidente)
    comprador = str(result.get("comprador") or "").strip()
    record = _stamp_credor(
        {
            "Numero_de_Processo": rec.numero_de_processo,
            "Numero_do_Incidente": _normalize_inc(rec.numero_do_incidente),
            "Classe": rec.classe or rec.label or None,
            "pipeline_stage": "pronto",
            "calculo_status": "ok",
            "skip_calculo": True,
            "monday_outcome": "ja_atribuido",
            "monday_item_id": result.get("monday_item_id"),
            "comprador": comprador,
            "proposta_enviada": result.get("proposta_enviada"),
            "motivo_retirada": result.get("motivo_retirada"),
            "mensagem_usuario": result.get("mensagem_usuario"),
            "pode_trocar_comprador": bool(result.get("pode_trocar_comprador")),
            "monday_label": result.get("label")
            or (f"Em tratamento — {comprador}" if comprador else "Em tratamento"),
        },
        requerente,
        cpf,
    )
    item = live.get(key) or {"rec": rec}
    item.update(
        {
            "rec": rec,
            "kind": "apto",
            "status": "apto",
            "stage": "pronto",
            "motivo": None,
            "record": record,
        }
    )
    live[key] = item


def _stamp_monday_context(
    live: dict[str, dict[str, Any]],
    rec: PrecatorioRecord,
    result: dict[str, Any],
) -> None:
    key = _record_key(rec.numero_de_processo, rec.numero_do_incidente)
    item = live.get(key)
    if not item:
        return
    recd = item.setdefault("record", {})
    outcome = str(result.get("outcome") or "").strip()
    if outcome:
        recd["monday_outcome"] = outcome
    if result.get("comprador"):
        recd["comprador"] = result.get("comprador")
    if result.get("proposta_enviada"):
        recd["proposta_enviada"] = result.get("proposta_enviada")
    if result.get("motivo_retirada"):
        recd["motivo_retirada"] = result.get("motivo_retirada")
    if result.get("mensagem_usuario"):
        recd["mensagem_usuario"] = result.get("mensagem_usuario")
    if result.get("pode_trocar_comprador") is not None:
        recd["pode_trocar_comprador"] = bool(result.get("pode_trocar_comprador"))
    if result.get("label"):
        recd["monday_label"] = result.get("label")
    if result.get("monday_item_id"):
        recd["monday_item_id"] = result.get("monday_item_id")


def _merge_monday_context(record: dict[str, Any], live_item: dict[str, Any] | None) -> dict[str, Any]:
    rec = dict(record or {})
    src = (live_item or {}).get("record") if isinstance(live_item, dict) else {}
    if not isinstance(src, dict):
        return rec
    for key in (
        "monday_outcome",
        "comprador",
        "proposta_enviada",
        "motivo_retirada",
        "mensagem_usuario",
        "pode_trocar_comprador",
        "monday_label",
        "monday_item_id",
    ):
        if src.get(key) and not rec.get(key):
            rec[key] = src[key]
    return rec


def _split_after_monday_precheck(
    to_enrich: list[PrecatorioRecord],
    live: dict[str, dict[str, Any]],
    *,
    lookup: Callable[..., Any] | None = None,
    requerente: str | None = None,
    cpf: str | None = None,
) -> tuple[list[PrecatorioRecord], list[PrecatorioRecord]]:
    finder = lookup or lookup_incident
    remaining: list[PrecatorioRecord] = []
    skipped: list[PrecatorioRecord] = []
    for rec in to_enrich:
        try:
            result = finder(rec.numero_de_processo, rec.numero_do_incidente)
        except Exception:
            result = None
        if isinstance(result, dict) and _monday_ja_atribuido(result):
            _mark_ja_atribuido(
                live, rec, result, requerente=requerente, cpf=cpf
            )
            skipped.append(rec)
        else:
            if isinstance(result, dict):
                _stamp_monday_context(live, rec, result)
            remaining.append(rec)
    return remaining, skipped


def _stamp_credor(record: dict[str, Any] | None, nome: str | None, cpf: str | None) -> dict[str, Any]:
    rec = dict(record or {})
    nome_txt = " ".join(str(nome or "").split()).strip()
    if nome_txt and not str(rec.get("Requerente") or rec.get("requerente") or "").strip():
        rec["Requerente"] = nome_txt
    digits = _cpf_digits(cpf)
    existing = _cpf_digits(rec.get("CPF") or rec.get("cpf"))
    if digits and not existing:
        rec["CPF"] = digits
    return rec


def _depre_numero(*bags: Any) -> str | None:
    for bag in bags:
        if isinstance(bag, str) and re.search(r"\d", bag):
            text = bag.strip()
            if text:
                return text
        if not isinstance(bag, dict):
            continue
        for key in ("DEPRE", "depre", "Depre"):
            val = str(bag.get(key) or "").strip()
            if val and re.search(r"\d", val):
                return val
        ctx = bag.get("depre_context")
        if isinstance(ctx, dict):
            val = str(ctx.get("depre") or ctx.get("DEPRE") or "").strip()
            if val and re.search(r"\d", val):
                return val
    return None


def _stamp_depre(record: dict[str, Any] | None, *bags: Any) -> dict[str, Any]:
    rec = dict(record or {})
    num = _depre_numero(rec, *bags)
    if num and not str(rec.get("DEPRE") or rec.get("depre") or "").strip():
        rec["DEPRE"] = num
    return rec


def _payload_bags(payload: dict[str, Any] | None) -> tuple[dict[str, Any], dict[str, Any]]:
    payload = payload if isinstance(payload, dict) else {}
    context = payload.get("context") or {}
    extras = context.get("extra_infos") if isinstance(context, dict) else {}
    if not isinstance(extras, dict):
        extras = {}
    updated = payload.get("updated_fields") or {}
    if not isinstance(updated, dict):
        updated = {}
    return extras, updated


def _apply_search_identity(
    record: dict[str, Any] | None,
    payload: dict[str, Any] | None,
    *,
    nome: str | None,
    cpf: str | None,
) -> dict[str, Any]:
    extras, updated = _payload_bags(payload)
    rec = _stamp_credor(record, nome, cpf)
    return _stamp_depre(rec, extras, updated, extras.get("depre_context"), extras.get("depre"))


def _with_depre_tag(motivo: str) -> str:
    text = str(motivo or "").strip() or "INAPTO: EXTINTO"
    if re.search(r"\(\s*DEPRE\s*\)\s*$", text, flags=re.I):
        return text
    return f"{text} (DEPRE)"


def _looks_sem_saldo(*values: Any) -> bool:
    for raw in values:
        if raw is None:
            continue
        text = str(raw).strip().casefold().replace("_", " ").replace("-", " ")
        text = " ".join(text.split())
        if not text:
            continue
        if text in {"sem saldo", "semsaldo"}:
            return True
        if "sem saldo" in text:
            return True
    return False


def _sem_saldo_from_payload(payload: dict[str, Any], extras: dict[str, Any], updated: dict[str, Any]) -> str | None:
    """Detecta Sem Saldo (prioridade DEPRE / cálculo) e devolve motivo legível."""
    status_candidates = (
        extras.get("prioridade_status"),
        updated.get("prioridade_status"),
        payload.get("prioridade_status"),
        extras.get("status"),
        updated.get("status"),
        updated.get("Calculo_Atualizado"),
        updated.get("calculo_atualizado"),
        extras.get("Calculo_Atualizado"),
        extras.get("calculo_atualizado"),
    )
    motivo_candidates = (
        extras.get("prioridade_motivo"),
        updated.get("prioridade_motivo"),
        payload.get("prioridade_motivo"),
        extras.get("motivo"),
        updated.get("motivo"),
    )
    if _looks_sem_saldo(*status_candidates, *motivo_candidates):
        motivo = next(
            (str(m).strip() for m in motivo_candidates if m and str(m).strip()),
            "Sem Saldo",
        )
        return f"INAPTO: Sem Saldo — {motivo}"

    record = payload.get("record")
    if isinstance(record, dict) and _looks_sem_saldo(
        record.get("Calculo_Atualizado"),
        record.get("calculo_atualizado"),
        record.get("Status"),
        record.get("status"),
    ):
        return "INAPTO: Sem Saldo (Calculo_Atualizado/Status no record)"
    return None


def _depre_block_motivo(*bags: Any) -> str | None:
    """DEPRE extinto/bloqueado — REFACTOR não chama a API de cálculo."""
    for bag in bags:
        if not isinstance(bag, dict):
            continue
        ctx = bag.get("depre_context")
        if not isinstance(ctx, dict):
            continue
        status = str(ctx.get("blocked_status") or "").strip()
        term = str(ctx.get("blocked_page_term") or "").strip()
        if ctx.get("is_blacklisted") is not True and not status and not term:
            continue
        key = status.casefold().replace("_", " ").replace("-", " ")
        if key in {"extinto", "extinta"}:
            return _with_depre_tag("INAPTO: EXTINTO")
        if status:
            return _with_depre_tag(f"INAPTO: {status.upper()}")
        if term:
            return _with_depre_tag(f"INAPTO: {term}")
        return _with_depre_tag("INAPTO: EXTINTO")
    return None


def _is_apto_from_json(payload: dict[str, Any] | None) -> tuple[bool, str | None]:
    if not payload:
        return False, "JSON ausente após pipeline (reprocessamento/cálculo)"
    context = payload.get("context") or {}
    extras = context.get("extra_infos") or {}
    if not isinstance(extras, dict):
        extras = {}
    updated = payload.get("updated_fields") or {}
    if not isinstance(updated, dict):
        updated = {}

    # 1) Sem Saldo (prioridade DEPRE / cálculo) — ex.: JOSE GAMERO 0006158…_5
    sem_saldo = _sem_saldo_from_payload(payload, extras, updated)
    if sem_saldo:
        return False, sem_saldo

    depre_block = _depre_block_motivo(extras, updated)
    if depre_block:
        return False, depre_block

    # 2) Blacklist
    docs_reason = _format_blacklist_docs(extras.get("blacklisted_docs"))
    if extras.get("is_blacklisted") is True:
        return False, (
            f"INAPTO: {docs_reason}" if docs_reason else "INAPTO: documento encontrado"
        )
    if docs_reason:
        return False, f"INAPTO: {docs_reason}"

    upd_docs = _format_blacklist_docs(updated.get("blacklisted_docs"))
    if updated.get("is_blacklisted") is True:
        return False, (
            f"INAPTO: {upd_docs}" if upd_docs else "INAPTO: documento encontrado"
        )
    if upd_docs:
        return False, f"INAPTO: {upd_docs}"

    depre = extras.get("depre") if isinstance(extras.get("depre"), dict) else None
    if isinstance(depre, dict) and depre.get("is_blacklisted") is True:
        depre_docs = _format_blacklist_docs(depre.get("blacklisted_docs"))
        return False, _with_depre_tag(
            f"INAPTO: {depre_docs}" if depre_docs else "INAPTO: documento encontrado"
        )
    if isinstance(depre, dict):
        if _looks_sem_saldo(depre.get("prioridade_status"), depre.get("status"), depre.get("motivo")):
            motivo = str(depre.get("prioridade_motivo") or depre.get("motivo") or "Sem Saldo").strip()
            return False, _with_depre_tag(f"INAPTO: Sem Saldo — {motivo}")

    # 3) Motivos explícitos de inaptidão no payload
    for key in ("motivo_inapto", "inapto_motivo", "skip_reason"):
        val = extras.get(key) or updated.get(key)
        if val and str(val).strip():
            if extras.get("apto") is False or str(extras.get("status") or "").lower() in {
                "inapto",
                "skipped",
                "blacklist",
                "sem saldo",
                "sem_saldo",
            }:
                return False, f"INAPTO: {str(val).strip()}"

    if extras.get("apto") is False:
        return False, "INAPTO: marcado como não apto no pipeline"

    return True, None


def _is_inapto_from_record(record: dict[str, Any] | None) -> str | None:
    """Fallback após merge com MySQL (Calculo_Atualizado / Status = Sem Saldo)."""
    if not isinstance(record, dict) or not record:
        return None
    if _looks_sem_saldo(
        record.get("Calculo_Atualizado"),
        record.get("calculo_atualizado"),
        record.get("Status"),
        record.get("status"),
    ):
        return "INAPTO: Sem Saldo (valor/status no cadastro)"
    return None


def _load_json(path: Path) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        logger.exception("Failed reading JSON %s", path)
        return None


def _artifact_paths(
    settings: Settings,
    processo: str,
    incidente: str,
) -> tuple[str | None, str | None, dict[str, Any] | None]:
    inc_norm = _normalize_inc(incidente)
    prefix = f"{_slug(processo)}_{_slug(inc_norm)}"
    json_dir = settings.final_output_dir / "json"
    main = json_dir / f"{prefix}.json"
    fanout = json_dir / f"{prefix}_fanout.json"
    if not main.is_file():
        alt = settings.refactor_path / "output" / "json" / f"{prefix}.json"
        if alt.is_file():
            main = alt
    if not fanout.is_file():
        alt_f = settings.refactor_path / "output" / "json" / f"{prefix}_fanout.json"
        if alt_f.is_file():
            fanout = alt_f
    payload = _load_json(main) or _load_json(fanout)
    return (
        str(main) if main.is_file() else None,
        str(fanout) if fanout.is_file() else None,
        payload,
    )


def _record_from_pipeline_json(payload: dict[str, Any] | None) -> dict[str, Any]:
    if not payload:
        return {}
    record = payload.get("record")
    if isinstance(record, dict):
        return record
    fanout = payload.get("fanout_records")
    if isinstance(fanout, list) and fanout and isinstance(fanout[0], dict):
        return fanout[0]
    return {}


def _rec_from_db_row(db_row: dict[str, Any]) -> PrecatorioRecord | None:
    processo = str(db_row.get("Numero_de_Processo") or "").strip()
    incidente = str(db_row.get("Numero_do_Incidente") or "").strip()
    if not processo or not incidente:
        return None
    classe = "RPV" if is_rpv_text(
        db_row.get("Classe"),
        db_row.get("classe"),
        db_row.get("Natureza"),
        db_row.get("natureza"),
        db_row.get("Tipo"),
        db_row.get("tipo"),
    ) else None
    return PrecatorioRecord(
        numero_de_processo=processo,
        numero_do_incidente=_normalize_inc(incidente),
        processo_codigo="",
        url="",
        label="database",
        processo_principal=None,
        classe=classe,
    )


def _rpv_display_record(
    rec: PrecatorioRecord,
    *,
    requerente: str | None = None,
    cpf: str | None = None,
) -> dict[str, Any]:
    return _stamp_credor(
        {
            "Numero_de_Processo": rec.numero_de_processo,
            "Numero_do_Incidente": rec.numero_do_incidente,
            "Classe": rec.classe or "RPV",
            "is_rpv": True,
            "label": rec.label,
        },
        requerente,
        cpf,
    )


def _append_rpv(
    aptos: list[AptoProcess],
    rec: PrecatorioRecord,
    *,
    requerente: str | None = None,
    cpf: str | None = None,
) -> None:
    incidente = _normalize_inc(rec.numero_do_incidente)
    aptos.append(
        AptoProcess(
            numero_de_processo=rec.numero_de_processo,
            numero_do_incidente=incidente,
            source="pipeline" if rec.label != "database" else "database",
            record=_rpv_display_record(rec, requerente=requerente, cpf=cpf),
            status="rpv",
            motivo=None,
        )
    )


def run_search_job(
    settings: Settings,
    nome: str | None = None,
    *,
    cpf: str | None = None,
    processo: str | None = None,
    search_kind: str | None = None,
    query_value: str | None = None,
    filter_nome: str | None = None,
    filter_incidente: str | None = None,
    incidente: str | None = None,
    on_progress: ProgressCb | None = None,
) -> dict[str, Any]:
    live: dict[str, dict[str, Any]] = {}
    last_pub = 0.0
    last_progress_msg = "Em processamento"
    analysis_complete_published = False
    all_records: list[PrecatorioRecord] = []
    errors: list[str] = []
    credor = {"nome": "", "cpf": ""}

    def progress(msg: str, snapshot: dict[str, Any] | None = None) -> None:
        nonlocal last_pub, last_progress_msg
        if analysis_complete_published and not str(msg or "").lower().startswith(
            "análise concluída"
        ):
            msg = last_progress_msg
        else:
            last_progress_msg = msg
        logger.info("[search] %s", msg)
        if not on_progress:
            return
        payload = snapshot
        now = time.monotonic()
        if payload is None and live and now - last_pub >= 0.8:
            payload = _live_snapshot()
        if payload is not None:
            last_pub = now
            try:
                on_progress(msg, payload)
                return
            except TypeError:
                pass
        on_progress(msg)

    def _live_snapshot() -> dict[str, Any]:
        aptos_out: list[dict[str, Any]] = []
        inaptos_out: list[dict[str, Any]] = []
        skipped_out: list[dict[str, Any]] = []
        for rec in all_records:
            item = live.get(_record_key(rec.numero_de_processo, rec.numero_do_incidente))
            if not item:
                continue
            incidente = _normalize_inc(rec.numero_do_incidente)
            source = "pipeline" if rec.label != "database" else "database"
            if item["kind"] == "inapto":
                motivo = str(item.get("motivo") or "INAPTO").strip()
                inaptos_out.append(
                    InaptoProcess(
                        numero_de_processo=rec.numero_de_processo,
                        numero_do_incidente=incidente,
                        source=source,
                        motivo=motivo,
                        record=item.get("record") or {},
                        json_path=item.get("json_path"),
                        fanout_json_path=item.get("fanout_path"),
                    ).model_dump()
                )
                skipped_out.append(
                    SkippedProcess(
                        numero_de_processo=rec.numero_de_processo,
                        numero_do_incidente=incidente,
                        reason=motivo,
                    ).model_dump()
                )
                continue
            status = "rpv" if item["kind"] == "rpv" else (
                "apto" if item["kind"] == "apto" else "processando"
            )
            aptos_out.append(
                AptoProcess(
                    numero_de_processo=rec.numero_de_processo,
                    numero_do_incidente=incidente,
                    source=source,
                    record=item.get("record") or {},
                    json_path=item.get("json_path"),
                    fanout_json_path=item.get("fanout_path"),
                    status=status,  # type: ignore[arg-type]
                    motivo=item.get("motivo"),
                    stage=item.get("stage"),
                ).model_dump()
            )
        return {
            "nome": query.label,
            "search_kind": query.kind.value,
            "query": query.label,
            "filter_nome": filtro or query.filter_nome,
            "filter_incidente": inc_filtro or query.filter_incidente,
            "processos_aptos": aptos_out,
            "processos_inaptos": inaptos_out,
            "skipped": skipped_out,
            "artifacts": {},
            "errors": list(errors),
            "analysis_complete": all(
                item.get("kind") in {"rpv", "inapto"}
                or (
                    item.get("kind") == "apto"
                    and str(item.get("stage") or "") == "pronto"
                )
                for item in live.values()
            ) if live else False,
        }

    def _refresh_live_item(item: dict[str, Any]) -> None:
        recd = item.get("record") or {}
        if str(recd.get("monday_outcome") or "") == "ja_atribuido":
            return
        rec: PrecatorioRecord = item["rec"]
        json_path, fanout_path, payload = _artifact_paths(
            settings, rec.numero_de_processo, rec.numero_do_incidente
        )
        item["json_path"] = json_path
        item["fanout_path"] = fanout_path
        if not payload:
            item["record"] = _apply_search_identity(
                item.get("record") or {},
                None,
                nome=credor["nome"],
                cpf=credor["cpf"],
            )
            return
        record = _record_from_pipeline_json(payload)
        merged = dict(item.get("record") or {})
        if record:
            merged.update(record)
        item["record"] = _apply_search_identity(
            merged, payload, nome=credor["nome"], cpf=credor["cpf"]
        )
        if item["kind"] in {"rpv", "inapto"}:
            return
        apto, reason = _is_apto_from_json(payload)
        if not apto:
            motivo = (reason or "INAPTO: motivo não informado").strip()
            if not motivo.upper().startswith("INAPTO"):
                motivo = f"INAPTO: {motivo}"
            item["kind"] = "inapto"
            item["status"] = "inapto"
            item["motivo"] = motivo
            return
        merged = item.get("record") or {}
        if merged.get("Calculo_Atualizado") or merged.get("calculo_atualizado"):
            merged["calculo_status"] = "ok"
            item["stage"] = "pronto"
            merged["pipeline_stage"] = "pronto"

    def _apply_log_line(line: str) -> None:
        info = parse_refactor_log_line(line)
        if not info:
            return
        key = _record_key(info["processo"], info["incidente"])
        item = live.get(key)
        if not item or item["kind"] in {"rpv", "inapto"}:
            return
        if str((item.get("record") or {}).get("monday_outcome") or "") == "ja_atribuido":
            return
        event = info["event"]
        recd = item.setdefault("record", {})
        if info.get("depre"):
            recd["DEPRE"] = info["depre"]
        if event == "calculo":
            item["stage"] = "calculo"
            recd["pipeline_stage"] = "calculo"
            recd["calculo_status"] = "aguardando"
            progress(
                f"Aguardando atualização de cálculo {info['processo']}/{info['incidente']}",
                _live_snapshot(),
            )
        elif event in {"start", "autos", "touch"}:
            if item.get("stage") in {None, "encontrado"}:
                item["stage"] = "autos"
                recd["pipeline_stage"] = "autos"
        elif event == "blacklist":
            item["kind"] = "inapto"
            item["status"] = "inapto"
            motivo = "INAPTO: documento encontrado"
            if info.get("from_depre"):
                motivo = _with_depre_tag(motivo)
            item["motivo"] = motivo
        elif event == "extinto":
            item["kind"] = "inapto"
            item["status"] = "inapto"
            status = str(info.get("blocked_status") or "EXTINTO").strip().upper()
            if status in {"200", "OK"}:
                status = "EXTINTO"
            item["motivo"] = _with_depre_tag(f"INAPTO: {status}")
        _refresh_live_item(item)
        _publish_if_analysis_complete()

    def _live_terminal() -> bool:
        if not live:
            return False
        for item in live.values():
            kind = item.get("kind")
            if kind in {"rpv", "inapto"}:
                continue
            if kind == "apto" and str(item.get("stage") or "") == "pronto":
                continue
            recd = item.get("record") or {}
            if recd.get("calculo_status") == "ok":
                continue
            return False
        return True

    def _publish_if_analysis_complete() -> None:
        nonlocal analysis_complete_published
        if analysis_complete_published or not _live_terminal():
            return
        analysis_complete_published = True
        n_rpv = sum(1 for item in live.values() if item.get("kind") == "rpv")
        n_inapto = sum(1 for item in live.values() if item.get("kind") == "inapto")
        n_apto = len(live) - n_rpv - n_inapto
        progress(
            f"Análise concluída · {n_apto} apto(s) · {n_rpv} RPV · {n_inapto} inapto(s)",
            _live_snapshot(),
        )

    def _tick_live() -> None:
        for item in live.values():
            if item["kind"] == "rpv":
                continue
            if str((item.get("record") or {}).get("monday_outcome") or "") == "ja_atribuido":
                continue
            _refresh_live_item(item)
        if _live_terminal():
            _publish_if_analysis_complete()
        else:
            progress(last_progress_msg)

    # Compat: chamadas antigas passavam só o label em `nome` (era sempre NMPARTE).
    # Novas chamadas passam search_kind + query_value ou nome/cpf/processo explícitos.
    kind = (search_kind or "").strip().lower()
    value = (query_value or "").strip()
    filtro = " ".join(str(filter_nome or "").split()).strip() or None
    inc_filtro = str(filter_incidente or incidente or "").strip() or None

    if kind in {"cpf", "credor"} and value and not (cpf and str(cpf).strip()):
        cpf = value
        processo = None
        if kind == "cpf":
            nome = None
        else:
            nome = filtro
    elif kind == "processo" and value and not (processo and str(processo).strip()):
        processo = value
        # filtro Monday: coluna dedicada ou rótulo "CNJ · NOME"
        if not filtro and nome and "·" in str(nome):
            # label ex.: "0017… · Jose Silva" — extrair parte após ·
            parts = str(nome).split("·", 1)
            if len(parts) == 2 and parts[1].strip():
                filtro = parts[1].strip()
        cpf = cpf if (cpf and str(cpf).strip()) else None
        who_digits = _cpf_digits(filtro)
        if len(who_digits) == 11 and (not cpf or who_digits == _cpf_digits(cpf)):
            cpf = cpf or who_digits
            filtro = None
            nome = None
        elif filtro:
            nome = filtro
        elif not (nome and str(nome).strip()) and not cpf:
            nome = None
    elif kind == "nome" and value and not (nome and str(nome).strip()):
        nome = value

    query = resolve_search_query(
        nome=nome, cpf=cpf, processo=processo, incidente=inc_filtro
    )
    if query.filter_nome:
        filtro = query.filter_nome
    if query.filter_incidente:
        inc_filtro = query.filter_incidente
    credor["nome"] = " ".join(
        str(filtro or query.filter_nome or nome or "").split()
    ).strip()
    credor["cpf"] = _cpf_digits(cpf)
    if query.kind in {SearchKind.CPF, SearchKind.CREDOR}:
        credor["cpf"] = _cpf_digits(query.value) or credor["cpf"]

    reader = PrecainfosReader(settings)
    aptos: list[AptoProcess] = []
    inaptos: list[InaptoProcess] = []
    skipped: list[SkippedProcess] = []
    seen: set[str] = set()

    progress("Consultando o e-SAJ")
    scrape = scrape_by_query(settings, query, on_progress=progress)
    errors.extend(scrape.errors)

    # Índice único: scrape + linhas extras do MySQL conforme o tipo de busca
    by_key: dict[str, PrecatorioRecord] = {}
    for rec in scrape.records:
        by_key[_record_key(rec.numero_de_processo, rec.numero_do_incidente)] = rec

    db_hits = 0
    for rec in list(by_key.values()):
        try:
            if reader.fetch_one(rec.numero_de_processo, rec.numero_do_incidente):
                db_hits += 1
        except Exception as exc:
            errors.append(
                f"DB lookup falhou para {_record_key(rec.numero_de_processo, rec.numero_do_incidente)}: {exc}"
            )

    progress("Consultando cadastro")
    try:
        extra_rows: list[dict[str, Any]] = []
        if query.kind == SearchKind.NOME:
            extra_rows = reader.fetch_by_requerente(query.value)
        elif query.kind in {SearchKind.CPF, SearchKind.CREDOR}:
            extra_rows = reader.fetch_by_cpf(query.value)
            if query.kind == SearchKind.CREDOR and filtro:
                extra_rows = [
                    row
                    for row in extra_rows
                    if names_match(
                        filtro,
                        [
                            str(row.get("Requerente") or ""),
                            str(row.get("requerente") or ""),
                        ],
                    )
                ]
        elif query.kind == SearchKind.PROCESSO:
            extra_rows = reader.fetch_by_processo(query.value)
            # Com filtro de nome na capa: só acrescentar do MySQL o que bate no nome
            if filtro:
                extra_rows = [
                    row
                    for row in extra_rows
                    if names_match(
                        filtro,
                        [
                            str(row.get("Requerente") or ""),
                            str(row.get("requerente") or ""),
                        ],
                    )
                ]
            if inc_filtro:
                extra_rows = [
                    row
                    for row in extra_rows
                    if _normalize_inc(
                        str(
                            row.get("Numero_do_Incidente")
                            or row.get("numero_do_incidente")
                            or row.get("Incidente")
                            or ""
                        )
                    )
                    == _normalize_inc(inc_filtro)
                ]
        for db_row in extra_rows:
            extra = _rec_from_db_row(db_row)
            if not extra:
                continue
            key = _record_key(extra.numero_de_processo, extra.numero_do_incidente)
            if key not in by_key:
                by_key[key] = extra
    except Exception as exc:
        errors.append(f"Merge MySQL falhou ({query.kind.value}): {exc}")

    all_records = list(by_key.values())
    if inc_filtro:
        wanted = _normalize_inc(inc_filtro)
        all_records = [
            rec
            for rec in all_records
            if _normalize_inc(rec.numero_do_incidente) == wanted
        ]
    rpv_records = [rec for rec in all_records if record_is_rpv(rec)]
    to_enrich = [rec for rec in all_records if not record_is_rpv(rec)]
    for rec in rpv_records:
        live[_record_key(rec.numero_de_processo, rec.numero_do_incidente)] = {
            "rec": rec,
            "kind": "rpv",
            "status": "rpv",
            "stage": "pronto",
            "motivo": None,
            "record": _rpv_display_record(
                rec, requerente=credor["nome"], cpf=credor["cpf"]
            ),
            "json_path": None,
            "fanout_path": None,
        }
    for rec in to_enrich:
        live[_record_key(rec.numero_de_processo, rec.numero_do_incidente)] = {
            "rec": rec,
            "kind": "processando",
            "status": "processando",
            "stage": "encontrado",
            "motivo": None,
            "record": _stamp_credor(
                {
                    "Numero_de_Processo": rec.numero_de_processo,
                    "Numero_do_Incidente": _normalize_inc(rec.numero_do_incidente),
                    "Classe": rec.classe or rec.label or None,
                    "pipeline_stage": "encontrado",
                    "calculo_status": "pendente",
                },
                credor["nome"],
                credor["cpf"],
            ),
            "json_path": None,
            "fanout_path": None,
        }
    found_msg = (
        f"Encontrados {len(all_records)} caso(s)"
        + (f" · {len(rpv_records)} RPV" if rpv_records else "")
        + (f" (filtro {filtro})" if filtro else "")
    )
    progress(found_msg, _live_snapshot())

    monday_skipped: list[PrecatorioRecord] = []
    if to_enrich:
        progress("Verificando Monday", _live_snapshot())
        to_enrich, monday_skipped = _split_after_monday_precheck(
            to_enrich,
            live,
            lookup=lookup_incident,
            requerente=credor["nome"],
            cpf=credor["cpf"],
        )
        if monday_skipped:
            progress(
                f"Monday: {len(monday_skipped)} caso(s) em tratamento",
                _live_snapshot(),
            )
            _publish_if_analysis_complete()

    enrich_result = None
    if to_enrich:
        progress(
            f"Baixando os autos e atualizando o cálculo ({len(to_enrich)} caso(s))",
            _live_snapshot(),
        )
        enrich_result = enrich_for_search(
            settings,
            to_enrich,
            search_url=scrape.search_url,
            links_count=scrape.precatorio_links_found,
            prior_errors=[],
            on_log_line=_apply_log_line,
            on_tick=_tick_live,
        )
        errors.extend(enrich_result.errors)

        for rec in to_enrich:
            key = _record_key(rec.numero_de_processo, rec.numero_do_incidente)
            json_path, fanout_path, payload = _artifact_paths(
                settings,
                rec.numero_de_processo,
                rec.numero_do_incidente,
            )
            apto, reason = _is_apto_from_json(payload)
            record = _record_from_pipeline_json(payload)
            try:
                db_row = reader.fetch_one(rec.numero_de_processo, rec.numero_do_incidente)
            except Exception:
                db_row = None
            if db_row:
                record = _stringify_record(db_row)
            record = _apply_search_identity(
                record, payload, nome=credor["nome"], cpf=credor["cpf"]
            )
            record = _merge_monday_context(record, live.get(key))

            if apto:
                db_inapto = _is_inapto_from_record(record)
                if db_inapto:
                    apto = False
                    reason = db_inapto

            incidente = _normalize_inc(rec.numero_do_incidente)
            source = "pipeline"
            if not apto:
                motivo = (reason or "INAPTO: motivo não informado").strip()
                if not motivo.upper().startswith("INAPTO"):
                    motivo = f"INAPTO: {motivo}"
                inapto = InaptoProcess(
                    numero_de_processo=rec.numero_de_processo,
                    numero_do_incidente=incidente,
                    source=source,
                    motivo=motivo,
                    record=record,
                    json_path=json_path,
                    fanout_json_path=fanout_path,
                )
                inaptos.append(inapto)
                skipped.append(
                    SkippedProcess(
                        numero_de_processo=rec.numero_de_processo,
                        numero_do_incidente=incidente,
                        reason=motivo,
                    )
                )
                seen.add(key)
                continue

            aptos.append(
                AptoProcess(
                    numero_de_processo=rec.numero_de_processo,
                    numero_do_incidente=incidente,
                    source=source,
                    record=record,
                    json_path=json_path,
                    fanout_json_path=fanout_path,
                )
            )
            seen.add(key)

    for rec in monday_skipped:
        key = _record_key(rec.numero_de_processo, rec.numero_do_incidente)
        if key in seen:
            continue
        item = live.get(key) or {}
        incidente = _normalize_inc(rec.numero_do_incidente)
        source = "pipeline" if rec.label != "database" else "database"
        aptos.append(
            AptoProcess(
                numero_de_processo=rec.numero_de_processo,
                numero_do_incidente=incidente,
                source=source,
                record=item.get("record") or {},
                json_path=item.get("json_path"),
                fanout_json_path=item.get("fanout_path"),
                status="apto",
                stage="pronto",
            )
        )
        seen.add(key)

    for rec in rpv_records:
        key = _record_key(rec.numero_de_processo, rec.numero_do_incidente)
        if key in seen:
            continue
        _append_rpv(aptos, rec, requerente=credor["nome"], cpf=credor["cpf"])
        seen.add(key)

    artifacts = SearchJobArtifacts(
        final_output_dir=str(settings.final_output_dir),
        scrape_snapshot=scrape.scrape_snapshot_path,
        manifest=enrich_result.manifest_path if enrich_result else scrape.scrape_snapshot_path,
        run_index=enrich_result.run_index_path if enrich_result else None,
    )

    n_rpv = sum(1 for p in aptos if p.status == "rpv")
    n_apto = len(aptos) - n_rpv
    progress(
        f"Concluído · {n_apto} apto(s) · {n_rpv} RPV · {len(inaptos)} inapto(s)"
    )
    return {
        "nome": query.label,
        "search_kind": query.kind.value,
        "query": query.label,
        "filter_nome": filtro or query.filter_nome,
        "filter_incidente": inc_filtro or query.filter_incidente,
        "processos_aptos": [p.model_dump() for p in aptos],
        "processos_inaptos": [p.model_dump() for p in inaptos],
        "skipped": [s.model_dump() for s in skipped],
        "artifacts": artifacts.model_dump(),
        "errors": errors,
        "scrape": {
            "kind": query.kind.value,
            "links_found": scrape.precatorio_links_found,
            "extracted": len(scrape.records),
            "search_url": scrape.search_url,
            "reprocessados": len(to_enrich),
            "monday_em_tratamento": len(monday_skipped),
            "rpv": len(rpv_records),
            "filter_nome": filtro or query.filter_nome,
            "filter_incidente": inc_filtro or query.filter_incidente,
        },
    }
