"""Search orchestration: scrape → reprocessamento+cálculo → APT / INAPTO lists."""

from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from typing import Any, Callable

from api.adapters.pipeline_adapter import (
    PrecatorioRecord,
    enrich_for_search,
    resolve_search_query,
    scrape_by_query,
)
from api.schemas import AptoProcess, InaptoProcess, SearchJobArtifacts, SkippedProcess
from api.services.precainfos_reader import PrecainfosReader
from tjsp_pipeline.config import Settings
from tjsp_pipeline.scraper.search_url import SearchKind
from tjsp_pipeline.scraper.show_page import names_match

logger = logging.getLogger(__name__)

ProgressCb = Callable[[str], None]


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


def _format_blacklist_docs(docs: Any) -> str | None:
    if not isinstance(docs, list) or not docs:
        return None
    parts: list[str] = []
    for item in docs[:8]:
        if isinstance(item, str) and item.strip():
            parts.append(item.strip())
        elif isinstance(item, dict):
            title = (
                item.get("title")
                or item.get("titulo")
                or item.get("documento")
                or item.get("doc")
                or item.get("tipo")
            )
            status = item.get("status") or item.get("motivo") or item.get("reason")
            if title and status:
                parts.append(f"{title} ({status})")
            elif title:
                parts.append(str(title).strip())
            elif status:
                parts.append(str(status).strip())
            else:
                bit = item.get("motivo_blacklist")
                if bit:
                    parts.append(str(bit).strip())
                else:
                    parts.append(json.dumps(item, ensure_ascii=False)[:120])
    if not parts:
        return None
    # dedupe preserving order
    seen: set[str] = set()
    uniq: list[str] = []
    for p in parts:
        if p not in seen:
            seen.add(p)
            uniq.append(p)
    return "blacklist: " + "; ".join(uniq)


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

    # 2) Blacklist
    docs_reason = _format_blacklist_docs(extras.get("blacklisted_docs"))
    if extras.get("is_blacklisted") is True:
        return False, (
            f"INAPTO: {docs_reason}" if docs_reason else "INAPTO: blacklisted (is_blacklisted)"
        )
    if docs_reason:
        return False, f"INAPTO: {docs_reason}"

    upd_docs = _format_blacklist_docs(updated.get("blacklisted_docs"))
    if updated.get("is_blacklisted") is True:
        return False, (
            f"INAPTO: {upd_docs}" if upd_docs else "INAPTO: blacklisted (updated_fields)"
        )
    if upd_docs:
        return False, f"INAPTO: {upd_docs}"

    depre = extras.get("depre") if isinstance(extras.get("depre"), dict) else None
    if isinstance(depre, dict) and depre.get("is_blacklisted") is True:
        depre_docs = _format_blacklist_docs(depre.get("blacklisted_docs"))
        return False, (
            f"INAPTO: {depre_docs}" if depre_docs else "INAPTO: blacklisted (depre)"
        )
    if isinstance(depre, dict):
        if _looks_sem_saldo(depre.get("prioridade_status"), depre.get("status"), depre.get("motivo")):
            motivo = str(depre.get("prioridade_motivo") or depre.get("motivo") or "Sem Saldo").strip()
            return False, f"INAPTO: Sem Saldo — {motivo}"

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
    return PrecatorioRecord(
        numero_de_processo=processo,
        numero_do_incidente=_normalize_inc(incidente),
        processo_codigo="",
        url="",
        label="database",
        processo_principal=None,
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
    on_progress: ProgressCb | None = None,
) -> dict[str, Any]:
    def progress(msg: str) -> None:
        logger.info("[search] %s", msg)
        if on_progress:
            on_progress(msg)

    # Compat: chamadas antigas passavam só o label em `nome` (era sempre NMPARTE).
    # Novas chamadas passam search_kind + query_value ou nome/cpf/processo explícitos.
    kind = (search_kind or "").strip().lower()
    value = (query_value or "").strip()
    filtro = " ".join(str(filter_nome or "").split()).strip() or None

    if kind == "cpf" and value and not (cpf and str(cpf).strip()):
        cpf = value
        nome = None
        processo = None
    elif kind == "processo" and value and not (processo and str(processo).strip()):
        processo = value
        # filtro Monday: coluna dedicada ou rótulo "CNJ · NOME"
        if not filtro and nome and "·" in str(nome):
            # label ex.: "0017… · Jose Silva" — extrair parte após ·
            parts = str(nome).split("·", 1)
            if len(parts) == 2 and parts[1].strip():
                filtro = parts[1].strip()
        if filtro:
            nome = filtro  # build_search_query exige processo+nome
        cpf = None
    elif kind == "nome" and value and not (nome and str(nome).strip()):
        nome = value

    query = resolve_search_query(nome=nome, cpf=cpf, processo=processo)
    if query.filter_nome:
        filtro = query.filter_nome

    reader = PrecainfosReader(settings)
    aptos: list[AptoProcess] = []
    inaptos: list[InaptoProcess] = []
    skipped: list[SkippedProcess] = []
    errors: list[str] = []
    seen: set[str] = set()

    progress(
        f"scrape:e-saj:{query.kind.value}"
        + (f":filtro_nome={filtro}" if filtro else "")
    )
    scrape = scrape_by_query(settings, query)
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

    progress(f"db:merge:{query.kind.value}")
    try:
        extra_rows: list[dict[str, Any]] = []
        if query.kind == SearchKind.NOME:
            extra_rows = reader.fetch_by_requerente(query.value)
        elif query.kind == SearchKind.CPF:
            extra_rows = reader.fetch_by_cpf(query.value)
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
    progress(
        f"scrape_done:kind={query.kind.value} links={scrape.precatorio_links_found} "
        f"extracted={len(scrape.records)} db_hits={db_hits} "
        f"to_reprocess={len(all_records)}"
        + (f" filter_nome={filtro}" if filtro else "")
    )

    enrich_result = None
    if all_records:
        progress(f"refactor:reprocessamento+calculo:{len(all_records)}")
        enrich_result = enrich_for_search(
            settings,
            all_records,
            search_url=scrape.search_url,
            links_count=scrape.precatorio_links_found,
            prior_errors=[],
        )
        errors.extend(enrich_result.errors)

        for rec in all_records:
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

    artifacts = SearchJobArtifacts(
        final_output_dir=str(settings.final_output_dir),
        scrape_snapshot=scrape.scrape_snapshot_path,
        manifest=enrich_result.manifest_path if enrich_result else scrape.scrape_snapshot_path,
        run_index=enrich_result.run_index_path if enrich_result else None,
    )

    progress(f"done:aptos={len(aptos)} inaptos={len(inaptos)}")
    return {
        "nome": query.label,
        "search_kind": query.kind.value,
        "query": query.label,
        "filter_nome": filtro or query.filter_nome,
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
            "reprocessados": len(all_records),
            "filter_nome": filtro or query.filter_nome,
        },
    }
