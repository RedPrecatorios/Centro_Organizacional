"""End-to-end pipeline: search → precatórios → REFACTOR JSON."""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass, replace
from datetime import datetime
from pathlib import Path

from tjsp_pipeline.browser.driver import esaj_browser
from tjsp_pipeline.browser.js_monitor import collect_precatorio_links_js
from tjsp_pipeline.config import Settings
from tjsp_pipeline.integration.final_output import (
    assert_output_layout,
    publish_final_output,
    write_run_index,
)
from tjsp_pipeline.integration.refactor_runner import (
    collect_refactor_json_outputs,
    json_paths_for_records,
    run_refactor,
    save_pipeline_manifest,
    write_processos_txt,
)
from tjsp_pipeline.scraper.expand_incidentes import expand_all_incidentes
from tjsp_pipeline.scraper.search import (
    filter_precatorio_links,
    merge_js_links,
    parse_all_incidente_links_from_html,
    parse_precatorio_links_from_html,
)
from tjsp_pipeline.scraper.show_page import (
    PrecatorioRecord,
    extract_from_show_html,
    extract_partes_capa,
    names_match,
)

logger = logging.getLogger(__name__)


@dataclass
class PipelineResult:
    search_url: str
    precatorio_links_found: int
    records: list[PrecatorioRecord]
    refactor_exit_code: int | None
    json_outputs: list[str]
    final_json_outputs: list[str]
    final_output_dir: str | None
    manifest_path: str | None
    run_index_path: str | None
    errors: list[str]
    search_kind: str | None = None
    search_label: str | None = None


@dataclass
class ScrapeResult:
    search_url: str
    precatorio_links_found: int
    records: list[PrecatorioRecord]
    scrape_snapshot_path: str
    errors: list[str]
    search_kind: str | None = None
    search_label: str | None = None


def _with_output_dir(settings: Settings, output_dir: Path | None) -> Settings:
    """Return settings pointing final_output_dir at a per-query folder."""
    if output_dir is None:
        return settings
    return replace(settings, final_output_dir=Path(output_dir))


def scrape_precatorios(
    settings: Settings,
    *,
    search_url: str | None = None,
    limit: int | None = None,
    processo_codigos: list[str] | None = None,
    search_kind: str | None = None,
    search_label: str | None = None,
    filter_nome: str | None = None,
) -> ScrapeResult:
    """Collect Precatório / incidente records from e-SAJ (no REFACTOR).

    When ``filter_nome`` is set (busca por processo + nome Monday), opens **all**
    incidente capas and keeps every match — no autos download; name is on the capa.
    Multiple hits with the same name are all returned.
    """
    settings.ensure_dirs()
    url = search_url or settings.esaj_search_url
    errors: list[str] = []
    records: list[PrecatorioRecord] = []
    links_count = 0
    nome_filtro = " ".join(str(filter_nome or "").split()).strip() or None
    filter_by_capa = bool(nome_filtro)

    logger.info(
        "=== TJSP Scrape START === kind=%s label=%s filter_nome=%s url=%s",
        search_kind,
        search_label,
        nome_filtro,
        url,
    )

    with esaj_browser(settings) as browser:
        search_html = browser.navigate(url)
        browser.save_debug_html("search_results", search_html)

        assert browser.driver is not None
        expand_all_incidentes(browser.driver)
        search_html = browser.driver.page_source or search_html
        browser.save_debug_html("search_expanded", search_html)
        current_url = browser.driver.current_url or url

        from tjsp_pipeline.scraper.search import PrecatorioLink, PROCESSO_CODIGO_PATTERN

        if filter_by_capa:
            # Todos os incidentes (não só Precatório) — validação pelo nome na capa.
            all_links = parse_all_incidente_links_from_html(search_html)
            if not all_links and "show.do" in current_url:
                m = PROCESSO_CODIGO_PATTERN.search(current_url)
                codigo = m.group(1) if m else "CURRENT"
                all_links = [
                    PrecatorioLink(
                        url=current_url,
                        processo_codigo=codigo,
                        label="show.do",
                        source="direct",
                    )
                ]
            # Também incluir a página atual se for show.do e não estiver na lista
            if "show.do" in current_url:
                m = PROCESSO_CODIGO_PATTERN.search(current_url)
                codigo = m.group(1) if m else "CURRENT"
                if not any(link.processo_codigo == codigo for link in all_links):
                    all_links.insert(
                        0,
                        PrecatorioLink(
                            url=current_url,
                            processo_codigo=codigo,
                            label="capa atual",
                            source="direct",
                        ),
                    )
        else:
            html_links = parse_precatorio_links_from_html(search_html)
            js_links = collect_precatorio_links_js(browser.driver)
            all_links = filter_precatorio_links(merge_js_links(html_links, js_links))

            # NUMPROC often redirects straight to show.do — treat current page if needed.
            if not all_links and "show.do" in current_url:
                m = PROCESSO_CODIGO_PATTERN.search(current_url)
                codigo = m.group(1) if m else "CURRENT"
                logger.info(
                    "NUMPROC/show.do redirect — trying extract from current page codigo=%s",
                    codigo,
                )
                direct = extract_from_show_html(
                    search_html,
                    url=current_url,
                    processo_codigo=codigo,
                    label="Precatório",
                )
                if direct:
                    records.append(direct)
                else:
                    all_links = [
                        PrecatorioLink(
                            url=current_url,
                            processo_codigo=codigo,
                            label="show.do",
                            source="direct",
                        )
                    ]

        links_count = len(all_links) if all_links else len(records)

        if processo_codigos:
            wanted = {c.strip().upper() for c in processo_codigos if c.strip()}
            all_links = [
                link for link in all_links if link.processo_codigo.upper() in wanted
            ]
            logger.info(
                "Filtered to processo_codigo=%s → %s link(s)",
                sorted(wanted),
                len(all_links),
            )

        if limit is not None and limit > 0:
            all_links = all_links[:limit]
            logger.info("Limiting to %s link(s) for show.do visits", len(all_links))

        if not all_links and not records:
            msg = (
                "Nenhum incidente encontrado na página de busca"
                if filter_by_capa
                else "Nenhum link de Precatório encontrado na página de busca"
            )
            errors.append(msg)
            browser.save_debug_html("search_no_precatorios", search_html)

        visited: set[str] = set()
        name_hits = 0
        current_codigo = None
        if "show.do" in current_url:
            _cm = PROCESSO_CODIGO_PATTERN.search(current_url)
            current_codigo = _cm.group(1) if _cm else None

        for link in all_links:
            if link.processo_codigo in visited:
                continue
            visited.add(link.processo_codigo)
            try:
                # Página já carregada (redirect NUMPROC) — reutilizar HTML
                if (
                    link.source == "direct"
                    and current_codigo
                    and link.processo_codigo == current_codigo
                ):
                    show_html = search_html
                else:
                    show_html = browser.navigate(link.url, wait_seconds=5)
                browser.save_debug_html(
                    f"show_{link.processo_codigo}",
                    show_html,
                )

                if filter_by_capa:
                    partes = extract_partes_capa(show_html)
                    if not names_match(nome_filtro or "", partes):
                        logger.info(
                            "Capa sem match de nome | codigo=%s | nome=%r | partes=%s",
                            link.processo_codigo,
                            nome_filtro,
                            partes[:8],
                        )
                        continue
                    name_hits += 1
                    logger.info(
                        "Capa MATCH nome | codigo=%s | nome=%r | partes=%s",
                        link.processo_codigo,
                        nome_filtro,
                        partes[:8],
                    )
                    record = extract_from_show_html(
                        show_html,
                        url=link.url,
                        processo_codigo=link.processo_codigo,
                        label=link.label,
                        require_precatorio_title=False,
                    )
                else:
                    record = extract_from_show_html(
                        show_html,
                        url=link.url,
                        processo_codigo=link.processo_codigo,
                        label=link.label,
                    )

                if record:
                    records.append(record)
                else:
                    errors.append(
                        f"Falha ao extrair dados de {link.processo_codigo} ({link.url})"
                    )
            except Exception as exc:
                msg = f"Erro ao processar {link.processo_codigo}: {exc}"
                logger.exception(msg)
                errors.append(msg)

        if filter_by_capa and all_links and name_hits == 0:
            errors.append(
                f"Nenhum incidente com o nome «{nome_filtro}» na capa "
                f"(verificados {len(visited)} incidente(s))"
            )

    if limit is not None and limit > 0:
        records = records[:limit]
        logger.info("Limiting to %s record(s) for processing", len(records))

    scrape_snapshot = settings.log_dir / f"scrape_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    scrape_snapshot.write_text(
        json.dumps(
            {
                "search_url": url,
                "search_kind": search_kind,
                "search_label": search_label,
                "filter_nome": nome_filtro,
                "output_dir": str(settings.final_output_dir),
                "links_found": links_count,
                "records": [asdict(r) for r in records],
                "errors": errors,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    logger.info("Scrape snapshot: %s", scrape_snapshot)
    logger.info("=== TJSP Scrape DONE === records=%s links=%s", len(records), links_count)

    return ScrapeResult(
        search_url=url,
        precatorio_links_found=links_count,
        records=records,
        scrape_snapshot_path=str(scrape_snapshot),
        errors=errors,
        search_kind=search_kind,
        search_label=search_label,
    )


def enrich_records(
    settings: Settings,
    records: list[PrecatorioRecord],
    *,
    search_url: str = "",
    links_count: int = 0,
    prior_errors: list[str] | None = None,
    refactor_mode: str | None = None,
    scrape_snapshot_path: str | None = None,
    search_kind: str | None = None,
    search_label: str | None = None,
) -> PipelineResult:
    """Run REFACTOR + publish artifacts for the given records only."""
    settings.ensure_dirs()
    errors: list[str] = list(prior_errors or [])
    refactor_exit: int | None = None
    run_started_at = datetime.now()

    if not records:
        return PipelineResult(
            search_url=search_url,
            precatorio_links_found=links_count,
            records=[],
            refactor_exit_code=None,
            json_outputs=[],
            final_json_outputs=[],
            final_output_dir=str(settings.final_output_dir),
            manifest_path=scrape_snapshot_path,
            run_index_path=None,
            errors=errors,
            search_kind=search_kind,
            search_label=search_label,
        )

    if not (settings.refactor_path / "main.py").is_file():
        msg = (
            f"REFACTOR_TJSP-main não encontrado em {settings.refactor_path}. "
            "Ajuste REFACTOR_TJSP_PATH no .env, ou use --scrape-only."
        )
        logger.error(msg)
        errors.append(msg)
        return PipelineResult(
            search_url=search_url,
            precatorio_links_found=links_count,
            records=records,
            refactor_exit_code=None,
            json_outputs=[],
            final_json_outputs=[],
            final_output_dir=str(settings.final_output_dir),
            manifest_path=scrape_snapshot_path,
            run_index_path=None,
            errors=errors,
            search_kind=search_kind,
            search_label=search_label,
        )

    txt_name = f"pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    txt_path = settings.refactor_path / txt_name
    write_processos_txt(records, txt_path)

    mode = (refactor_mode or settings.refactor_mode or "preenchimento").strip().lower()
    proc = run_refactor(settings, txt_name, mode=mode)
    refactor_exit = proc.returncode
    stdout_all = proc.stdout or ""
    stderr_all = proc.stderr or ""
    if proc.stdout:
        logger.info("REFACTOR stdout (%s):\n%s", mode, proc.stdout[-2000:])
    if proc.stderr:
        logger.warning("REFACTOR stderr (%s):\n%s", mode, proc.stderr[-2000:])
    if proc.returncode != 0:
        errors.append(f"REFACTOR ({mode}) exit code {proc.returncode}")

    produced = json_paths_for_records(settings, records)
    if (
        mode != "reprocessamento"
        and proc.returncode == 0
        and not produced
        and records
    ):
        logger.warning(
            "REFACTOR preenchimento produced no JSON for %s record(s) "
            "(likely already in precainfosnew) — retrying with --reprocessamento",
            len(records),
        )
        proc2 = run_refactor(settings, txt_name, mode="reprocessamento")
        refactor_exit = proc2.returncode
        stdout_all = (stdout_all + "\n" + (proc2.stdout or "")).strip()
        stderr_all = (stderr_all + "\n" + (proc2.stderr or "")).strip()
        if proc2.stdout:
            logger.info("REFACTOR stdout (reprocessamento):\n%s", proc2.stdout[-2000:])
        if proc2.stderr:
            logger.warning(
                "REFACTOR stderr (reprocessamento):\n%s", proc2.stderr[-2000:]
            )
        if proc2.returncode != 0:
            errors.append(f"REFACTOR (reprocessamento) exit code {proc2.returncode}")
        else:
            produced = json_paths_for_records(settings, records)

    json_paths = collect_refactor_json_outputs(
        settings,
        settings.log_dir,
        records=records,
    )
    manifest = save_pipeline_manifest(
        settings.log_dir,
        records=records,
        refactor_stdout=stdout_all,
        refactor_stderr=stderr_all,
        refactor_exit_code=refactor_exit if refactor_exit is not None else -1,
        json_paths=json_paths,
    )
    manifest_path = str(manifest)

    published = publish_final_output(
        settings,
        records,
        run_started_at=run_started_at,
    )
    layout_missing = assert_output_layout(settings.final_output_dir)
    if layout_missing:
        errors.append(
            f"Final output layout incomplete (missing: {', '.join(layout_missing)})"
        )

    final_json = published.get("json", [])
    if records and not final_json:
        errors.append(
            "Nenhum JSON publicado em output/json para os precatórios coletados"
        )

    run_index = write_run_index(
        settings.log_dir,
        records=records,
        published=published,
        refactor_exit_code=refactor_exit if refactor_exit is not None else -1,
        final_output_dir=settings.final_output_dir,
    )

    logger.info(
        "=== TJSP Enrich DONE === kind=%s label=%s records=%s final_json=%s "
        "output=%s refactor_exit=%s",
        search_kind,
        search_label,
        len(records),
        len(final_json),
        settings.final_output_dir,
        refactor_exit,
    )

    return PipelineResult(
        search_url=search_url,
        precatorio_links_found=links_count,
        records=records,
        refactor_exit_code=refactor_exit,
        json_outputs=[str(p) for p in json_paths],
        final_json_outputs=[str(p) for p in final_json],
        final_output_dir=str(settings.final_output_dir),
        manifest_path=manifest_path,
        run_index_path=str(run_index),
        errors=errors,
        search_kind=search_kind,
        search_label=search_label,
    )


def run_pipeline(
    settings: Settings,
    *,
    search_url: str | None = None,
    skip_refactor: bool = False,
    limit: int | None = None,
    processo_codigos: list[str] | None = None,
    refactor_mode: str | None = None,
    only_records: list[PrecatorioRecord] | None = None,
    output_dir: Path | None = None,
    search_kind: str | None = None,
    search_label: str | None = None,
) -> PipelineResult:
    """Full scrape → REFACTOR pipeline (CLI entry).

    If ``only_records`` is provided, skips scrape and enriches that subset only.
    ``output_dir`` redirects FINAL_OUTPUT_DIR for this run (per-query folders).
    """
    run_settings = _with_output_dir(settings, output_dir)

    if only_records is not None:
        return enrich_records(
            run_settings,
            only_records,
            search_url=search_url or run_settings.esaj_search_url,
            links_count=len(only_records),
            refactor_mode=refactor_mode,
            search_kind=search_kind,
            search_label=search_label,
        )

    scrape = scrape_precatorios(
        run_settings,
        search_url=search_url,
        limit=limit,
        processo_codigos=processo_codigos,
        search_kind=search_kind,
        search_label=search_label,
        filter_nome=None,
    )

    if not scrape.records or skip_refactor:
        return PipelineResult(
            search_url=scrape.search_url,
            precatorio_links_found=scrape.precatorio_links_found,
            records=scrape.records,
            refactor_exit_code=None,
            json_outputs=[],
            final_json_outputs=[],
            final_output_dir=str(run_settings.final_output_dir),
            manifest_path=scrape.scrape_snapshot_path,
            run_index_path=None,
            errors=scrape.errors,
            search_kind=scrape.search_kind,
            search_label=scrape.search_label,
        )

    return enrich_records(
        run_settings,
        scrape.records,
        search_url=scrape.search_url,
        links_count=scrape.precatorio_links_found,
        prior_errors=scrape.errors,
        refactor_mode=refactor_mode,
        scrape_snapshot_path=scrape.scrape_snapshot_path,
        search_kind=scrape.search_kind,
        search_label=scrape.search_label,
    )
