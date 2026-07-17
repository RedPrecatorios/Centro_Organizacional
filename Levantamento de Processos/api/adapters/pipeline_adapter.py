"""Thin adapter — sole coupling point to tjsp_pipeline (survives REFACTOR churn)."""

from __future__ import annotations

from tjsp_pipeline.config import Settings
from tjsp_pipeline.pipeline import PipelineResult, ScrapeResult, enrich_records, scrape_precatorios
from tjsp_pipeline.scraper.search_url import (
    SearchQuery,
    build_nmparte_search_url,
    build_search_query,
    normalize_party_name,
)
from tjsp_pipeline.scraper.show_page import PrecatorioRecord


def normalize_nome(nome: str) -> str:
    return normalize_party_name(nome)


def build_search_url(nome: str) -> str:
    return build_nmparte_search_url(normalize_nome(nome))


def resolve_search_query(
    *,
    nome: str | None = None,
    cpf: str | None = None,
    processo: str | None = None,
) -> SearchQuery:
    return build_search_query(nome=nome, cpf=cpf, processo=processo)


def scrape_by_query(settings: Settings, query: SearchQuery) -> ScrapeResult:
    return scrape_precatorios(
        settings,
        search_url=query.url,
        search_kind=query.kind.value,
        search_label=query.label,
        filter_nome=query.filter_nome,
    )


def scrape_by_nome(settings: Settings, nome: str) -> ScrapeResult:
    query = resolve_search_query(nome=nome)
    return scrape_by_query(settings, query)


def enrich_for_search(
    settings: Settings,
    records: list[PrecatorioRecord],
    *,
    search_url: str,
    links_count: int,
    prior_errors: list[str] | None = None,
) -> PipelineResult:
    """
    Toda pesquisa da API: REFACTOR em --reprocessamento + persistência + API_CALCULO
    (quando configurada), para atualizar valores mesmo de processos já no banco.
    """
    api_settings = settings.with_api_runtime(
        persist=True,
        test_mode=False,
        force_reprocessamento=True,
        force_calculo=True,
    )
    return enrich_records(
        api_settings,
        records,
        search_url=search_url,
        links_count=links_count,
        prior_errors=prior_errors,
        refactor_mode="reprocessamento",
    )


# Compat com imports antigos
def enrich_new_records(
    settings: Settings,
    records: list[PrecatorioRecord],
    *,
    search_url: str,
    links_count: int,
    prior_errors: list[str] | None = None,
) -> PipelineResult:
    return enrich_for_search(
        settings,
        records,
        search_url=search_url,
        links_count=links_count,
        prior_errors=prior_errors,
    )


__all__ = [
    "PrecatorioRecord",
    "PipelineResult",
    "ScrapeResult",
    "SearchQuery",
    "normalize_nome",
    "build_search_url",
    "resolve_search_query",
    "scrape_by_query",
    "scrape_by_nome",
    "enrich_for_search",
    "enrich_new_records",
]
