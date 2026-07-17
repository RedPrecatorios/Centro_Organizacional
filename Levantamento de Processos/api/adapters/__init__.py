from api.adapters.pipeline_adapter import (
    PrecatorioRecord,
    PipelineResult,
    ScrapeResult,
    build_search_url,
    enrich_new_records,
    normalize_nome,
    scrape_by_nome,
)

__all__ = [
    "PrecatorioRecord",
    "PipelineResult",
    "ScrapeResult",
    "build_search_url",
    "enrich_new_records",
    "normalize_nome",
    "scrape_by_nome",
]
