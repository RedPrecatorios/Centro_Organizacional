"""HTTP routes for search jobs (API v1)."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request

from api.adapters.pipeline_adapter import resolve_search_query
from api.auth import require_api_token
from api.jobs.store import JobStore
from api.schemas import (
    AptoProcess,
    HealthResponse,
    InaptoProcess,
    SearchCreateRequest,
    SearchCreateResponse,
    SearchJobArtifacts,
    SearchJobResponse,
    SkippedProcess,
)

router = APIRouter()


def _store(request: Request) -> JobStore:
    return request.app.state.job_store


@router.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    """Public healthcheck (no token)."""
    return HealthResponse()


@router.post(
    "/searches",
    response_model=SearchCreateResponse,
    status_code=202,
    dependencies=[Depends(require_api_token)],
)
def create_search(body: SearchCreateRequest, request: Request) -> SearchCreateResponse:
    try:
        query = resolve_search_query(
            nome=body.nome,
            cpf=body.cpf,
            processo=body.processo,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    job = _store(request).create(
        label=query.label,
        search_kind=query.kind.value,
        query_value=query.value,
        filter_nome=query.filter_nome,
    )
    return SearchCreateResponse(
        job_id=job.job_id,
        status="queued",
        search_kind=query.kind.value,  # type: ignore[arg-type]
        query=query.label,
        filter_nome=query.filter_nome,
    )


@router.get(
    "/searches/{job_id}",
    response_model=SearchJobResponse,
    dependencies=[Depends(require_api_token)],
)
def get_search(job_id: str, request: Request) -> SearchJobResponse:
    job = _store(request).get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job nao encontrado.")

    result = job.result_json or {}
    aptos_raw = result.get("processos_aptos") or []
    inaptos_raw = result.get("processos_inaptos") or []
    skipped_raw = result.get("skipped") or []
    artifacts_raw = result.get("artifacts") or {}
    errors = list(result.get("errors") or [])
    if job.error:
        errors = errors + [job.error]

    # Compat: jobs antigos só tinham `skipped`
    if not inaptos_raw and skipped_raw:
        inaptos_raw = [
            {
                "numero_de_processo": s.get("numero_de_processo"),
                "numero_do_incidente": s.get("numero_do_incidente"),
                "source": "pipeline",
                "motivo": s.get("reason") or "INAPTO",
                "record": {},
                "status": "inapto",
            }
            for s in skipped_raw
            if isinstance(s, dict)
        ]

    kind = result.get("search_kind") or job.search_kind or "nome"
    query_label = result.get("query") or result.get("nome") or job.nome
    filter_nome = result.get("filter_nome") or job.filter_nome

    return SearchJobResponse(
        job_id=job.job_id,
        status=job.status,  # type: ignore[arg-type]
        nome=job.nome,
        search_kind=kind if kind in {"nome", "cpf", "processo"} else "nome",
        query=query_label,
        filter_nome=filter_nome,
        progress=job.progress,
        processos_aptos=[AptoProcess.model_validate(x) for x in aptos_raw],
        processos_inaptos=[InaptoProcess.model_validate(x) for x in inaptos_raw],
        skipped=[SkippedProcess.model_validate(x) for x in skipped_raw],
        artifacts=SearchJobArtifacts.model_validate(artifacts_raw),
        errors=errors,
        created_at=job.created_at,
        updated_at=job.updated_at,
    )
