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


def _job_to_response(job) -> SearchJobResponse:
    result = job.result_json or {}
    aptos_raw = result.get("processos_aptos") or []
    inaptos_raw = result.get("processos_inaptos") or []
    skipped_raw = result.get("skipped") or []
    artifacts_raw = result.get("artifacts") or {}
    errors = list(result.get("errors") or [])
    if job.error:
        errors = errors + [job.error]

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
    filter_incidente = result.get("filter_incidente") or job.filter_incidente
    allowed_kinds = {"nome", "cpf", "processo", "credor"}

    def _safe(model, items):
        out = []
        for item in items:
            try:
                out.append(model.model_validate(item))
            except Exception:
                continue
        return out

    try:
        artifacts = SearchJobArtifacts.model_validate(artifacts_raw)
    except Exception:
        artifacts = SearchJobArtifacts()

    return SearchJobResponse(
        job_id=job.job_id,
        status=job.status,  # type: ignore[arg-type]
        nome=job.nome,
        search_kind=kind if kind in allowed_kinds else "nome",
        query=query_label,
        filter_nome=filter_nome,
        filter_incidente=filter_incidente,
        progress=job.progress,
        processos_aptos=_safe(AptoProcess, aptos_raw),
        processos_inaptos=_safe(InaptoProcess, inaptos_raw),
        skipped=_safe(SkippedProcess, skipped_raw),
        artifacts=artifacts,
        errors=errors,
        analysis_complete=bool(result.get("analysis_complete")),
        created_at=job.created_at,
        updated_at=job.updated_at,
    )


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
            incidente=body.incidente,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    job = _store(request).create(
        label=query.label,
        search_kind=query.kind.value,
        query_value=query.value,
        filter_nome=query.filter_nome,
        filter_incidente=query.filter_incidente,
        filter_cpf=query.filter_cpf,
    )
    return SearchCreateResponse(
        job_id=job.job_id,
        status="queued",
        search_kind=query.kind.value,  # type: ignore[arg-type]
        query=query.label,
        filter_nome=query.filter_nome,
        filter_incidente=query.filter_incidente,
    )


@router.get(
    "/searches",
    response_model=list[SearchJobResponse],
    dependencies=[Depends(require_api_token)],
)
def list_searches(request: Request, limit: int = 50) -> list[SearchJobResponse]:
    jobs = _store(request).list_recent(limit=limit)
    out: list[SearchJobResponse] = []
    for job in jobs:
        try:
            out.append(_job_to_response(job))
        except Exception:
            continue
    return out


@router.get(
    "/searches/{job_id}",
    response_model=SearchJobResponse,
    dependencies=[Depends(require_api_token)],
)
def get_search(job_id: str, request: Request) -> SearchJobResponse:
    job = _store(request).get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job nao encontrado.")
    return _job_to_response(job)
