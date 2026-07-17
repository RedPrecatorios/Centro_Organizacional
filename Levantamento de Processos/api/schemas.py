"""Pydantic contracts for API v1 — keep stable for the web UI / viewer."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator


JobStatus = Literal["queued", "running", "done", "failed"]
ProcessSource = Literal["database", "pipeline"]
SearchKind = Literal["nome", "cpf", "processo"]


class SearchCreateRequest(BaseModel):
    """
    Modos:
      - nome sozinho
      - cpf sozinho
      - processo + nome (nome = filtro Monday na capa dos incidentes)
    """

    nome: str | None = Field(
        default=None,
        description="Nome da parte (NMPARTE) ou, com processo, filtro na capa",
    )
    cpf: str | None = Field(
        default=None,
        description="CPF da parte — 11 dígitos (e-SAJ DOCPARTE)",
    )
    processo: str | None = Field(
        default=None,
        description="Número CNJ (NUMPROC). Exige também nome para filtrar incidentes.",
    )

    @model_validator(mode="after")
    def _validate_criteria(self):
        n = (self.nome or "").strip()
        c = (self.cpf or "").strip()
        p = (self.processo or "").strip()
        if c:
            if n or p:
                raise ValueError("CPF deve ser informado sozinho (sem nome nem processo).")
            return self
        if p:
            if not n:
                raise ValueError(
                    "Ao pesquisar por processo, informe também o nome (Monday) "
                    "para filtrar os incidentes na capa."
                )
            return self
        if not n:
            raise ValueError(
                "Informe nome, CPF, ou processo + nome (filtro de incidente na capa)."
            )
        return self


class SearchCreateResponse(BaseModel):
    job_id: str
    status: JobStatus = "queued"
    search_kind: SearchKind | None = None
    query: str | None = None
    filter_nome: str | None = None


class AptoProcess(BaseModel):
    numero_de_processo: str
    numero_do_incidente: str
    source: ProcessSource
    record: dict[str, Any] = Field(default_factory=dict)
    json_path: str | None = None
    fanout_json_path: str | None = None
    status: Literal["apto"] = "apto"


class InaptoProcess(BaseModel):
    numero_de_processo: str
    numero_do_incidente: str
    source: ProcessSource
    motivo: str
    record: dict[str, Any] = Field(default_factory=dict)
    json_path: str | None = None
    fanout_json_path: str | None = None
    status: Literal["inapto"] = "inapto"


class SkippedProcess(BaseModel):
    """Compat: mesmo conteúdo resumido de um inapto (processo/incidente/motivo)."""

    numero_de_processo: str
    numero_do_incidente: str
    reason: str


class SearchJobArtifacts(BaseModel):
    final_output_dir: str | None = None
    scrape_snapshot: str | None = None
    manifest: str | None = None
    run_index: str | None = None


class SearchJobResponse(BaseModel):
    job_id: str
    status: JobStatus
    nome: str  # label da pesquisa (nome / cpf / processo) — compat UI antiga
    search_kind: SearchKind | None = None
    query: str | None = None
    filter_nome: str | None = None
    progress: str | None = None
    processos_aptos: list[AptoProcess] = Field(default_factory=list)
    processos_inaptos: list[InaptoProcess] = Field(default_factory=list)
    skipped: list[SkippedProcess] = Field(default_factory=list)
    artifacts: SearchJobArtifacts = Field(default_factory=SearchJobArtifacts)
    errors: list[str] = Field(default_factory=list)
    created_at: str | None = None
    updated_at: str | None = None


class HealthResponse(BaseModel):
    status: str = "ok"
    service: str = "dashboard-backend-api"
