"""Build e-SAJ cpopg search URLs from nome, CPF or número de processo."""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum
from urllib.parse import quote_plus


ESAJ_CPOPG_SEARCH = "https://esaj.tjsp.jus.br/cpopg/search.do"


class SearchKind(str, Enum):
    NOME = "nome"
    CPF = "cpf"
    PROCESSO = "processo"


@dataclass(frozen=True)
class SearchQuery:
    kind: SearchKind
    value: str
    label: str
    url: str
    filter_nome: str | None = None

    @property
    def slug(self) -> str:
        """Filesystem-safe folder name for this query."""
        raw = self.label.strip()
        if self.kind == SearchKind.CPF:
            raw = normalize_cpf(raw)
        elif self.kind == SearchKind.PROCESSO:
            base = normalize_processo(self.value)
            if self.filter_nome:
                raw = f"{base}_{normalize_party_name(self.filter_nome)}"
            else:
                raw = base
        else:
            raw = normalize_party_name(raw)
        slug = re.sub(r"[^\w.\-]+", "_", raw, flags=re.UNICODE).strip("._")
        return slug[:120] or "query"

    @property
    def relative_output_dir(self) -> str:
        """output/<kind>/<slug>/"""
        return f"{self.kind.value}/{self.slug}"


def normalize_party_name(nome: str) -> str:
    """Collapse whitespace; preserve accents (e-SAJ phonetic search handles them)."""
    return " ".join(str(nome or "").split()).strip()


def normalize_cpf(cpf: str) -> str:
    """Keep digits only; must be 11 digits for CPF."""
    digits = re.sub(r"\D+", "", str(cpf or ""))
    if not digits:
        raise ValueError("CPF vazio.")
    if len(digits) != 11:
        raise ValueError(f"CPF deve ter 11 dígitos (recebido {len(digits)}): {cpf!r}")
    return digits


def normalize_processo(numero: str) -> str:
    """
    Normalize CNJ / unificado TJSP number.

    Accepts:
      0017669-72.2021.8.26.0053
      0017669-72.2021.8.26.0053/24
      00176697220218260053
    Returns CNJ with punctuation when possible; strips incident suffix for search.
    """
    raw = str(numero or "").strip()
    if not raw:
        raise ValueError("Número de processo vazio.")

    # Drop incidental suffix for NUMPROC search (parent CNJ).
    base = raw.split("/")[0].strip()
    digits = re.sub(r"\D+", "", base)

    # Already well-formed CNJ → keep as-is (without incident)
    if re.fullmatch(
        r"\d{7}-\d{2}\.\d{4}\.\d\.\d{2}\.\d{4}",
        base,
    ):
        return base

    if len(digits) == 20:
        # NNNNNNN-DD.AAAA.J.TR.OOOO
        return (
            f"{digits[0:7]}-{digits[7:9]}.{digits[9:13]}."
            f"{digits[13]}.{digits[14:16]}.{digits[16:20]}"
        )

    # Fall back: keep cleaned base (may still work as valorConsultaNuUnificado)
    if base:
        return base
    raise ValueError(f"Número de processo inválido: {numero!r}")


def build_nmparte_search_url(
    nome: str,
    *,
    nome_completo: bool = True,
    cd_foro: int = -1,
) -> str:
    """
    Build NMPARTE (nome da parte) search URL for 1º grau e-SAJ.

    Example:
      Heloisa Maria Fernandes Queiroz →
      .../search.do?...&cbPesquisa=NMPARTE&dadosConsulta.valorConsulta=Heloisa+Maria+...
    """
    cleaned = normalize_party_name(nome)
    if not cleaned:
        raise ValueError("Nome da parte vazio.")

    valor = quote_plus(cleaned.upper())
    ch = "true" if nome_completo else "false"
    return (
        f"{ESAJ_CPOPG_SEARCH}"
        f"?conversationId="
        f"&cbPesquisa=NMPARTE"
        f"&dadosConsulta.valorConsulta={valor}"
        f"&chNmCompleto={ch}"
        f"&cdForo={cd_foro}"
    )


def build_docparte_search_url(cpf: str, *, cd_foro: int = -1) -> str:
    """
    Build DOCPARTE (documento da parte / CPF) search URL.

    e-SAJ expects digits only (no dots/dashes). Same field also accepts RG.
    """
    digits = normalize_cpf(cpf)
    return (
        f"{ESAJ_CPOPG_SEARCH}"
        f"?conversationId="
        f"&cbPesquisa=DOCPARTE"
        f"&dadosConsulta.valorConsulta={digits}"
        f"&cdForo={cd_foro}"
    )


def build_numproc_search_url(numero: str, *, cd_foro: int = -1) -> str:
    """
    Build NUMPROC (número unificado) search URL.

    Matches REFACTOR / COLETA URL pattern:
      cbPesquisa=NUMPROC
      &dadosConsulta.valorConsultaNuUnificado=<CNJ>
      &dadosConsulta.valorConsultaNuUnificado=UNIFICADO
      &dadosConsulta.tipoNuProcesso=UNIFICADO
    """
    cnj = normalize_processo(numero)
    encoded = quote_plus(cnj)
    return (
        f"{ESAJ_CPOPG_SEARCH}"
        f"?conversationId="
        f"&cbPesquisa=NUMPROC"
        f"&numeroDigitoAnoUnificado="
        f"&foroNumeroUnificado="
        f"&dadosConsulta.valorConsultaNuUnificado={encoded}"
        f"&dadosConsulta.valorConsultaNuUnificado=UNIFICADO"
        f"&dadosConsulta.valorConsulta="
        f"&dadosConsulta.tipoNuProcesso=UNIFICADO"
        f"&cdForo={cd_foro}"
    )


def build_search_query(
    *,
    nome: str | None = None,
    cpf: str | None = None,
    processo: str | None = None,
) -> SearchQuery:
    """
    Build SearchQuery from:
      - nome sozinho (NMPARTE)
      - cpf sozinho (DOCPARTE)
      - processo + nome (NUMPROC; nome = filtro na capa dos incidentes)
    """
    n = normalize_party_name(nome) if nome and str(nome).strip() else ""
    c_raw = str(cpf).strip() if cpf and str(cpf).strip() else ""
    p_raw = str(processo).strip() if processo and str(processo).strip() else ""

    if c_raw:
        if n or p_raw:
            raise ValueError("CPF deve ser informado sozinho (sem nome nem processo).")
        digits = normalize_cpf(c_raw)
        return SearchQuery(
            kind=SearchKind.CPF,
            value=digits,
            label=digits,
            url=build_docparte_search_url(digits),
        )

    if p_raw:
        if not n:
            raise ValueError(
                "Ao pesquisar por processo, informe também o nome (Monday) "
                "para filtrar os incidentes na capa."
            )
        cnj = normalize_processo(p_raw)
        return SearchQuery(
            kind=SearchKind.PROCESSO,
            value=cnj,
            label=f"{cnj} · {n}",
            url=build_numproc_search_url(cnj),
            filter_nome=n,
        )

    if not n:
        raise ValueError(
            "Informe nome, CPF, ou processo + nome (filtro de incidente na capa)."
        )
    return SearchQuery(
        kind=SearchKind.NOME,
        value=n,
        label=n,
        url=build_nmparte_search_url(n),
    )
