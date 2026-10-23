"""Contrato da busca unificada: credor (nome+CPF) e processo+incidente."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from api.schemas import SearchCreateRequest
from tjsp_pipeline.scraper.search_url import SearchKind, build_search_query


def test_credor_nome_e_cpf_juntos():
    body = SearchCreateRequest(nome="Jose Silva", cpf="123.456.789-01")
    query = build_search_query(nome=body.nome, cpf=body.cpf)
    assert query.kind == SearchKind.CREDOR
    assert query.value == "12345678901"
    assert query.filter_nome == "Jose Silva"
    assert "docparte" in query.url.lower() or "DOCPARTE" in query.url


def test_processo_incidente_com_nome():
    body = SearchCreateRequest(
        processo="0017669-72.2021.8.26.0053",
        incidente="12",
        nome="Maria Souza",
    )
    query = build_search_query(
        nome=body.nome, processo=body.processo, incidente=body.incidente
    )
    assert query.kind == SearchKind.PROCESSO
    assert query.filter_incidente == "12"
    assert query.filter_nome == "Maria Souza"


def test_processo_incidente_com_cpf_sem_nome():
    query = build_search_query(
        cpf="12345678901",
        processo="0017669-72.2021.8.26.0053",
        incidente="4",
    )
    assert query.kind == SearchKind.PROCESSO
    assert query.filter_incidente == "4"
    assert query.filter_nome is None
    assert query.filter_cpf == "12345678901"


def test_processo_cpf_sobrevive_rehidratacao_do_job():
    query = build_search_query(
        cpf="09118200869",
        processo="0036473-54.2022.8.26.0053",
        incidente="14",
    )
    again = build_search_query(
        nome=query.filter_nome,
        cpf=query.filter_cpf,
        processo=query.value,
        incidente=query.filter_incidente,
    )
    assert again.kind == SearchKind.PROCESSO
    assert again.filter_cpf == "09118200869"
    assert again.filter_incidente == "14"


def test_rejeita_nome_sozinho():
    with pytest.raises(ValidationError):
        SearchCreateRequest(nome="Fulano de Tal")


def test_rejeita_cpf_sozinho():
    with pytest.raises(ValidationError):
        SearchCreateRequest(cpf="12345678901")


def test_rejeita_processo_sem_incidente():
    with pytest.raises(ValidationError):
        SearchCreateRequest(processo="0017669-72.2021.8.26.0053", nome="Maria")
