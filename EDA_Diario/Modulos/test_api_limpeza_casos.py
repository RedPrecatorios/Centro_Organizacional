"""
Testes da API de limpeza (Etapa 0) — sem rede.
Executar: python3 EDA_Diario/Modulos/test_api_limpeza_casos.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))

from modulo_api_limpeza_casos import (  # noqa: E402
    chave_caso_limpeza,
    extrair_lista_casos,
    filtrar_planilha_remover_casos,
    limpeza_api_aplica_ao_modelo,
    limpeza_api_habilitada,
    normalizar_caso_api,
)


def _assert(cond: bool, msg) -> None:
    if not cond:
        raise AssertionError(msg)


def test_normalizar_e_extrair() -> None:
    c = normalizar_caso_api(
        {
            "Requerente": "José Silva",
            "CPF": "123.456.789-09",
            "Numero_de_Processo": "PROC-1",
            "Numero_do_Incidente": "2",
        }
    )
    _assert(c is not None, c)
    _assert(c[0] == "JOSE SILVA", c)
    _assert(c[1] == "12345678909", c)
    _assert(c[2] == "PROC-1", c)
    _assert(c[3] == "2", c)

    lista = extrair_lista_casos(
        {
            "casos": [
                {
                    "requerente": "A",
                    "cpf": "11111111111",
                    "numero_processo": "P1",
                    "incidente": "1",
                },
                ["B", "22222222222", "P2", "9"],
            ]
        }
    )
    _assert(len(lista) == 2, lista)


def test_filtrar_planilha() -> None:
    df = pd.DataFrame(
        {
            "Requerente": ["Maria", "João", "Ana"],
            "CPF": ["11111111111", "22222222222", "33333333333"],
            "Numero_de_Processo": ["P1", "P2", "P3"],
            "Numero_do_Incidente": ["1", "2", "3"],
        }
    )
    remover = {
        chave_caso_limpeza(
            requerente="João",
            cpf="22222222222",
            processo="P2",
            incidente="2",
        )
    }
    out, n = filtrar_planilha_remover_casos(
        df,
        remover,
        col_requerente="Requerente",
        col_cpf="CPF",
        col_processo="Numero_de_Processo",
        col_incidente="Numero_do_Incidente",
    )
    _assert(n == 1, n)
    _assert(list(out["Requerente"]) == ["Maria", "Ana"], list(out["Requerente"]))


def test_desabilitada_sem_env(monkeypatch_env=None) -> None:
    import os

    old = {k: os.environ.get(k) for k in (
        "EDA_LIMPEZA_API_URL",
        "EDA_LIMPEZA_API_TOKEN",
        "EDA_LIMPEZA_API_ENABLED",
    )}
    try:
        for k in old:
            os.environ.pop(k, None)
        _assert(limpeza_api_habilitada() is False, "sem config deve estar desligada")
    finally:
        for k, v in old.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def test_modelos_elegiveis() -> None:
    _assert(limpeza_api_aplica_ao_modelo("prc_tjsp"), "tjsp")
    _assert(limpeza_api_aplica_ao_modelo("PRC_CMP"), "cmp")
    _assert(not limpeza_api_aplica_ao_modelo("prc_imp"), "imp fora")
    _assert(not limpeza_api_aplica_ao_modelo(""), "vazio")


if __name__ == "__main__":
    test_normalizar_e_extrair()
    test_filtrar_planilha()
    test_desabilitada_sem_env()
    test_modelos_elegiveis()
    print("OK test_api_limpeza_casos")
