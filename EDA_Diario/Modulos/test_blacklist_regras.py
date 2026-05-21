"""
Testes rápidos das regras de blacklist (sem MySQL).
Executar: python3 EDA_Diario/Modulos/test_blacklist_regras.py
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from modulo_blacklist import (  # noqa: E402
    campanha_destinatario_bloqueado,
    separar_relatorio_blacklist,
    normalizar_chave_processo_incidente,
    normalizar_valor_para_blacklist,
    _processo_incidente_bloqueado,
    _telefone_bloqueado,
)
import pandas as pd  # noqa: E402


def _assert(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)


def test_normalizacao_processo_incidente() -> None:
    k1 = normalizar_chave_processo_incidente(" 0001234-56.2023.8.26.0100 ", " 99 ")
    k2 = normalizar_chave_processo_incidente("0001234-56.2023.8.26.0100", "99")
    _assert(k1 == k2, f"chaves diferentes: {k1!r} vs {k2!r}")
    v = normalizar_valor_para_blacklist("PROCESSO_INCIDENTE", "0001234-56.2023.8.26.0100|99")
    _assert(v == k1, "valor composto não normaliza igual")


def test_bloqueio_processo_incidente() -> None:
    chave = normalizar_chave_processo_incidente("PROC-1", "INC-2")
    bl = {"PROCESSO_INCIDENTE": {chave}, "CPF": set(), "NOME": set(), "TELEFONE": set(), "EMAIL": set()}
    _assert(_processo_incidente_bloqueado("PROC-1", "INC-2", bl), "deveria bloquear")
    _assert(not _processo_incidente_bloqueado("PROC-1", "OUTRO", bl), "incidente errado não bloqueia")


def test_campanha_destinatario() -> None:
    chave = normalizar_chave_processo_incidente("P1", "I1")
    bl = {
        "EMAIL": {"A@B.COM"},
        "CPF": {"12345678901"},
        "NOME": {"JOAO SILVA"},
        "PROCESSO_INCIDENTE": {chave},
        "TELEFONE": set(),
    }
    b, m = campanha_destinatario_bloqueado("a@b.com", "X", "", "", "", bl)
    _assert(b and m == "EMAIL", "email")
    b, m = campanha_destinatario_bloqueado("z@z.com", "Joao Silva", "", "", "", bl)
    _assert(b and m == "PESSOA_NOME", "nome")
    b, m = campanha_destinatario_bloqueado("z@z.com", "X", "12345678901", "", "", bl)
    _assert(b and m == "PESSOA_CPF", "cpf")
    b, m = campanha_destinatario_bloqueado("z@z.com", "X", "", "P1", "I1", bl)
    _assert(b and m == "PROCESSO_INCIDENTE", "processo")


def test_telefone_prefixo_55() -> None:
    bl_tel = {normalizar_valor_para_blacklist("TELEFONE", "11999990000")}
    _assert(_telefone_bloqueado("5511999990000", bl_tel), "55 prefix")
    _assert(_telefone_bloqueado("11999990000", bl_tel), "sem 55")


def test_separar_relatorio() -> None:
    bl = {
        "PROCESSO_INCIDENTE": {normalizar_chave_processo_incidente("P9", "")},
        "CPF": set(),
        "NOME": set(),
        "TELEFONE": set(),
        "EMAIL": set(),
    }
    df = pd.DataFrame([
        {"processo": "P9", "numero_incidente": "", "telefone": "11988887777", "nome": "A"},
        {"processo": "P8", "numero_incidente": "", "telefone": "11977776666", "nome": "B"},
    ])
    ativos, bl_df = separar_relatorio_blacklist(df, bl, "Outros Resultados")
    _assert(len(ativos) == 2, "todas as linhas ficam na aba de resultado")
    _assert(ativos.iloc[0]["processo"] == "P9" and ativos.iloc[1]["processo"] == "P8", "ativos")
    _assert(len(bl_df) == 1 and bl_df.iloc[0]["processo"] == "P9", "blacklist sheet")
    _assert(bl_df.iloc[0]["motivo_blacklist"] == "PROCESSO_INCIDENTE", "motivo col")


def main() -> None:
    test_normalizacao_processo_incidente()
    test_bloqueio_processo_incidente()
    test_campanha_destinatario()
    test_telefone_prefixo_55()
    test_separar_relatorio()
    print("OK — todas as regras de blacklist passaram.")


if __name__ == "__main__":
    main()
