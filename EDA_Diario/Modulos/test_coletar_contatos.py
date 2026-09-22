"""
Teste: CPF da Assertiva nao deve entrar como telefone.
Executar: python3 EDA_Diario/Modulos/test_coletar_contatos.py
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))

from modulo_merge import _coletar_contatos, _coluna_e_contato_telefone  # noqa: E402


def _assert(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)


def test_assertiva_cpf_nao_vira_telefone() -> None:
    df = pd.DataFrame(
        {
            "CPF": ["14973928887"],
            "NOME": ["FULANO"],
            "TELEFONE1": ["5511999998888"],
            "_CPF_NORM": ["14973928887"],
        }
    )
    tels, emails, hsm = _coletar_contatos(df, "14973928887", "cpf")
    _assert("14973928887" not in tels, f"CPF na lista de telefones: {tels}")
    _assert(
        any("99998888" in re.sub(r"\D", "", t) for t in tels),
        f"telefone real ausente: {tels}",
    )
    _assert(not _coluna_e_contato_telefone("CPF"), "coluna CPF classificada como telefone")
    _assert(hsm == [], f"HSM inesperado sem coluna WhatsApp: {hsm}")


def test_assertiva_familiares_e_whatsapp() -> None:
    df = pd.DataFrame(
        {
            "CPF": ["14973928887"],
            "NOME": ["FULANO"],
            "TELEFONE1": ["1133334444"],
            "CELULAR_WHATSAPP_1": ["5511988887777"],
            "CONJUGE_TELEFONE_1": ["11911112222"],
            "_CPF_NORM": ["14973928887"],
        }
    )
    tels, emails, hsm = _coletar_contatos(df, "14973928887", "cpf")
    _assert("1133334444" in tels, tels)
    _assert("11911112222" in tels, tels)
    _assert(any("988887777" in re.sub(r"\D", "", t) for t in tels), tels)
    _assert(any("988887777" in re.sub(r"\D", "", t) for t in hsm), hsm)
    _assert("11911112222" not in hsm, hsm)


if __name__ == "__main__":
    test_assertiva_cpf_nao_vira_telefone()
    test_assertiva_familiares_e_whatsapp()
    print("OK — CPF nao e coletado como telefone; WhatsApp/familiares entram.")
