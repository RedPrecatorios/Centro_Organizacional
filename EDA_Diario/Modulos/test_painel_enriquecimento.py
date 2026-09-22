"""Testes do pipeline do Painel (Etapa 1/2) — sem MySQL.

Executar:
  python3 EDA_Diario/Modulos/test_painel_enriquecimento.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))

from modulo_enriquecimento_contatos import (  # noqa: E402
    coletar_hsm_por_whatsapp,
    processar_enriquecimento_contatos,
)
from modulo_enriquecimento_relacionados import (  # noqa: E402
    _filtrar_colunas as filtrar_p3,
    processar_enriquecimento_relacionados,
)
from modulo_merge import (  # noqa: E402
    COL_COOLDOWN,
    PREFIXO_HSM_LEMITTI,
    _colunas_telefone_regular,
    _coluna_e_contato_telefone,
    _coluna_marca_whatsapp,
    _coluna_nome_disparo,
    _coluna_processo_disparo,
    _deduplicar,
    _normalizar_cpf,
    diagnostico_intermediaria_etapa2,
    gravar_modelo_planilha,
    inferir_modelo_planilha,
)


def _assert(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)


def test_cpf_notacao_cientifica() -> None:
    _assert(_normalizar_cpf(1.2345678901e10) == "12345678901", "float cientifico")
    _assert(_normalizar_cpf("1.2345678901e10") == "12345678901", "str cientifico")
    _assert(_normalizar_cpf("123.456.789-01") == "12345678901", "mascarado")
    _assert(_normalizar_cpf("123456789") == "00123456789", "zfill")


def test_dedup_telefone_55() -> None:
    itens = _deduplicar(["11988887777", "5511988887777", "11988887777"], True)
    _assert(len(itens) == 1, f"esperava 1, veio {itens}")


def test_p3_mantem_whatsapp_e_familiares() -> None:
    cols = [
        "CPF",
        "NOME",
        "TELEFONE1",
        "TELEFONE_WHATSAPP_1",
        "CELULAR1",
        "CELULAR_WHATSAPP_1",
        "EMAIL1",
        "CONJUGE_TELEFONE_1",
        "CONJUGE_CELULAR_1",
        "FILHO_1_TELEFONE_1",
        "FILHO_1_CELULAR_1",
        "IRMAO_1_TELEFONE_1",
        "IRMAO_1_CELULAR_1",
        "IGNORAR",
    ]
    kept = filtrar_p3(cols)
    for obrig in (
        "CELULAR_WHATSAPP_1",
        "CONJUGE_TELEFONE_1",
        "FILHO_1_CELULAR_1",
        "IRMAO_1_TELEFONE_1",
    ):
        _assert(obrig in kept, f"P3 descartou {obrig}: {kept}")
    _assert("IGNORAR" not in kept, str(kept))
    _assert(_coluna_e_contato_telefone("CONJUGE_TELEFONE_1"), "CONJUGE_TELEFONE_1")
    _assert(_coluna_marca_whatsapp("CELULAR_WHATSAPP_1"), "CELULAR_WHATSAPP")
    _assert(not _coluna_marca_whatsapp("CONJUGE_TELEFONE_1"), "conjugue HSM")


def test_hsm_por_flag_whatsapp(tmp_path: Path) -> None:
    csv = tmp_path / "lemitti.csv"
    csv.write_text(
        "NOME,CPF/CNPJ,DDD,FONE,POSSUI-WHATSAPP,DDD.1,FONE.1,POSSUI-WHATSAPP.1\n"
        "ANA,12345678901,11,988887777,0,11,977776666,1\n",
        encoding="utf-8",
    )
    df = pd.read_csv(csv, dtype=str)
    hsm = coletar_hsm_por_whatsapp(df)
    _assert(hsm.iloc[0] == "11977776666", f"HSM inesperado: {hsm.iloc[0]!r}")

    df_p2, modo = processar_enriquecimento_contatos(str(csv))
    _assert(modo == "cpf", f"modo={modo}")
    _assert("11977776666" in str(df_p2["_HSM_JOIN"].iloc[0]), str(df_p2["_HSM_JOIN"].iloc[0]))


def test_hsm_nao_entra_como_telefone_regular() -> None:
    df = pd.DataFrame(
        columns=["TELEFONE_1", "TELEFONE_2", "TELEFONE_HSM_1", "EMAIL_1"]
    )
    regs = _colunas_telefone_regular(df)
    _assert(regs == ["TELEFONE_1", "TELEFONE_2"], regs)
    _assert(not _coluna_e_contato_telefone("TELEFONE_HSM_1"), "HSM classificado como telefone")
    _assert(
        str(df.columns[2]).startswith(PREFIXO_HSM_LEMITTI + "_"),
        str(df.columns[2]),
    )


def test_p3_arquivo_real_nao_descarta_colunas(tmp_path: Path) -> None:
    csv = tmp_path / "assertiva.csv"
    csv.write_text(
        "CPF,NOME,TELEFONE1,CELULAR_WHATSAPP_1,CONJUGE_TELEFONE_1,EMAIL1\n"
        "12345678901,ANA,1133334444,11988887777,11911112222,a@x.com\n",
        encoding="utf-8",
    )
    df = processar_enriquecimento_relacionados(str(csv))
    _assert("CELULAR_WHATSAPP_1" in df.columns, str(list(df.columns)))
    _assert("CONJUGE_TELEFONE_1" in df.columns, str(list(df.columns)))
    _assert(str(df["CELULAR_WHATSAPP_1"].iloc[0]) == "11988887777", "whatsapp vazio")


def test_inferir_modelo_cmp_vs_tjsp() -> None:
    df_cmp = pd.DataFrame(
        columns=["processo_principal", "numero_do_cumprimento", "requerente", "CPF"]
    )
    df_tjsp = pd.DataFrame(
        columns=["Natureza", "Numero_de_Processo", "Numero_do_Incidente", "Requerente", "CPF"]
    )
    _assert(inferir_modelo_planilha(df_cmp) == "prc_cmp", "CMP")
    _assert(inferir_modelo_planilha(df_tjsp) == "prc_tjsp", "TJSP")
    _assert(_coluna_processo_disparo(df_cmp) == "numero_do_cumprimento", "processo CMP")
    _assert(_coluna_nome_disparo(df_cmp) == "requerente", "nome CMP")


def test_etapa2_recusa_intermediaria_stale_e_outro_modelo(tmp_path: Path) -> None:
    inter = tmp_path / "INTERMEDIARIA.xlsx"
    prin = tmp_path / "principal.xlsx"
    pd.DataFrame({"Numero_de_Processo": [1], "Natureza": ["x"], "Numero_do_Incidente": [1]}).to_excel(
        inter, index=False
    )
    gravar_modelo_planilha(str(inter), "prc_tjsp")
    diag = diagnostico_intermediaria_etapa2(str(inter), "prc_cmp")
    _assert(not diag["ok"], diag)
    _assert("PRC TJSP" in (diag["motivo"] or ""), diag)

    pd.DataFrame({"numero_do_cumprimento": [1]}).to_excel(prin, index=False)
    gravar_modelo_planilha(str(inter), "prc_cmp")
    t_inter = inter.stat().st_mtime
    os.utime(prin, (t_inter + 10, t_inter + 10))
    diag2 = diagnostico_intermediaria_etapa2(str(inter), "prc_cmp", str(prin))
    _assert(not diag2["ok"], diag2)
    _assert(diag2["stale"], diag2)

    os.utime(prin, (t_inter - 10, t_inter - 10))
    diag3 = diagnostico_intermediaria_etapa2(str(inter), "prc_cmp", str(prin))
    _assert(diag3["ok"], diag3)


def test_cooldown_constante_existe() -> None:
    _assert(COL_COOLDOWN == "COOLDOWN", COL_COOLDOWN)


def main() -> None:
    import tempfile

    test_cpf_notacao_cientifica()
    test_dedup_telefone_55()
    test_p3_mantem_whatsapp_e_familiares()
    test_hsm_nao_entra_como_telefone_regular()
    test_cooldown_constante_existe()
    with tempfile.TemporaryDirectory() as d:
        p = Path(d)
        test_hsm_por_flag_whatsapp(p)
        test_p3_arquivo_real_nao_descarta_colunas(p)
        test_etapa2_recusa_intermediaria_stale_e_outro_modelo(p)
    test_inferir_modelo_cmp_vs_tjsp()
    print("OK — testes do Painel (enriquecimento).")


if __name__ == "__main__":
    main()
