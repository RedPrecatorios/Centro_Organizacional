# -*- coding: utf-8 -*-
"""
Modelo 3 — PRC IMP. Pré-processamento da planilha principal (antes do merge).

O enriquecimento em si (Lemitti P2, CSV de CPFs não encontrados, segunda base
P3) é o mesmo que no PRC TJSP: `modulo_merge.etapa1_enriquecer_com_p2` e
`etapa2_enriquecer_com_p3` com o `modelo=prc_imp` na etapa 1.

- Folha: `processos` (se ausente, usa a primeira e avisa).
- Remove **Meses IR**; restantes colunas como vierem.
- `Processo` -> `Numero_de_Processo`.
- Duas colunas «CPF»: cruzamento P2/P3 com a segunda (`CPF.1` / `cpf.1`); ver `modulo_merge`.
- Acrescenta `INDEX` (1..N).
"""
from __future__ import annotations

import pandas as pd

SHEET_IMP = "processos"
COL_MESES_IR = "Meses IR"


def processar_planilha_prc_imp(caminho_entrada: str) -> pd.DataFrame:
    xl = pd.ExcelFile(caminho_entrada)
    if SHEET_IMP in xl.sheet_names:
        df = pd.read_excel(xl, sheet_name=SHEET_IMP)
    else:
        df = pd.read_excel(xl, sheet_name=0)
        print(
            f"     [PRC IMP] Aviso: folha '{SHEET_IMP}' nao encontrada; "
            "usada a primeira folha do arquivo."
        )

    if COL_MESES_IR in df.columns:
        df = df.drop(columns=[COL_MESES_IR])
        print(f"     [PRC IMP] Coluna removida: '{COL_MESES_IR}'.")

    if "Processo" in df.columns and "Numero_de_Processo" not in df.columns:
        df = df.rename(columns={"Processo": "Numero_de_Processo"})

    df = df.copy()
    if "INDEX" in df.columns:
        df = df.drop(columns=["INDEX"])
    df["INDEX"] = range(1, len(df) + 1)

    if "CPF.1" in df.columns:
        print("     [PRC IMP] Coluna 'CPF.1': chave de cruzamento P2/P3.")
    elif "cpf.1" in df.columns:
        print("     [PRC IMP] Coluna 'cpf.1': chave de cruzamento P2/P3.")
    else:
        print("     [PRC IMP] Uma coluna CPF: cruzamento P2/P3 com 'CPF'.")

    print(f"     [PRC IMP] Linhas: {len(df)} | Colunas: {len(df.columns)}")
    return df
