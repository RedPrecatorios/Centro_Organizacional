# -*- coding: utf-8 -*-
"""
Planilha principal — modelo PRC CMP (modelo 2).

Remove colunas operativas específicas do layout CMP; o número de linhas é qualquer.

CPF duplicado no Excel (duas colunas «cpf» → pandas: `cpf` e `cpf.1`):
Nas planilhas de enriquecimento (P2/P3), CPFs com zeros à esquerda perdem esses zeros
(quando tratados como número ou na exportação). Por isso o layout traz duas colunas
com o mesmo rótulo: uma para o valor «oficial» e outra para manter o vínculo correcto
com as planilhas de enriquecimento. Não removemos `cpf.1`; a primeira vira `CPF` (exibição);
o cruzamento P2/P3, cooldown e banco usam `cpf.1` com `_normalizar_cpf` (mesmo formato
que o enriquecimento, sem ambiguidade de zeros à esquerda).
"""

from __future__ import annotations

import pandas as pd

# Check e relação: certidão, impugnação, intimação; metadados pedidos para remoção.
COLUNAS_REMOVER_PRC_CMP = [
    "check_certidao",
    "relacao_certidao",
    "check_impugnacao",
    "relacao_impugnacao",
    "check_intimacao",
    "relacao_intimacao",
    "data_expirado",
    "ult_alt_status_calculo",
    "processo_codigo",
    "controle",
]


def processar_planilha_prc_cmp(caminho_entrada: str) -> pd.DataFrame:
    """
    Lê o .xlsx, remove as colunas do modelo CMP, adiciona INDEX.
    """
    df = pd.read_excel(caminho_entrada)

    existentes = [c for c in COLUNAS_REMOVER_PRC_CMP if c in df.columns]
    ausentes = [c for c in COLUNAS_REMOVER_PRC_CMP if c not in df.columns]

    if ausentes:
        print(f"     [PRC CMP] Colunas já ausentes (ignoradas): {ausentes}")
    if existentes:
        df.drop(columns=existentes, inplace=True)
        print(f"     [PRC CMP] Colunas removidas: {existentes}")
    else:
        print("     [PRC CMP] Nenhuma das colunas listadas estava presente; nada removido.")

    df["INDEX"] = range(1, len(df) + 1)

    if "cpf" in df.columns and "CPF" not in df.columns:
        df.rename(columns={"cpf": "CPF"}, inplace=True)
        print(
            "     [PRC CMP] Coluna 'cpf' -> 'CPF' (exibicao; cruzamento P2/P3 usa 'cpf.1')."
        )
    if "cpf.1" in df.columns:
        print(
            "     [PRC CMP] Coluna 'cpf.1' mantida (2a coluna cpf do Excel: vinculo com "
            "enriquecimento quando zeros a esquerda somem na outra coluna)."
        )

    print(f"     [PRC CMP] Linhas: {len(df)} | Colunas: {len(df.columns)}")
    return df
