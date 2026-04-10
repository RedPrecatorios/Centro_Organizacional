import pandas as pd

# ─────────────────────────────────────────────
# Colunas a serem removidas da planilha principal
# Edite essa lista para adicionar ou remover colunas conforme necessário
# ─────────────────────────────────────────────
COLUNAS_PARA_REMOVER = [
    "Quantidade_de_Oficios",
    "Vara",
    "SPPRE",
    "IAMSPE",
    "IPES",
    "ASSIT_MED_HOSPITAL",
    "INST_PREV_CAIXA_BENEF",
    "ASSIST_MED_CAIXA_BENEF",
    "Termo_Inicial",
    "Termo_Final",
    "Termo_Total",
    "CPF_CNPJ",
    "Script",
    "DEPRE",
    "Numero_de_Meses",
    "Numero_de_Meses_TERMO",
    "update_in",
]


def processar_planilha_principal(caminho_entrada: str) -> pd.DataFrame:
    """
    Le a planilha principal, remove as colunas configuradas e adiciona a coluna INDEX.
    Retorna o DataFrame processado sem salvar em disco.

    Args:
        caminho_entrada: Caminho para o arquivo .xlsx de entrada.

    Returns:
        DataFrame ja processado.
    """
    df = pd.read_excel(caminho_entrada)

    colunas_existentes = [col for col in COLUNAS_PARA_REMOVER if col in df.columns]
    colunas_ausentes   = [col for col in COLUNAS_PARA_REMOVER if col not in df.columns]

    if colunas_ausentes:
        print(f"     [AVISO] Colunas nao encontradas (ignoradas): {colunas_ausentes}")

    df.drop(columns=colunas_existentes, inplace=True)
    df["INDEX"] = range(1, len(df) + 1)

    print(f"     Linhas: {len(df)} | Colunas: {len(df.columns)}")
    return df
