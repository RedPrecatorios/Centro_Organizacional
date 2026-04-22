import pandas as pd
import re

# ─────────────────────────────────────────────
# Colunas fixas a manter
# ─────────────────────────────────────────────
COLUNA_NOME = "NOME"
COLUNA_CPF  = "CPF"

# Padroes elasticos: detecta qualquer coluna que comece com TELEFONE, CELULAR ou EMAIL
# Exemplos cobertos: TELEFONE1, TELEFONE2MAE, CELULAR1IRMAO1, EMAIL1, EMAIL2, etc.
PADRAO_TELEFONE = re.compile(r"^TELEFONE\d+.*$", re.IGNORECASE)
PADRAO_CELULAR  = re.compile(r"^CELULAR\d+.*$",  re.IGNORECASE)
PADRAO_EMAIL    = re.compile(r"^EMAIL\d+.*$",     re.IGNORECASE)


def _filtrar_colunas(colunas: list[str]) -> list[str]:
    """Retorna apenas as colunas de interesse, mantendo a ordem original."""
    selecionadas = []
    for col in colunas:
        if col in (COLUNA_NOME, COLUNA_CPF):
            selecionadas.append(col)
        elif PADRAO_TELEFONE.match(col):
            selecionadas.append(col)
        elif PADRAO_CELULAR.match(col):
            selecionadas.append(col)
        elif PADRAO_EMAIL.match(col):
            selecionadas.append(col)
    return selecionadas


def processar_enriquecimento_relacionados(caminho_entrada: str) -> pd.DataFrame:
    """
    Le o CSV de relacionados, mantem apenas NOME, CPF, telefones/celulares e emails.
    Remove colunas completamente vazias. Retorna o DataFrame sem salvar em disco.

    Args:
        caminho_entrada: Caminho para o arquivo .csv de entrada.

    Returns:
        DataFrame ja processado.
    """
    df = pd.read_csv(caminho_entrada, sep=None, engine="python", dtype=str)

    colunas_selecionadas = _filtrar_colunas(df.columns.tolist())

    colunas_ausentes = [c for c in (COLUNA_NOME, COLUNA_CPF) if c not in colunas_selecionadas]
    if colunas_ausentes:
        print(f"     [AVISO] Colunas obrigatorias nao encontradas: {colunas_ausentes}")

    df = df[colunas_selecionadas]

    # Remove colunas completamente vazias
    colunas_variaveis = [c for c in colunas_selecionadas if c not in (COLUNA_NOME, COLUNA_CPF)]
    colunas_vazias    = [c for c in colunas_variaveis if df[c].isna().all() or (df[c] == "").all()]
    if colunas_vazias:
        df.drop(columns=colunas_vazias, inplace=True)
        print(f"     [INFO] Colunas vazias removidas: {len(colunas_vazias)}")

    telefones = [c for c in df.columns if PADRAO_TELEFONE.match(c)]
    celulares = [c for c in df.columns if PADRAO_CELULAR.match(c)]
    emails    = [c for c in df.columns if PADRAO_EMAIL.match(c)]

    print(f"     Linhas: {len(df)} | Telefones: {len(telefones)} | Celulares: {len(celulares)} | EMAILs: {len(emails)}")
    return df
