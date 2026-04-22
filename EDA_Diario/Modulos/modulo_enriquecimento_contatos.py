import pandas as pd
import re

# ─────────────────────────────────────────────
# Nomes base das colunas fixas a manter
# ─────────────────────────────────────────────
COLUNA_NOME = "NOME"
COLUNA_CPF  = "CPF/CNPJ"

# Prefixo usado para nomear as colunas de telefone unificadas (ex: TELEFONE_1, TELEFONE_2...)
PREFIXO_TELEFONE = "TELEFONE"

# Padroes para deteccao elastica de colunas (aceita variacoes como DDD, DDD.1, DDD.2...)
PADRAO_DDD   = re.compile(r"^DDD(\.\d+)?$",  re.IGNORECASE)
PADRAO_FONE  = re.compile(r"^FONE(\.\d+)?$", re.IGNORECASE)
PADRAO_EMAIL = re.compile(r"^EMAIL(-\d+)?$",  re.IGNORECASE)


def _filtrar_colunas(colunas: list[str]) -> list[str]:
    """Retorna apenas as colunas de interesse, mantendo a ordem original."""
    selecionadas = []
    for col in colunas:
        if col in (COLUNA_NOME, COLUNA_CPF):
            selecionadas.append(col)
        elif PADRAO_DDD.match(col):
            selecionadas.append(col)
        elif PADRAO_FONE.match(col):
            selecionadas.append(col)
        elif PADRAO_EMAIL.match(col):
            selecionadas.append(col)
    return selecionadas


def _unificar_telefones(df: pd.DataFrame, ddds: list[str], fones: list[str]) -> pd.DataFrame:
    """
    Combina cada par DDD + FONE em uma coluna TELEFONE_N.
    Remove as colunas originais de DDD e FONE apos a unificacao.
    """
    for i, (ddd_col, fone_col) in enumerate(zip(ddds, fones), start=1):
        nome_col = f"{PREFIXO_TELEFONE}_{i}"
        ddd_str  = df[ddd_col].apply(lambda v: str(int(float(v))) if pd.notna(v) and v != "" else "")
        fone_str = df[fone_col].apply(lambda v: str(int(float(v))) if pd.notna(v) and v != "" else "")
        df[nome_col] = (ddd_str + fone_str).replace("", pd.NA)

    df.drop(columns=ddds + fones, inplace=True)
    return df


def processar_enriquecimento_contatos(caminho_entrada: str) -> pd.DataFrame:
    """
    Le o CSV de enriquecimento, mantem apenas NOME, CPF, telefones (DDD+FONE unificados) e EMAILs.
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

    ddds  = [c for c in colunas_selecionadas if PADRAO_DDD.match(c)]
    fones = [c for c in colunas_selecionadas if PADRAO_FONE.match(c)]

    df = _unificar_telefones(df, ddds, fones)

    # Remove colunas completamente vazias
    colunas_variaveis = [c for c in df.columns if c not in (COLUNA_NOME, COLUNA_CPF)]
    colunas_vazias    = [c for c in colunas_variaveis if df[c].isna().all() or (df[c] == "").all()]
    if colunas_vazias:
        df.drop(columns=colunas_vazias, inplace=True)
        print(f"     [INFO] Colunas vazias removidas: {colunas_vazias}")

    telefones = [c for c in df.columns if c.startswith(PREFIXO_TELEFONE)]
    emails    = [c for c in df.columns if PADRAO_EMAIL.match(c)]

    print(f"     Linhas: {len(df)} | Telefones: {len(telefones)} | EMAILs: {len(emails)}")
    return df
