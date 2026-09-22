import pandas as pd
import re

# ─────────────────────────────────────────────
# Colunas fixas a manter
# ─────────────────────────────────────────────
COLUNA_NOME = "NOME"
COLUNA_CPF  = "CPF"

# Export Assertiva real: TELEFONE1, CELULAR_WHATSAPP_1, CONJUGE_TELEFONE_1,
# FILHO_1_CELULAR_1, IRMAO_1_TELEFONE_1, EMAIL1, etc.
# O padrão antigo (`^TELEFONE\d+`) descartava WhatsApp e familiares.
PADRAO_TELEFONE = re.compile(r"^TELEFONE\d+.*$", re.IGNORECASE)
PADRAO_CELULAR  = re.compile(r"^CELULAR\d+.*$",  re.IGNORECASE)
PADRAO_EMAIL    = re.compile(r"^EMAIL\d+.*$",     re.IGNORECASE)
PADRAO_CONTATO_FLEX = re.compile(
    r"(TELEFONE|CELULAR|EMAIL)", re.IGNORECASE
)


def _filtrar_colunas(colunas: list[str]) -> list[str]:
    """Retorna NOME/CPF e qualquer coluna de telefone, celular ou e-mail."""
    selecionadas = []
    vistos: set[str] = set()
    for col in colunas:
        nome = str(col).strip()
        if not nome or nome in vistos:
            continue
        if nome in (COLUNA_NOME, COLUNA_CPF) or PADRAO_CONTATO_FLEX.search(nome):
            selecionadas.append(col)
            vistos.add(nome)
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

    telefones = [c for c in df.columns if re.search(r"TELEFONE", str(c), re.I)]
    celulares = [c for c in df.columns if re.search(r"CELULAR", str(c), re.I)]
    emails    = [c for c in df.columns if re.search(r"EMAIL", str(c), re.I)]

    print(
        f"     Linhas: {len(df)} | Telefones: {len(telefones)} | "
        f"Celulares: {len(celulares)} | EMAILs: {len(emails)}"
    )
    return df
