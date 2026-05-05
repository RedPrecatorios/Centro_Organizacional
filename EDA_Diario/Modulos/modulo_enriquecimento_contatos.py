import pandas as pd
import re


def _excel_col_letras_para_indice(letras: str) -> int:
    """Índice 0-based da coluna tipo Excel (A, Z, AA, BA, BB...)."""
    n = 0
    for c in str(letras).strip().upper():
        if not ("A" <= c <= "Z"):
            raise ValueError(f"Letra de coluna invalida em {letras!r}: {c!r}")
        n = n * 26 + (ord(c) - ord("A") + 1)
    return n - 1


def _celula_csv_para_digitos(val: object) -> str:
    """Converte célula do CSV Lemitti para string só com dígitos (alinhado a DDD+FONE)."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return ""
    s = str(val).strip()
    if not s or s.lower() in ("nan", "none"):
        return ""
    if s.endswith(".0"):
        core = s[:-2].lstrip("-")
        if core.isdigit():
            s = s[:-2]
    try:
        return str(int(float(s.replace(",", "."))))
    except (ValueError, TypeError):
        pass
    dig = re.sub(r"\D", "", s)
    return dig


def ler_serie_telefone_concat_colunas_excel(
    caminho_entrada: str,
    col_a_letras: str = "BA",
    col_b_letras: str = "BB",
) -> pd.Series:
    """
    Lê o mesmo CSV da Lemitti (`processar_enriquecimento_contatos`), por posição
    de coluna estilo Excel, e devolve uma série por linha com BA concatenado com BB.

    Export «largo» Lemitti costuma usar colunas físicas BA (DDD) e BB (fone).
    Se o ficheiro tiver menos colunas, devolve série vazia por linha.
    """
    df = pd.read_csv(
        caminho_entrada,
        sep=None,
        engine="python",
        dtype=str,
        encoding="utf-8-sig",
    )
    n = df.shape[1]
    try:
        ia = _excel_col_letras_para_indice(col_a_letras)
        ib = _excel_col_letras_para_indice(col_b_letras)
    except ValueError:
        return pd.Series([""] * len(df), dtype=object)

    if ia >= n or ib >= n:
        return pd.Series([""] * len(df), dtype=object)

    out: list[str] = []
    for i in range(len(df)):
        a = _celula_csv_para_digitos(df.iat[i, ia])
        b = _celula_csv_para_digitos(df.iat[i, ib])
        cat = (a + b).strip()
        out.append(cat if cat else "")
    return pd.Series(out, dtype=object, index=df.index)

# ─────────────────────────────────────────────
# Nomes base das colunas fixas a manter
# ─────────────────────────────────────────────
COLUNA_NOME = "NOME"
COLUNA_CPF  = "CPF/CNPJ"


def _norm_cab_contato(val: object) -> str:
    """Chave canonica para comparar nomes de coluna (maiúsculas, sem espacos/barra)."""
    return str(val).strip().upper().replace(" ", "").replace("/", "")


def _padronizar_cabecalhos_contatos(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove espacos nos titulos e alinha sinonimos aos nomes esperados pelo merge.
    Muitos CSVs exportam apenas 'CPF' em vez de 'CPF/CNPJ'.
    """
    df.columns = [str(c).strip() for c in df.columns]
    ren: dict[str, str] = {}
    if COLUNA_CPF not in df.columns:
        for c in df.columns.tolist():
            k = _norm_cab_contato(c)
            if k in ("CPF", "CPFCNPJ", "CNPJ"):
                ren[c] = COLUNA_CPF
                break
    if COLUNA_NOME not in df.columns:
        for c in df.columns.tolist():
            if c in ren:
                continue
            if _norm_cab_contato(c) == "NOME":
                ren[c] = COLUNA_NOME
                break
    if ren:
        df = df.rename(columns=ren)
    return df

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


def processar_enriquecimento_contatos(
    caminho_entrada: str,
) -> tuple[pd.DataFrame, str]:
    """
    Le o CSV de enriquecimento, mantem NOME e/ou CPF/CNPJ, telefones (DDD+FONE) e EMAILs.

    Retorna também o modo de cruzamento na etapa 1:
      - `"cpf"`: há coluna de documento (CPF/CNPJ ou sinonimo `CPF`/`CNPJ`);
      - `"nome"`: há só `NOME` (sem documento); o merge com a principal usa nome normalizado.

    Args:
        caminho_entrada: Caminho para o arquivo .csv de entrada.

    Returns:
        ``(dataframe, modo)`` onde ``modo`` é ``\"cpf\"`` ou ``\"nome\"``.
    """
    df = pd.read_csv(
        caminho_entrada,
        sep=None,
        engine="python",
        dtype=str,
        encoding="utf-8-sig",
    )
    df = _padronizar_cabecalhos_contatos(df)

    colunas_selecionadas = _filtrar_colunas(df.columns.tolist())

    tem_doc = COLUNA_CPF in colunas_selecionadas
    tem_nome = COLUNA_NOME in colunas_selecionadas
    if not tem_doc and not tem_nome:
        raise ValueError(
            "Planilha 2 (contatos) precisa da coluna de documento "
            f"({COLUNA_CPF!r}, 'CPF', 'CNPJ') ou da coluna {COLUNA_NOME!r}. "
            f"Colunas encontradas: {list(df.columns)!r}"
        )
    modo = "cpf" if tem_doc else "nome"

    if modo == "cpf" and COLUNA_NOME not in colunas_selecionadas:
        print("     [AVISO] Planilha 2 sem coluna NOME (apenas documento/contatos).")
    if modo == "nome":
        print(
            "     [INFO] Planilha 2 sem CPF/CNPJ — Etapa 1 cruza NOME na P2 com "
            "Requerente/NOME na principal. Homónimos podem receber telefones trocados."
        )

    df = df[colunas_selecionadas]

    ddds  = [c for c in colunas_selecionadas if PADRAO_DDD.match(c)]
    fones = [c for c in colunas_selecionadas if PADRAO_FONE.match(c)]

    df = _unificar_telefones(df, ddds, fones)

    # Remove colunas completamente vazias
    fixos = ({COLUNA_NOME, COLUNA_CPF} & set(df.columns))
    colunas_variaveis = [c for c in df.columns if c not in fixos]
    colunas_vazias    = [c for c in colunas_variaveis if df[c].isna().all() or (df[c] == "").all()]
    if colunas_vazias:
        df.drop(columns=colunas_vazias, inplace=True)
        print(f"     [INFO] Colunas vazias removidas: {colunas_vazias}")

    telefones = [c for c in df.columns if c.startswith(PREFIXO_TELEFONE)]
    emails    = [c for c in df.columns if PADRAO_EMAIL.match(c)]

    print(f"     Linhas: {len(df)} | Telefones: {len(telefones)} | EMAILs: {len(emails)}")
    return df, modo
