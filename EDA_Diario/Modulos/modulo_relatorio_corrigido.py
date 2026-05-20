"""
Parse de exportações de discagem (PRC TJSP / PRC CMP / PRC IMP) e geração de
Excel com abas por resultado.

Após separar as linhas:
  - Telefone_Recado: grava TELEFONE na blacklist (motivo {resultado}_SysCall),
    acrescenta coluna MOTIVO no Excel.
  - Sem Interesse_Remover: grava TELEFONE e NOME na blacklist (mesmo motivo),
    acrescenta coluna Resultado no Excel.
  - Outros Resultados: apenas exportação no Excel (sem blacklist).
"""
from __future__ import annotations

import io
import unicodedata
from pathlib import Path
from typing import Any, List, Optional

import pandas as pd

# Nomes oficiais dos mailings na BD e na UI.
FORMATO_PRC_IMP = "PRC IMP"
FORMATO_PRC_TJSP = "PRC TJSP"
FORMATO_PRC_CMP = "PRC CMP"
FORMATO_DESCONHECIDO = "desconhecido"

FORMATOS_RELATORIO_VALIDOS = frozenset(
    {FORMATO_PRC_IMP, FORMATO_PRC_TJSP, FORMATO_PRC_CMP, FORMATO_DESCONHECIDO}
)

# Aliases antigos (legado / campanha / federal) → nome actual.
_ALIASES_FORMATO_ANTIGO: dict[str, str] = {
    "federal": FORMATO_PRC_IMP,
    "legado": FORMATO_PRC_TJSP,
    "campanha": FORMATO_PRC_CMP,
}

# Valor fixo PRC CMP (ordem e numero_incidente).
LITERAL_CUMPRIMENTO_CMP = "Cumprimento"

# Linhas com estes estados não entram na tabela relatorio_discagem.
_STATUS_RESULTADO_IGNORAR_BD = frozenset({"indisponivel", "automatico"})
_STATUS_LIGACAO_IGNORAR_BD = frozenset(
    {"caixa postal", "cx. postal", "cx postal", "caixa postal."}
)

# Colunas da BD — alinhadas à tabela de mapeamento PRC IMP / TJSP / CMP.
COLUNAS_RELATORIO_DADOS: tuple[str, ...] = (
    "telefone",
    "nome",
    "cpf",
    "ordem",
    "processo_principal",
    "processo",
    "numero_incidente",
    "data_base",
    "desconto_previdenciario",
    "desconto_assistencia_medica",
    "honorarios",
    "principal",
    "pre_calculo",
    "ir_retido",
    "advogado",
    "entidade_devedora",
    "assunto",
    "telefone_discagem",
    "status_ligacao",
    "origem",
    "resultado",
    "tempo",
)


def _parte(partes: list, indice: int, default: str = "") -> str:
    if indice < 0 or indice >= len(partes):
        return default
    return partes[indice].strip()


def _registro_vazio() -> dict[str, Any]:
    return {c: "" for c in COLUNAS_RELATORIO_DADOS}


def obter_csv_mais_recente(pasta: Path) -> Path:
    """Retorna o CSV mais recente em `pasta` (por mtime)."""
    arquivos_csv = list(pasta.glob("*.csv"))
    if not arquivos_csv:
        raise FileNotFoundError("Nenhum arquivo CSV encontrado na pasta.")
    return max(arquivos_csv, key=lambda f: f.stat().st_mtime)


def limpar_label(valor: str, label: str):
    if not isinstance(valor, str):
        return valor
    v = valor.strip()
    for sep in (": ", ":"):
        pref = f"{label}{sep}"
        if v.lower().startswith(pref.lower()):
            return v[len(pref) :].strip()
    return v


def corrigir_encoding(texto: str):
    try:
        return texto.encode("latin1").decode("utf-8")
    except Exception:
        return texto


def normalizar_formato_relatorio(valor: str) -> str:
    """Aceita nome novo ou alias antigo (legado/campanha/federal)."""
    v = (valor or "").strip()
    if not v:
        return FORMATO_DESCONHECIDO
    baixo = v.lower()
    if baixo in _ALIASES_FORMATO_ANTIGO:
        return _ALIASES_FORMATO_ANTIGO[baixo]
    if v in FORMATOS_RELATORIO_VALIDOS:
        return v
    return FORMATO_DESCONHECIDO


def detectar_padrao(partes: list) -> str:
    if not partes or not (partes[0] and partes[0].strip()):
        return FORMATO_DESCONHECIDO
    p0 = partes[0].strip().lower()
    p1 = partes[1].strip().lower() if len(partes) > 1 else ""
    p3 = partes[3].strip().lower() if len(partes) > 3 else ""

    if p0.startswith("contato:"):
        return FORMATO_PRC_IMP
    if not p0.startswith("telefone:"):
        return FORMATO_DESCONHECIDO
    if p1.startswith("requerente:"):
        return FORMATO_PRC_CMP
    if "processo_principal" in p3:
        return FORMATO_PRC_CMP
    if "numero do cumprimento" in (partes[4].strip().lower() if len(partes) > 4 else ""):
        return FORMATO_PRC_CMP
    return FORMATO_PRC_TJSP


def parse_prc_tjsp(partes: list) -> Optional[dict[str, Any]]:
    """PRC TJSP — mapeamento coluna a coluna (ver tabela de analogia)."""
    if len(partes) < 14:
        return None
    reg = _registro_vazio()
    reg.update(
        {
            "telefone": limpar_label(_parte(partes, 0), "Telefone"),
            "nome": _parte(partes, 1),
            "cpf": limpar_label(_parte(partes, 2), "CPF"),
            "ordem": limpar_label(_parte(partes, 3), "Ordem"),
            "processo": limpar_label(_parte(partes, 4), "Processo"),
            "numero_incidente": limpar_label(_parte(partes, 5), "Numero do Incidente"),
            "principal": limpar_label(_parte(partes, 6), "Principal"),
            "pre_calculo": limpar_label(_parte(partes, 7), "Pre Calculo"),
            "advogado": limpar_label(_parte(partes, 8), "Advogado"),
            "entidade_devedora": limpar_label(_parte(partes, 9), "Entidade devedora"),
            "telefone_discagem": _parte(partes, 10),
            "status_ligacao": _parte(partes, 11),
            "origem": _parte(partes, 12),
            "resultado": _parte(partes, 13),
            "tempo": _parte(partes, -1),
        }
    )
    return reg


def parse_prc_cmp(partes: list) -> Optional[dict[str, Any]]:
    """PRC CMP — ordem e numero_incidente fixos em Cumprimento; processo = numero do cumprimento."""
    if len(partes) < 18:
        return None
    reg = _registro_vazio()
    reg.update(
        {
            "telefone": limpar_label(_parte(partes, 0), "Telefone"),
            "nome": limpar_label(_parte(partes, 1), "Requerente"),
            "cpf": limpar_label(_parte(partes, 2), "CPF"),
            "ordem": LITERAL_CUMPRIMENTO_CMP,
            "processo_principal": limpar_label(_parte(partes, 3), "Processo_principal"),
            "processo": limpar_label(_parte(partes, 4), "Numero do cumprimento"),
            "numero_incidente": LITERAL_CUMPRIMENTO_CMP,
            "advogado": limpar_label(_parte(partes, 5), "Advogado"),
            "entidade_devedora": limpar_label(_parte(partes, 6), "Entidade devedora"),
            "assunto": limpar_label(_parte(partes, 7), "Assunto"),
            "data_base": limpar_label(_parte(partes, 8), "Data base"),
            "desconto_previdenciario": limpar_label(
                _parte(partes, 9), "Desconto previdenciário"
            ),
            "desconto_assistencia_medica": limpar_label(
                _parte(partes, 10), "Desconto assistência médica"
            ),
            "honorarios": limpar_label(_parte(partes, 11), "Honorários"),
            "principal": limpar_label(_parte(partes, 12), "Valor Total"),
            "pre_calculo": limpar_label(_parte(partes, 13), "Pré Calculo"),
            "telefone_discagem": _parte(partes, 14),
            "status_ligacao": _parte(partes, 15),
            "origem": _parte(partes, 16),
            "resultado": _parte(partes, 17),
            "tempo": _parte(partes, -1),
        }
    )
    return reg


def parse_prc_imp(partes: list) -> Optional[dict[str, Any]]:
    """PRC IMP — ordem=OC, principal=Ofício, processo=processosOriginarios, ir_retido=IR Retido."""
    if len(partes) < 15:
        return None
    ir = limpar_label(_parte(partes, 8), "IR Retido")
    reg = _registro_vazio()
    reg.update(
        {
            "telefone": limpar_label(_parte(partes, 0), "Contato"),
            "processo": limpar_label(_parte(partes, 1), "processosOriginarios"),
            "nome": limpar_label(_parte(partes, 2), "requerentes"),
            "cpf": limpar_label(_parte(partes, 3), "Cpf Geral"),
            "entidade_devedora": limpar_label(_parte(partes, 4), "Devedor"),
            "advogado": limpar_label(_parte(partes, 5), "Advogado"),
            "ordem": limpar_label(_parte(partes, 6), "OC"),
            "principal": limpar_label(_parte(partes, 7), "Ofício"),
            "pre_calculo": ir,
            "ir_retido": ir,
            "telefone_discagem": _parte(partes, 9),
            "status_ligacao": _parte(partes, 10),
            "origem": _parte(partes, 11),
            "resultado": _parte(partes, 12),
            "tempo": _parte(partes, -1),
        }
    )
    return reg


def parse_linha(partes: list) -> Optional[dict[str, Any]]:
    pad = detectar_padrao(partes)
    reg: Optional[dict[str, Any]] = None
    if pad == FORMATO_PRC_IMP:
        reg = parse_prc_imp(partes)
    elif pad == FORMATO_PRC_CMP:
        reg = parse_prc_cmp(partes)
    elif pad == FORMATO_PRC_TJSP:
        reg = parse_prc_tjsp(partes)
    if reg is not None and pad in FORMATOS_RELATORIO_VALIDOS - {FORMATO_DESCONHECIDO}:
        reg["formato"] = pad
    return reg


# Compatibilidade com nomes antigos das funções de parse.
parse_legado = parse_prc_tjsp
parse_campanha = parse_prc_cmp
parse_federal = parse_prc_imp


def carregar_dados(caminho: Path) -> List[dict[str, Any]]:
    dados: List[dict[str, Any]] = []
    with open(caminho, "r", encoding="latin1") as fh:
        for linha in fh:
            linha = corrigir_encoding(linha)
            partes = linha.strip().split(";")
            if len(partes) < 10:
                continue
            reg = parse_linha(partes)
            if reg is not None:
                dados.append(reg)
    return dados


def detectar_formato_arquivo(caminho: Path) -> str:
    """Formato predominante no ficheiro (contagem por linha)."""
    contagem: dict[str, int] = {}
    with open(caminho, "r", encoding="latin1") as fh:
        for linha in fh:
            linha = corrigir_encoding(linha)
            partes = linha.strip().split(";")
            if len(partes) < 10:
                continue
            pad = detectar_padrao(partes)
            if pad != FORMATO_DESCONHECIDO:
                contagem[pad] = contagem.get(pad, 0) + 1
    if not contagem:
        return FORMATO_DESCONHECIDO
    return max(contagem, key=contagem.get)


def listar_csvs_relatorio(pasta: Path) -> List[Path]:
    """CSV da pasta de relatório (ignora lock ~$ do Excel)."""
    out: List[Path] = []
    for p in sorted(pasta.glob("*.csv")):
        if p.name.startswith("~$"):
            continue
        out.append(p)
    return out


def apagar_csvs_relatorio_pasta(pasta: Path) -> list[str]:
    """Remove CSV já gravados na BD (após processamento com sucesso)."""
    apagados: list[str] = []
    for arq in listar_csvs_relatorio(pasta):
        try:
            arq.unlink(missing_ok=True)
            apagados.append(arq.name)
            print(f"[relatorio] CSV apagado: {arq.name}")
        except OSError as exc:
            print(f"[relatorio] Aviso: não foi possível apagar {arq.name} — {exc}")
    return apagados


ABA_1_STATUS = [
    "Telefone Incorreto",
    "Deixou Recado",
]

ABA_2_STATUS = [
    "Sem Interesse",
    "Inclusão Monday",
    "Inclusão Pré Cálculo",
    "Remover do Mailing",
]

ABA_BLACKLIST_EXCEL = "Localizados_Blacklist"
ABA_TELEFONE_RECADO = "Telefone_Recado"
ABA_SEM_INTERESSE = "Sem Interesse_Remover"
ABA_OUTROS = "Outros Resultados"


def _resultado_coincide_lista(resultado: str, valores: list[str]) -> bool:
    """Compara resultado ignorando maiúsculas e acentos."""
    chave = _norm_status_comparacao(_texto_resultado_linha(resultado))
    if not chave:
        return False
    for v in valores:
        if _norm_status_comparacao(v) == chave:
            return True
    return False


def aba_por_resultado(resultado: str) -> str:
    """Nome da aba Excel/BD conforme o campo ``resultado``."""
    if _resultado_coincide_lista(resultado, ABA_1_STATUS):
        return ABA_TELEFONE_RECADO
    if _resultado_coincide_lista(resultado, ABA_2_STATUS):
        return ABA_SEM_INTERESSE
    return ABA_OUTROS


def _reclassificar_abas_linhas_banco(linhas: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Recalcula ``aba`` a partir de ``resultado`` (export e contagens).
    Mantém ``Localizados_Blacklist``; corrige linhas antigas gravadas em Outros.
    """
    out: list[dict[str, Any]] = []
    for row in linhas:
        item = dict(row)
        if (item.get("aba") or "").strip() == ABA_BLACKLIST_EXCEL:
            out.append(item)
            continue
        item["aba"] = aba_por_resultado(item.get("resultado"))
        out.append(item)
    return out


def montar_dataframes(caminho_csv: Path) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    dados = carregar_dados(caminho_csv)
    if not dados:
        raise ValueError(
            "Nenhuma linha válida pôde ser lida (formato não reconhecido ou colunas a menos)."
        )
    df = pd.DataFrame(dados)
    df["resultado"] = df["resultado"].fillna("").astype(str).str.strip()

    df_aba1 = df[df["resultado"].apply(lambda r: _resultado_coincide_lista(r, ABA_1_STATUS))]
    df_aba2 = df[df["resultado"].apply(lambda r: _resultado_coincide_lista(r, ABA_2_STATUS))]
    usadas = ABA_1_STATUS + ABA_2_STATUS
    df_aba3 = df[
        ~df["resultado"].apply(
            lambda r: _resultado_coincide_lista(r, usadas)
        )
    ]
    return df_aba1, df_aba2, df_aba3


def _texto_resultado_linha(resultado) -> str:
    if resultado is None or (isinstance(resultado, float) and pd.isna(resultado)):
        return ""
    return str(resultado).strip()


def motivo_blacklist_sys_call(resultado) -> str:
    """{resultado}_SysCall — valor gravado em blacklist.motivo (e espelhado no Excel)."""
    return f"{_texto_resultado_linha(resultado)}_SysCall"


def _celula_str(valor) -> str:
    if valor is None or (isinstance(valor, float) and pd.isna(valor)):
        return ""
    return str(valor).strip()


def _norm_status_comparacao(texto: str) -> str:
    """Minúsculas sem acentos para comparar status de discagem."""
    s = (texto or "").strip().lower()
    nfd = unicodedata.normalize("NFD", s)
    return "".join(c for c in nfd if unicodedata.category(c) != "Mn")


def deve_salvar_linha_no_banco(row) -> bool:
    """
    Exclui da BD: resultado Indisponível ou Automático;
    status_ligacao Caixa Postal (Cx. Postal).
    """
    resultado = _norm_status_comparacao(_celula_str(row.get("resultado")))
    ligacao = _norm_status_comparacao(_celula_str(row.get("status_ligacao")))
    if resultado in _STATUS_RESULTADO_IGNORAR_BD:
        return False
    if ligacao in _STATUS_LIGACAO_IGNORAR_BD:
        return False
    return True


def _linhas_de_dataframe(
    df: pd.DataFrame,
    aba: str,
    *,
    apenas_para_banco: bool = False,
) -> list[dict[str, Any]]:
    if df.empty:
        return []
    campos = COLUNAS_RELATORIO_DADOS
    out: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        if apenas_para_banco and not deve_salvar_linha_no_banco(row):
            continue
        item: dict[str, Any] = {"aba": aba}
        for c in campos:
            item[c] = _celula_str(row.get(c))
        if aba == ABA_BLACKLIST_EXCEL:
            item["aba_origem"] = _celula_str(row.get("aba_origem"))
            item["motivo_blacklist"] = _celula_str(row.get("motivo_blacklist"))
        fmt = _celula_str(row.get("formato"))
        if fmt:
            item["formato"] = fmt
        out.append(item)
    return out


def persistir_relatorio_no_banco(
    caminho_csv: Path,
    df_aba1: pd.DataFrame,
    df_aba2: pd.DataFrame,
    df_aba3: pd.DataFrame,
    df_blacklist: pd.DataFrame,
) -> int:
    from modulo_banco import salvar_relatorio_discagem

    linhas: list[dict[str, Any]] = []
    linhas.extend(_linhas_de_dataframe(df_aba1, "Telefone_Recado", apenas_para_banco=True))
    linhas.extend(
        _linhas_de_dataframe(df_aba2, "Sem Interesse_Remover", apenas_para_banco=True)
    )
    linhas.extend(_linhas_de_dataframe(df_aba3, "Outros Resultados", apenas_para_banco=True))
    linhas.extend(
        _linhas_de_dataframe(df_blacklist, ABA_BLACKLIST_EXCEL, apenas_para_banco=True)
    )
    return salvar_relatorio_discagem(caminho_csv.name, linhas)


def persistir_arquivo_relatorio(caminho_csv: Path) -> int:
    """Grava um CSV na tabela relatorio_discagem (sem gerar Excel)."""
    df_aba1, df_aba2, df_aba3, df_blacklist = _preparar_abas_com_blacklist(caminho_csv)
    return persistir_relatorio_no_banco(
        caminho_csv, df_aba1, df_aba2, df_aba3, df_blacklist
    )


def sincronizar_relatorio_discagem_pasta(pasta: Path) -> dict[str, int]:
    """Grava cada .csv da pasta na BD (substitui por nome de ficheiro)."""
    resumo: dict[str, int] = {}
    for arq in listar_csvs_relatorio(pasta):
        try:
            resumo[arq.name] = persistir_arquivo_relatorio(arq)
        except ValueError:
            resumo[arq.name] = 0
            print(f"[relatorio_discagem] {arq.name}: nenhuma linha válida (ignorado)")
        except Exception as exc:
            resumo[arq.name] = -1
            print(f"[relatorio_discagem] {arq.name}: erro — {exc}")
    return resumo


def _preparar_abas_com_blacklist(
    caminho_csv: Path,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    from modulo_banco import carregar_blacklist

    df_aba1, df_aba2, df_aba3 = montar_dataframes(caminho_csv)
    bl = carregar_blacklist()
    return _separar_abas_por_blacklist(df_aba1, df_aba2, df_aba3, bl)


def _separar_abas_por_blacklist(
    df_aba1: pd.DataFrame,
    df_aba2: pd.DataFrame,
    df_aba3: pd.DataFrame,
    bl: dict,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    from modulo_blacklist import separar_relatorio_blacklist

    a1, b1 = separar_relatorio_blacklist(df_aba1, bl, "Telefone_Recado")
    a2, b2 = separar_relatorio_blacklist(df_aba2, bl, "Sem Interesse_Remover")
    a3, b3 = separar_relatorio_blacklist(df_aba3, bl, "Outros Resultados")
    partes = [b1, b2, b3]
    if any(not p.empty for p in partes):
        df_bl = pd.concat([p for p in partes if not p.empty], ignore_index=True)
    else:
        df_bl = pd.DataFrame()
    return a1, a2, a3, df_bl


_ROTULOS_COLUNAS_EXCEL: dict[str, str] = {
    "telefone": "Telefone",
    "nome": "Nome",
    "cpf": "CPF",
    "ordem": "Ordem",
    "processo_principal": "Processo principal",
    "processo": "Processo / Cumprimento",
    "numero_incidente": "Nº incidente",
    "data_base": "Data base",
    "desconto_previdenciario": "Desconto previdenciário",
    "desconto_assistencia_medica": "Desconto assist. médica",
    "honorarios": "Honorários",
    "principal": "Principal / Valor total",
    "pre_calculo": "Pré-cálculo",
    "ir_retido": "IR retido",
    "advogado": "Advogado",
    "entidade_devedora": "Entidade devedora",
    "assunto": "Assunto",
    "telefone_discagem": "Tel. discagem",
    "status_ligacao": "Status ligação",
    "origem": "Origem",
    "resultado": "Resultado",
    "tempo": "Tempo",
    "formato": "Tipo mailing",
    "arquivo": "Arquivo origem",
    "aba_origem": "Aba origem",
    "motivo_blacklist": "Motivo blacklist",
    "MOTIVO": "MOTIVO",
    "Resultado": "Resultado",
}


def _formatar_df_excel_apresentacao(df: pd.DataFrame, aba: str) -> pd.DataFrame:
    """Cabeçalhos legíveis, remove colunas vazias e ordena para leitura no Excel."""
    extras: list[str] = []
    if aba == "Telefone_Recado":
        extras = ["MOTIVO"]
    elif aba == "Sem Interesse_Remover":
        extras = ["Resultado"]
    elif aba == ABA_BLACKLIST_EXCEL:
        extras = ["aba_origem", "motivo_blacklist"]

    if df.empty:
        cols_int = extras + list(COLUNAS_RELATORIO_DADOS) + ["formato", "arquivo"]
        return pd.DataFrame(columns=[_ROTULOS_COLUNAS_EXCEL.get(c, c) for c in cols_int])

    out = df.copy()
    protegidas = set(extras) | {"formato", "arquivo", "aba_origem", "motivo_blacklist"}
    for col in list(out.columns):
        if col in protegidas:
            continue
        serie = out[col].fillna("").astype(str).str.strip()
        if serie.eq("").all():
            out = out.drop(columns=[col])

    ordem: list[str] = []
    for c in extras:
        if c in out.columns:
            ordem.append(c)
    for c in COLUNAS_RELATORIO_DADOS:
        if c in out.columns:
            ordem.append(c)
    for c in ("formato", "arquivo"):
        if c in out.columns:
            ordem.append(c)
    for c in out.columns:
        if c not in ordem:
            ordem.append(c)
    out = out[ordem]
    return out.rename(
        columns={
            k: v
            for k, v in _ROTULOS_COLUNAS_EXCEL.items()
            if k in out.columns
        }
    )


def _escrever_excel_relatorio(
    df_aba1_x: pd.DataFrame,
    df_aba2_x: pd.DataFrame,
    df_aba3: pd.DataFrame,
    df_blacklist: pd.DataFrame,
) -> bytes:
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        _formatar_df_excel_apresentacao(df_aba1_x, "Telefone_Recado").to_excel(
            writer, sheet_name="Telefone_Recado", index=False
        )
        _formatar_df_excel_apresentacao(df_aba2_x, "Sem Interesse_Remover").to_excel(
            writer, sheet_name="Sem Interesse_Remover", index=False
        )
        _formatar_df_excel_apresentacao(df_aba3, "Outros Resultados").to_excel(
            writer, sheet_name="Outros Resultados", index=False
        )
        _formatar_df_excel_apresentacao(df_blacklist, ABA_BLACKLIST_EXCEL).to_excel(
            writer, sheet_name=ABA_BLACKLIST_EXCEL, index=False
        )
    buffer.seek(0)
    return buffer.read()


def _df_com_motivo_excel(df: pd.DataFrame, nome_coluna: str) -> pd.DataFrame:
    """Acrescenta coluna MOTIVO ou Resultado com resultado_SysCall por linha."""
    if df.empty:
        return df
    out = df.copy()
    out[nome_coluna] = out["resultado"].map(motivo_blacklist_sys_call)
    return out


def aplicar_blacklist_telefone_recado(df: pd.DataFrame) -> tuple[int, int]:
    """
    Insere cada telefone (normalizado) na blacklist com motivo {resultado}_SysCall.
    Retorna (inseridos_com_sucesso, ignorados_sem_telefone).
    Ignora telefones já na blacklist.
    """
    from modulo_banco import adicionar_blacklist, carregar_blacklist
    from modulo_blacklist import normalizar_valor_para_blacklist

    bl = carregar_blacklist()
    bl_tel = bl.get("TELEFONE", set())
    ok = 0
    ign = 0
    for _, row in df.iterrows():
        tel = normalizar_valor_para_blacklist("TELEFONE", row.get("telefone"))
        if not tel:
            ign += 1
            continue
        if tel in bl_tel:
            continue
        adicionar_blacklist("TELEFONE", tel, motivo_blacklist_sys_call(row.get("resultado")))
        bl_tel.add(tel)
        ok += 1
    return ok, ign


def aplicar_blacklist_sem_interesse_remover(df: pd.DataFrame) -> tuple[int, int, int]:
    """
    Telefone + Nome na blacklist; motivo {resultado}_SysCall.
    Retorna (telefones_ok, nomes_ok, tel_ignorados). Ignora já bloqueados.
    """
    from modulo_banco import adicionar_blacklist, carregar_blacklist
    from modulo_blacklist import normalizar_valor_para_blacklist

    bl = carregar_blacklist()
    bl_tel = bl.get("TELEFONE", set())
    bl_nom = bl.get("NOME", set())
    ok_tel = 0
    ok_nom = 0
    ign_tel = 0
    motivo_fn = motivo_blacklist_sys_call

    for _, row in df.iterrows():
        m = motivo_fn(row.get("resultado"))
        tel = normalizar_valor_para_blacklist("TELEFONE", row.get("telefone"))
        if tel:
            if tel not in bl_tel:
                adicionar_blacklist("TELEFONE", tel, m)
                bl_tel.add(tel)
            ok_tel += 1
        else:
            ign_tel += 1

        nom = normalizar_valor_para_blacklist("NOME", row.get("nome"))
        if nom:
            if nom not in bl_nom:
                adicionar_blacklist("NOME", nom, m)
                bl_nom.add(nom)
            ok_nom += 1

    return ok_tel, ok_nom, ign_tel


def gerar_relatorio_com_blacklist(
    caminho_csv: Path,
    pasta_sync_bd: Path | None = None,
) -> tuple[bytes, dict[str, int]]:
    """
    Monta os três dataframes, grava blacklist a partir das abas 1 e 2, enriquece
    o Excel com MOTIVO (aba 1) e Resultado (aba 2, texto resultado_SysCall), e
    devolve o .xlsx em bytes com estatísticas simples.
    """
    df_aba1, df_aba2, df_aba3, df_blacklist = _preparar_abas_com_blacklist(caminho_csv)
    n_bd = persistir_relatorio_no_banco(
        caminho_csv, df_aba1, df_aba2, df_aba3, df_blacklist
    )

    t_ok, t_ign = aplicar_blacklist_telefone_recado(df_aba1)
    s_tel, s_nom, s_ign = aplicar_blacklist_sem_interesse_remover(df_aba2)

    # Excel do ficheiro processado (registos desse arquivo na BD).
    data = gerar_excel_do_banco(arquivo=caminho_csv.name)

    csv_apagados: list[str] = []
    if caminho_csv.is_file():
        try:
            caminho_csv.unlink(missing_ok=True)
            csv_apagados = [caminho_csv.name]
        except OSError as exc:
            print(f"[relatorio] Aviso: não apagou {caminho_csv.name} — {exc}")

    stats = {
        "bl_telefone_recado": t_ok,
        "bl_recado_sem_tel": t_ign,
        "bl_sem_interesse_tel": s_tel,
        "bl_sem_interesse_nome": s_nom,
        "bl_sem_interesse_sem_tel": s_ign,
        "linhas_localizadas_blacklist": len(df_blacklist),
        "linhas_gravadas_bd": n_bd,
        "csv_apagados": len(csv_apagados),
    }
    return data, stats


def gerar_excel_somente(
    caminho_csv: Path,
    pasta_sync_bd: Path | None = None,
) -> bytes:
    """Gera o Excel a partir do CSV (uso interno / legado). Preferir ``gerar_excel_do_banco``."""
    df_aba1, df_aba2, df_aba3, df_blacklist = _preparar_abas_com_blacklist(caminho_csv)
    persistir_relatorio_no_banco(caminho_csv, df_aba1, df_aba2, df_aba3, df_blacklist)
    if pasta_sync_bd is not None and pasta_sync_bd.is_dir():
        sincronizar_relatorio_discagem_pasta(pasta_sync_bd)

    df_aba1_x = _df_com_motivo_excel(df_aba1, "MOTIVO")
    df_aba2_x = _df_com_motivo_excel(df_aba2, "Resultado")

    return _escrever_excel_relatorio(df_aba1_x, df_aba2_x, df_aba3, df_blacklist)


_COLUNAS_EXCEL_EXPORT: tuple[str, ...] = COLUNAS_RELATORIO_DADOS + (
    "formato",
    "arquivo",
)

_COLUNAS_EXCEL_BLACKLIST_EXTRA: tuple[str, ...] = ("aba_origem", "motivo_blacklist")


def _dataframe_exportacao(linhas: list[dict], aba: str) -> pd.DataFrame:
    """Filtra por aba e mantém colunas de dados (sem coluna ``aba`` interna)."""
    if not linhas:
        return pd.DataFrame()
    df = pd.DataFrame(linhas)
    if "aba" in df.columns:
        df = df[df["aba"].astype(str) == aba]
    cols = list(_COLUNAS_EXCEL_EXPORT)
    if aba == ABA_BLACKLIST_EXCEL:
        cols.extend(_COLUNAS_EXCEL_BLACKLIST_EXTRA)
    presentes = [c for c in cols if c in df.columns]
    if df.empty:
        return pd.DataFrame(columns=presentes)
    out = df[presentes].reset_index(drop=True)
    if "resultado" in out.columns:
        out = out.sort_values(
            by=[c for c in ("processo", "nome", "telefone") if c in out.columns],
            kind="stable",
        )
    return out.reset_index(drop=True)


def gerar_excel_do_banco(
    arquivo: str | None = None,
    formato: str | None = None,
) -> bytes:
    """
    Monta o .xlsx a partir da tabela ``relatorio_discagem`` (não lê CSV).
    ``arquivo``: só linhas desse CSV na base.
    ``formato``: PRC TJSP, PRC CMP ou PRC IMP (None = todos).
    """
    from modulo_banco import carregar_relatorio_discagem

    linhas = _reclassificar_abas_linhas_banco(
        carregar_relatorio_discagem(arquivo, formato)
    )
    if not linhas:
        if arquivo:
            raise ValueError(
                f"Nenhum registo na base para o ficheiro «{arquivo}». "
                "Use «Processar + Blacklist» para gravar o CSV primeiro."
            )
        if formato:
            raise ValueError(
                f"Nenhum registo na base para o formato «{formato}». "
                "Processe um CSV desse tipo ou exporte «Todos»."
            )
        raise ValueError(
            "A tabela relatorio_discagem está vazia. "
            "Use «Processar + Blacklist» para gravar relatórios na base antes de exportar."
        )

    df_aba1 = _dataframe_exportacao(linhas, "Telefone_Recado")
    df_aba2 = _dataframe_exportacao(linhas, "Sem Interesse_Remover")
    df_aba3 = _dataframe_exportacao(linhas, "Outros Resultados")
    df_bl = _dataframe_exportacao(linhas, ABA_BLACKLIST_EXCEL)

    df_aba1_x = _df_com_motivo_excel(df_aba1, "MOTIVO")
    df_aba2_x = _df_com_motivo_excel(df_aba2, "Resultado")

    return _escrever_excel_relatorio(df_aba1_x, df_aba2_x, df_aba3, df_bl)


def resumo_exportacao_banco(
    arquivo: str | None = None,
    formato: str | None = None,
) -> dict[str, int]:
    """Contagens por aba na BD (para mensagens na UI)."""
    from modulo_banco import carregar_relatorio_discagem

    linhas = _reclassificar_abas_linhas_banco(
        carregar_relatorio_discagem(arquivo, formato)
    )
    return {
        "total": len(linhas),
        "telefone_recado": sum(1 for r in linhas if r.get("aba") == ABA_TELEFONE_RECADO),
        "sem_interesse": sum(1 for r in linhas if r.get("aba") == ABA_SEM_INTERESSE),
        "outros": sum(1 for r in linhas if r.get("aba") == ABA_OUTROS),
        "blacklist": sum(1 for r in linhas if r.get("aba") == ABA_BLACKLIST_EXCEL),
    }


def gerar_excel_bytes(caminho_csv: Path) -> bytes:
    """Compat: Excel + blacklist (use `gerar_relatorio_com_blacklist` para estatísticas)."""
    data, _stats = gerar_relatorio_com_blacklist(caminho_csv)
    return data
