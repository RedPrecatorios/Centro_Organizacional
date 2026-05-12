"""
Parse de exportações de discagem (legado / campanha / federal) e geração de
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
from pathlib import Path
from typing import Any, List, Optional

import pandas as pd


def obter_csv_mais_recente(pasta: Path) -> Path:
    """Retorna o CSV mais recente em `pasta` (por mtime)."""
    arquivos_csv = list(pasta.glob("*.csv"))
    if not arquivos_csv:
        raise FileNotFoundError("Nenhum arquivo CSV encontrado na pasta.")
    return max(arquivos_csv, key=lambda f: f.stat().st_mtime)


def limpar_label(valor: str, label: str):
    if isinstance(valor, str):
        return valor.replace(f"{label}: ", "").strip()
    return valor


def corrigir_encoding(texto: str):
    try:
        return texto.encode("latin1").decode("utf-8")
    except Exception:
        return texto


def detectar_padrao(partes: list) -> str:
    if not partes or not (partes[0] and partes[0].strip()):
        return "desconhecido"
    p0 = partes[0].strip()
    p1 = partes[1].strip() if len(partes) > 1 else ""

    if p0.lower().startswith("contato:"):
        return "federal"
    if p0.lower().startswith("telefone:") and p1.lower().startswith("requerente:"):
        return "campanha"
    if p0.lower().startswith("telefone:"):
        return "legado"
    return "desconhecido"


def parse_legado(partes: list) -> Optional[dict[str, Any]]:
    if len(partes) < 14:
        return None
    return {
        "telefone": limpar_label(partes[0], "Telefone"),
        "nome": partes[1].strip(),
        "cpf": limpar_label(partes[2], "CPF"),
        "ordem": limpar_label(partes[3], "Ordem"),
        "processo": limpar_label(partes[4], "Processo"),
        "numero_incidente": limpar_label(partes[5], "Numero do Incidente"),
        "principal": limpar_label(partes[6], "Principal"),
        "pre_calculo": limpar_label(partes[7], "Pre Calculo"),
        "advogado": limpar_label(partes[8], "Advogado"),
        "entidade_devedora": limpar_label(partes[9], "Entidade devedora"),
        "telefone_discagem": partes[10] if len(partes) > 10 else "",
        "status_ligacao": partes[11] if len(partes) > 11 else "",
        "origem": partes[12] if len(partes) > 12 else "",
        "resultado": partes[13] if len(partes) > 13 else "",
        "tempo": partes[-1] if partes else "",
    }


def parse_campanha(partes: list) -> Optional[dict[str, Any]]:
    if len(partes) < 20:
        return None
    return {
        "telefone": limpar_label(partes[0], "Telefone"),
        "nome": limpar_label(partes[1], "Requerente"),
        "cpf": limpar_label(partes[2], "CPF"),
        "ordem": "",
        "processo": limpar_label(partes[3], "Processo_principal"),
        "numero_incidente": limpar_label(partes[4], "Numero do cumprimento"),
        "principal": limpar_label(partes[7], "Assunto"),
        "pre_calculo": limpar_label(partes[13], "Pré Calculo"),
        "advogado": limpar_label(partes[5], "Advogado"),
        "entidade_devedora": limpar_label(partes[6], "Entidade devedora"),
        "telefone_discagem": partes[14],
        "status_ligacao": partes[15],
        "origem": partes[16],
        "resultado": partes[17],
        "tempo": partes[-1],
    }


def parse_federal(partes: list) -> Optional[dict[str, Any]]:
    if len(partes) < 15:
        return None
    return {
        "telefone": limpar_label(partes[0], "Contato"),
        "nome": limpar_label(partes[2], "requerentes"),
        "cpf": limpar_label(partes[3], "Cpf Geral"),
        "ordem": "",
        "processo": limpar_label(partes[1], "processosOriginarios"),
        "numero_incidente": "",
        "principal": limpar_label(partes[6], "OC") + " / " + limpar_label(partes[7], "Ofício")
        if len(partes) > 7
        else limpar_label(partes[6], "OC"),
        "pre_calculo": limpar_label(partes[8], "IR Retido"),
        "advogado": limpar_label(partes[5], "Advogado"),
        "entidade_devedora": limpar_label(partes[4], "Devedor"),
        "telefone_discagem": partes[9],
        "status_ligacao": partes[10],
        "origem": partes[11],
        "resultado": partes[12],
        "tempo": partes[-1],
    }


def parse_linha(partes: list) -> Optional[dict[str, Any]]:
    pad = detectar_padrao(partes)
    if pad == "federal":
        return parse_federal(partes)
    if pad == "campanha":
        return parse_campanha(partes)
    if pad == "legado":
        return parse_legado(partes)
    return None


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


ABA_1_STATUS = [
    "Telefone Incorreto",
    "Deixou Recado",
]

ABA_2_STATUS = [
    "Sem Interesse",
    "Inclusão Monday",
    "Remover do Mailing",
]


def montar_dataframes(caminho_csv: Path) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    dados = carregar_dados(caminho_csv)
    if not dados:
        raise ValueError(
            "Nenhuma linha válida pôde ser lida (formato não reconhecido ou colunas a menos)."
        )
    df = pd.DataFrame(dados)
    df["resultado"] = df["resultado"].fillna("").astype(str).str.strip()

    df_aba1 = df[df["resultado"].isin(ABA_1_STATUS)]
    df_aba2 = df[df["resultado"].isin(ABA_2_STATUS)]
    df_aba3 = df[~df["resultado"].isin(ABA_1_STATUS + ABA_2_STATUS)]
    return df_aba1, df_aba2, df_aba3


def _texto_resultado_linha(resultado) -> str:
    if resultado is None or (isinstance(resultado, float) and pd.isna(resultado)):
        return ""
    return str(resultado).strip()


def motivo_blacklist_sys_call(resultado) -> str:
    """{resultado}_SysCall — valor gravado em blacklist.motivo (e espelhado no Excel)."""
    return f"{_texto_resultado_linha(resultado)}_SysCall"


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
    """
    from modulo_banco import adicionar_blacklist
    from modulo_blacklist import normalizar_valor_para_blacklist

    ok = 0
    ign = 0
    for _, row in df.iterrows():
        tel = normalizar_valor_para_blacklist("TELEFONE", row.get("telefone"))
        if not tel:
            ign += 1
            continue
        adicionar_blacklist("TELEFONE", tel, motivo_blacklist_sys_call(row.get("resultado")))
        ok += 1
    return ok, ign


def aplicar_blacklist_sem_interesse_remover(df: pd.DataFrame) -> tuple[int, int, int]:
    """
    Telefone + Nome na blacklist; motivo {resultado}_SysCall.
    Retorna (telefones_ok, nomes_ok, tel_ignorados).
    """
    from modulo_banco import adicionar_blacklist
    from modulo_blacklist import normalizar_valor_para_blacklist

    ok_tel = 0
    ok_nom = 0
    ign_tel = 0
    motivo_fn = motivo_blacklist_sys_call

    for _, row in df.iterrows():
        m = motivo_fn(row.get("resultado"))
        tel = normalizar_valor_para_blacklist("TELEFONE", row.get("telefone"))
        if tel:
            adicionar_blacklist("TELEFONE", tel, m)
            ok_tel += 1
        else:
            ign_tel += 1

        nom = normalizar_valor_para_blacklist("NOME", row.get("nome"))
        if nom:
            adicionar_blacklist("NOME", nom, m)
            ok_nom += 1

    return ok_tel, ok_nom, ign_tel


def gerar_relatorio_com_blacklist(caminho_csv: Path) -> tuple[bytes, dict[str, int]]:
    """
    Monta os três dataframes, grava blacklist a partir das abas 1 e 2, enriquece
    o Excel com MOTIVO (aba 1) e Resultado (aba 2, texto resultado_SysCall), e
    devolve o .xlsx em bytes com estatísticas simples.
    """
    df_aba1, df_aba2, df_aba3 = montar_dataframes(caminho_csv)

    t_ok, t_ign = aplicar_blacklist_telefone_recado(df_aba1)
    s_tel, s_nom, s_ign = aplicar_blacklist_sem_interesse_remover(df_aba2)

    df_aba1_x = _df_com_motivo_excel(df_aba1, "MOTIVO")
    df_aba2_x = _df_com_motivo_excel(df_aba2, "Resultado")

    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        df_aba1_x.to_excel(writer, sheet_name="Telefone_Recado", index=False)
        df_aba2_x.to_excel(writer, sheet_name="Sem Interesse_Remover", index=False)
        df_aba3.to_excel(writer, sheet_name="Outros Resultados", index=False)
    buffer.seek(0)

    stats = {
        "bl_telefone_recado": t_ok,
        "bl_recado_sem_tel": t_ign,
        "bl_sem_interesse_tel": s_tel,
        "bl_sem_interesse_nome": s_nom,
        "bl_sem_interesse_sem_tel": s_ign,
    }
    return buffer.read(), stats


def gerar_excel_somente(caminho_csv: Path) -> bytes:
    """Gera o Excel formatado (com colunas MOTIVO/Resultado) SEM gravar na blacklist."""
    df_aba1, df_aba2, df_aba3 = montar_dataframes(caminho_csv)

    df_aba1_x = _df_com_motivo_excel(df_aba1, "MOTIVO")
    df_aba2_x = _df_com_motivo_excel(df_aba2, "Resultado")

    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        df_aba1_x.to_excel(writer, sheet_name="Telefone_Recado", index=False)
        df_aba2_x.to_excel(writer, sheet_name="Sem Interesse_Remover", index=False)
        df_aba3.to_excel(writer, sheet_name="Outros Resultados", index=False)
    buffer.seek(0)
    return buffer.read()


def gerar_excel_bytes(caminho_csv: Path) -> bytes:
    """Compat: Excel + blacklist (use `gerar_relatorio_com_blacklist` para estatísticas)."""
    data, _stats = gerar_relatorio_com_blacklist(caminho_csv)
    return data
