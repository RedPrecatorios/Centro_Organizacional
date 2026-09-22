"""
Etapa 0 do painel EDA: enriquece a planilha principal com telefones/e-mails
já existentes na base da plataforma e anota blacklist (motivo + data).
"""
from __future__ import annotations

import re
import unicodedata
from typing import Any

import pandas as pd

from modulo_banco import conectar, criar_banco_e_tabelas, registrar_execucao
from modulo_blacklist import (
    _normalizar_cpf_cmp,
    _normalizar_email_cmp,
    _normalizar_tel_cmp,
    motivo_marca_so_contato,
    normalizar_chave_processo_incidente,
)
from modulo_enriquecimento_cpf import (
    _anexar_contatos,
    _carregar_bl_e_motivos,
    _chunks,
    _in_clause,
    _telefones_na_blacklist,
    classificar_blacklist_caso,
)
from modulo_merge import (
    COL_ENRIQUECIDO,
    PREFIXO_EMAIL,
    PREFIXO_HSM_LEMITTI,
    PREFIXO_TELEFONE,
    _coluna_cpf_cruzamento_enriquecimento,
    _colunas_telefone_regular,
    _deduplicar,
    _deduplicar_hsm,
    _drop_colunas_enriquecimento_previas,
    _formatar_cpfs_para_excel,
    _normalizar_cpf,
    _preencher_colunas,
    _salvar_com_cores,
    gravar_modelo_planilha,
    processar_planilha_principal,
)

COL_BLACKLIST = "Blacklist"
COL_MOTIVO_BL = "Motivo_blacklist"
COL_DATA_BL = "Data_inclusao_blacklist"

_COLS_BL = (COL_BLACKLIST, COL_MOTIVO_BL, COL_DATA_BL)

# Motivos que excluem o caso de TODAS as abas (match flexível; _SysCall é ignorado).
_MOTIVOS_EXCLUIR_CASO = (
    "RETIRAR DO MAILING",
    "REMOVER DO MAILING",
    "INAPTO",
    "CEDIDO",
    "BLACKLIST",
    "IMPORTADO DE BACKUP DELETED PROCESSES",
    "SOLICITOU REMOCAO",
    "PF",
    "FEZ ACORDO",
    "ACORDO",
    "PEDIU PRA RETIRAR DUAS VEZES",
    "BOUNCE",
    "INVALIDO",
    "INVESTIDOR",
    "INVESTIDORA",
    "TESTE",
)

# Motivos de contato que só omitem o valor da aba de explosão (caso permanece).
# Telefone → aba sms; e-mail → aba Emails. Ver motivo_marca_so_contato.


def _normalizar_motivo_cmp(motivo: str) -> str:
    s = unicodedata.normalize("NFD", str(motivo or ""))
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    s = re.sub(r"[\s_]+", " ", s.upper()).strip()
    s = re.sub(r"\s*SYSCALL\s*$", "", s).strip()
    return s


def motivo_exclui_caso(motivo: str) -> bool:
    """True se algum motivo do caso deve sumir de todas as abas da planilha."""
    bruto = str(motivo or "")
    if not bruto.strip():
        return False
    for part in bruto.split(";"):
        n = _normalizar_motivo_cmp(part)
        if not n:
            continue
        for chave in _MOTIVOS_EXCLUIR_CASO:
            if n == chave or n.startswith(chave + " "):
                return True
    return False


def _motivo_omite_contato_soft(motivo: str) -> bool:
    """Telefone Incorreto / Deixou Recado / Engano → omitir só o contato na aba."""
    return motivo_marca_so_contato(motivo)


def _motivo_email_bl(
    email: str,
    motivo_map: dict[tuple[str, str], str] | None,
) -> str:
    motivo_map = motivo_map or {}
    em = _normalizar_email_cmp(email)
    if not em:
        return ""
    return motivo_map.get(("EMAIL", em), "") or ""


def _email_na_blacklist(
    email: str,
    bl: dict[str, set],
    motivo_map: dict[tuple[str, str], str] | None,
) -> tuple[str, str] | None:
    em = _normalizar_email_cmp(email)
    if not em:
        return None
    if em not in bl.get("EMAIL", set()):
        return None
    return em, _motivo_email_bl(em, motivo_map)


def _telefone_omite_sms(
    tel: str,
    bl: dict[str, set],
    motivo_map: dict[tuple[str, str], str] | None,
) -> bool:
    for _tel_n, motivo in _telefones_na_blacklist([tel], bl, motivo_map):
        if _motivo_omite_contato_soft(motivo):
            return True
    return False


def _email_omite_aba(
    email: str,
    bl: dict[str, set],
    motivo_map: dict[tuple[str, str], str] | None,
) -> bool:
    hit = _email_na_blacklist(email, bl, motivo_map)
    if not hit:
        return False
    return _motivo_omite_contato_soft(hit[1])


def filtrar_exportacao_blacklist(
    df: pd.DataFrame,
    registros_tel: list[list[tuple]],
    registros_email: list[list[tuple]],
    registros_hsm: list[list[tuple]] | None,
    *,
    bl: dict[str, set],
    motivo_map: dict[tuple[str, str], str] | None,
) -> tuple[
    pd.DataFrame,
    list[list[tuple]],
    list[list[tuple]],
    list[list[tuple]] | None,
    list[list[tuple]],
    list[list[tuple]],
    dict[str, int],
]:
    """
    Regras de exportação:
    - Motivos duros → remove o caso de todas as abas.
    - Soft (Telefone Incorreto / Deixou Recado / Engano) → mantém o caso;
      omite o telefone na aba sms e o e-mail na aba Emails.
    """
    df = df.reset_index(drop=True)
    n = len(df)
    if len(registros_tel) != n or len(registros_email) != n:
        raise ValueError("registros desalinhados do DataFrame na filtragem de blacklist.")
    if registros_hsm is not None and len(registros_hsm) != n:
        raise ValueError("registros_hsm desalinhados do DataFrame.")

    keep_idx: list[int] = []
    n_casos_excluidos = 0
    for i in range(n):
        motivo = ""
        if COL_MOTIVO_BL in df.columns:
            motivo = str(df.at[i, COL_MOTIVO_BL] or "")
        if motivo_exclui_caso(motivo):
            n_casos_excluidos += 1
            continue
        keep_idx.append(i)

    df_out = df.iloc[keep_idx].reset_index(drop=True)
    regs_t = [registros_tel[i] for i in keep_idx]
    regs_e = [registros_email[i] for i in keep_idx]
    regs_h = [registros_hsm[i] for i in keep_idx] if registros_hsm is not None else None

    regs_sms: list[list[tuple]] = []
    n_tels_omitidos_sms = 0
    for itens in regs_t:
        filtrados: list[tuple] = []
        for tel, flag in itens:
            if _telefone_omite_sms(str(tel), bl, motivo_map):
                n_tels_omitidos_sms += 1
                continue
            filtrados.append((tel, flag))
        regs_sms.append(filtrados)

    regs_email_aba: list[list[tuple]] = []
    n_emails_omitidos = 0
    for itens in regs_e:
        filtrados_e: list[tuple] = []
        for em, flag in itens:
            if _email_omite_aba(str(em), bl, motivo_map):
                n_emails_omitidos += 1
                continue
            filtrados_e.append((em, flag))
        regs_email_aba.append(filtrados_e)

    stats = {
        "casos_excluidos": n_casos_excluidos,
        "tels_incorretos_omitidos_sms": n_tels_omitidos_sms,
        "emails_omitidos_aba": n_emails_omitidos,
        "linhas_restantes": len(df_out),
    }
    return df_out, regs_t, regs_e, regs_h, regs_sms, regs_email_aba, stats


def _col_processo(df: pd.DataFrame) -> str | None:
    for c in ("Numero_de_Processo", "numero_processo", "processo_principal", "Processo"):
        if c in df.columns:
            return c
    return None


def _col_incidente(df: pd.DataFrame) -> str | None:
    for c in ("Numero_do_Incidente", "numero_incidente", "Incidente"):
        if c in df.columns:
            return c
    return None


def _col_requerente(df: pd.DataFrame) -> str | None:
    for c in ("Requerente", "requerente", "NOME", "Nome", "nome"):
        if c in df.columns:
            return c
    return None


def _consultar_processos_por_cpf(cur, cpfs: list[str]) -> list[dict[str, Any]]:
    if not cpfs:
        return []
    rows: list[dict[str, Any]] = []
    for chunk in _chunks(cpfs):
        ph = _in_clause(len(chunk))
        cur.execute(
            f"""
            SELECT
                pj.id AS processo_id,
                p.cpf,
                p.nome AS pessoa_nome,
                pj.numero_processo,
                pj.numero_incidente,
                pj.requerente
            FROM pessoas p
            INNER JOIN processos_juridicos pj ON pj.id_pessoa = p.id
            WHERE p.cpf IN ({ph})
            """,
            chunk,
        )
        rows.extend(cur.fetchall() or [])
    return rows


def _indice_contatos_base(
    processos: list[dict[str, Any]],
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, dict[str, Any]]]:
    """cpf -> [procs]; chave processo|incidente (com cpf) -> proc."""
    by_cpf: dict[str, list[dict[str, Any]]] = {}
    by_chave: dict[str, dict[str, Any]] = {}
    for p in processos:
        cpf = _normalizar_cpf_cmp(p.get("cpf"))
        if not cpf:
            continue
        by_cpf.setdefault(cpf, []).append(p)
        chave = normalizar_chave_processo_incidente(
            p.get("numero_processo"), p.get("numero_incidente")
        )
        if chave:
            by_chave[f"{cpf}|{chave}"] = p
    return by_cpf, by_chave


def _tels_emails_hsm(proc: dict[str, Any]) -> tuple[list[str], list[str], list[str]]:
    tels = [
        t
        for t in (_normalizar_tel_cmp(x) for x in str(proc.get("telefones_sms") or "").split(";"))
        if t
    ]
    hsm = [
        t
        for t in (_normalizar_tel_cmp(x) for x in str(proc.get("telefones_hsm") or "").split(";"))
        if t
    ]
    emails = [e.strip() for e in str(proc.get("emails") or "").split(";") if e.strip()]
    return tels, emails, hsm


def _contatos_para_linha(
    cpf: str,
    processo: str,
    incidente: str,
    by_cpf: dict[str, list[dict[str, Any]]],
    by_chave: dict[str, dict[str, Any]],
) -> tuple[list[str], list[str], list[str], bool]:
    """
    Retorna (tels, emails, hsm, encontrado_na_base).
    Preferência: mesmo processo+incidente; senão agrega todos os processos do CPF.
    """
    if not cpf or cpf not in by_cpf:
        return [], [], [], False
    chave = normalizar_chave_processo_incidente(processo, incidente)
    if chave:
        hit = by_chave.get(f"{cpf}|{chave}")
        if hit:
            t, e, h = _tels_emails_hsm(hit)
            return t, e, h, True
    tels: list[str] = []
    emails: list[str] = []
    hsm: list[str] = []
    for proc in by_cpf[cpf]:
        t, e, h = _tels_emails_hsm(proc)
        for x in t:
            if x not in tels:
                tels.append(x)
        for x in e:
            if x not in emails:
                emails.append(x)
        for x in h:
            if x not in hsm:
                hsm.append(x)
    return tels, emails, hsm, True


def _anotar_blacklist_df(
    df: pd.DataFrame,
    registros_tel: list[list[tuple]],
    registros_email: list[list[tuple]],
    bl: dict[str, set],
    motivo_map: dict[tuple[str, str], str],
    data_map: dict[tuple[str, str], Any],
    modelo: str | None,
) -> pd.DataFrame:
    col_cpf = _coluna_cpf_cruzamento_enriquecimento(df, modelo)
    col_proc = _col_processo(df)
    col_inc = _col_incidente(df)
    col_req = _col_requerente(df)

    blacks: list[str] = []
    motivos: list[str] = []
    datas: list[str] = []
    df = df.reset_index(drop=True)
    for pos, row in df.iterrows():
        cpf = _normalizar_cpf(row.get(col_cpf))
        nome = str(row.get(col_req) or "") if col_req else ""
        processo = str(row.get(col_proc) or "") if col_proc else ""
        incidente = str(row.get(col_inc) or "") if col_inc else ""
        tels = [t for t, _ in (registros_tel[pos] if pos < len(registros_tel) else [])]
        emails = [e for e, _ in (registros_email[pos] if pos < len(registros_email) else [])]
        st = classificar_blacklist_caso(
            cpf=cpf,
            nome=nome,
            requerente=nome,
            processo=processo,
            incidente=incidente,
            telefones=tels,
            emails=emails,
            bl=bl,
            motivo_map=motivo_map,
            data_map=data_map,
        )
        blacks.append(st.get("Blacklist") or "Não")
        motivos.append(st.get("Motivo_blacklist") or "")
        datas.append(st.get("Data_inclusao_blacklist") or "")

    df[COL_BLACKLIST] = blacks
    df[COL_MOTIVO_BL] = motivos
    df[COL_DATA_BL] = datas
    return df


def _carregar_bl_maps():
    """Carrega blacklist + motivo + data numa conexão curta."""
    conn = conectar()
    try:
        cur = conn.cursor(dictionary=True)
        try:
            return _carregar_bl_e_motivos(cur)
        finally:
            cur.close()
    finally:
        conn.close()


def etapa0_enriquecer_com_base(
    caminho_principal: str,
    caminho_saida_intermediaria: str,
    caminho_csv_nao_encontrados: str,
    modelo: str = "prc_tjsp",
) -> None:
    """
    ETAPA 0 — Cruza a planilha principal com a base (sms/emails/hsm).
    Antes do enriquecimento (somente PRC TJSP e PRC CMP), consulta API remota
    (token) e remove casos indicados.
    Saídas: INTERMEDIARIA.xlsx + CSV de CPFs não localizados na base.
    """
    print("\n[0/5] Preparando banco...")
    criar_banco_e_tabelas()

    print("\n[1/5] Processando planilha principal...")
    df_main = processar_planilha_principal(caminho_principal, modelo=modelo)
    df_main = _drop_colunas_enriquecimento_previas(df_main)
    for c in _COLS_BL:
        if c in df_main.columns:
            df_main = df_main.drop(columns=[c])

    col_cpf = _coluna_cpf_cruzamento_enriquecimento(df_main, modelo)
    if col_cpf not in df_main.columns:
        raise KeyError(
            f"Coluna de CPF inexistente: {col_cpf!r} (modelo={modelo!r}). "
            f"Colunas: {list(df_main.columns)}"
        )
    col_proc = _col_processo(df_main)
    col_inc = _col_incidente(df_main)
    col_req = _col_requerente(df_main)

    df_main = df_main.reset_index(drop=True)

    print("\n[2/5] Limpeza via API (casos a remover)...")
    from modulo_api_limpeza_casos import aplicar_limpeza_api_na_planilha

    df_main, limpeza_stats = aplicar_limpeza_api_na_planilha(
        df_main,
        col_requerente=col_req,
        col_cpf=col_cpf,
        col_processo=col_proc,
        col_incidente=col_inc,
        modelo=modelo,
        falhar_se_erro=False,
    )

    df_main["_CPF_NORM"] = df_main[col_cpf].apply(_normalizar_cpf)
    cpfs = [c for c in df_main["_CPF_NORM"].tolist() if c]
    cpfs_unicos = list(dict.fromkeys(cpfs))
    print(f"     Linhas após limpeza: {len(df_main)} | CPFs únicos: {len(cpfs_unicos)}")

    print("\n[3/5] Consultando base (telefones / e-mails / HSM)...")
    conn = conectar()
    try:
        cur = conn.cursor(dictionary=True)
        try:
            bl, motivo_map, data_map = _carregar_bl_e_motivos(cur)
            processos = _consultar_processos_por_cpf(cur, cpfs_unicos)
            print(f"     Processos na base para esses CPFs: {len(processos)}")
            _anexar_contatos(cur, processos)
        finally:
            cur.close()
    finally:
        conn.close()

    by_cpf, by_chave = _indice_contatos_base(processos)

    print("\n[4/5] Populando contatos e blacklist...")
    registros_tel: list[list[tuple[str, bool]]] = []
    registros_email: list[list[tuple[str, bool]]] = []
    registros_hsm: list[list[tuple[str, bool]]] = []
    cpfs_nao: list[dict[str, str]] = []
    vistos_nao: set[str] = set()
    encontrados = 0

    for _, row in df_main.iterrows():
        cpf = str(row["_CPF_NORM"] or "")
        processo = str(row.get(col_proc) or "") if col_proc else ""
        incidente = str(row.get(col_inc) or "") if col_inc else ""
        tels, emails, hsm, achou = _contatos_para_linha(
            cpf, processo, incidente, by_cpf, by_chave
        )
        if achou:
            encontrados += 1
            registros_tel.append(_deduplicar(tels, is_red=False))
            registros_email.append(_deduplicar(emails, is_red=False))
            registros_hsm.append(_deduplicar_hsm([(h, False) for h in hsm]))
        else:
            registros_tel.append([])
            registros_email.append([])
            registros_hsm.append([])
            if cpf and cpf not in vistos_nao:
                vistos_nao.add(cpf)
                cpfs_nao.append({"CPF": cpf})

    pd.DataFrame(cpfs_nao).to_csv(caminho_csv_nao_encontrados, index=False)

    df_main[COL_ENRIQUECIDO] = [bool(t or e) for t, e in zip(registros_tel, registros_email)]
    df_main.drop(columns=["_CPF_NORM"], inplace=True)

    df_main = _anotar_blacklist_df(
        df_main, registros_tel, registros_email, bl, motivo_map, data_map, modelo
    )

    (
        df_main,
        registros_tel,
        registros_email,
        registros_hsm,
        registros_tel_sms,
        registros_email_aba,
        bl_exp,
    ) = filtrar_exportacao_blacklist(
        df_main,
        registros_tel,
        registros_email,
        registros_hsm,
        bl=bl,
        motivo_map=motivo_map,
    )
    print(
        f"     Exportação BL: {bl_exp['casos_excluidos']} caso(s) fora de todas as abas | "
        f"{bl_exp['tels_incorretos_omitidos_sms']} tel(s) omitido(s) na aba sms | "
        f"{bl_exp['emails_omitidos_aba']} email(s) omitido(s) na aba Emails."
    )

    df_main, colunas_tel = _preencher_colunas(df_main, registros_tel, PREFIXO_TELEFONE)
    df_main, colunas_email = _preencher_colunas(df_main, registros_email, PREFIXO_EMAIL)
    df_main, colunas_hsm = _preencher_colunas(df_main, registros_hsm, PREFIXO_HSM_LEMITTI)
    df_main = _formatar_cpfs_para_excel(df_main)

    print("\n[5/5] Gravando intermediária...")
    _salvar_com_cores(
        df_main,
        registros_tel,
        colunas_tel,
        registros_email,
        colunas_email,
        caminho_saida_intermediaria,
        colunas_hsm=colunas_hsm,
        registros_hsm=registros_hsm,
        registros_tel_sms=registros_tel_sms,
        registros_email_aba=registros_email_aba,
    )
    gravar_modelo_planilha(caminho_saida_intermediaria, modelo)

    registrar_execucao(
        etapa=0,
        arquivo_principal=caminho_principal,
        total_registros=len(df_main),
        total_enriquecidos_p2=encontrados,
        total_sem_contato=len(cpfs_nao),
    )

    n_bl = int((df_main[COL_BLACKLIST] == "Sim").sum()) if COL_BLACKLIST in df_main.columns else 0
    print(f"\n[OK] Etapa 0 concluída.")
    print(f"     Linhas com CPF na base     : {encontrados}")
    print(f"     CPFs NÃO localizados       : {len(cpfs_nao)}")
    print(f"     Linhas na blacklist        : {n_bl}")
    print(f"     Casos excluídos (export)   : {bl_exp['casos_excluidos']}")
    print(f"     Removidos via API limpeza  : {limpeza_stats.get('linhas_removidas', 0)}")
    print(f"     Planilha intermediária     : {caminho_saida_intermediaria}")
    print(f"     CSV não encontrados        : {caminho_csv_nao_encontrados}")
    print(f"\n  >> Baixe o CSV, consulte Lemitti/Assertiva e rode as Etapas 1 e 2.")


def preservar_colunas_blacklist(df: pd.DataFrame) -> dict[str, pd.Series]:
    """Guarda colunas de blacklist antes de reconstruir contatos."""
    out = {}
    for c in _COLS_BL:
        if c in df.columns:
            out[c] = df[c].copy()
    return out


def restaurar_colunas_blacklist(df: pd.DataFrame, guardadas: dict[str, pd.Series]) -> pd.DataFrame:
    for c, s in guardadas.items():
        if len(s) == len(df):
            df[c] = s.values
        elif c not in df.columns:
            df[c] = ""
    return df
