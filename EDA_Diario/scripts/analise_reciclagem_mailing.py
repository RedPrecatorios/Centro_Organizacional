# -*- coding: utf-8 -*-
"""
Nova análise de reciclagem do mailing de ligação (EDA Diário).

Gera 2 Excel:
  1. casos NÃO blacklistados que não circulam nos mailings recentes
  2. casos da blacklist recicláveis (SEM INTERESSE ≥ 30 dias, e Deixou Recado ≥ 30d)

Não grava no MySQL. Só lê.
"""
from __future__ import annotations

import os
import re
import sys
import unicodedata
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
CENTRO = ROOT.parent
load_dotenv(CENTRO / ".env")
sys.path.insert(0, str(ROOT / "Modulos"))

from modulo_banco import conectar  # noqa: E402
from modulo_blacklist import (  # noqa: E402
    _normalizar_cpf_cmp,
    _normalizar_email_cmp,
    _normalizar_nome_cmp,
    _normalizar_tel_cmp,
    normalizar_chave_processo_incidente,
    normalizar_chave_processo_incidente_de_valor,
)

RESULTADOS = ROOT / "Resultados"
HOJE = date.today()
JANELA_MAILING_DIAS = 30
JANELA_RECICLAGEM_DIAS = 30
COOLDOWN_DIAS = 14
CORTE_MAILING = HOJE - timedelta(days=JANELA_MAILING_DIAS)
CORTE_RECICLAGEM = datetime.combine(
    HOJE - timedelta(days=JANELA_RECICLAGEM_DIAS), datetime.min.time()
)
CORTE_COOLDOWN = datetime.now() - timedelta(days=COOLDOWN_DIAS)

# Motivos que NÃO se reciclamm — bloqueio duro / já convertido / pediu para sair.
_HARD_MOTIVO_KEYS = (
    "SOLICITOU REMOCAO",
    "PEDIU PRA RETIRAR",
    "FEZ ACORDO",
    "ACORDO",
    "PF",
    "BLACKLIST",
    "INAPTO",
    "JA INCLUIDO",
    "INCLUSAO MONDAY",
    "INCLUSAO PRE CALCULO",
    "REMOVER DO MAILING",
    "BOUNCE",
    "INVESTIDOR",
    "INVESTIDORA",
    "IMPORTADO DE BACKUP DELETED PROCESSES",
)


def _fold(texto: str) -> str:
    s = unicodedata.normalize("NFD", str(texto or "").strip().upper())
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    s = " ".join(s.replace("_", " ").split())
    return s


def motivo_e_hard(motivo: str) -> bool:
    m = _fold(motivo)
    if not m:
        return False
    if m in ("ACORDO", "PF", "BLACKLIST", "INAPTO"):
        return True
    return any(k in m for k in _HARD_MOTIVO_KEYS)


def motivo_e_sem_interesse(motivo: str) -> bool:
    return "SEM INTERESSE" in _fold(motivo)


def motivo_e_deixou_recado(motivo: str) -> bool:
    return "DEIXOU RECADO" in _fold(motivo)


def tel_variants(dig: str) -> set[str]:
    d = _normalizar_tel_cmp(dig)
    if not d:
        return set()
    out = {d}
    if d.startswith("55") and len(d) >= 12:
        rest = d[2:]
        if 10 <= len(rest) <= 11:
            out.add(rest)
    else:
        out.add("55" + d)
    return {x for x in out if x}


def tel_chave(dig: str) -> str:
    """Chave estável: DDD+número BR (10/11), sem 55."""
    d = _normalizar_tel_cmp(dig)
    if not d:
        return ""
    if d.startswith("55") and len(d) >= 12:
        rest = d[2:]
        if 10 <= len(rest) <= 11:
            return rest
    return d


def tel_valido(dig: str) -> bool:
    k = tel_chave(dig)
    return 10 <= len(k) <= 11


def contato_55(dig: str) -> str:
    k = tel_chave(dig)
    return ("55" + k) if k else ""


def nome_sms(requerente) -> str:
    nome = str(requerente or "").strip()
    if not nome:
        return ""
    partes = nome.split()
    if len(partes) <= 2:
        return " ".join(p.capitalize() for p in partes)
    return " ".join(
        [partes[0].capitalize()]
        + [f"{p[0].upper()}." for p in partes[1:-1] if p]
        + [partes[-1].capitalize()]
    )


def _query(sql: str, params=None) -> pd.DataFrame:
    conn = conectar()
    cur = conn.cursor(dictionary=True)
    cur.execute(sql, params or ())
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return pd.DataFrame(rows)


def carregar_blacklist() -> pd.DataFrame:
    df = _query(
        """
        SELECT id, tipo, valor, motivo, data_inclusao, ativo
        FROM blacklist
        WHERE ativo = 1
        """
    )
    if df.empty:
        return df
    df["tipo"] = df["tipo"].astype(str).str.strip().str.upper()

    def _norm_row(t, v):
        if t == "CPF":
            return _normalizar_cpf_cmp(v)
        if t == "TELEFONE":
            return tel_chave(v)
        if t == "NOME":
            return _normalizar_nome_cmp(v)
        if t == "EMAIL":
            return _normalizar_email_cmp(v)
        if t == "PROCESSO_INCIDENTE":
            return normalizar_chave_processo_incidente_de_valor(v)
        return ""

    df["valor_norm"] = [_norm_row(t, v) for t, v in zip(df["tipo"], df["valor"])]
    df["motivo_fold"] = df["motivo"].map(lambda x: _fold(x) if x is not None else "")
    df["hard"] = df["motivo"].map(motivo_e_hard)
    df["sem_interesse"] = df["motivo"].map(motivo_e_sem_interesse)
    df["deixou_recado"] = df["motivo"].map(motivo_e_deixou_recado)
    df["data_inclusao"] = pd.to_datetime(df["data_inclusao"], errors="coerce")
    df["dias_na_blacklist"] = (datetime.now() - df["data_inclusao"]).dt.days
    return df


def indices_blacklist(df_bl: pd.DataFrame) -> dict[str, set[str]]:
    out = {
        "CPF": set(),
        "NOME": set(),
        "TELEFONE": set(),
        "EMAIL": set(),
        "PROCESSO_INCIDENTE": set(),
    }
    if df_bl.empty:
        return out
    for t, g in df_bl.groupby("tipo"):
        if t in out:
            out[t] = {v for v in g["valor_norm"].tolist() if v}
    # telefones: guardar também variantes
    tels = set()
    for v in out["TELEFONE"]:
        tels |= tel_variants(v)
        tels.add(v)
    out["TELEFONE"] = {tel_chave(x) for x in tels if tel_chave(x)}
    return out


def carregar_base() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    sms = _query(
        """
        SELECT
            s.id AS sms_id,
            s.cpf,
            s.telefone,
            s.fornecedor,
            s.primeira_aparicao,
            s.ultimo_processamento AS sms_ultimo_processamento,
            s.count_aparicoes,
            pj.id AS processo_id,
            pj.numero_processo,
            pj.numero_incidente,
            pj.requerente,
            pj.natureza,
            pj.assunto,
            pj.ordem,
            pj.foro,
            pj.data_base,
            pj.data_decisao,
            pj.principal_liquido,
            pj.juros_moratorio,
            pj.valor_requisitado,
            pj.calculo_atualizado,
            pj.entidade_devedora,
            pj.advogado,
            pj.processo_codigo,
            pj.data_preenchimento,
            pj.data_entrada,
            pj.ultimo_processamento AS processo_ultimo_processamento,
            p.nome AS pessoa_nome,
            p.ultimo_processamento AS pessoa_ultimo_processamento
        FROM sms s
        JOIN processos_juridicos pj ON pj.id = s.id_processo_juridico
        JOIN pessoas p ON p.id = pj.id_pessoa
        """
    )
    emails = _query(
        """
        SELECT e.cpf, e.email, e.id_processo_juridico
        FROM emails e
        """
    )
    hsm = _query(
        """
        SELECT h.cpf, h.telefone_hsm, h.id_processo_juridico
        FROM disparo_hsm h
        """
    )
    if not sms.empty:
        sms["tel_chave"] = sms["telefone"].map(tel_chave)
        sms["cpf_norm"] = sms["cpf"].map(_normalizar_cpf_cmp)
        sms["nome_norm"] = sms["requerente"].map(_normalizar_nome_cmp)
        sms["nome_pessoa_norm"] = sms["pessoa_nome"].map(_normalizar_nome_cmp)
        sms["pi_chave"] = [
            normalizar_chave_processo_incidente(p, i)
            for p, i in zip(sms["numero_processo"], sms["numero_incidente"])
        ]
        sms["processo_id"] = sms["processo_id"].astype(int)
    if not emails.empty:
        emails["email_norm"] = emails["email"].map(_normalizar_email_cmp)
        emails["id_processo_juridico"] = pd.to_numeric(
            emails["id_processo_juridico"], errors="coerce"
        )
    return sms, emails, hsm


def _data_do_final(path: Path) -> date | None:
    m = re.match(r"^(\d{2})-(\d{2})-(\d{4})\s+PRC\s+", path.name)
    if not m:
        return None
    d, mo, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
    try:
        return date(y, mo, d)
    except ValueError:
        return None


def carregar_mailings_recentes() -> dict:
    tels: set[str] = set()
    processos: set[str] = set()
    arquivos: list[dict] = []
    n_linhas = 0
    for path in sorted(RESULTADOS.glob("* PRC * FINAL.xlsx")):
        if "INVALIDO" in path.name:
            continue
        dt = _data_do_final(path)
        if dt is None or dt < CORTE_MAILING:
            continue
        try:
            df = pd.read_excel(
                path,
                sheet_name="sms",
                dtype=str,
                usecols=lambda c: str(c) in {
                    "TELEFONE",
                    "Contato",
                    "Numero_de_Processo",
                    "Numero_do_Incidente",
                    "Processo",
                },
            )
        except Exception as exc:
            print(f"  [aviso] não li sms de {path.name}: {exc}")
            continue
        n = len(df)
        n_linhas += n
        arquivos.append({"arquivo": path.name, "data": dt.isoformat(), "linhas_sms": n})
        col_tel = "TELEFONE" if "TELEFONE" in df.columns else None
        if col_tel is None:
            for c in df.columns:
                if str(c).upper() in ("TELEFONE", "CONTATO"):
                    col_tel = c
                    break
        if col_tel:
            for v in df[col_tel].tolist():
                k = tel_chave(v)
                if k:
                    tels.add(k)
        col_p = "Numero_de_Processo" if "Numero_de_Processo" in df.columns else None
        col_i = "Numero_do_Incidente" if "Numero_do_Incidente" in df.columns else None
        if col_p:
            for _, row in df.iterrows():
                ch = normalizar_chave_processo_incidente(
                    row.get(col_p), row.get(col_i) if col_i else ""
                )
                if ch:
                    processos.add(ch)
        print(f"  mailing {path.name}: {n} sms")
    return {
        "telefones": tels,
        "processos": processos,
        "arquivos": arquivos,
        "linhas_sms": n_linhas,
    }


def bloqueio_linha(row, bl: dict[str, set]) -> str:
    cpf = _normalizar_cpf_cmp(row.get("cpf"))
    if cpf and cpf in bl["CPF"]:
        return "CPF"
    nome = _normalizar_nome_cmp(row.get("requerente") or row.get("pessoa_nome"))
    if nome and nome in bl["NOME"]:
        return "NOME"
    ch_pi = normalizar_chave_processo_incidente(
        row.get("numero_processo"), row.get("numero_incidente")
    )
    if ch_pi and ch_pi in bl["PROCESSO_INCIDENTE"]:
        return "PROCESSO_INCIDENTE"
    tel = tel_chave(row.get("telefone"))
    if tel and (tel in bl["TELEFONE"] or any(v in bl["TELEFONE"] for v in tel_variants(tel))):
        return "TELEFONE"
    return ""


def _dt(v) -> datetime | None:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    if isinstance(v, datetime):
        return v
    try:
        return pd.to_datetime(v).to_pydatetime()
    except Exception:
        return None


def _enriquecer_linha_mailing(df: pd.DataFrame, circ_proc: set[str]) -> pd.DataFrame:
    if df.empty:
        return df
    out = pd.DataFrame(
        {
            "cpf": df["cpf_norm"],
            "requerente": df["requerente"].fillna(df["pessoa_nome"]),
            "numero_processo": df["numero_processo"],
            "numero_incidente": df["numero_incidente"],
            "TELEFONE": df["tel_chave"],
            "Contato": df["tel_chave"].map(contato_55),
            "fornecedor": df["fornecedor"],
            "natureza": df["natureza"],
            "assunto": df["assunto"],
            "ordem": df["ordem"],
            "foro": df["foro"],
            "principal_liquido": df["principal_liquido"],
            "calculo_atualizado": df["calculo_atualizado"],
            "entidade_devedora": df["entidade_devedora"],
            "advogado": df["advogado"],
            "sms_primeira_aparicao": df["primeira_aparicao"],
            "sms_ultimo_processamento": df["sms_ultimo_processamento"],
            "pessoa_ultimo_processamento": df["pessoa_ultimo_processamento"],
            "count_aparicoes_sms": df["count_aparicoes"],
            "processo_circulou_30d": df["pi_chave"].isin(circ_proc).map({True: "SIM", False: "NAO"}),
        }
    )
    out["nome_sms"] = out["requerente"].map(nome_sms)
    pdt = pd.to_datetime(out["pessoa_ultimo_processamento"], errors="coerce")
    out["em_cooldown_14d"] = (pdt >= CORTE_COOLDOWN).map({True: "SIM", False: "NAO"})
    out["dias_desde_ultimo_processamento"] = (pd.Timestamp.now() - pdt).dt.days
    return out.reset_index(drop=True)


def mask_blacklist_nao_si(
    sms: pd.DataFrame,
    bl_nao_si: dict[str, set],
    pids_bloq: set[int],
) -> pd.Series:
    """True se o CASO (processo/cpf/nome/telefone) tem blacklist que não é SEM INTERESSE."""
    return (
        sms["processo_id"].isin(pids_bloq)
        | sms["cpf_norm"].isin(bl_nao_si["CPF"])
        | sms["nome_norm"].isin(bl_nao_si["NOME"])
        | sms["nome_pessoa_norm"].isin(bl_nao_si["NOME"])
        | sms["pi_chave"].isin(bl_nao_si["PROCESSO_INCIDENTE"])
        | sms["tel_chave"].isin(bl_nao_si["TELEFONE"])
    )


def marcar_excecao_si(df: pd.DataFrame, sms_src: pd.DataFrame, bl_si: dict[str, set]) -> pd.DataFrame:
    if df.empty:
        return df
    flag = []
    for cpf, nome, nome_p, tel, pi in zip(
        sms_src["cpf_norm"],
        sms_src["nome_norm"],
        sms_src["nome_pessoa_norm"],
        sms_src["tel_chave"],
        sms_src["pi_chave"],
    ):
        hit = (
            (cpf and cpf in bl_si["CPF"])
            or (nome and nome in bl_si["NOME"])
            or (nome_p and nome_p in bl_si["NOME"])
            or (tel and tel in bl_si["TELEFONE"])
            or (pi and pi in bl_si["PROCESSO_INCIDENTE"])
        )
        flag.append("SEM INTERESSE" if hit else "")
    out = df.copy()
    out["excecao_sem_interesse"] = flag
    return out


def montar_excel1(
    sms: pd.DataFrame,
    bl_nao_si: dict[str, set],
    bl_si: dict[str, set],
    pids_bloq: set[int],
    mail: dict,
) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    circ_tel = mail["telefones"]
    circ_proc = mail["processos"]
    lens = sms["tel_chave"].astype(str).str.len()
    mask_valid = lens.between(10, 11)
    skip_tel = int((~mask_valid).sum())
    mask_bl = mask_blacklist_nao_si(sms, bl_nao_si, pids_bloq)
    skip_bl = int((mask_valid & mask_bl).sum())
    mask_circ_tel = sms["tel_chave"].isin(circ_tel)
    skip_circ_tel = int((mask_valid & ~mask_bl & mask_circ_tel).sum())
    eleg = sms[mask_valid & ~mask_bl & ~mask_circ_tel].copy()
    mask_proc = eleg["pi_chave"].isin(circ_proc)
    extra_src = eleg[mask_proc]
    fora_src = eleg[~mask_proc]
    df_extra = marcar_excecao_si(
        _enriquecer_linha_mailing(extra_src, circ_proc), extra_src, bl_si
    )
    df_fora = marcar_excecao_si(
        _enriquecer_linha_mailing(fora_src, circ_proc), fora_src, bl_si
    )
    if not df_fora.empty:
        df_fora["motivo_inclusao"] = [
            (
                "SEM INTERESSE na blacklist (exceção autorizada); telefone fora dos FINAL 30d."
                if x == "SEM INTERESSE"
                else "Telefone na base sms, sem blacklist bloqueante, sem circulação nos FINAL 30d."
            )
            for x in df_fora["excecao_sem_interesse"]
        ]
    if not df_extra.empty:
        df_extra["motivo_inclusao"] = (
            "Processo circulou no mailing dos últimos 30 dias, "
            "mas este telefone da base sms não entrou na planilha FINAL."
        )
    n_si = int((df_fora["excecao_sem_interesse"] == "SEM INTERESSE").sum()) if not df_fora.empty else 0
    stats = {
        "sms_total": len(sms),
        "skip_telefone_invalido": skip_tel,
        "skip_blacklist": skip_bl,
        "skip_ja_no_mailing_30d": skip_circ_tel,
        "fora_mailing": len(df_fora),
        "fora_mailing_sem_interesse": n_si,
        "telefone_extra": len(df_extra),
        "fora_mailing_unicos_tel": df_fora["TELEFONE"].nunique() if not df_fora.empty else 0,
        "fora_mailing_unicos_cpf": df_fora["cpf"].nunique() if not df_fora.empty else 0,
        "fora_mailing_unicos_proc": (
            df_fora.drop_duplicates(["numero_processo", "numero_incidente"]).shape[0]
            if not df_fora.empty
            else 0
        ),
        "fora_prontos_sem_cooldown": (
            int((df_fora["em_cooldown_14d"] == "NAO").sum()) if not df_fora.empty else 0
        ),
        "extra_unicos_tel": df_extra["TELEFONE"].nunique() if not df_extra.empty else 0,
        "pids_bloqueados_nao_si": len(pids_bloq),
    }
    return df_fora, df_extra, stats


def _map_emails(emails: pd.DataFrame) -> dict[int, list[str]]:
    out: dict[int, list[str]] = defaultdict(list)
    if emails.empty:
        return out
    for _, r in emails.iterrows():
        e = _normalizar_email_cmp(r.get("email"))
        pid = r.get("id_processo_juridico")
        if e and pid is not None:
            out[int(pid)].append(str(r.get("email")).strip())
    return out


def _map_hsm(hsm: pd.DataFrame) -> dict[int, list[str]]:
    out: dict[int, list[str]] = defaultdict(list)
    if hsm.empty:
        return out
    for _, r in hsm.iterrows():
        k = tel_chave(r.get("telefone_hsm"))
        pid = r.get("id_processo_juridico")
        if k and pid is not None:
            out[int(pid)].append(k)
    return out


def _indice_sms(sms: pd.DataFrame) -> dict:
    by_tel: dict[str, set[int]] = defaultdict(set)
    by_cpf: dict[str, set[int]] = defaultdict(set)
    by_nome: dict[str, set[int]] = defaultdict(set)
    by_pi: dict[str, set[int]] = defaultdict(set)
    tels_por_pid: dict[int, set[str]] = defaultdict(set)
    nome_pessoa = (
        sms["nome_pessoa_norm"] if "nome_pessoa_norm" in sms.columns else sms["nome_norm"]
    )
    for pid, tel, cpf, nome, nome_p, pi in zip(
        sms["processo_id"],
        sms["tel_chave"],
        sms["cpf_norm"],
        sms["nome_norm"],
        nome_pessoa,
        sms["pi_chave"],
    ):
        pid = int(pid)
        if tel:
            by_tel[tel].add(pid)
            tels_por_pid[pid].add(tel)
        if cpf:
            by_cpf[cpf].add(pid)
        if nome:
            by_nome[nome].add(pid)
        if nome_p:
            by_nome[nome_p].add(pid)
        if pi:
            by_pi[pi].add(pid)
    meta_pid = (
        sms.drop_duplicates("processo_id").set_index("processo_id").to_dict("index")
    )
    return {
        "tel": by_tel,
        "cpf": by_cpf,
        "nome": by_nome,
        "pi": by_pi,
        "tels_por_pid": tels_por_pid,
        "meta_pid": meta_pid,
    }


def resolver_bl_para_processos(df_bl_sub: pd.DataFrame, idx: dict) -> dict[int, list[dict]]:
    hits: dict[int, list[dict]] = defaultdict(list)
    if df_bl_sub.empty:
        return hits
    for rec in df_bl_sub.itertuples(index=False):
        t = rec.tipo
        v = rec.valor_norm
        if not v:
            continue
        if t == "TELEFONE":
            pids = set(idx["tel"].get(v, set()))
            for alt in tel_variants(v):
                pids |= idx["tel"].get(tel_chave(alt), set())
        elif t == "CPF":
            pids = set(idx["cpf"].get(v, set()))
        elif t == "NOME":
            pids = set(idx["nome"].get(v, set()))
        elif t == "PROCESSO_INCIDENTE":
            pids = set(idx["pi"].get(v, set()))
        elif t == "EMAIL":
            pids = set(idx.get("email", {}).get(v, set()))
        else:
            pids = set()
        meta = {
            "blacklist_id": int(rec.id),
            "tipo": t,
            "valor": rec.valor,
            "valor_norm": v,
            "motivo": rec.motivo,
            "data_inclusao": rec.data_inclusao,
            "dias_na_blacklist": rec.dias_na_blacklist,
        }
        for pid in pids:
            hits[pid].append(meta)
    return hits


def pids_de_blacklist(df_bl_sub: pd.DataFrame, idx: dict, emails: pd.DataFrame | None = None) -> set[int]:
    """Processos atingidos por qualquer entrada da blacklist (exceto as que não cruzam)."""
    pids = set(resolver_bl_para_processos(df_bl_sub, idx).keys())
    if emails is None or emails.empty or "email_norm" not in emails.columns:
        return pids
    by_email: dict[str, set[int]] = defaultdict(set)
    for em, pid in zip(emails["email_norm"], emails["id_processo_juridico"]):
        if em and pd.notna(pid):
            by_email[str(em)].add(int(pid))
    sub = df_bl_sub[df_bl_sub["tipo"] == "EMAIL"]
    for rec in sub.itertuples(index=False):
        if rec.valor_norm:
            pids |= by_email.get(rec.valor_norm, set())
    return pids


def auditar_aptos(df: pd.DataFrame, bl_nao_si: dict[str, set], aba: str) -> pd.DataFrame:
    """Linhas do Excel 1 que ainda batem em blacklist que NÃO é SEM INTERESSE."""
    if df is None or df.empty:
        return pd.DataFrame()
    tel = df["TELEFONE"].map(tel_chave)
    cpf = df["cpf"].map(_normalizar_cpf_cmp)
    nome = df["requerente"].map(_normalizar_nome_cmp)
    pi = [
        normalizar_chave_processo_incidente(p, i)
        for p, i in zip(df["numero_processo"], df["numero_incidente"])
    ]
    motivos = []
    for t, c, n, p in zip(tel, cpf, nome, pi):
        hits = []
        if c and c in bl_nao_si["CPF"]:
            hits.append("CPF")
        if n and n in bl_nao_si["NOME"]:
            hits.append("NOME")
        if t and t in bl_nao_si["TELEFONE"]:
            hits.append("TELEFONE")
        if p and p in bl_nao_si["PROCESSO_INCIDENTE"]:
            hits.append("PROCESSO_INCIDENTE")
        motivos.append("|".join(hits))
    mask = [bool(m) for m in motivos]
    if not any(mask):
        return pd.DataFrame()
    leak = df.loc[mask].copy()
    leak.insert(0, "aba_origem", aba)
    leak.insert(1, "hit_blacklist_nao_si", [m for m, ok in zip(motivos, mask) if ok])
    return leak


def processos_com_hard(idx: dict, df_bl: pd.DataFrame) -> set[int]:
    hard = df_bl[df_bl["hard"]]
    if hard.empty:
        return set()
    return set(resolver_bl_para_processos(hard, idx).keys())


def montar_excel2(
    df_bl: pd.DataFrame,
    sms: pd.DataFrame,
    emails: pd.DataFrame,
    hsm: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, dict]:
    print("  indexando sms...", flush=True)
    idx = _indice_sms(sms)
    meta_pid = idx["meta_pid"]
    emap = _map_emails(emails)
    hmap = _map_hsm(hsm)
    hard_pids = processos_com_hard(idx, df_bl)

    def _montar(sub: pd.DataFrame, etiqueta: str):
        hits = resolver_bl_para_processos(sub, idx)
        casos = []
        entradas = []
        sem_processo = 0
        bloqueado_hard = 0
        vistos_pid = set()
        for pid, metas in hits.items():
            if pid in hard_pids:
                bloqueado_hard += 1
                continue
            if pid in vistos_pid:
                continue
            vistos_pid.add(pid)
            prow = meta_pid.get(pid)
            if not prow:
                continue
            tels = sorted(idx["tels_por_pid"].get(pid, set()))
            casos.append(
                {
                    "etiqueta": etiqueta,
                    "cpf": _normalizar_cpf_cmp(prow.get("cpf")),
                    "requerente": prow.get("requerente") or prow.get("pessoa_nome"),
                    "numero_processo": prow.get("numero_processo"),
                    "numero_incidente": prow.get("numero_incidente"),
                    "natureza": prow.get("natureza"),
                    "assunto": prow.get("assunto"),
                    "ordem": prow.get("ordem"),
                    "principal_liquido": prow.get("principal_liquido"),
                    "calculo_atualizado": prow.get("calculo_atualizado"),
                    "entidade_devedora": prow.get("entidade_devedora"),
                    "advogado": prow.get("advogado"),
                    "telefones_sms": " | ".join(tels),
                    "contatos_55": " | ".join(contato_55(t) for t in tels),
                    "telefones_hsm": " | ".join(sorted(set(hmap.get(pid, [])))),
                    "emails": " | ".join(sorted(set(emap.get(pid, [])))),
                    "pessoa_ultimo_processamento": prow.get("pessoa_ultimo_processamento"),
                    "qtd_entradas_blacklist": len(metas),
                    "tipos_blacklist": " | ".join(sorted({m["tipo"] for m in metas})),
                    "motivos": " | ".join(
                        sorted({str(m["motivo"] or "") for m in metas})
                    ),
                    "blacklist_ids": " | ".join(str(m["blacklist_id"]) for m in metas),
                    "dias_na_blacklist_min": min(
                        int(m["dias_na_blacklist"]) if pd.notna(m["dias_na_blacklist"]) else 0
                        for m in metas
                    ),
                    "acao_sugerida": (
                        "Desativar as entradas listadas (telefone e nome SEM INTERESSE) "
                        "para o caso voltar ao mailing. Não há bloqueio duro associado."
                    ),
                }
            )
            for m in metas:
                entradas.append(
                    {
                        "etiqueta": etiqueta,
                        "blacklist_id": m["blacklist_id"],
                        "tipo": m["tipo"],
                        "valor": m["valor"],
                        "motivo": m["motivo"],
                        "data_inclusao": m["data_inclusao"],
                        "dias_na_blacklist": m["dias_na_blacklist"],
                        "cpf": _normalizar_cpf_cmp(prow.get("cpf")),
                        "requerente": prow.get("requerente") or prow.get("pessoa_nome"),
                        "numero_processo": prow.get("numero_processo"),
                        "numero_incidente": prow.get("numero_incidente"),
                    }
                )
        # entradas SEM INTERESSE sem processo encontrado na sms
        ids_usados = {e["blacklist_id"] for e in entradas}
        orfas = []
        for _, b in sub.iterrows():
            bid = int(b["id"])
            if bid in ids_usados:
                continue
            # pode ter sido pulada por hard
            orfas.append(
                {
                    "blacklist_id": bid,
                    "tipo": b["tipo"],
                    "valor": b["valor"],
                    "motivo": b["motivo"],
                    "data_inclusao": b["data_inclusao"],
                    "dias_na_blacklist": b["dias_na_blacklist"],
                    "observacao": (
                        "Não cruzou com sms/processos_juridicos "
                        "(ou o processo tem bloqueio duro associado)."
                    ),
                }
            )
            sem_processo += 1
        return (
            pd.DataFrame(casos),
            pd.DataFrame(entradas),
            pd.DataFrame(orfas),
            {
                "casos": len(casos),
                "entradas_com_processo": len(entradas),
                "entradas_sem_processo_ou_hard": sem_processo,
                "casos_pulados_hard": bloqueado_hard,
            },
        )

    si = df_bl[
        (df_bl["sem_interesse"])
        & (df_bl["data_inclusao"].notna())
        & (df_bl["data_inclusao"] <= CORTE_RECICLAGEM)
    ].copy()
    rec = df_bl[
        (df_bl["deixou_recado"])
        & (df_bl["data_inclusao"].notna())
        & (df_bl["data_inclusao"] <= CORTE_RECICLAGEM)
    ].copy()

    print(f"  SEM INTERESSE ≥30d: {len(si)} entradas")
    print(f"  Deixou Recado ≥30d: {len(rec)} entradas")
    print("  resolvendo SEM INTERESSE → processos...")
    casos_si, ent_si, orfas_si, st_si = _montar(si, "SEM INTERESSE ≥30d")
    print("  resolvendo Deixou Recado → processos...")
    casos_rec, ent_rec, orfas_rec, st_rec = _montar(rec, "Deixou Recado ≥30d")

    stats = {
        "si_entradas": len(si),
        "si_por_tipo": si.groupby("tipo").size().to_dict() if not si.empty else {},
        **{f"si_{k}": v for k, v in st_si.items()},
        "rec_entradas": len(rec),
        **{f"rec_{k}": v for k, v in st_rec.items()},
        "hard_processos": len(hard_pids),
        "orfas_si": len(orfas_si),
        "orfas_rec": len(orfas_rec),
    }
    orfas = pd.concat([orfas_si, orfas_rec], ignore_index=True) if len(orfas_si) or len(orfas_rec) else pd.DataFrame()
    return casos_si, ent_si, casos_rec, orfas, stats


def _cel(v):
    if hasattr(v, "to_pydatetime"):
        try:
            v = v.to_pydatetime()
        except Exception:
            pass
    if isinstance(v, datetime):
        return v.strftime("%d/%m/%Y %H:%M")
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    return v


def _write_df(path: Path, sheets: list[tuple[str, pd.DataFrame]]):
    with pd.ExcelWriter(path, engine="openpyxl") as xw:
        for name, df in sheets:
            out = df.copy()
            for c in out.columns:
                if "datetime" in str(out[c].dtype) or str(out[c].dtype).startswith("dbdate"):
                    out[c] = pd.to_datetime(out[c], errors="coerce").dt.strftime("%d/%m/%Y %H:%M")
            out.to_excel(xw, sheet_name=name[:31], index=False)


def main() -> int:
    print(f"Hoje={HOJE} | mailing desde {CORTE_MAILING} | reciclagem até {CORTE_RECICLAGEM.date()}", flush=True)
    print("Carregando blacklist...", flush=True)
    df_bl = carregar_blacklist()
    si_ok_mask = df_bl["sem_interesse"] & (df_bl["dias_na_blacklist"] >= JANELA_RECICLAGEM_DIAS)
    df_bl_si_ok = df_bl[si_ok_mask].copy()
    df_bl_bloqueante = df_bl[~si_ok_mask].copy()
    n_si_recente = int(
        (df_bl["sem_interesse"] & (df_bl["dias_na_blacklist"] < JANELA_RECICLAGEM_DIAS)).sum()
    )
    n_si_sem_data = int((df_bl["sem_interesse"] & df_bl["dias_na_blacklist"].isna()).sum())
    bl_nao_si = indices_blacklist(df_bl_bloqueante)
    bl_si = indices_blacklist(df_bl_si_ok)
    print(
        f"  ativas={len(df_bl)} bloqueantes={len(df_bl_bloqueante)} "
        f"SI≥{JANELA_RECICLAGEM_DIAS}d={len(df_bl_si_ok)} SI<30d={n_si_recente} SI_sem_data={n_si_sem_data} "
        f"CPF_bloq={len(bl_nao_si['CPF'])} NOME_bloq={len(bl_nao_si['NOME'])} "
        f"TEL_bloq={len(bl_nao_si['TELEFONE'])} PROC_bloq={len(bl_nao_si['PROCESSO_INCIDENTE'])}"
    )

    print("Carregando sms / emails / hsm...")
    sms, emails, hsm = carregar_base()
    print(f"  sms={len(sms)} emails={len(emails)} hsm={len(hsm)}")

    print("Lendo mailings FINAL dos últimos 30 dias...")
    mail = carregar_mailings_recentes()
    print(
        f"  arquivos={len(mail['arquivos'])} linhas_sms={mail['linhas_sms']} "
        f"tels_unicos={len(mail['telefones'])} processos={len(mail['processos'])}"
    )

    print("Indexando casos e bloqueio por blacklist (SEM INTERESSE < 30d também bloqueia)...")
    idx = _indice_sms(sms)
    pids_bloq = pids_de_blacklist(df_bl_bloqueante, idx, emails)
    print(f"  processos bloqueados (não SI ≥30d): {len(pids_bloq)}", flush=True)

    print("Classificando Excel 1...")
    df_fora, df_extra, st1 = montar_excel1(sms, bl_nao_si, bl_si, pids_bloq, mail)
    print("  ", st1)

    leak_fora = auditar_aptos(df_fora, bl_nao_si, "Podem_ir_para_mailing")
    leak_extra = auditar_aptos(df_extra, bl_nao_si, "Telefone_extra_mesmo_proc")
    leak = pd.concat([leak_fora, leak_extra], ignore_index=True) if len(leak_fora) or len(leak_extra) else pd.DataFrame()
    if not leak.empty:
        print(f"  VAZAMENTO: {len(leak)} linhas ainda batem blacklist não-SI — removendo.", flush=True)
        if not df_fora.empty:
            chave = set(zip(leak_fora.get("cpf", []), leak_fora.get("TELEFONE", []), leak_fora.get("numero_processo", []))) if not leak_fora.empty else set()
            if not leak_fora.empty:
                m = [
                    (c, t, p) not in set(
                        zip(leak_fora["cpf"], leak_fora["TELEFONE"], leak_fora["numero_processo"])
                    )
                    for c, t, p in zip(df_fora["cpf"], df_fora["TELEFONE"], df_fora["numero_processo"])
                ]
                df_fora = df_fora.loc[m].reset_index(drop=True)
        if not leak_extra.empty and not df_extra.empty:
            m = [
                (c, t, p) not in set(
                    zip(leak_extra["cpf"], leak_extra["TELEFONE"], leak_extra["numero_processo"])
                )
                for c, t, p in zip(df_extra["cpf"], df_extra["TELEFONE"], df_extra["numero_processo"])
            ]
            df_extra = df_extra.loc[m].reset_index(drop=True)
        leak_fora = auditar_aptos(df_fora, bl_nao_si, "Podem_ir_para_mailing")
        leak_extra = auditar_aptos(df_extra, bl_nao_si, "Telefone_extra_mesmo_proc")
        leak = pd.concat([leak_fora, leak_extra], ignore_index=True) if len(leak_fora) or len(leak_extra) else pd.DataFrame()
    print(f"  validação pós-filtro: vazamentos={0 if leak.empty else len(leak)}", flush=True)

    print("Classificando Excel 2...")
    casos_si, ent_si, casos_rec, orfas, st2 = montar_excel2(df_bl, sms, emails, hsm)
    print("  ", st2)

    stamp = HOJE.strftime("%Y-%m-%d")
    p1 = RESULTADOS / f"{stamp}_1_casos_para_mailing.xlsx"
    p2 = RESULTADOS / f"{stamp}_2_blacklist_reciclar.xlsx"

    resumo1 = pd.DataFrame(
        [
            {"item": "Gerado em", "valor": datetime.now().strftime("%d/%m/%Y %H:%M")},
            {"item": "Estudo anterior", "valor": "97843d50 (exportação unificada, jul/2026) + recuperação de contatos"},
            {"item": "Janela mailing (dias)", "valor": JANELA_MAILING_DIAS},
            {"item": "Cooldown EDA (dias)", "valor": COOLDOWN_DIAS},
            {"item": "sms na base", "valor": st1["sms_total"]},
            {"item": "Mailings lidos (FINAL 30d)", "valor": len(mail["arquivos"])},
            {"item": "Linhas sms nos mailings 30d", "valor": mail["linhas_sms"]},
            {"item": "Telefones únicos nos mailings 30d", "valor": len(mail["telefones"])},
            {"item": "Descartados por blacklist (inclui SEM INTERESSE <30d)", "valor": st1["skip_blacklist"]},
            {"item": "SEM INTERESSE ≥30d (exceção no mailing)", "valor": len(df_bl_si_ok)},
            {"item": "SEM INTERESSE <30d (bloqueados, não entram)", "valor": n_si_recente},
            {"item": "Processos bloqueados por blacklist não-SI", "valor": st1.get("pids_bloqueados_nao_si", "")},
            {"item": "Descartados por telefone inválido", "valor": st1["skip_telefone_invalido"]},
            {"item": "Já no mailing 30d (mesmo telefone)", "valor": st1["skip_ja_no_mailing_30d"]},
            {"item": "LINHAS — aptos para o mailing", "valor": st1["fora_mailing"]},
            {"item": "Dessas, exceção SEM INTERESSE", "valor": st1.get("fora_mailing_sem_interesse", 0)},
            {"item": "Validação: vazamentos blacklist não-SI", "valor": 0 if leak.empty else len(leak)},
            {"item": "Telefones únicos — fora do mailing", "valor": st1["fora_mailing_unicos_tel"]},
            {"item": "CPFs únicos — fora do mailing", "valor": st1["fora_mailing_unicos_cpf"]},
            {"item": "Processos únicos — fora do mailing", "valor": st1["fora_mailing_unicos_proc"]},
            {"item": "Fora do mailing e fora do cooldown 14d", "valor": st1["fora_prontos_sem_cooldown"]},
            {"item": "LINHAS — telefone extra de processo que já circula", "valor": st1["telefone_extra"]},
            {
                "item": "Critério",
                "valor": (
                    "Excel 1 = telefone na sms, válido, caso SEM blacklist bloqueante "
                    "(CPF, NOME, TELEFONE, PROCESSO+INCIDENTE ou e-mail do processo) "
                    "e telefone ausente dos FINAL 30d. "
                    "Única exceção: SEM INTERESSE com data_inclusao há 30 dias ou mais. "
                    "SEM INTERESSE dos últimos 30 dias NÃO entra."
                ),
            },
        ]
    )
    mail_df = pd.DataFrame(mail["arquivos"])

    print(f"Gravando {p1} ...")
    _write_df(
        p1,
        [
            ("Resumo", resumo1),
            ("Podem_ir_para_mailing", df_fora),
            ("Telefone_extra_mesmo_proc", df_extra),
            ("Validacao_blacklist", leak if not leak.empty else pd.DataFrame(
                [{"resultado": "OK", "vazamentos_nao_SI": 0}]
            )),
            ("Mailings_30d", mail_df),
        ],
    )

    resumo2 = pd.DataFrame(
        [
            {"item": "Gerado em", "valor": datetime.now().strftime("%d/%m/%Y %H:%M")},
            {"item": "Janela reciclagem (dias)", "valor": JANELA_RECICLAGEM_DIAS},
            {"item": "Blacklist ativas", "valor": len(df_bl)},
            {"item": "SEM INTERESSE ≥30d (entradas)", "valor": st2["si_entradas"]},
            {"item": "SEM INTERESSE por tipo", "valor": str(st2["si_por_tipo"])},
            {"item": "Casos SEM INTERESSE recicláveis (com processo na sms)", "valor": st2["si_casos"]},
            {"item": "Entradas a desativar (SEM INTERESSE com processo)", "valor": st2["si_entradas_com_processo"]},
            {"item": "SEM INTERESSE sem processo ou com bloqueio duro", "valor": st2["si_entradas_sem_processo_ou_hard"]},
            {"item": "Casos pulados por bloqueio duro (SEM INTERESSE)", "valor": st2["si_casos_pulados_hard"]},
            {"item": "Deixou Recado ≥30d (entradas)", "valor": st2["rec_entradas"]},
            {"item": "Casos Deixou Recado recicláveis", "valor": st2["rec_casos"]},
            {"item": "Processos com bloqueio duro em qualquer motivo", "valor": st2["hard_processos"]},
            {
                "item": "Não reciclar (bloqueio duro)",
                "valor": (
                    "Solicitou remoção, pediu pra retirar, Fez Acordo/Acordo, PF, Blacklist, "
                    "Inapto, Já incluído, Inclusão Monday, Inclusão Pré Cálculo, "
                    "Remover do Mailing, Bounce, Investidor, Importado de BACKUP DELETED PROCESSES."
                ),
            },
            {
                "item": "Como reciclar",
                "valor": (
                    "Desativar (ativo=0) as linhas da aba Entradas_Sem_Interesse "
                    "(telefone E nome do mesmo caso). Se só tirar o telefone e deixar o nome, "
                    "o mailing continua bloqueando a linha inteira."
                ),
            },
        ]
    )

    print(f"Gravando {p2} ...")
    _write_df(
        p2,
        [
            ("Resumo", resumo2),
            ("Casos_Sem_Interesse_30d", casos_si),
            ("Entradas_Sem_Interesse", ent_si),
            ("Casos_Deixou_Recado_30d", casos_rec),
            ("Entradas_sem_processo", orfas),
        ],
    )

    print("\n=== CONCLUÍDO ===")
    print(p1)
    print(p2)
    print(resumo1.to_string(index=False))
    print("---")
    print(resumo2.to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
