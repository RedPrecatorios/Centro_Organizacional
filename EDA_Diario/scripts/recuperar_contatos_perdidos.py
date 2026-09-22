# -*- coding: utf-8 -*-
"""
Recupera contactos que o Painel deveria ter na planilha FINAL e que se perderam
(Assertiva filtrada, HSM incompleto, cooldown a apagar linhas).

- Não grava no MySQL.
- Usa os ficheiros atuais em Entrada/ + a FINAL mais recente em Resultados/.
- Cruza também sms/emails/disparo_hsm na base (cooldown: estavam no MySQL, não na planilha).

Saída: Resultados/recuperacao_contatos_perdidos.xlsx
"""
from __future__ import annotations

import os
import sys
import tempfile
from datetime import datetime
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
CENTRO = ROOT.parent
load_dotenv(CENTRO / ".env")

sys.path.insert(0, str(ROOT / "Modulos"))

import modulo_merge as mm  # noqa: E402
from modulo_banco import conectar, carregar_blacklist  # noqa: E402
from modulo_merge import _digitos_telefone, _normalizar_cpf  # noqa: E402

ENTRADA = ROOT / "Entrada"
RESULTADOS = ROOT / "Resultados"


def _noop_execucao(**kwargs):
    return 0


def _noop_salvar_processos(df, id_execucao):
    return {}


def _silenciar_persistencia():
    mm.registrar_execucao = _noop_execucao
    mm.salvar_processos = _noop_salvar_processos
    mm.salvar_contatos = lambda *a, **k: None
    mm.salvar_disparo_hsm = lambda *a, **k: None


def _final_mais_recente() -> Path | None:
    cands = sorted(
        RESULTADOS.glob("* PRC * FINAL.xlsx"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    return cands[0] if cands else None


def _chave_tel(val) -> str:
    return _digitos_telefone(str(val or ""))


def _chave_email(val) -> str:
    s = str(val or "").strip().lower()
    return "" if s in ("", "nan", "none") else s


def _celulas_contacto(df: pd.DataFrame, prefixo: str, excluir_hsm: bool = True) -> list[str]:
    cols = []
    for c in df.columns:
        s = str(c)
        if excluir_hsm and s.startswith("TELEFONE_HSM"):
            continue
        if s.startswith(prefixo):
            cols.append(c)
    vals: list[str] = []
    for c in cols:
        for v in df[c].tolist():
            s = str(v).strip() if v is not None and not (isinstance(v, float) and pd.isna(v)) else ""
            if s and s.lower() not in ("nan", "none"):
                vals.append(s)
    return vals


def _aba_valores(path: Path, aba: str, colunas: tuple[str, ...]) -> list[str]:
    try:
        df = pd.read_excel(path, sheet_name=aba, dtype=str)
    except ValueError:
        return []
    out = []
    for col in colunas:
        if col not in df.columns:
            continue
        for v in df[col].tolist():
            s = str(v).strip() if v is not None else ""
            if s and s.lower() not in ("nan", "none"):
                out.append(s)
    return out


def _cpfs_principal(df: pd.DataFrame) -> list[str]:
    col = "CPF" if "CPF" in df.columns else None
    if col is None:
        for c in df.columns:
            if str(c).upper() == "CPF" or str(c) in ("CPF.1", "cpf.1"):
                col = c
                break
    if col is None:
        return []
    return [_normalizar_cpf(v) for v in df[col].tolist() if _normalizar_cpf(v)]


def _query_df(sql: str, params: list) -> pd.DataFrame:
    conn = conectar()
    cur = conn.cursor(dictionary=True)
    cur.execute(sql, params)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return pd.DataFrame(rows)


def _puxar_base(cpfs: set[str]) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if not cpfs:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    lista = sorted(cpfs)
    sms_parts, em_parts, hsm_parts = [], [], []
    chunk = 400
    for i in range(0, len(lista), chunk):
        bloco = lista[i : i + chunk]
        ph = ",".join(["%s"] * len(bloco))
        sms_parts.append(
            _query_df(
                f"""
                SELECT s.cpf, s.telefone, s.fornecedor, s.ultimo_processamento,
                       pj.requerente, pj.numero_processo, pj.numero_incidente
                FROM sms s
                JOIN processos_juridicos pj ON pj.id = s.id_processo_juridico
                WHERE s.cpf IN ({ph})
                """,
                bloco,
            )
        )
        em_parts.append(
            _query_df(
                f"""
                SELECT e.cpf, e.email, e.fornecedor, e.ultimo_processamento,
                       pj.requerente, pj.numero_processo, pj.numero_incidente
                FROM emails e
                JOIN processos_juridicos pj ON pj.id = e.id_processo_juridico
                WHERE e.cpf IN ({ph})
                """,
                bloco,
            )
        )
        hsm_parts.append(
            _query_df(
                f"""
                SELECT h.cpf, h.telefone_hsm, h.fornecedor, h.ultimo_processamento,
                       h.requerente, h.numero_processo, h.numero_incidente
                FROM disparo_hsm h
                WHERE h.cpf IN ({ph})
                """,
                bloco,
            )
        )
    sms = pd.concat(sms_parts, ignore_index=True) if sms_parts else pd.DataFrame()
    emails = pd.concat(em_parts, ignore_index=True) if em_parts else pd.DataFrame()
    hsm = pd.concat(hsm_parts, ignore_index=True) if hsm_parts else pd.DataFrame()
    return sms, emails, hsm


def main() -> int:
    principal = ENTRADA / "principal.xlsx"
    p2 = ENTRADA / "enriquecimento_lemitti.csv"
    p3 = ENTRADA / "enriquecimento_assertiva.csv"
    if not principal.is_file() or not p2.is_file() or not p3.is_file():
        print("Faltam ficheiros em Entrada/ (principal.xlsx, lemitti, assertiva).")
        return 1

    antigo = _final_mais_recente()
    print(f"FINAL de referência: {antigo.name if antigo else '(nenhuma)'}")

    _silenciar_persistencia()

    with tempfile.TemporaryDirectory(prefix="eda_recup_") as tmp:
        tmp_path = Path(tmp)
        inter = tmp_path / "INTERMEDIARIA.xlsx"
        csv_miss = tmp_path / "nao_encontrados.csv"
        final_ok = tmp_path / "FINAL_corrigido.xlsx"

        print("A recalcular Etapa 1 (sem gravar na base)...")
        mm.etapa1_enriquecer_com_p2(
            caminho_principal=str(principal),
            caminho_p2=str(p2),
            caminho_saida_intermediaria=str(inter),
            caminho_csv_nao_encontrados=str(csv_miss),
            modelo="prc_tjsp",
        )
        print("A recalcular Etapa 2 (sem gravar na base)...")
        mm.etapa2_enriquecer_com_p3(
            caminho_intermediaria=str(inter),
            caminho_p3=str(p3),
            caminho_saida_final=str(final_ok),
        )

        df_ok = pd.read_excel(final_ok, sheet_name="Principal", dtype=str)
        dest_final = RESULTADOS / "FINAL_recalculado_correcao.xlsx"
        dest_final.write_bytes(final_ok.read_bytes())
        print(f"Planilha recalculada: {dest_final}")

    cpfs = set(_cpfs_principal(df_ok))
    tels_ok = {_chave_tel(v) for v in _celulas_contacto(df_ok, "TELEFONE_")}
    tels_ok.discard("")
    emails_ok = {
        _chave_email(v)
        for v in _celulas_contacto(df_ok, "EMAIL_", excluir_hsm=False)
    }
    emails_ok.discard("")
    hsm_ok = {
        _chave_tel(v) for v in _celulas_contacto(df_ok, "TELEFONE_HSM_", excluir_hsm=False)
    }
    hsm_ok.discard("")

    tels_old: set[str] = set()
    emails_old: set[str] = set()
    hsm_old: set[str] = set()
    if antigo:
        df_old = pd.read_excel(antigo, sheet_name="Principal", dtype=str)
        tels_old = {_chave_tel(v) for v in _celulas_contacto(df_old, "TELEFONE_")}
        tels_old |= {_chave_tel(v) for v in _aba_valores(antigo, "sms", ("TELEFONE", "Contato"))}
        tels_old.discard("")
        emails_old = {
            _chave_email(v)
            for v in _celulas_contacto(df_old, "EMAIL_", excluir_hsm=False)
        }
        emails_old |= {_chave_email(v) for v in _aba_valores(antigo, "Emails", ("EMAIL",))}
        emails_old.discard("")
        hsm_old = {
            _chave_tel(v)
            for v in _celulas_contacto(df_old, "TELEFONE_HSM_", excluir_hsm=False)
        }
        hsm_old |= {_chave_tel(v) for v in _aba_valores(antigo, "Disparo_HSM", ("Telefone HSM",))}
        hsm_old.discard("")

    print("A ler MySQL (sms / emails / disparo_hsm)...")
    sms_db, em_db, hsm_db = _puxar_base(cpfs)

    def _linhas_perdidas_planilha(tipo: str, valores_ok: set[str], valores_old: set[str], df_src: pd.DataFrame, col_val: str, chave_fn):
        rows = []
        for _, row in df_src.iterrows():
            raw = row.get(col_val)
            k = chave_fn(raw)
            if not k or k in valores_old:
                continue
            if k not in valores_ok and tipo != "base":
                pass
            rows.append(
                {
                    "origem": tipo,
                    "cpf": row.get("CPF") or row.get("cpf"),
                    "requerente": row.get("Requerente") or row.get("requerente"),
                    "processo": row.get("Numero_de_Processo") or row.get("numero_processo"),
                    "incidente": row.get("Numero_do_Incidente") or row.get("numero_incidente"),
                    "valor": raw,
                    "chave": k,
                }
            )
        return rows

    # Explode Principal recalculada para listar cada contacto novo vs FINAL antiga
    perdidos_tel = []
    perdidos_email = []
    perdidos_hsm = []
    for _, row in df_ok.iterrows():
        meta = {
            "cpf": row.get("CPF"),
            "requerente": row.get("Requerente"),
            "processo": row.get("Numero_de_Processo"),
            "incidente": row.get("Numero_do_Incidente"),
        }
        for c in df_ok.columns:
            s = str(c)
            val = row.get(c)
            txt = str(val).strip() if val is not None and not (isinstance(val, float) and pd.isna(val)) else ""
            if not txt or txt.lower() in ("nan", "none"):
                continue
            if s.startswith("TELEFONE_HSM"):
                k = _chave_tel(txt)
                if k and k not in hsm_old:
                    perdidos_hsm.append({**meta, "coluna": s, "valor": txt, "chave": k})
            elif s.startswith("TELEFONE_"):
                k = _chave_tel(txt)
                if k and k not in tels_old:
                    perdidos_tel.append({**meta, "coluna": s, "valor": txt, "chave": k})
            elif s.startswith("EMAIL_"):
                k = _chave_email(txt)
                if k and k not in emails_old:
                    perdidos_email.append({**meta, "coluna": s, "valor": txt, "chave": k})

    # Base: números no MySQL que não estavam na FINAL antiga (mesmo CPF do lote)
    extra_base_tel = []
    if not sms_db.empty:
        for _, row in sms_db.iterrows():
            k = _chave_tel(row.get("telefone"))
            if k and k not in tels_old:
                extra_base_tel.append(
                    {
                        "cpf": row.get("cpf"),
                        "requerente": row.get("requerente"),
                        "processo": row.get("numero_processo"),
                        "incidente": row.get("numero_incidente"),
                        "valor": row.get("telefone"),
                        "fornecedor": row.get("fornecedor"),
                        "ultimo_processamento": row.get("ultimo_processamento"),
                        "chave": k,
                    }
                )
    extra_base_email = []
    if not em_db.empty:
        for _, row in em_db.iterrows():
            k = _chave_email(row.get("email"))
            if k and k not in emails_old:
                extra_base_email.append(
                    {
                        "cpf": row.get("cpf"),
                        "requerente": row.get("requerente"),
                        "processo": row.get("numero_processo"),
                        "incidente": row.get("numero_incidente"),
                        "valor": row.get("email"),
                        "fornecedor": row.get("fornecedor"),
                        "ultimo_processamento": row.get("ultimo_processamento"),
                        "chave": k,
                    }
                )
    extra_base_hsm = []
    if not hsm_db.empty:
        for _, row in hsm_db.iterrows():
            k = _chave_tel(row.get("telefone_hsm"))
            if k and k not in hsm_old:
                extra_base_hsm.append(
                    {
                        "cpf": row.get("cpf"),
                        "requerente": row.get("requerente"),
                        "processo": row.get("numero_processo"),
                        "incidente": row.get("numero_incidente"),
                        "valor": row.get("telefone_hsm"),
                        "fornecedor": row.get("fornecedor"),
                        "ultimo_processamento": row.get("ultimo_processamento"),
                        "chave": k,
                    }
                )

    pares_lote = set()
    for _, row in df_ok.iterrows():
        pares_lote.add(
            (
                _normalizar_cpf(row.get("CPF")),
                str(row.get("Numero_de_Processo") or "").strip().upper(),
                str(row.get("Numero_do_Incidente") or "").strip().upper(),
            )
        )
    bl = carregar_blacklist()
    bl_tel = bl.get("TELEFONE", set())
    bl_email = {str(x).strip().lower() for x in bl.get("EMAIL", set())}
    bl_cpf = bl.get("CPF", set())
    bl_nome = bl.get("NOME", set())

    def _tel_blacklisted(d: str) -> bool:
        if not d:
            return False
        return d in bl_tel or (d.startswith("55") and d[2:] in bl_tel) or ("55" + d) in bl_tel

    def _classificar(row: dict, tipo: str) -> str:
        cpf = _normalizar_cpf(row.get("cpf"))
        nome = " ".join(str(row.get("requerente") or "").split()).upper()
        proc = str(row.get("processo") or "").strip().upper()
        inc = str(row.get("incidente") or "").strip().upper()
        if cpf in bl_cpf or nome in bl_nome:
            return "blacklist"
        if tipo == "email":
            if _chave_email(row.get("valor")) in bl_email:
                return "blacklist"
        elif _tel_blacklisted(row.get("chave") or ""):
            return "blacklist"
        if (cpf, proc, inc) in pares_lote:
            return "mesmo_processo_historico_na_base"
        return "outro_processo_do_mesmo_cpf"

    for r in extra_base_tel:
        r["motivo"] = _classificar(r, "tel")
    for r in extra_base_email:
        r["motivo"] = _classificar(r, "email")
    for r in extra_base_hsm:
        r["motivo"] = _classificar(r, "hsm")

    tel_hist = [r for r in extra_base_tel if r["motivo"] == "mesmo_processo_historico_na_base"]
    em_hist = [r for r in extra_base_email if r["motivo"] == "mesmo_processo_historico_na_base"]
    hsm_hist = [r for r in extra_base_hsm if r["motivo"] == "mesmo_processo_historico_na_base"]

    agora = datetime.now().strftime("%d/%m/%Y %H:%M")
    resumo = pd.DataFrame(
        [
            {"item": "Gerado em", "valor": agora},
            {"item": "FINAL de referência", "valor": antigo.name if antigo else ""},
            {"item": "CPFs do lote (principal atual)", "valor": len(cpfs)},
            {"item": "Telefones na FINAL antiga (únicos)", "valor": len(tels_old)},
            {"item": "Telefones no recálculo corrigido (únicos)", "valor": len(tels_ok)},
            {"item": "Telefones que faltavam na FINAL (recálculo)", "valor": len({r["chave"] for r in perdidos_tel})},
            {"item": "Linhas telefone faltantes (recálculo)", "valor": len(perdidos_tel)},
            {"item": "E-mails que faltavam na FINAL (recálculo)", "valor": len({r["chave"] for r in perdidos_email})},
            {"item": "HSM que faltavam na FINAL (recálculo)", "valor": len({r["chave"] for r in perdidos_hsm})},
            {"item": "Telefones na base e não na FINAL antiga", "valor": len({r["chave"] for r in extra_base_tel})},
            {"item": "E-mails na base e não na FINAL antiga", "valor": len({r["chave"] for r in extra_base_email})},
            {"item": "HSM na base e não na FINAL antiga", "valor": len({r["chave"] for r in extra_base_hsm})},
            {
                "item": "Tel mesmo processo na base (fora blacklist, fora da FINAL)",
                "valor": len({r["chave"] for r in tel_hist}),
            },
            {
                "item": "Tel outro processo do mesmo CPF",
                "valor": len({r["chave"] for r in extra_base_tel if r["motivo"] == "outro_processo_do_mesmo_cpf"}),
            },
            {
                "item": "Tel na base mas blacklist (fora da FINAL de propósito)",
                "valor": len({r["chave"] for r in extra_base_tel if r["motivo"] == "blacklist"}),
            },
            {
                "item": "Nota",
                "valor": (
                    "Colunas Assertiva (WhatsApp/familiares) das execuções antigas nunca "
                    "entraram no MySQL; o lote atual foi recalculado a partir do CSV em Entrada. "
                    "Cooldown: números já estavam na base e sumiam só da planilha."
                ),
            },
        ]
    )

    saida = RESULTADOS / "recuperacao_contatos_perdidos.xlsx"
    with pd.ExcelWriter(saida, engine="openpyxl") as xw:
        resumo.to_excel(xw, sheet_name="Resumo", index=False)
        pd.DataFrame(perdidos_tel).to_excel(xw, sheet_name="Tel_faltantes_recalculo", index=False)
        pd.DataFrame(perdidos_email).to_excel(xw, sheet_name="Email_faltantes_recalculo", index=False)
        pd.DataFrame(perdidos_hsm).to_excel(xw, sheet_name="HSM_faltantes_recalculo", index=False)
        pd.DataFrame(extra_base_tel).to_excel(xw, sheet_name="Tel_na_base_fora_FINAL", index=False)
        pd.DataFrame(extra_base_email).to_excel(xw, sheet_name="Email_na_base_fora_FINAL", index=False)
        pd.DataFrame(extra_base_hsm).to_excel(xw, sheet_name="HSM_na_base_fora_FINAL", index=False)
        pd.DataFrame(tel_hist).to_excel(xw, sheet_name="Tel_historico_mesmo_proc", index=False)
        pd.DataFrame(em_hist).to_excel(xw, sheet_name="Email_historico_mesmo_proc", index=False)
        pd.DataFrame(hsm_hist).to_excel(xw, sheet_name="HSM_historico_mesmo_proc", index=False)

    print(f"\nRecuperação gravada em: {saida}")
    print(resumo.to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
