"""
Enriquecimento por CSV de CPFs: busca telefones/e-mails na base e classifica blacklist.

Entrada típica: CSV com uma coluna CPF (ou CPF/CNPJ). Saída: Excel com
contatos da base e colunas Blacklist / Motivo.
"""
from __future__ import annotations

import csv
import io
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

from modulo_banco import conectar
from modulo_blacklist import (
    _normalizar_cpf_cmp,
    _normalizar_email_cmp,
    _normalizar_tel_cmp,
    motivo_marca_so_contato,
    normalizar_valor_para_blacklist,
)
from modulo_exportacao_unificada import (
    _blacklist_status_for_row,
    _celula_excel,
    _tel_variants,
)

_CHUNK = 400
MAX_CPFS = 80_000

_COLUNAS_ALIAS_CPF = {
    "cpf",
    "cpfs",
    "cpf/cnpj",
    "cpf_cnpj",
    "documento",
    "doc",
    "cpf_requerente",
}


class EnriquecimentoCpfErro(ValueError):
    """CSV inválido ou lista vazia."""


def _normalizar_header(h: str) -> str:
    h = (h or "").strip().lower()
    h = h.replace("\ufeff", "")
    h = re.sub(r"\s+", "_", h)
    return h


def _detectar_delimitador(sample: str) -> str:
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=";,|\t")
        return dialect.delimiter
    except csv.Error:
        if sample.count(";") >= sample.count(","):
            return ";"
        return ","


def ler_cpfs_csv(origem: bytes | str | Path) -> list[str]:
    """
    Lê CPFs únicos de um CSV.

    Aceita: uma coluna sem cabeçalho; ou cabeçalho CPF / CPF/CNPJ / documento.
    Se houver várias colunas e nenhuma se chamar CPF, usa a primeira.
    """
    if isinstance(origem, Path):
        raw = origem.read_bytes()
    elif isinstance(origem, bytes):
        raw = origem
    else:
        raw = origem.encode("utf-8")

    text = raw.decode("utf-8-sig", errors="replace").replace("\r\n", "\n").replace("\r", "\n")
    if not text.strip():
        raise EnriquecimentoCpfErro("CSV vazio.")

    first_lines = "\n".join(text.splitlines()[:20])
    delim = _detectar_delimitador(first_lines)
    reader = csv.reader(io.StringIO(text), delimiter=delim)
    rows = [r for r in reader if any(str(c).strip() for c in r)]
    if not rows:
        raise EnriquecimentoCpfErro("CSV sem linhas úteis.")

    headers = [_normalizar_header(c) for c in rows[0]]
    idx = 0
    start = 0
    if any(h in _COLUNAS_ALIAS_CPF for h in headers):
        for i, h in enumerate(headers):
            if h in _COLUNAS_ALIAS_CPF:
                idx = i
                break
        start = 1
    elif len(headers) > 1:
        idx = 0
        start = 1 if any(not re.search(r"\d{8,}", str(c)) for c in rows[0]) else 0

    seen: set[str] = set()
    out: list[str] = []
    for row in rows[start:]:
        if idx >= len(row):
            continue
        cpf = _normalizar_cpf_cmp(row[idx])
        if not cpf or cpf in seen:
            continue
        seen.add(cpf)
        out.append(cpf)
        if len(out) >= MAX_CPFS:
            break
    if not out:
        raise EnriquecimentoCpfErro(
            "Nenhum CPF válido encontrado. Use um CSV com coluna CPF "
            "(apenas dígitos ou máscara xxx.xxx.xxx-xx)."
        )
    return out


def _chunks(seq: list, n: int = _CHUNK) -> Iterable[list]:
    for i in range(0, len(seq), n):
        yield seq[i : i + n]


def _split_concat(valor: Any) -> list[str]:
    if valor is None:
        return []
    s = str(valor).strip()
    if not s or s.lower() in ("nan", "none"):
        return []
    parts: list[str] = []
    seen: set[str] = set()
    for p in re.split(r"[;|,]+", s):
        t = p.strip()
        if t and t not in seen:
            seen.add(t)
            parts.append(t)
    return parts


def _carregar_bl_e_motivos(
    cur,
) -> tuple[dict[str, set], dict[tuple[str, str], str], dict[tuple[str, str], Any]]:
    """Uma leitura da blacklist: conjuntos, motivo e data_inclusao."""
    cur.execute("SELECT tipo, valor, motivo, data_inclusao FROM blacklist WHERE ativo = 1")
    bl: dict[str, set] = {
        "CPF": set(),
        "NOME": set(),
        "TELEFONE": set(),
        "EMAIL": set(),
        "PROCESSO_INCIDENTE": set(),
    }
    motivo_map: dict[tuple[str, str], str] = {}
    data_map: dict[tuple[str, str], Any] = {}
    valid = frozenset(bl.keys())
    for row in cur.fetchall() or []:
        tipo = str(row.get("tipo") or "").strip().upper()
        if tipo not in valid:
            continue
        valor = normalizar_valor_para_blacklist(tipo, row.get("valor"))
        if not valor:
            continue
        bl[tipo].add(valor)
        chave = (tipo, valor)
        motivo = str(row.get("motivo") or "").strip()
        if motivo and chave not in motivo_map:
            motivo_map[chave] = motivo
        if chave not in data_map and row.get("data_inclusao") is not None:
            data_map[chave] = row.get("data_inclusao")
    return bl, motivo_map, data_map


def _in_clause(n: int) -> str:
    return ",".join(["%s"] * n)


def _consultar_processos(cur, cpfs: list[str]) -> list[dict[str, Any]]:
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
                p.data_nascimento
            FROM pessoas p
            INNER JOIN processos_juridicos pj ON pj.id_pessoa = p.id
            WHERE p.cpf IN ({ph})
            """,
            chunk,
        )
        rows.extend(cur.fetchall() or [])
    return rows


def _anexar_contatos(cur, processos: list[dict[str, Any]]) -> None:
    ids = [int(p["processo_id"]) for p in processos if p.get("processo_id") is not None]
    by_id: dict[int, dict[str, list[str]]] = {
        i: {"sms": [], "hsm": [], "emails": []} for i in ids
    }
    if not ids:
        for p in processos:
            p["telefones_sms"] = ""
            p["telefones_hsm"] = ""
            p["emails"] = ""
        return

    def _add(pid: int, kind: str, valor: str) -> None:
        if pid not in by_id or not valor:
            return
        bucket = by_id[pid][kind]
        if valor not in bucket:
            bucket.append(valor)

    for chunk in _chunks(ids):
        ph = _in_clause(len(chunk))
        cur.execute(
            f"SELECT id_processo_juridico, telefone FROM sms WHERE id_processo_juridico IN ({ph})",
            chunk,
        )
        for row in cur.fetchall() or []:
            _add(int(row["id_processo_juridico"]), "sms", str(row.get("telefone") or "").strip())
        cur.execute(
            f"SELECT id_processo_juridico, telefone_hsm FROM disparo_hsm WHERE id_processo_juridico IN ({ph})",
            chunk,
        )
        for row in cur.fetchall() or []:
            _add(int(row["id_processo_juridico"]), "hsm", str(row.get("telefone_hsm") or "").strip())
        cur.execute(
            f"SELECT id_processo_juridico, email FROM emails WHERE id_processo_juridico IN ({ph})",
            chunk,
        )
        for row in cur.fetchall() or []:
            _add(int(row["id_processo_juridico"]), "emails", str(row.get("email") or "").strip())

    for p in processos:
        ct = by_id.get(int(p["processo_id"]), {"sms": [], "hsm": [], "emails": []})
        p["telefones_sms"] = "; ".join(ct["sms"])
        p["telefones_hsm"] = "; ".join(ct["hsm"])
        p["emails"] = "; ".join(ct["emails"])


def _combinar_status(*parts: dict[str, str]) -> dict[str, str]:
    hits = [p for p in parts if (p or {}).get("Blacklist") == "Sim"]
    if not hits:
        return {
            "Blacklist": "Não",
            "Motivo_blacklist": "",
            "Tipos_blacklist": "",
            "Valores_blacklist": "",
            "Data_inclusao_blacklist": "",
        }
    motivos: list[str] = []
    tipos: list[str] = []
    valores: list[str] = []
    datas: list[str] = []
    buckets = (
        ("Motivo_blacklist", motivos),
        ("Tipos_blacklist", tipos),
        ("Valores_blacklist", valores),
        ("Data_inclusao_blacklist", datas),
    )
    for p in hits:
        for key, acc in buckets:
            for item in str(p.get(key) or "").split(";"):
                item = item.strip()
                if item and item not in acc:
                    acc.append(item)
    return {
        "Blacklist": "Sim",
        "Motivo_blacklist": "; ".join(motivos),
        "Tipos_blacklist": "; ".join(tipos),
        "Valores_blacklist": "; ".join(valores),
        "Data_inclusao_blacklist": "; ".join(datas),
    }


def _formatar_data_bl(v: Any) -> str:
    if v is None or v == "":
        return ""
    if hasattr(v, "strftime"):
        return v.strftime("%d/%m/%Y %H:%M")
    return str(v).strip()


def _data_de_hit(
    tipo: str,
    valor: str,
    data_map: dict[tuple[str, str], Any] | None,
) -> str:
    data_map = data_map or {}
    tipo_u = str(tipo or "").strip().upper()
    if tipo_u == "TELEFONE":
        for cand in _tel_variants(_normalizar_tel_cmp(valor)):
            dt = data_map.get((tipo_u, cand))
            if dt is not None:
                return _formatar_data_bl(dt)
    dt = data_map.get((tipo_u, str(valor or "").strip()))
    return _formatar_data_bl(dt) if dt is not None else ""


def _datas_de_valores(
    valores: str,
    data_map: dict[tuple[str, str], Any] | None,
) -> str:
    datas: list[str] = []
    for part in str(valores or "").split(";"):
        part = part.strip()
        if "=" not in part:
            continue
        tipo, _, valor = part.partition("=")
        dt = _data_de_hit(tipo.strip(), valor.strip(), data_map)
        if dt and dt not in datas:
            datas.append(dt)
    return "; ".join(datas)


def _eh_motivo_telefone_incorreto(motivo: str) -> bool:
    """Compat: Telefone Incorreto (e demais tags só-do-contato)."""
    return motivo_marca_so_contato(motivo)


def _mesmo_telefone(a: str, b: str) -> bool:
    da = _normalizar_tel_cmp(a)
    db = _normalizar_tel_cmp(b)
    if not da or not db:
        return False
    return bool(_tel_variants(da) & _tel_variants(db))


def _motivo_telefone(valor: str, motivo_map: dict[tuple[str, str], str] | None) -> str:
    motivo_map = motivo_map or {}
    for cand in _tel_variants(_normalizar_tel_cmp(valor)):
        m = motivo_map.get(("TELEFONE", cand))
        if m:
            return m
    return ""


def _telefones_na_blacklist(
    telefones: list[str],
    bl: dict[str, set],
    motivo_map: dict[tuple[str, str], str] | None,
) -> list[tuple[str, str]]:
    """[(telefone_normalizado, motivo), ...] entradas TELEFONE da blacklist."""
    out: list[tuple[str, str]] = []
    seen: set[str] = set()
    bl_tel = bl.get("TELEFONE", set())
    for t in telefones:
        d = _normalizar_tel_cmp(t)
        if not d:
            continue
        for v in _tel_variants(d):
            if v not in bl_tel:
                continue
            chave = d
            if chave in seen:
                break
            seen.add(chave)
            out.append((d, _motivo_telefone(v, motivo_map)))
            break
    return out


def _status_tel_hit(
    tel: str,
    motivo: str,
    data_map: dict[tuple[str, str], Any] | None = None,
) -> dict[str, str]:
    return {
        "Blacklist": "Sim",
        "Motivo_blacklist": motivo or "",
        "Tipos_blacklist": "TELEFONE",
        "Valores_blacklist": f"TELEFONE={tel}",
        "Data_inclusao_blacklist": _data_de_hit("TELEFONE", tel, data_map),
    }


def status_linha_sms(
    *,
    cpf: str,
    nome: str,
    requerente: str,
    processo: str,
    incidente: str,
    telefone: str,
    bl: dict[str, set],
    motivo_map: dict[tuple[str, str], str] | None = None,
    data_map: dict[tuple[str, str], Any] | None = None,
    telefones_caso: list[str] | None = None,
    emails_caso: list[str] | None = None,
) -> dict[str, str]:
    """
    Blacklist da linha SMS.

    Telefone Incorreto / Deixou Recado / Engano: só nesta linha, se for o número marcado.
    Qualquer outra tag: em todas as linhas do caso.
    """
    caso = classificar_blacklist_caso(
        cpf=cpf,
        nome=nome,
        requerente=requerente,
        processo=processo,
        incidente=incidente,
        telefones=[],
        emails=list(emails_caso or []),
        bl=bl,
        motivo_map=motivo_map,
        data_map=data_map,
    )
    parts: list[dict[str, str]] = [caso]
    todos = list(telefones_caso or ([telefone] if telefone else []))
    for tel_n, motivo in _telefones_na_blacklist(todos, bl, motivo_map):
        if motivo_marca_so_contato(motivo):
            if telefone and _mesmo_telefone(tel_n, telefone):
                parts.append(_status_tel_hit(tel_n, motivo, data_map))
            continue
        parts.append(_status_tel_hit(tel_n, motivo, data_map))
    return _combinar_status(*parts)


def _status_email_hit(
    email: str,
    motivo: str,
    data_map: dict[tuple[str, str], Any] | None = None,
) -> dict[str, str]:
    em = _normalizar_email_cmp(email)
    return {
        "Blacklist": "Sim",
        "Motivo_blacklist": motivo or "",
        "Tipos_blacklist": "EMAIL",
        "Valores_blacklist": f"EMAIL={em}",
        "Data_inclusao_blacklist": _data_de_hit("EMAIL", em, data_map),
    }


def status_linha_email(
    *,
    cpf: str,
    nome: str,
    requerente: str,
    processo: str,
    incidente: str,
    email: str,
    bl: dict[str, set],
    motivo_map: dict[tuple[str, str], str] | None = None,
    data_map: dict[tuple[str, str], Any] | None = None,
    telefones_caso: list[str] | None = None,
    emails_caso: list[str] | None = None,
) -> dict[str, str]:
    caso = classificar_blacklist_caso(
        cpf=cpf,
        nome=nome,
        requerente=requerente,
        processo=processo,
        incidente=incidente,
        telefones=[],
        emails=list(emails_caso or ([email] if email else [])),
        bl=bl,
        motivo_map=motivo_map,
        data_map=data_map,
    )
    parts: list[dict[str, str]] = [caso]
    for tel_n, motivo in _telefones_na_blacklist(list(telefones_caso or []), bl, motivo_map):
        if motivo_marca_so_contato(motivo):
            continue
        parts.append(_status_tel_hit(tel_n, motivo, data_map))
    # Engano (e tags soft) só na linha deste e-mail
    em_n = _normalizar_email_cmp(email)
    if em_n and em_n in bl.get("EMAIL", set()):
        motivo_em = (motivo_map or {}).get(("EMAIL", em_n), "")
        if motivo_marca_so_contato(motivo_em):
            parts.append(_status_email_hit(em_n, motivo_em, data_map))
    return _combinar_status(*parts)


def classificar_blacklist_caso(
    *,
    cpf: str,
    nome: str,
    requerente: str,
    processo: str,
    incidente: str,
    telefones: list[str],
    emails: list[str],
    bl: dict[str, set],
    motivo_map: dict[tuple[str, str], str] | None = None,
    data_map: dict[tuple[str, str], Any] | None = None,
) -> dict[str, str]:
    na, tipos, motivos, valores = _blacklist_status_for_row(
        cpf=cpf,
        nome=nome,
        requerente=requerente,
        processo=processo,
        incidente=incidente,
        telefones=telefones,
        emails=emails,
        bl=bl,
        motivo_map=motivo_map,
    )
    return {
        "Blacklist": na or "Não",
        "Motivo_blacklist": motivos or "",
        "Tipos_blacklist": tipos or "",
        "Valores_blacklist": valores or "",
        "Data_inclusao_blacklist": _datas_de_valores(valores, data_map) if na == "Sim" else "",
    }


def _linha_processo(
    proc: dict[str, Any],
    bl: dict[str, set],
    motivo_map: dict[tuple[str, str], str],
    data_map: dict[tuple[str, str], Any] | None = None,
) -> dict[str, Any]:
    cpf = _normalizar_cpf_cmp(proc.get("cpf"))
    tels = [t for t in (_normalizar_tel_cmp(x) for x in _split_concat(proc.get("telefones_sms"))) if t]
    hsm = [t for t in (_normalizar_tel_cmp(x) for x in _split_concat(proc.get("telefones_hsm"))) if t]
    emails = [e for e in _split_concat(proc.get("emails")) if e]
    tels_all = list(dict.fromkeys(tels + hsm))
    nome = str(proc.get("pessoa_nome") or "")
    requerente = str(proc.get("requerente") or "")
    processo = str(proc.get("numero_processo") or "")
    incidente = str(proc.get("numero_incidente") or "")
    bl_cols = classificar_blacklist_caso(
        cpf=cpf,
        nome=nome,
        requerente=requerente,
        processo=processo,
        incidente=incidente,
        telefones=tels_all,
        emails=emails,
        bl=bl,
        motivo_map=motivo_map,
        data_map=data_map,
    )
    sms_status = {
        tel: status_linha_sms(
            cpf=cpf,
            nome=nome,
            requerente=requerente,
            processo=processo,
            incidente=incidente,
            telefone=tel,
            bl=bl,
            motivo_map=motivo_map,
            data_map=data_map,
            telefones_caso=tels,
            emails_caso=emails,
        )
        for tel in tels
    }
    email_status = {
        em: status_linha_email(
            cpf=cpf,
            nome=nome,
            requerente=requerente,
            processo=processo,
            incidente=incidente,
            email=em,
            bl=bl,
            motivo_map=motivo_map,
            data_map=data_map,
            telefones_caso=tels,
            emails_caso=emails,
        )
        for em in emails
    }
    return {
        "Natureza": proc.get("natureza"),
        "Assunto": proc.get("assunto"),
        "Ordem": proc.get("ordem"),
        "Numero_de_Processo": proc.get("numero_processo"),
        "Numero_do_Incidente": proc.get("numero_incidente"),
        "Foro": proc.get("foro"),
        "Data_Base": proc.get("data_base"),
        "Data_Decisao": proc.get("data_decisao"),
        "Principal_Liquido": proc.get("principal_liquido"),
        "Juros_Moratorio": proc.get("juros_moratorio"),
        "Valor_Requisitado": proc.get("valor_requisitado"),
        "Calculo_Atualizado": proc.get("calculo_atualizado"),
        "Entidade_Devedora": proc.get("entidade_devedora"),
        "Advogado": proc.get("advogado"),
        "Requerente": proc.get("pessoa_nome") or proc.get("requerente"),
        "Data_de_Nascimento": proc.get("data_nascimento"),
        "CPF": cpf,
        "Processo_Codigo": proc.get("processo_codigo"),
        "Data_Preenchimento": proc.get("data_preenchimento"),
        **bl_cols,
        "_tels": tels,
        "_emails": emails,
        "_hsm": hsm,
        "_sms_status": sms_status,
        "_email_status": email_status,
    }


def _motivo_cpf_isolado(
    cpf: str,
    bl: dict[str, set],
    motivo_map: dict[tuple[str, str], str],
    data_map: dict[tuple[str, str], Any] | None = None,
) -> dict[str, str]:
    return classificar_blacklist_caso(
        cpf=cpf,
        nome="",
        requerente="",
        processo="",
        incidente="",
        telefones=[],
        emails=[],
        bl=bl,
        motivo_map=motivo_map,
        data_map=data_map,
    )


_COLS_BASE_PRINCIPAL = [
    "Natureza",
    "Assunto",
    "Ordem",
    "Numero_de_Processo",
    "Numero_do_Incidente",
    "Foro",
    "Data_Base",
    "Data_Decisao",
    "Principal_Liquido",
    "Juros_Moratorio",
    "Valor_Requisitado",
    "Calculo_Atualizado",
    "Entidade_Devedora",
    "Advogado",
    "Requerente",
    "Data_de_Nascimento",
    "CPF",
    "Processo_Codigo",
    "Data_Preenchimento",
]


def _formatar_contato_55(telefone) -> str:
    s = _normalizar_tel_cmp(telefone)
    if not s:
        return ""
    if s.startswith("55"):
        return s
    return "55" + s


def _formatar_nome_sms(requerente) -> str:
    nome = str(requerente or "").strip()
    if not nome or nome.lower() == "nan":
        return ""
    partes = nome.split()
    if not partes:
        return ""
    if len(partes) == 1:
        return partes[0].capitalize()
    if len(partes) == 2:
        return f"{partes[0].capitalize()} {partes[1].capitalize()}"
    primeira = partes[0].capitalize()
    ultima = partes[-1].capitalize()
    meio = [f"{p[0].upper()}." for p in partes[1:-1] if p]
    return " ".join([primeira] + meio + [ultima])


def _expandir_prefixo(linhas: list[dict[str, Any]], chave: str, prefixo: str) -> list[str]:
    max_n = max((len(r.get(chave) or []) for r in linhas), default=0)
    return [f"{prefixo}_{i}" for i in range(1, max_n + 1)]


def _row_base(rec: dict[str, Any]) -> dict[str, Any]:
    return {c: _celula_excel(rec.get(c)) for c in _COLS_BASE_PRINCIPAL}


def gerar_excel_enriquecimento(
    linhas: list[dict[str, Any]],
    nao_encontrados: list[dict[str, Any]],
    meta: dict[str, Any],
) -> io.BytesIO:
    """Excel no padrão da Etapa 2 (Principal / sms / Emails / Disparo_HSM), sem wrap."""
    import pandas as pd

    cols_tel = _expandir_prefixo(linhas, "_tels", "TELEFONE")
    cols_email = _expandir_prefixo(linhas, "_emails", "EMAIL")
    cols_hsm = _expandir_prefixo(linhas, "_hsm", "TELEFONE_HSM")

    princ_rows: list[dict[str, Any]] = []
    for rec in linhas:
        row = _row_base(rec)
        tels = rec.get("_tels") or []
        emails = rec.get("_emails") or []
        hsm = rec.get("_hsm") or []
        for i, col in enumerate(cols_tel, start=1):
            row[col] = tels[i - 1] if i <= len(tels) else None
        for i, col in enumerate(cols_email, start=1):
            row[col] = emails[i - 1] if i <= len(emails) else None
        for i, col in enumerate(cols_hsm, start=1):
            row[col] = hsm[i - 1] if i <= len(hsm) else None
        row["Blacklist"] = rec.get("Blacklist") or "Não"
        row["Motivo_blacklist"] = rec.get("Motivo_blacklist") or ""
        row["Data_inclusao_blacklist"] = rec.get("Data_inclusao_blacklist") or ""
        princ_rows.append(row)
    cols_princ = _COLS_BASE_PRINCIPAL + cols_tel + cols_email + cols_hsm + [
        "Blacklist",
        "Motivo_blacklist",
        "Data_inclusao_blacklist",
    ]
    df_princ = pd.DataFrame(princ_rows, columns=cols_princ)

    sms_rows: list[dict[str, Any]] = []
    for rec in linhas:
        sms_status = rec.get("_sms_status") or {}
        for tel in rec.get("_tels") or []:
            st = sms_status.get(tel) or {}
            row = _row_base(rec)
            row["TELEFONE"] = tel
            row["Contato"] = _formatar_contato_55(tel)
            row["Nome"] = _formatar_nome_sms(rec.get("Requerente"))
            row["Processo"] = rec.get("Numero_de_Processo")
            row["Blacklist"] = st.get("Blacklist") or "Não"
            row["Motivo_blacklist"] = st.get("Motivo_blacklist") or ""
            row["Data_inclusao_blacklist"] = st.get("Data_inclusao_blacklist") or ""
            sms_rows.append(row)
    cols_sms = _COLS_BASE_PRINCIPAL + [
        "TELEFONE",
        "Contato",
        "Nome",
        "Processo",
        "Blacklist",
        "Motivo_blacklist",
        "Data_inclusao_blacklist",
    ]
    df_sms = pd.DataFrame(sms_rows, columns=cols_sms)

    email_rows: list[dict[str, Any]] = []
    for rec in linhas:
        email_status = rec.get("_email_status") or {}
        for em in rec.get("_emails") or []:
            st = email_status.get(em) or {}
            row = _row_base(rec)
            row["EMAIL"] = em
            row["Blacklist"] = st.get("Blacklist") or "Não"
            row["Motivo_blacklist"] = st.get("Motivo_blacklist") or ""
            row["Data_inclusao_blacklist"] = st.get("Data_inclusao_blacklist") or ""
            email_rows.append(row)
    cols_emails = _COLS_BASE_PRINCIPAL + [
        "EMAIL",
        "Blacklist",
        "Motivo_blacklist",
        "Data_inclusao_blacklist",
    ]
    df_emails = pd.DataFrame(email_rows, columns=cols_emails)

    hsm_rows: list[dict[str, Any]] = []
    for rec in linhas:
        for tel in rec.get("_hsm") or []:
            hsm_rows.append(
                {
                    "Telefone HSM": tel,
                    "Nome": rec.get("Requerente"),
                    "Numero de Processo": rec.get("Numero_de_Processo"),
                    "Incidente": rec.get("Numero_do_Incidente"),
                }
            )
    df_hsm = pd.DataFrame(
        hsm_rows,
        columns=["Telefone HSM", "Nome", "Numero de Processo", "Incidente"],
    )

    df_nao = pd.DataFrame(
        [
            {
                "CPF": r.get("CPF"),
                "Blacklist": r.get("Blacklist") or "Não",
                "Motivo_blacklist": r.get("Motivo_blacklist") or "",
                "Data_inclusao_blacklist": r.get("Data_inclusao_blacklist") or "",
            }
            for r in nao_encontrados
        ],
        columns=["CPF", "Blacklist", "Motivo_blacklist", "Data_inclusao_blacklist"],
    )
    df_resumo = pd.DataFrame(
        [{"Métrica": k, "Valor": v} for k, v in meta.items()],
        columns=["Métrica", "Valor"],
    )

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df_princ.to_excel(writer, sheet_name="Principal", index=False)
        df_sms.to_excel(writer, sheet_name="sms", index=False)
        df_emails.to_excel(writer, sheet_name="Emails", index=False)
        if not df_hsm.empty:
            df_hsm.to_excel(writer, sheet_name="Disparo_HSM", index=False)
        if not df_nao.empty:
            df_nao.to_excel(writer, sheet_name="CPFs_nao_encontrados", index=False)
        df_resumo.to_excel(writer, sheet_name="Resumo", index=False)
    buf.seek(0)
    return buf


def enriquecer_cpfs(
    cpfs: list[str],
    *,
    log=None,
) -> tuple[io.BytesIO, dict[str, Any]]:
    """Consulta a base e devolve (buffer xlsx, meta)."""
    cpfs = [_normalizar_cpf_cmp(c) for c in cpfs]
    cpfs = [c for c in dict.fromkeys(cpfs) if c]
    if not cpfs:
        raise EnriquecimentoCpfErro("Nenhum CPF válido.")

    def _emit(msg: str) -> None:
        if log:
            log(msg)

    _emit(f"Consultando {len(cpfs)} CPF(s) na base…")
    conn = conectar()
    try:
        cur = conn.cursor(dictionary=True)
        try:
            bl, motivo_map, data_map = _carregar_bl_e_motivos(cur)
            _emit("Blacklist carregada. Buscando processos…")
            processos = _consultar_processos(cur, cpfs)
            _emit(f"{len(processos)} processo(s) encontrado(s). Buscando telefones e e-mails…")
            _anexar_contatos(cur, processos)
        finally:
            cur.close()
    finally:
        conn.close()

    linhas: list[dict[str, Any]] = []
    cpfs_encontrados: set[str] = set()
    for proc in processos:
        cpf = _normalizar_cpf_cmp(proc.get("cpf"))
        cpfs_encontrados.add(cpf)
        linhas.append(_linha_processo(proc, bl, motivo_map, data_map))

    nao_encontrados: list[dict[str, Any]] = []
    for cpf in cpfs:
        if cpf in cpfs_encontrados:
            continue
        bl_cols = _motivo_cpf_isolado(cpf, bl, motivo_map, data_map)
        nao_encontrados.append({"CPF": cpf, **bl_cols})

    n_bl = sum(1 for r in linhas if r.get("Blacklist") == "Sim")
    n_com_tel = sum(1 for r in linhas if r.get("_tels"))
    n_com_email = sum(1 for r in linhas if r.get("_emails"))
    agora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    meta = {
        "Gerado_em": agora,
        "CPFs_enviados": len(cpfs),
        "CPFs_encontrados_na_base": len(cpfs_encontrados),
        "CPFs_nao_encontrados": len(nao_encontrados),
        "Processos_enriquecidos": len(linhas),
        "Processos_na_blacklist": n_bl,
        "Processos_com_telefone": n_com_tel,
        "Processos_com_email": n_com_email,
    }
    _emit("Montando Excel…")
    buf = gerar_excel_enriquecimento(linhas, nao_encontrados, meta)
    return buf, meta
