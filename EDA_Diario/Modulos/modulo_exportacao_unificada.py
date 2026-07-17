"""
Exportação unificada: pesquisa a partir de uma tabela/coluna/valor e devolve
uma planilha com pessoa + processo + telefones + e-mails + status blacklist.

Fallback via request_audit fica sempre ligado (igualdade normalizada).
"""
from __future__ import annotations

import io
import json
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Iterable

from modulo_banco import carregar_blacklist, conectar
from modulo_blacklist import (
    _normalizar_cpf_cmp,
    _normalizar_email_cmp,
    _normalizar_nome_cmp,
    _normalizar_tel_cmp,
    normalizar_chave_processo_incidente,
    normalizar_chave_processo_incidente_de_valor,
    normalizar_valor_para_blacklist,
)

# ── Schema da UI ──────────────────────────────────────────────────────────────

ENTRY_SCHEMA: dict[str, dict[str, Any]] = {
    "blacklist": {
        "label": "blacklist",
        "columns": [
            {"key": "motivo", "label": "motivo"},
            {"key": "tipo", "label": "tipo"},
            {"key": "valor", "label": "valor"},
            {"key": "ativo", "label": "ativo"},
        ],
    },
    "processos_juridicos": {
        "label": "processos_juridicos",
        "columns": [
            {"key": "requerente", "label": "requerente (Nome)"},
            {"key": "numero_processo", "label": "numero_processo"},
            {"key": "numero_incidente", "label": "numero_incidente"},
            {"key": "cpf", "label": "cpf"},
        ],
    },
    "emails": {
        "label": "emails",
        "columns": [
            {"key": "email", "label": "email"},
            {"key": "cpf", "label": "cpf"},
        ],
    },
    "sms": {
        "label": "sms",
        "columns": [
            {"key": "telefone", "label": "telefone"},
            {"key": "cpf", "label": "cpf"},
        ],
    },
    "pessoas": {
        "label": "pessoas",
        "columns": [
            {"key": "cpf", "label": "cpf"},
            {"key": "nome", "label": "nome"},
        ],
    },
}

_BLACKLIST_TIPOS = frozenset(
    {"CPF", "NOME", "TELEFONE", "EMAIL", "PROCESSO_INCIDENTE"}
)

_BATCH = 800


@dataclass
class ResolveResult:
    processo_ids: set[int] = field(default_factory=set)
    # ids resolvidos (também) via request_audit
    via_fallback: set[int] = field(default_factory=set)
    hits_entrada: int = 0
    avisos: list[str] = field(default_factory=list)


# ══════════════════════════════════════════════════════════════════════════════
# Helpers
# ══════════════════════════════════════════════════════════════════════════════


def _chunks(seq: list[Any], size: int = _BATCH) -> Iterable[list[Any]]:
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


def _tel_variants(dig: str) -> set[str]:
    out = {dig} if dig else set()
    if not dig:
        return out
    if dig.startswith("55") and len(dig) > 11:
        out.add(dig[2:])
    else:
        out.add("55" + dig)
    return {x for x in out if x}


def _parse_audit_custom_data(payload: Any) -> dict[str, str]:
    """Extrai CPF / processo / incidente do custom_data do request_audit."""
    out: dict[str, str] = {}
    if payload is None:
        return out
    data = payload
    if isinstance(payload, (bytes, bytearray)):
        try:
            data = json.loads(payload.decode("utf-8", errors="ignore"))
        except Exception:
            return out
    elif isinstance(payload, str):
        try:
            data = json.loads(payload)
        except Exception:
            data = payload

    custom = data.get("custom_data") if isinstance(data, dict) else data
    if isinstance(custom, str):
        try:
            custom = json.loads(custom)
        except Exception:
            custom = [custom]

    items: list[str] = []
    if isinstance(custom, list):
        items = [str(x) for x in custom]
    elif isinstance(custom, dict):
        items = [f"{k}: {v}" for k, v in custom.items()]
    elif custom is not None:
        items = [str(custom)]

    key_map = {
        "cpf": "cpf",
        "nome": "nome",
        "requerente": "nome",
        "telefone": "telefone",
        "processo": "processo",
        "processo_principal": "processo",
        "numero do cumprimento": "processo",
        "numero_do_cumprimento": "processo",
        "numero do incidente": "incidente",
        "numero_do_incidente": "incidente",
        "incidente": "incidente",
    }
    for item in items:
        if ":" not in item:
            continue
        k, _, v = item.partition(":")
        k_norm = re.sub(r"\s+", " ", k.strip().lower())
        v = v.strip()
        mapped = key_map.get(k_norm)
        if mapped and v and mapped not in out:
            out[mapped] = v
    return out


def listar_motivos_blacklist() -> list[str]:
    conn = conectar()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT DISTINCT motivo
        FROM blacklist
        WHERE motivo IS NOT NULL AND TRIM(motivo) <> ''
        ORDER BY motivo
        """
    )
    rows = [str(r[0]) for r in cur.fetchall()]
    cur.close()
    conn.close()
    return rows


# ══════════════════════════════════════════════════════════════════════════════
# Resolução → ids de processos_juridicos
# ══════════════════════════════════════════════════════════════════════════════


def _fetch_pj_ids_by_cpf(cur, cpfs: set[str], into: set[int]) -> None:
    vals = [c for c in cpfs if c]
    for batch in _chunks(vals):
        ph = ",".join(["%s"] * len(batch))
        cur.execute(
            f"SELECT id FROM processos_juridicos WHERE cpf IN ({ph})",
            batch,
        )
        into.update(int(r[0]) for r in cur.fetchall())


def _fetch_pj_ids_by_pessoa_cpf(cur, cpfs: set[str], into: set[int]) -> None:
    vals = [c for c in cpfs if c]
    for batch in _chunks(vals):
        ph = ",".join(["%s"] * len(batch))
        cur.execute(
            f"""
            SELECT pj.id
            FROM processos_juridicos pj
            JOIN pessoas p ON p.id = pj.id_pessoa
            WHERE p.cpf IN ({ph})
            """,
            batch,
        )
        into.update(int(r[0]) for r in cur.fetchall())


def _fetch_pj_ids_by_requerente(cur, nomes: set[str], into: set[int]) -> None:
    vals = [n for n in nomes if n]
    for batch in _chunks(vals):
        ph = ",".join(["%s"] * len(batch))
        cur.execute(
            f"""
            SELECT id FROM processos_juridicos
            WHERE UPPER(TRIM(requerente)) IN ({ph})
            """,
            batch,
        )
        into.update(int(r[0]) for r in cur.fetchall())


def _fetch_pj_ids_by_proc_inc(
    cur, pares: set[tuple[str, str]], into: set[int]
) -> None:
    items = list(pares)
    for batch in _chunks(items):
        conds = []
        params: list[Any] = []
        for proc, inc in batch:
            conds.append("(numero_processo = %s AND numero_incidente = %s)")
            params.extend([proc, inc])
        cur.execute(
            f"SELECT id FROM processos_juridicos WHERE {' OR '.join(conds)}",
            params,
        )
        into.update(int(r[0]) for r in cur.fetchall())


def _rows_as_dicts(cur) -> list[dict]:
    cols = [d[0] for d in (cur.description or [])]
    return [dict(zip(cols, row)) for row in cur.fetchall()]

def _fallback_audit_by_telefones(
    cur, telefones: set[str], into: set[int], via_fb: set[int]
) -> None:
    if not telefones:
        return
    variants: set[str] = set()
    for t in telefones:
        variants |= _tel_variants(_normalizar_tel_cmp(t))
    vals = list(variants)
    before = set(into)
    cpfs: set[str] = set()
    pares: set[tuple[str, str]] = set()

    for batch in _chunks(vals):
        ph = ",".join(["%s"] * len(batch))
        cur.execute(
            f"""
            SELECT credor_cpf, credor_telefone, payload_json
            FROM request_audit
            WHERE credor_telefone IN ({ph})
            """,
            batch,
        )
        for cpf, _tel, payload in cur.fetchall():
            cpf_n = _normalizar_cpf_cmp(cpf)
            if cpf_n:
                cpfs.add(cpf_n)
            parsed = _parse_audit_custom_data(payload)
            if not cpf_n and parsed.get("cpf"):
                cpf_n = _normalizar_cpf_cmp(parsed["cpf"])
                if cpf_n:
                    cpfs.add(cpf_n)
            proc = parsed.get("processo") or ""
            inc = parsed.get("incidente") or ""
            if proc:
                chave = normalizar_chave_processo_incidente(proc, inc)
                if chave:
                    p, _, i = chave.partition("|")
                    pares.add((p, i))

    tmp: set[int] = set()
    _fetch_pj_ids_by_pessoa_cpf(cur, cpfs, tmp)
    _fetch_pj_ids_by_cpf(cur, cpfs, tmp)
    if pares:
        # numero_processo guardado com formatação original — tentar chave e bruto
        raw_pares = set(pares)
        for proc, inc in list(pares):
            raw_pares.add((proc, inc))
        _fetch_pj_ids_by_proc_inc(cur, raw_pares, tmp)

    novos = tmp - before
    into.update(tmp)
    via_fb.update(novos)


def _fallback_audit_by_nomes(
    cur, nomes: set[str], into: set[int], via_fb: set[int]
) -> None:
    if not nomes:
        return
    vals = [n for n in nomes if n]
    before = set(into)
    cpfs: set[str] = set()
    pares: set[tuple[str, str]] = set()

    for batch in _chunks(vals):
        ph = ",".join(["%s"] * len(batch))
        cur.execute(
            f"""
            SELECT credor_cpf, credor_nome, payload_json
            FROM request_audit
            WHERE UPPER(TRIM(credor_nome)) IN ({ph})
            """,
            batch,
        )
        for cpf, _nome, payload in cur.fetchall():
            cpf_n = _normalizar_cpf_cmp(cpf)
            if cpf_n:
                cpfs.add(cpf_n)
            parsed = _parse_audit_custom_data(payload)
            if not cpf_n and parsed.get("cpf"):
                cpf_n = _normalizar_cpf_cmp(parsed["cpf"])
                if cpf_n:
                    cpfs.add(cpf_n)
            proc = parsed.get("processo") or ""
            inc = parsed.get("incidente") or ""
            if proc:
                chave = normalizar_chave_processo_incidente(proc, inc)
                if chave:
                    p, _, i = chave.partition("|")
                    pares.add((p, i))

    tmp: set[int] = set()
    _fetch_pj_ids_by_pessoa_cpf(cur, cpfs, tmp)
    _fetch_pj_ids_by_cpf(cur, cpfs, tmp)
    if pares:
        _fetch_pj_ids_by_proc_inc(cur, pares, tmp)

    novos = tmp - before
    into.update(tmp)
    via_fb.update(novos)


def _fallback_audit_by_cpfs(
    cur, cpfs: set[str], into: set[int], via_fb: set[int]
) -> None:
    if not cpfs:
        return
    vals = [c for c in cpfs if c]
    before = set(into)
    pares: set[tuple[str, str]] = set()
    found_cpfs: set[str] = set()

    for batch in _chunks(vals):
        ph = ",".join(["%s"] * len(batch))
        cur.execute(
            f"""
            SELECT credor_cpf, payload_json
            FROM request_audit
            WHERE credor_cpf IN ({ph})
            """,
            batch,
        )
        for cpf, payload in cur.fetchall():
            cpf_n = _normalizar_cpf_cmp(cpf)
            if cpf_n:
                found_cpfs.add(cpf_n)
            parsed = _parse_audit_custom_data(payload)
            proc = parsed.get("processo") or ""
            inc = parsed.get("incidente") or ""
            if proc:
                chave = normalizar_chave_processo_incidente(proc, inc)
                if chave:
                    p, _, i = chave.partition("|")
                    pares.add((p, i))

    tmp: set[int] = set()
    _fetch_pj_ids_by_pessoa_cpf(cur, found_cpfs or set(vals), tmp)
    _fetch_pj_ids_by_cpf(cur, found_cpfs or set(vals), tmp)
    if pares:
        _fetch_pj_ids_by_proc_inc(cur, pares, tmp)

    novos = tmp - before
    into.update(tmp)
    via_fb.update(novos)


def _resolve_telefones(
    cur, telefones: set[str], into: set[int], via_fb: set[int]
) -> None:
    variants: set[str] = set()
    for t in telefones:
        variants |= _tel_variants(_normalizar_tel_cmp(t))
    if not variants:
        return
    vals = list(variants)
    before = set(into)
    for batch in _chunks(vals):
        ph = ",".join(["%s"] * len(batch))
        cur.execute(
            f"SELECT DISTINCT id_processo_juridico FROM sms WHERE telefone IN ({ph})",
            batch,
        )
        into.update(int(r[0]) for r in cur.fetchall())
        cur.execute(
            f"""
            SELECT DISTINCT id_processo_juridico
            FROM disparo_hsm
            WHERE telefone_hsm IN ({ph})
            """,
            batch,
        )
        into.update(int(r[0]) for r in cur.fetchall())
    # Fallback sempre
    ainda = variants  # tenta todos; fallback só acrescenta novos
    _fallback_audit_by_telefones(cur, ainda, into, via_fb)
    # se não cresceu pelo principal mas fallback sim, via_fb já marcado
    _ = before


def _resolve_emails(cur, emails: set[str], into: set[int], via_fb: set[int]) -> None:
    vals = [_normalizar_email_cmp(e) for e in emails]
    vals = [e for e in vals if e]
    if not vals:
        return
    # DB guarda e-mail original; comparar em UPPER
    for batch in _chunks(vals):
        ph = ",".join(["%s"] * len(batch))
        cur.execute(
            f"""
            SELECT DISTINCT id_processo_juridico
            FROM emails
            WHERE UPPER(email) IN ({ph})
            """,
            batch,
        )
        into.update(int(r[0]) for r in cur.fetchall())
    # e-mail quase não está no audit; fallback por CPF se existir em blacklist path
    # (nada extra aqui)


def _resolve_nomes(
    cur, nomes: set[str], into: set[int], via_fb: set[int]
) -> None:
    vals = {_normalizar_nome_cmp(n) for n in nomes}
    vals.discard("")
    if not vals:
        return
    before = set(into)
    _fetch_pj_ids_by_requerente(cur, vals, into)
    # pessoas.nome → processos
    for batch in _chunks(list(vals)):
        ph = ",".join(["%s"] * len(batch))
        cur.execute(
            f"""
            SELECT pj.id
            FROM pessoas p
            JOIN processos_juridicos pj ON pj.id_pessoa = p.id
            WHERE UPPER(TRIM(p.nome)) IN ({ph})
            """,
            batch,
        )
        into.update(int(r[0]) for r in cur.fetchall())
    _fallback_audit_by_nomes(cur, vals, into, via_fb)
    _ = before


def _resolve_cpfs(cur, cpfs: set[str], into: set[int], via_fb: set[int]) -> None:
    vals = {_normalizar_cpf_cmp(c) for c in cpfs}
    vals.discard("")
    if not vals:
        return
    _fetch_pj_ids_by_pessoa_cpf(cur, vals, into)
    _fetch_pj_ids_by_cpf(cur, vals, into)
    _fallback_audit_by_cpfs(cur, vals, into, via_fb)


def _resolve_processo_incidente(
    cur, chaves: set[str], into: set[int], via_fb: set[int]
) -> None:
    pares: set[tuple[str, str]] = set()
    for chave in chaves:
        norm = normalizar_chave_processo_incidente_de_valor(chave)
        if not norm:
            continue
        p, _, i = norm.partition("|")
        pares.add((p, i))
    if not pares:
        return
    before = set(into)
    _fetch_pj_ids_by_proc_inc(cur, pares, into)
    # fallback: procura audit por processo no custom_data é caro; se não achou,
    # tenta credor via? skip if already found
    missing = {f"{p}|{i}" for p, i in pares}
    if into - before:
        # remove found from missing roughly
        pass
    # Para os não encontrados, busca no audit por LIKE no payload é frágil;
    # tenta match de numero_processo só (sem incidente) se incidente vazio.
    so_proc = {p for p, i in pares if not i}
    if so_proc:
        for batch in _chunks(list(so_proc)):
            ph = ",".join(["%s"] * len(batch))
            cur.execute(
                f"""
                SELECT id FROM processos_juridicos
                WHERE numero_processo IN ({ph})
                """,
                batch,
            )
            into.update(int(r[0]) for r in cur.fetchall())
    _ = missing
    _ = via_fb


def _resolve_blacklist_rows(
    cur, rows: list[dict], into: set[int], via_fb: set[int]
) -> None:
    by_tipo: dict[str, set[str]] = {t: set() for t in _BLACKLIST_TIPOS}
    for row in rows:
        tipo = str(row.get("tipo") or "").strip().upper()
        if tipo not in by_tipo:
            continue
        valor = normalizar_valor_para_blacklist(tipo, row.get("valor"))
        if valor:
            by_tipo[tipo].add(valor)

    _resolve_cpfs(cur, by_tipo["CPF"], into, via_fb)
    _resolve_nomes(cur, by_tipo["NOME"], into, via_fb)
    _resolve_telefones(cur, by_tipo["TELEFONE"], into, via_fb)
    _resolve_emails(cur, by_tipo["EMAIL"], into, via_fb)
    _resolve_processo_incidente(cur, by_tipo["PROCESSO_INCIDENTE"], into, via_fb)


def resolver_pesquisa(tabela: str, coluna: str, valor: str) -> ResolveResult:
    tabela = (tabela or "").strip()
    coluna = (coluna or "").strip()
    valor_raw = valor if valor is not None else ""

    if tabela not in ENTRY_SCHEMA:
        raise ValueError(f"Tabela de entrada inválida: {tabela}")
    colunas_ok = {c["key"] for c in ENTRY_SCHEMA[tabela]["columns"]}
    if coluna not in colunas_ok:
        raise ValueError(f"Coluna inválida para {tabela}: {coluna}")

    valor_str = str(valor_raw).strip()
    if valor_str == "":
        raise ValueError("Informe um valor para pesquisar.")

    result = ResolveResult()
    conn = conectar()
    cur = conn.cursor()  # tuplas — helpers de id usam r[0]

    try:
        if tabela == "blacklist":
            if coluna == "motivo":
                cur.execute(
                    """
                    SELECT id, tipo, valor, motivo, ativo
                    FROM blacklist
                    WHERE motivo = %s
                    """,
                    (valor_str,),
                )
                rows = _rows_as_dicts(cur)
            elif coluna == "tipo":
                tipo_n = valor_str.strip().upper()
                if tipo_n not in _BLACKLIST_TIPOS:
                    raise ValueError(
                        "tipo deve ser CPF, NOME, TELEFONE, EMAIL ou PROCESSO_INCIDENTE"
                    )
                cur.execute(
                    """
                    SELECT id, tipo, valor, motivo, ativo
                    FROM blacklist
                    WHERE tipo = %s
                    """,
                    (tipo_n,),
                )
                rows = _rows_as_dicts(cur)
            elif coluna == "valor":
                matched: list[dict] = []
                for tipo in sorted(_BLACKLIST_TIPOS):
                    alvo = normalizar_valor_para_blacklist(tipo, valor_str)
                    if not alvo:
                        continue
                    if tipo == "TELEFONE":
                        variants = list(_tel_variants(alvo))
                        ph = ",".join(["%s"] * len(variants))
                        cur.execute(
                            f"""
                            SELECT id, tipo, valor, motivo, ativo
                            FROM blacklist
                            WHERE tipo = 'TELEFONE' AND valor IN ({ph})
                            """,
                            variants,
                        )
                    elif tipo in ("CPF", "EMAIL", "NOME", "PROCESSO_INCIDENTE"):
                        cur.execute(
                            """
                            SELECT id, tipo, valor, motivo, ativo
                            FROM blacklist
                            WHERE tipo = %s AND valor = %s
                            """,
                            (tipo, alvo),
                        )
                    else:
                        continue
                    matched.extend(_rows_as_dicts(cur))
                by_id = {int(r["id"]): r for r in matched}
                matched = list(by_id.values())
                result.hits_entrada = len(matched)
                _resolve_blacklist_rows(
                    cur, matched, result.processo_ids, result.via_fallback
                )
                return result
            elif coluna == "ativo":
                ativo = (
                    1
                    if str(valor_str).strip() in ("1", "true", "True", "sim", "SIM")
                    else 0
                )
                cur.execute(
                    """
                    SELECT id, tipo, valor, motivo, ativo
                    FROM blacklist
                    WHERE ativo = %s
                    """,
                    (ativo,),
                )
                rows = _rows_as_dicts(cur)
            else:
                raise ValueError(coluna)

            result.hits_entrada = len(rows)
            if not rows:
                result.avisos.append("Nenhuma entrada na blacklist para o filtro.")
                return result
            _resolve_blacklist_rows(
                cur, rows, result.processo_ids, result.via_fallback
            )
            return result

        if tabela == "processos_juridicos":
            if coluna == "requerente":
                nome = _normalizar_nome_cmp(valor_str)
                cur.execute(
                    """
                    SELECT id FROM processos_juridicos
                    WHERE UPPER(TRIM(requerente)) = %s
                    """,
                    (nome,),
                )
                ids = {int(r[0]) for r in cur.fetchall()}
                result.hits_entrada = len(ids)
                result.processo_ids |= ids
                if not ids:
                    _fallback_audit_by_nomes(
                        cur, {nome}, result.processo_ids, result.via_fallback
                    )
            elif coluna == "numero_processo":
                proc = " ".join(valor_str.split()).upper()
                cur.execute(
                    """
                    SELECT id FROM processos_juridicos
                    WHERE UPPER(TRIM(numero_processo)) = %s
                    """,
                    (proc,),
                )
                ids = {int(r[0]) for r in cur.fetchall()}
                result.hits_entrada = len(ids)
                result.processo_ids |= ids
            elif coluna == "numero_incidente":
                inc = " ".join(valor_str.split()).upper()
                cur.execute(
                    """
                    SELECT id FROM processos_juridicos
                    WHERE UPPER(TRIM(numero_incidente)) = %s
                    """,
                    (inc,),
                )
                ids = {int(r[0]) for r in cur.fetchall()}
                result.hits_entrada = len(ids)
                result.processo_ids |= ids
            elif coluna == "cpf":
                cpf = _normalizar_cpf_cmp(valor_str)
                cur.execute(
                    "SELECT id FROM processos_juridicos WHERE cpf = %s",
                    (cpf,),
                )
                ids = {int(r[0]) for r in cur.fetchall()}
                result.hits_entrada = len(ids)
                result.processo_ids |= ids
                if not ids:
                    _fallback_audit_by_cpfs(
                        cur, {cpf}, result.processo_ids, result.via_fallback
                    )
            return result

        if tabela == "emails":
            if coluna == "email":
                email = _normalizar_email_cmp(valor_str)
                cur.execute(
                    """
                    SELECT DISTINCT id_processo_juridico
                    FROM emails
                    WHERE UPPER(email) = %s
                    """,
                    (email,),
                )
                ids = {int(r[0]) for r in cur.fetchall()}
                result.hits_entrada = len(ids)
                result.processo_ids |= ids
            elif coluna == "cpf":
                cpf = _normalizar_cpf_cmp(valor_str)
                cur.execute(
                    """
                    SELECT DISTINCT id_processo_juridico
                    FROM emails
                    WHERE cpf = %s
                    """,
                    (cpf,),
                )
                ids = {int(r[0]) for r in cur.fetchall()}
                result.hits_entrada = len(ids)
                result.processo_ids |= ids
                if not ids:
                    _resolve_cpfs(cur, {cpf}, result.processo_ids, result.via_fallback)
            return result

        if tabela == "sms":
            if coluna == "telefone":
                tels = _tel_variants(_normalizar_tel_cmp(valor_str))
                if tels:
                    ph = ",".join(["%s"] * len(tels))
                    params = list(tels)
                    cur.execute(
                        f"SELECT COUNT(DISTINCT id_processo_juridico) FROM sms WHERE telefone IN ({ph})",
                        params,
                    )
                    c1 = int(cur.fetchone()[0] or 0)
                    cur.execute(
                        f"SELECT COUNT(DISTINCT id_processo_juridico) FROM disparo_hsm WHERE telefone_hsm IN ({ph})",
                        params,
                    )
                    c2 = int(cur.fetchone()[0] or 0)
                    result.hits_entrada = c1 + c2
                _resolve_telefones(
                    cur, tels, result.processo_ids, result.via_fallback
                )
            elif coluna == "cpf":
                cpf = _normalizar_cpf_cmp(valor_str)
                cur.execute(
                    "SELECT DISTINCT id_processo_juridico FROM sms WHERE cpf = %s",
                    (cpf,),
                )
                ids = {int(r[0]) for r in cur.fetchall()}
                result.hits_entrada = len(ids)
                result.processo_ids |= ids
                if not ids:
                    _resolve_cpfs(cur, {cpf}, result.processo_ids, result.via_fallback)
            return result

        if tabela == "pessoas":
            if coluna == "cpf":
                cpf = _normalizar_cpf_cmp(valor_str)
                cur.execute("SELECT id FROM pessoas WHERE cpf = %s", (cpf,))
                pessoa_ids = [int(r[0]) for r in cur.fetchall()]
                result.hits_entrada = len(pessoa_ids)
                if pessoa_ids:
                    ph = ",".join(["%s"] * len(pessoa_ids))
                    cur.execute(
                        f"SELECT id FROM processos_juridicos WHERE id_pessoa IN ({ph})",
                        pessoa_ids,
                    )
                    result.processo_ids.update(int(r[0]) for r in cur.fetchall())
                else:
                    _fallback_audit_by_cpfs(
                        cur, {cpf}, result.processo_ids, result.via_fallback
                    )
            elif coluna == "nome":
                nome = _normalizar_nome_cmp(valor_str)
                cur.execute(
                    "SELECT id FROM pessoas WHERE UPPER(TRIM(nome)) = %s",
                    (nome,),
                )
                pessoa_ids = [int(r[0]) for r in cur.fetchall()]
                result.hits_entrada = len(pessoa_ids)
                if pessoa_ids:
                    ph = ",".join(["%s"] * len(pessoa_ids))
                    cur.execute(
                        f"SELECT id FROM processos_juridicos WHERE id_pessoa IN ({ph})",
                        pessoa_ids,
                    )
                    result.processo_ids.update(int(r[0]) for r in cur.fetchall())
                _resolve_nomes(cur, {nome}, result.processo_ids, result.via_fallback)
            return result

        raise ValueError(f"Tabela não suportada: {tabela}")
    finally:
        cur.close()
        conn.close()

# ══════════════════════════════════════════════════════════════════════════════
# Montagem da planilha
# ══════════════════════════════════════════════════════════════════════════════


def _blacklist_status_for_row(
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
) -> tuple[str, str, str, str]:
    """Retorna (na_blacklist, tipos, motivos, valores)."""
    hits: list[tuple[str, str]] = []  # (tipo, valor)
    motivo_map = motivo_map or {}

    cpf_n = _normalizar_cpf_cmp(cpf)
    if cpf_n and cpf_n in bl.get("CPF", set()):
        hits.append(("CPF", cpf_n))

    for nome_c in (_normalizar_nome_cmp(nome), _normalizar_nome_cmp(requerente)):
        if nome_c and nome_c in bl.get("NOME", set()):
            hits.append(("NOME", nome_c))

    chave = normalizar_chave_processo_incidente(processo, incidente)
    if chave and chave in bl.get("PROCESSO_INCIDENTE", set()):
        hits.append(("PROCESSO_INCIDENTE", chave))

    for t in telefones:
        d = _normalizar_tel_cmp(t)
        for v in _tel_variants(d):
            if v in bl.get("TELEFONE", set()):
                hits.append(("TELEFONE", v))
                break

    for e in emails:
        em = _normalizar_email_cmp(e)
        if em and em in bl.get("EMAIL", set()):
            hits.append(("EMAIL", em))

    if not hits:
        return ("Não", "", "", "")

    seen = set()
    uniq: list[tuple[str, str]] = []
    for t, v in hits:
        key = (t, v)
        if key in seen:
            continue
        seen.add(key)
        uniq.append((t, v))

    def _motivo_de(tipo: str, valor: str) -> str:
        m = motivo_map.get((tipo, valor))
        if m:
            return m
        if tipo == "TELEFONE":
            for cand in _tel_variants(valor):
                m = motivo_map.get((tipo, cand))
                if m:
                    return m
        return ""

    tipos = "; ".join(sorted({t for t, _ in uniq}))
    valores = "; ".join(f"{t}={v}" for t, v in uniq)
    motivos_list: list[str] = []
    seen_m: set[str] = set()
    for t, v in uniq:
        m = _motivo_de(t, v)
        if m and m not in seen_m:
            seen_m.add(m)
            motivos_list.append(m)
    motivos = "; ".join(motivos_list)
    return ("Sim", tipos, motivos, valores)


def montar_linhas_unificadas(
    processo_ids: set[int],
    via_fallback: set[int],
    *,
    origem_busca: str,
) -> list[dict[str, Any]]:
    if not processo_ids:
        return []

    bl = carregar_blacklist()
    # mapa (tipo, valor_normalizado) -> motivo (ativos)
    motivo_map: dict[tuple[str, str], str] = {}
    conn = conectar()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT tipo, valor, motivo
            FROM blacklist
            WHERE ativo = 1
            """
        )
        for row in cur.fetchall() or []:
            tipo = str(row.get("tipo") or "").strip().upper()
            valor = normalizar_valor_para_blacklist(tipo, row.get("valor"))
            motivo = str(row.get("motivo") or "").strip()
            if tipo and valor and (tipo, valor) not in motivo_map:
                motivo_map[(tipo, valor)] = motivo
    finally:
        cur.close()

    cur = conn.cursor(dictionary=True)
    rows_out: list[dict[str, Any]] = []

    try:
        ids = list(processo_ids)
        for batch in _chunks(ids):
            ph = ",".join(["%s"] * len(batch))
            cur.execute(
                f"""
                SELECT
                    pj.id AS id_processo,
                    pj.cpf,
                    pj.numero_processo,
                    pj.numero_incidente,
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
                    pj.requerente,
                    pj.processo_codigo,
                    pj.data_preenchimento,
                    pj.data_entrada,
                    pj.ultimo_processamento,
                    p.nome AS pessoa_nome,
                    p.data_nascimento,
                    p.count_processamentos,
                    (
                        SELECT GROUP_CONCAT(DISTINCT s.telefone ORDER BY s.telefone SEPARATOR '; ')
                        FROM sms s WHERE s.id_processo_juridico = pj.id
                    ) AS telefones_sms,
                    (
                        SELECT GROUP_CONCAT(DISTINCT h.telefone_hsm ORDER BY h.telefone_hsm SEPARATOR '; ')
                        FROM disparo_hsm h WHERE h.id_processo_juridico = pj.id
                    ) AS telefones_hsm,
                    (
                        SELECT GROUP_CONCAT(DISTINCT e.email ORDER BY e.email SEPARATOR '; ')
                        FROM emails e WHERE e.id_processo_juridico = pj.id
                    ) AS emails
                FROM processos_juridicos pj
                JOIN pessoas p ON p.id = pj.id_pessoa
                WHERE pj.id IN ({ph})
                ORDER BY pj.requerente, pj.numero_processo, pj.numero_incidente
                """,
                batch,
            )
            for r in cur.fetchall() or []:
                tels_sms = [
                    t.strip()
                    for t in str(r.get("telefones_sms") or "").split(";")
                    if t.strip()
                ]
                tels_hsm = [
                    t.strip()
                    for t in str(r.get("telefones_hsm") or "").split(";")
                    if t.strip()
                ]
                emails = [
                    e.strip()
                    for e in str(r.get("emails") or "").split(";")
                    if e.strip()
                ]
                na_bl, tipos_bl, motivos, valores_bl = _blacklist_status_for_row(
                    cpf=r.get("cpf") or "",
                    nome=r.get("pessoa_nome") or "",
                    requerente=r.get("requerente") or "",
                    processo=r.get("numero_processo") or "",
                    incidente=r.get("numero_incidente") or "",
                    telefones=tels_sms + tels_hsm,
                    emails=emails,
                    bl=bl,
                    motivo_map=motivo_map,
                )

                pid = int(r["id_processo"])
                rows_out.append(
                    {
                        "origem_busca": origem_busca,
                        "usou_fallback_audit": "Sim" if pid in via_fallback else "Não",
                        "cpf": r.get("cpf"),
                        "pessoa_nome": r.get("pessoa_nome"),
                        "data_nascimento": r.get("data_nascimento"),
                        "numero_processo": r.get("numero_processo"),
                        "numero_incidente": r.get("numero_incidente"),
                        "requerente": r.get("requerente"),
                        "natureza": r.get("natureza"),
                        "assunto": r.get("assunto"),
                        "ordem": r.get("ordem"),
                        "foro": r.get("foro"),
                        "data_base": r.get("data_base"),
                        "data_decisao": r.get("data_decisao"),
                        "principal_liquido": r.get("principal_liquido"),
                        "juros_moratorio": r.get("juros_moratorio"),
                        "valor_requisitado": r.get("valor_requisitado"),
                        "calculo_atualizado": r.get("calculo_atualizado"),
                        "entidade_devedora": r.get("entidade_devedora"),
                        "advogado": r.get("advogado"),
                        "processo_codigo": r.get("processo_codigo"),
                        "data_preenchimento": r.get("data_preenchimento"),
                        "data_entrada": r.get("data_entrada"),
                        "ultimo_processamento": r.get("ultimo_processamento"),
                        "telefones_sms": r.get("telefones_sms") or "",
                        "telefones_hsm": r.get("telefones_hsm") or "",
                        "emails": r.get("emails") or "",
                        "na_blacklist": na_bl,
                        "motivo": motivos,
                        "blacklist_tipos": tipos_bl,
                        "blacklist_motivos": motivos,
                        "blacklist_valores": valores_bl,
                    }
                )
    finally:
        cur.close()
        conn.close()

    return rows_out


def _motivos_blacklist_para_valores(
    cur, hits_valores: list[tuple[str, str]]
) -> str:
    """Mantido por compatibilidade; preferir motivo_map em montar_linhas."""
    if not hits_valores:
        return ""
    motivos: list[str] = []
    for tipo, valor in hits_valores:
        cur.execute(
            """
            SELECT motivo FROM blacklist
            WHERE ativo = 1 AND tipo = %s AND valor = %s
            LIMIT 1
            """,
            (tipo, valor),
        )
        row = cur.fetchone()
        if row and row[0]:
            motivos.append(str(row[0]))
    out = []
    seen = set()
    for m in motivos:
        if m not in seen:
            seen.add(m)
            out.append(m)
    return "; ".join(out)
_COLUNAS_EXCEL = [
    ("origem_busca", "Origem da busca"),
    ("usou_fallback_audit", "Usou fallback audit"),
    ("cpf", "CPF"),
    ("pessoa_nome", "Nome (pessoa)"),
    ("data_nascimento", "Data nascimento"),
    ("numero_processo", "Número processo"),
    ("numero_incidente", "Número incidente"),
    ("requerente", "Requerente"),
    ("natureza", "Natureza"),
    ("assunto", "Assunto"),
    ("ordem", "Ordem"),
    ("foro", "Foro"),
    ("data_base", "Data base"),
    ("data_decisao", "Data decisão"),
    ("principal_liquido", "Principal líquido"),
    ("juros_moratorio", "Juros moratório"),
    ("valor_requisitado", "Valor requisitado"),
    ("calculo_atualizado", "Cálculo atualizado"),
    ("entidade_devedora", "Entidade devedora"),
    ("advogado", "Advogado"),
    ("processo_codigo", "Processo código"),
    ("data_preenchimento", "Data preenchimento"),
    ("data_entrada", "Data entrada"),
    ("ultimo_processamento", "Último processamento"),
    ("telefones_sms", "Telefones SMS"),
    ("telefones_hsm", "Telefones HSM"),
    ("emails", "E-mails"),
    ("na_blacklist", "Na blacklist"),
    ("motivo", "Motivo"),
    ("blacklist_tipos", "Blacklist tipos"),
    ("blacklist_valores", "Blacklist valores"),
]


def _celula_excel(v: Any) -> Any:
    if hasattr(v, "strftime"):
        return v.strftime("%d/%m/%Y %H:%M")
    if v is None or str(v) in ("nan", "NaT", "None"):
        return None
    return v


def gerar_excel_unificado(
    linhas: list[dict[str, Any]], *, write_only: bool = False
) -> io.BytesIO:
    import openpyxl

    wb = openpyxl.Workbook(write_only=write_only)
    if write_only:
        ws = wb.create_sheet("Unificada")
        ws.append([label for _k, label in _COLUNAS_EXCEL])
        for row in linhas:
            ws.append([_celula_excel(row.get(k)) for k, _label in _COLUNAS_EXCEL])
    else:
        ws = wb.active
        ws.title = "Unificada"
        ws.append([label for _k, label in _COLUNAS_EXCEL])
        for row in linhas:
            ws.append([_celula_excel(row.get(k)) for k, _label in _COLUNAS_EXCEL])
    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf


def exportar_pesquisa_unificada(
    tabela: str, coluna: str, valor: str
) -> tuple[io.BytesIO, str, dict[str, Any]]:
    """
    Executa a pesquisa e devolve (buffer_xlsx, nome_arquivo, meta).
    """
    resolved = resolver_pesquisa(tabela, coluna, valor)
    origem = f"{tabela}.{coluna}={valor}"
    linhas = montar_linhas_unificadas(
        resolved.processo_ids,
        resolved.via_fallback,
        origem_busca=origem,
    )
    if not linhas:
        raise LookupError(
            "Nenhum processo interligado encontrado para essa pesquisa "
            f"(entradas: {resolved.hits_entrada})."
        )

    stamp = datetime.now().strftime("%d-%m-%Y_%H%M")
    safe_tab = re.sub(r"[^\w\-]+", "_", tabela)[:40]
    safe_col = re.sub(r"[^\w\-]+", "_", coluna)[:40]
    nome = f"Exportacao_unificada_{safe_tab}_{safe_col}_{stamp}.xlsx"
    buf = gerar_excel_unificado(linhas)
    meta = {
        "hits_entrada": resolved.hits_entrada,
        "processos": len(resolved.processo_ids),
        "linhas": len(linhas),
        "via_fallback": len(resolved.via_fallback),
        "avisos": resolved.avisos,
    }
    return buf, nome, meta


def exportar_tudo_unificado() -> tuple[io.BytesIO, str, dict[str, Any]]:
    """
    Exporta todos os processos_juridicos no formato unificado
    (pessoa + contatos + status blacklist).
    """
    conn = conectar()
    cur = conn.cursor()
    try:
        cur.execute("SELECT id FROM processos_juridicos")
        processo_ids = {int(r[0]) for r in cur.fetchall()}
    finally:
        cur.close()
        conn.close()

    if not processo_ids:
        raise LookupError("Nenhum processo encontrado na base.")

    linhas = montar_linhas_unificadas(
        processo_ids,
        set(),
        origem_busca="exportar_tudo",
    )
    if not linhas:
        raise LookupError("Nenhuma linha gerada para exportação completa.")

    stamp = datetime.now().strftime("%d-%m-%Y_%H%M")
    nome = f"Exportacao_unificada_TUDO_{stamp}.xlsx"
    # write_only reduz memória com ~30k linhas
    buf = gerar_excel_unificado(linhas, write_only=True)
    meta = {
        "hits_entrada": len(processo_ids),
        "processos": len(processo_ids),
        "linhas": len(linhas),
        "via_fallback": 0,
        "avisos": [],
    }
    return buf, nome, meta
