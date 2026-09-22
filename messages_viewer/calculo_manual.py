# -*- coding: utf-8 -*-
"""
Cálculo manual: se o caso já existir em ``precainfosnew``, actualiza o cadastro
e segue a automação. Se não existir, não cria linha nessa tabela — grava só
em ``memoria_calculo``.
"""
from __future__ import annotations

import hashlib
import logging
import os
import re
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

import mysql.connector

_LOG = logging.getLogger(__name__)
_HONORARIOS_PERCENT = Decimal("30.00")

# Chave do form → candidatos de coluna em precainfosnew (1.º que existir).
_COLUMN_CANDIDATES: dict[str, tuple[str, ...]] = {
    "requerente": ("Requerente",),
    "entidade_devedora": ("Entidade_Devedora", "entidade_devedora"),
    "numero_de_processo": (
        "Numero_de_Processo",
        "Numero_de_processo",
        "processo",
        "Processo",
    ),
    "numero_do_incidente": (
        "Numero_do_Incidente",
        "Numero_do_incidente",
        "numero_de_incidente",
        "incidente",
        "Incidente",
    ),
    "oc": ("Ordem", "OC", "Oc", "oc"),
    "cumprimento": ("Processo_Principal", "processo_principal"),
    "data_base": ("Data_Base", "data_base"),
    "data_decisao": ("Data_Decisao", "Data_Decisão", "data_decisao"),
    "principal_liquido": ("Principal_Liquido", "principal_liquido"),
    "juros_moratorio": ("Juros_Moratorio", "juros_moratorio"),
    "spprev": ("SPPREV", "SPPRE", "spprev"),
    "iamspe": ("IAMSPE", "iamspe"),
    "ipesp": ("IPESP", "IPES", "ipesp"),
    "assit_med_hospital": ("ASSIT_MED_HOSPITAL", "assit_med_hospital"),
    "inst_prev_caixa_benef": ("INST_PREV_CAIXA_BENEF",),
    "assist_med_caixa_benef": ("ASSIST_MED_CAIXA_BENEF",),
    "inst_prev": ("INST_PREV",),
    "numero_de_meses": ("Numero_de_Meses", "numero_de_meses"),
}

_REQUIRED = (
    "requerente",
    "entidade_devedora",
    "numero_de_processo",
    "data_base",
    "data_decisao",
    "principal_liquido",
    "juros_moratorio",
    "numero_de_meses",
)

_MONEY_KEYS = (
    "principal_liquido",
    "juros_moratorio",
    "descontos",
)

_DESCONTO_RUBRICAS = (
    "spprev",
    "iamspe",
    "ipesp",
    "assit_med_hospital",
    "inst_prev_caixa_benef",
    "assist_med_caixa_benef",
)


def _flask_cfg() -> dict[str, Any] | None:
    name = (os.getenv("FLASK_MYSQL_DATABASE") or "").strip().strip("'\"")
    if not name:
        return None
    try:
        port = int(str(os.getenv("FLASK_MYSQL_PORT") or "3306").strip())
    except ValueError:
        port = 3306
    try:
        timeout = int((os.getenv("FLASK_MYSQL_CONNECT_TIMEOUT") or "10").strip())
    except ValueError:
        timeout = 10
    timeout = min(30, max(1, timeout))
    return {
        "host": (os.getenv("FLASK_MYSQL_HOST") or "127.0.0.1").strip().strip("'\""),
        "port": port,
        "database": name,
        "user": (os.getenv("FLASK_MYSQL_USER") or "root").strip().strip("'\""),
        "password": (os.getenv("FLASK_MYSQL_PASSWORD") or "").strip().strip("'\""),
        "connection_timeout": timeout,
    }


def is_configured() -> bool:
    return bool(_flask_cfg())


def _memoria_cfg() -> dict[str, Any] | None:
    name = (os.getenv("MEMORIA_MYSQL_DATABASE") or "").strip().strip("'\"")
    if not name:
        return None
    try:
        port = int(str(os.getenv("MEMORIA_MYSQL_PORT") or "3306").strip())
    except ValueError:
        port = 3306
    try:
        timeout = int((os.getenv("MEMORIA_MYSQL_CONNECT_TIMEOUT") or "10").strip())
    except ValueError:
        timeout = 10
    timeout = min(30, max(1, timeout))
    return {
        "host": (os.getenv("MEMORIA_MYSQL_HOST") or "127.0.0.1").strip().strip("'\""),
        "port": port,
        "database": name,
        "user": (os.getenv("MEMORIA_MYSQL_USER") or "root").strip().strip("'\""),
        "password": (os.getenv("MEMORIA_MYSQL_PASSWORD") or "").strip().strip("'\""),
        "connection_timeout": timeout,
    }


def _pick_field(fields: set[str], *candidates: str) -> str | None:
    if not fields:
        return None
    lc = {f.lower(): f for f in fields}
    for c in candidates:
        if c in fields:
            return c
        k = c.lower()
        if k in lc:
            return lc[k]
    return None


def _table_columns(cur) -> set[str]:
    cur.execute("SHOW COLUMNS FROM `precainfosnew`")
    out: set[str] = set()
    for r in cur.fetchall() or []:
        if isinstance(r, dict):
            nm = r.get("Field") or r.get("field")
            if nm:
                out.add(str(nm))
        else:
            out.add(str(r[0]))
    return out


def _parse_money(raw: Any) -> Decimal | None:
    """Aceita 150000.50, 150.000,50 e o formato do banco ``199.922.30``."""
    if raw is None:
        return None
    if isinstance(raw, bool):
        return None
    if isinstance(raw, Decimal):
        return raw
    if isinstance(raw, (int, float)):
        return Decimal(str(raw))
    s = str(raw).strip().replace("R$", "").replace(" ", "")
    if not s or s in (".", ",", "0.00", "0,00"):
        return Decimal("0") if s in ("0.00", "0,00") else None
    s = re.sub(r"[^\d.,-]", "", s)
    if not s or s in ("-", ".", ","):
        return None
    if "," in s:
        s = s.replace(".", "").replace(",", ".")
    elif s.count(".") > 1:
        parts = s.split(".")
        s = "".join(parts[:-1]) + "." + parts[-1]
    try:
        return Decimal(s)
    except (InvalidOperation, ValueError):
        return None


def _parse_date(raw: Any) -> date | None:
    if raw is None or raw == "":
        return None
    if isinstance(raw, datetime):
        return raw.date()
    if isinstance(raw, date):
        return raw
    s = str(raw).strip()
    if not s:
        return None
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(s[:10], fmt).date()
        except ValueError:
            continue
    return None


def _parse_bool_flag(raw: Any) -> bool:
    if isinstance(raw, bool):
        return raw
    if isinstance(raw, (int, float)) and not isinstance(raw, bool):
        return raw != 0
    s = str(raw or "").strip().lower()
    return s in ("1", "true", "yes", "sim", "on")


def _parse_int(raw: Any) -> int | None:
    if raw is None or raw == "":
        return None
    try:
        return int(float(str(raw).strip().replace(",", ".")))
    except (TypeError, ValueError):
        return None


def normalize_form(data: dict[str, Any]) -> tuple[dict[str, Any] | None, str | None]:
    """Valida o JSON do formulário. Devolve (payload, erro)."""
    if not isinstance(data, dict):
        return None, "JSON inválido."
    out: dict[str, Any] = {}
    out["requerente"] = str(data.get("requerente") or "").strip()
    out["entidade_devedora"] = str(data.get("entidade_devedora") or "").strip()
    out["numero_de_processo"] = str(data.get("numero_de_processo") or "").strip()
    out["numero_do_incidente"] = str(data.get("numero_do_incidente") or "").strip()
    out["oc"] = str(data.get("oc") or data.get("ordem") or "").strip()
    out["cumprimento"] = str(
        data.get("cumprimento") or data.get("processo_principal") or ""
    ).strip()
    out["data_base"] = _parse_date(data.get("data_base"))
    out["data_decisao"] = _parse_date(data.get("data_decisao"))
    for k in _MONEY_KEYS:
        parsed = _parse_money(data.get(k))
        out[k] = parsed if parsed is not None else Decimal("0")
    # Planilha tem uma célula de desconto (I23). O total vai para INST_PREV;
    # as outras rubricas ficam 0 para não somar em dobro num UPDATE.
    out["inst_prev"] = out["descontos"]
    for k in _DESCONTO_RUBRICAS:
        out[k] = Decimal("0")
    meses = _parse_int(data.get("numero_de_meses"))
    out["numero_de_meses"] = meses
    out["prioridade"] = _parse_bool_flag(data.get("prioridade"))

    raw_id = data.get("id_precainfosnew")
    if raw_id is None:
        raw_id = data.get("id")
    if raw_id is None or str(raw_id).strip() == "":
        out["id_precainfosnew"] = None
    else:
        try:
            out["id_precainfosnew"] = int(raw_id)
        except (TypeError, ValueError):
            return None, "Identificador do caso inválido."
        if out["id_precainfosnew"] <= 0:
            return None, "Identificador do caso inválido."

    missing = [k for k in _REQUIRED if k not in ("principal_liquido", "juros_moratorio", "numero_de_meses", "data_base", "data_decisao") and not out.get(k)]
    if missing:
        return None, "Campos obrigatórios em falta: " + ", ".join(missing) + "."
    if out["data_base"] is None:
        return None, "Data-base inválida ou em falta."
    if out["data_decisao"] is None:
        return None, "Data da decisão/inscrição inválida ou em falta."
    if out["numero_de_meses"] is None:
        return None, "Número de meses inválido."
    entidade = out["entidade_devedora"].upper()
    if out["numero_de_meses"] < 1 and "INSS" not in entidade:
        return None, "Número de meses deve ser pelo menos 1 (exceto entidade INSS)."
    if out["numero_de_meses"] < 0:
        return None, "Número de meses não pode ser negativo."
    if not re.search(r"\d", out["numero_de_processo"]):
        return None, "Nº do processo inválido."
    return out, None


def _incidente_variants(value: str) -> list[str]:
    raw = str(value or "").strip()
    variants = [raw]
    try:
        n = str(int(raw))
        if n not in variants:
            variants.append(n)
        for pad in (2, 4):
            p = n.zfill(pad)
            if p not in variants:
                variants.append(p)
    except (TypeError, ValueError):
        pass
    return variants


def _digits(value: str) -> str:
    return re.sub(r"\D", "", str(value or ""))


def synthetic_memoria_id(processo: str, incidente: str) -> int:
    """Id negativo estável para casos que só existem em ``memoria_calculo``."""
    proc_d = _digits(processo)
    raw_inc = str(incidente or "").strip()
    try:
        inc_n = str(int(raw_inc or "0"))
    except ValueError:
        inc_n = raw_inc or "0"
    digest = hashlib.sha256(f"{proc_d}|{inc_n}".encode("utf-8")).digest()
    n = int.from_bytes(digest[:8], "big") & 0x7FFFFFFFFFFFFFFF
    return -n if n else -1


def _format_db_date(value: Any) -> str | None:
    parsed = value if isinstance(value, date) and not isinstance(value, datetime) else _parse_date(value)
    if parsed is None:
        return None
    return parsed.strftime("%d/%m/%Y")


def _format_money_db(value: Any) -> str:
    """Formato do banco em precainfosnew: ``107.469.80``."""
    parsed = value if isinstance(value, Decimal) else _parse_money(value)
    if parsed is None:
        parsed = Decimal("0")
    br = _format_money_br(parsed)
    return br.replace(",", ".")


def _persist_values(
    payload: dict[str, Any],
    colmap: dict[str, str],
    fields: set[str],
) -> dict[str, Any]:
    money_keys = set(_MONEY_KEYS) | set(_DESCONTO_RUBRICAS) | {"inst_prev"}
    values: dict[str, Any] = {}
    for key, col in colmap.items():
        raw = payload.get(key)
        if key in ("data_base", "data_decisao"):
            values[col] = _format_db_date(raw)
        elif key in money_keys:
            values[col] = _format_money_db(raw)
        elif key == "numero_de_meses":
            values[col] = "" if raw is None else str(int(raw))
        elif key == "numero_do_incidente":
            text = str(raw or "").strip()
            if not text:
                values[col] = "0"
            else:
                try:
                    values[col] = str(int(text))
                except ValueError:
                    values[col] = text
        else:
            values[col] = raw
    f_meses_termo = _pick_field(fields, "Numero_de_Meses_TERMO", "numero_de_meses_termo")
    if f_meses_termo and payload.get("numero_de_meses") is not None:
        values[f_meses_termo] = str(int(payload["numero_de_meses"]))
    return values


def _find_existing_id(
    cur,
    f_proc: str,
    f_inc: str | None,
    proc: str,
    inc: str,
) -> int | None:
    """Localiza id já existente por processo + incidente."""
    proc = str(proc or "").strip()
    inc = str(inc or "").strip()
    if not proc:
        return None

    def _one(sql: str, params: tuple) -> int | None:
        cur.execute(sql, params)
        row = cur.fetchone() or {}
        if row.get("id") is not None:
            return int(row["id"])
        return None

    if f_inc:
        if inc:
            variants = _incidente_variants(inc)
            ph = ",".join(["%s"] * len(variants))
            found = _one(
                f"""
                SELECT id FROM precainfosnew
                WHERE TRIM(COALESCE(`{f_proc}`, '')) = %s
                  AND TRIM(COALESCE(`{f_inc}`, '')) IN ({ph})
                ORDER BY id DESC LIMIT 1
                """,
                tuple([proc] + variants),
            )
        else:
            found = _one(
                f"""
                SELECT id FROM precainfosnew
                WHERE TRIM(COALESCE(`{f_proc}`, '')) = %s
                  AND TRIM(COALESCE(`{f_inc}`, '')) IN ('', '0', '00')
                ORDER BY id DESC LIMIT 1
                """,
                (proc,),
            )
        if found is not None:
            return found
        # Fallback: processo só com dígitos (CNJ com/sem pontuação).
        proc_d = _digits(proc)
        if len(proc_d) >= 8:
            if inc:
                variants = _incidente_variants(inc)
                ph = ",".join(["%s"] * len(variants))
                found = _one(
                    f"""
                    SELECT id FROM precainfosnew
                    WHERE REPLACE(REPLACE(REPLACE(TRIM(COALESCE(`{f_proc}`, '')), '-', ''), '.', ''), ' ', '') = %s
                      AND TRIM(COALESCE(`{f_inc}`, '')) IN ({ph})
                    ORDER BY id DESC LIMIT 1
                    """,
                    tuple([proc_d] + variants),
                )
            else:
                found = _one(
                    f"""
                    SELECT id FROM precainfosnew
                    WHERE REPLACE(REPLACE(REPLACE(TRIM(COALESCE(`{f_proc}`, '')), '-', ''), '.', ''), ' ', '') = %s
                      AND TRIM(COALESCE(`{f_inc}`, '')) IN ('', '0', '00')
                    ORDER BY id DESC LIMIT 1
                    """,
                    (proc_d,),
                )
            if found is not None:
                return found
        return None

    found = _one(
        f"""
        SELECT id FROM precainfosnew
        WHERE TRIM(COALESCE(`{f_proc}`, '')) = %s
        ORDER BY id DESC LIMIT 1
        """,
        (proc,),
    )
    if found is not None:
        return found
    proc_d = _digits(proc)
    if len(proc_d) >= 8:
        return _one(
            f"""
            SELECT id FROM precainfosnew
            WHERE REPLACE(REPLACE(REPLACE(TRIM(COALESCE(`{f_proc}`, '')), '-', ''), '.', ''), ' ', '') = %s
            ORDER BY id DESC LIMIT 1
            """,
            (proc_d,),
        )
    return None


def upsert_precainfos_from_form(payload: dict[str, Any]) -> tuple[dict[str, Any] | None, str | None]:
    """
    UPDATE se o caso já existir em ``precainfosnew``.
    Sem INSERT: caso novo devolve ``action=memoria_only``.
    """
    cfg = _flask_cfg()
    if not cfg:
        return None, "Cadastro não configurado."
    conn = None
    cur = None
    try:
        conn = mysql.connector.connect(
            **cfg, charset="utf8mb4", collation="utf8mb4_unicode_ci"
        )
        cur = conn.cursor(dictionary=True)
        cur.execute("SHOW TABLES LIKE 'precainfosnew'")
        if not cur.fetchone():
            return None, "Cadastro indisponível."
        fields = _table_columns(cur)

        colmap: dict[str, str] = {}
        for key, cands in _COLUMN_CANDIDATES.items():
            col = _pick_field(fields, *cands)
            if col:
                colmap[key] = col
        f_proc = colmap.get("numero_de_processo")
        f_inc = colmap.get("numero_do_incidente")
        if not f_proc:
            return None, "Cadastro sem campo de processo."

        proc = payload["numero_de_processo"]
        inc = payload["numero_do_incidente"]
        existing_id = payload.get("id_precainfosnew")
        if existing_id is not None:
            try:
                existing_id = int(existing_id)
            except (TypeError, ValueError):
                return None, "Identificador do caso inválido."
            if existing_id <= 0:
                existing_id = None
            else:
                cur.execute(
                    "SELECT id FROM precainfosnew WHERE id = %s LIMIT 1",
                    (existing_id,),
                )
                if not cur.fetchone():
                    existing_id = None
        if existing_id is None:
            existing_id = _find_existing_id(cur, f_proc, f_inc, proc, inc)

        if existing_id is None:
            return {
                "id": synthetic_memoria_id(proc, inc),
                "action": "memoria_only",
            }, None

        values = _persist_values(payload, colmap, fields)
        sets = ", ".join(f"`{c}` = %s" for c in values)
        cur.execute(
            f"UPDATE precainfosnew SET {sets} WHERE id = %s",
            tuple(list(values.values()) + [existing_id]),
        )
        conn.commit()
        return {"id": existing_id, "action": "updated"}, None
    except mysql.connector.Error as e:
        errno = getattr(e, "errno", None)
        _LOG.exception("calculo_manual: falha ao gravar precainfosnew (errno=%s)", errno)
        return None, "Erro ao gravar o cadastro."
    finally:
        if cur is not None:
            try:
                cur.close()
            except Exception:
                pass
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def _q2(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.01"))


def memoria_valores_from_form(payload: dict[str, Any]) -> dict[str, Decimal]:
    """Placeholder de linha em ``memoria_calculo`` até a planilha gravar R30–R36.

    Não aplica regra própria de honorários: R35/R36 só existem depois do
    LibreOffice na planilha V33.
    """
    return {
        "principal_bruto": Decimal("0.00"),
        "juros": Decimal("0.00"),
        "desc_saude_prev": Decimal("0.00"),
        "desc_ir": Decimal("0.00"),
        "percentual_honorarios": _HONORARIOS_PERCENT,
        "total_bruto": Decimal("0.00"),
        "reserva_honorarios": Decimal("0.00"),
        "total_liquido": Decimal("0.00"),
    }


def automation_payload_from_form(
    payload: dict[str, Any],
    *,
    memoria_id: int,
    feito_por: str,
) -> dict[str, Any]:
    """main_dict serializável para a API de cálculo, sem depender de precainfosnew."""
    inc = str(payload.get("numero_do_incidente") or "").strip()
    try:
        inc = str(int(inc or "0"))
    except ValueError:
        inc = inc or "0"
    return {
        "id": int(memoria_id),
        "Requerente": str(payload.get("requerente") or "").strip(),
        "Entidade_Devedora": str(payload.get("entidade_devedora") or "").strip(),
        "Processo": str(payload.get("numero_de_processo") or "").strip(),
        "Oc": str(payload.get("oc") or "").strip(),
        "EP": "",
        "Cumprimento": str(payload.get("cumprimento") or "").strip(),
        "Incidente": inc,
        "Data_Base": _format_db_date(payload.get("data_base")) or "",
        "Data_Inscrição": _format_db_date(payload.get("data_decisao")) or "",
        "Principal_Liquido": float(_q2(payload.get("principal_liquido") or Decimal("0"))),
        "Juros_Moratorio": float(_q2(payload.get("juros_moratorio") or Decimal("0"))),
        "Despesas": 0,
        "SPPREV": 0,
        "IAMSPE": 0,
        "IPESP": 0,
        "ASSIT_MED_HOSPITAL": 0,
        "INST_PREV_CAIXA_BENEF": 0,
        "ASSIST_MED_CAIXA_BENEF": 0,
        "INST_PREV": float(_q2(payload.get("inst_prev") or payload.get("descontos") or Decimal("0"))),
        "Numero_de_Meses": int(payload.get("numero_de_meses") or 0),
        "feito_por": (feito_por or "").strip()[:200] or "automação",
    }


def upsert_memoria_from_form(
    payload: dict[str, Any],
    *,
    memoria_id: int,
    feito_por: str,
) -> tuple[dict[str, Any] | None, str | None]:
    """Cria ou actualiza ``memoria_calculo`` sem tocar em ``precainfosnew``."""
    cfg = _memoria_cfg()
    if not cfg:
        return None, "Memória de cálculo não configurada."
    vals = memoria_valores_from_form(payload)
    proc = str(payload.get("numero_de_processo") or "").strip()[:200]
    inc_raw = str(payload.get("numero_do_incidente") or "").strip()
    try:
        inc = str(int(inc_raw or "0"))
    except ValueError:
        inc = inc_raw[:200] if inc_raw else "0"
    req = str(payload.get("requerente") or "").strip()[:500]
    meses = payload.get("numero_de_meses")
    meses_txt = "" if meses is None else str(int(meses))
    fp = (feito_por or "").strip()[:200] or "automação"
    conn = None
    cur = None
    try:
        conn = mysql.connector.connect(
            **cfg, charset="utf8mb4", collation="utf8mb4_unicode_ci"
        )
        cur = conn.cursor(dictionary=True)
        cur.execute("SHOW TABLES LIKE 'memoria_calculo'")
        if not cur.fetchone():
            return None, "Memória de cálculo indisponível."
        pid = int(memoria_id)
        if pid == 0:
            return None, "Identificador da memória inválido."
        cur.execute(
            """
            SELECT id_precainfosnew
            FROM memoria_calculo
            WHERE TRIM(COALESCE(numero_de_processo, '')) = %s
              AND TRIM(COALESCE(numero_do_incidente, '')) IN (%s, %s)
            ORDER BY id DESC
            LIMIT 1
            """,
            (proc, inc, inc_raw or inc),
        )
        found = cur.fetchone() or {}
        if found.get("id_precainfosnew") is not None:
            try:
                pid = int(found["id_precainfosnew"])
            except (TypeError, ValueError):
                pass
        has_meses = False
        cur.execute("SHOW COLUMNS FROM `memoria_calculo` LIKE 'numero_de_meses'")
        has_meses = bool(cur.fetchone())
        if has_meses:
            sql = """
                INSERT INTO memoria_calculo (
                    id_precainfosnew, requerente, numero_de_processo, numero_do_incidente,
                    principal_bruto, juros, desc_saude_prev, desc_ir, percentual_honorarios,
                    total_bruto, reserva_honorarios, total_liquido, numero_de_meses, feito_por
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON DUPLICATE KEY UPDATE
                    requerente = VALUES(requerente),
                    numero_de_processo = VALUES(numero_de_processo),
                    numero_do_incidente = VALUES(numero_do_incidente),
                    numero_de_meses = VALUES(numero_de_meses),
                    feito_por = VALUES(feito_por),
                    atualizado_em = NOW(6)
            """
            params = (
                pid, req or None, proc or None, inc,
                vals["principal_bruto"], vals["juros"], vals["desc_saude_prev"],
                vals["desc_ir"], vals["percentual_honorarios"], vals["total_bruto"],
                vals["reserva_honorarios"], vals["total_liquido"], meses_txt, fp,
            )
        else:
            sql = """
                INSERT INTO memoria_calculo (
                    id_precainfosnew, requerente, numero_de_processo, numero_do_incidente,
                    principal_bruto, juros, desc_saude_prev, desc_ir, percentual_honorarios,
                    total_bruto, reserva_honorarios, total_liquido, feito_por
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON DUPLICATE KEY UPDATE
                    requerente = VALUES(requerente),
                    numero_de_processo = VALUES(numero_de_processo),
                    numero_do_incidente = VALUES(numero_do_incidente),
                    feito_por = VALUES(feito_por),
                    atualizado_em = NOW(6)
            """
            params = (
                pid, req or None, proc or None, inc,
                vals["principal_bruto"], vals["juros"], vals["desc_saude_prev"],
                vals["desc_ir"], vals["percentual_honorarios"], vals["total_bruto"],
                vals["reserva_honorarios"], vals["total_liquido"], fp,
            )
        cur.execute(sql, params)
        conn.commit()
        return {"id": pid, "action": "memoria_only"}, None
    except mysql.connector.Error as e:
        _LOG.exception("calculo_manual: falha ao gravar memoria_calculo: %s", e)
        return None, "Erro ao gravar a memória de cálculo."
    finally:
        if cur is not None:
            try:
                cur.close()
            except Exception:
                pass
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def _format_money_br(value: Decimal) -> str:
    sign = "-" if value < 0 else ""
    quantized = abs(value).quantize(Decimal("0.01"))
    text = f"{quantized:.2f}"
    ints, frac = text.split(".")
    groups: list[str] = []
    while len(ints) > 3:
        groups.append(ints[-3:])
        ints = ints[:-3]
    groups.append(ints)
    groups.reverse()
    return sign + ".".join(groups) + "," + frac


def _money_from_row(row: dict[str, Any], col: str | None) -> Decimal:
    if not col:
        return Decimal("0")
    parsed = _parse_money(row.get(col))
    return parsed if parsed is not None else Decimal("0")


def _descontos_total_from_row(row: dict[str, Any], colmap: dict[str, str]) -> Decimal:
    """Mesma regra da planilha: soma rubricas uma vez + INST_PREV se não duplicar."""
    spprev = _money_from_row(row, colmap.get("spprev"))
    iamspe = _money_from_row(row, colmap.get("iamspe"))
    ipesp = _money_from_row(row, colmap.get("ipesp"))
    assit = _money_from_row(row, colmap.get("assit_med_hospital"))
    inst_caixa = _money_from_row(row, colmap.get("inst_prev_caixa_benef"))
    assist = _money_from_row(row, colmap.get("assist_med_caixa_benef"))
    inst_prev = _money_from_row(row, colmap.get("inst_prev"))
    if assit == assist:
        assist = Decimal("0")
    componentes = [spprev, iamspe, ipesp, assit, inst_caixa, assist]
    if inst_prev == 0 or inst_prev not in componentes:
        return sum(componentes, Decimal("0")) + inst_prev
    return sum(componentes, Decimal("0"))


def form_fields_from_row(row: dict[str, Any], fields: set[str]) -> dict[str, Any]:
    """Converte uma linha de precainfosnew nos names do formulário manual."""
    colmap: dict[str, str] = {}
    for key, cands in _COLUMN_CANDIDATES.items():
        col = _pick_field(fields, *cands)
        if col:
            colmap[key] = col

    def text_key(key: str) -> str:
        col = colmap.get(key)
        if not col:
            return ""
        raw = row.get(col)
        if raw is None:
            return ""
        return str(raw).strip()

    data_base = _parse_date(row.get(colmap["data_base"])) if colmap.get("data_base") else None
    data_decisao = (
        _parse_date(row.get(colmap["data_decisao"])) if colmap.get("data_decisao") else None
    )
    meses = (
        _parse_int(row.get(colmap["numero_de_meses"])) if colmap.get("numero_de_meses") else None
    )
    principal = _money_from_row(row, colmap.get("principal_liquido"))
    juros = _money_from_row(row, colmap.get("juros_moratorio"))
    descontos = _descontos_total_from_row(row, colmap)
    try:
        pid = int(row["id"]) if row.get("id") is not None else None
    except (TypeError, ValueError):
        pid = None
    return {
        "id_precainfosnew": pid,
        "requerente": text_key("requerente"),
        "entidade_devedora": text_key("entidade_devedora"),
        "numero_de_processo": text_key("numero_de_processo"),
        "numero_do_incidente": text_key("numero_do_incidente"),
        "oc": text_key("oc"),
        "cumprimento": text_key("cumprimento"),
        "data_base": data_base.isoformat() if data_base else "",
        "data_decisao": data_decisao.isoformat() if data_decisao else "",
        "principal_liquido": _format_money_br(principal),
        "juros_moratorio": _format_money_br(juros),
        "descontos": _format_money_br(descontos),
        "numero_de_meses": "" if meses is None else str(meses),
    }


def load_caso_for_form(
    *,
    prec_id: int | None = None,
    processo: str = "",
    incidente: str = "",
) -> tuple[dict[str, Any] | None, str | None]:
    """
    Lê ``precainfosnew`` para auto-preencher o formulário.
    Prefere ``id``; senão localiza por processo + incidente.
    """
    cfg = _flask_cfg()
    if not cfg:
        return None, "Cadastro não configurado."
    conn = None
    cur = None
    try:
        conn = mysql.connector.connect(
            **cfg, charset="utf8mb4", collation="utf8mb4_unicode_ci"
        )
        cur = conn.cursor(dictionary=True)
        cur.execute("SHOW TABLES LIKE 'precainfosnew'")
        if not cur.fetchone():
            return None, "Cadastro indisponível."
        fields = _table_columns(cur)
        f_proc = _pick_field(fields, *_COLUMN_CANDIDATES["numero_de_processo"])
        f_inc = _pick_field(fields, *_COLUMN_CANDIDATES["numero_do_incidente"])
        row = None
        if prec_id is not None:
            cur.execute("SELECT * FROM precainfosnew WHERE id = %s LIMIT 1", (int(prec_id),))
            row = cur.fetchone()
            if not row:
                return None, "Caso não encontrado no cadastro."
        else:
            if not f_proc:
                return None, "Cadastro sem campo de processo."
            found_id = _find_existing_id(
                cur, f_proc, f_inc, processo, incidente
            )
            if found_id is None:
                inc_txt = incidente.strip() or "(sem incidente)"
                return None, (
                    f"Não existe cadastro para o processo {processo} / incidente {inc_txt}."
                )
            cur.execute("SELECT * FROM precainfosnew WHERE id = %s LIMIT 1", (found_id,))
            row = cur.fetchone()
            if not row:
                return None, "Caso não encontrado no cadastro."
        form = form_fields_from_row(dict(row), fields)
        return {"id_precainfosnew": form.get("id_precainfosnew"), "form": form}, None
    except mysql.connector.Error as e:
        return None, "Erro ao ler o cadastro."
    finally:
        if cur is not None:
            try:
                cur.close()
            except Exception:
                pass
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
