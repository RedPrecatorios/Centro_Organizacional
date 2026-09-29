# -*- coding: utf-8 -*-
"""Consulta e inclusão na tabela flaskdb.blacklist (host do cálculo)."""
from __future__ import annotations

import json
import os
import re
import time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo

import mysql.connector

PAGE_SIZE = 25
_SEP = "\x1f"
_TIPOS = ("CREDOR", "COMPLETO")
_SUBTIPOS_CREDOR = ("NOME", "CPF")
_SEARCH_COLUMNS = ("nome", "cpf", "processo", "incidente", "advogado")
# DATETIME do flaskdb é gravado no fuso da sessão MySQL (+03:00), não em America/Sao_Paulo.
_DB_TZ = timezone(timedelta(hours=3))
_SP_TZ = ZoneInfo("America/Sao_Paulo")
SORT_COLUMNS = {
    "id": "id",
    "tipo": "tipo",
    "nome": "nome",
    "cpf": "cpf",
    "processo_codigo": "processo_codigo",
    "processo": "processo",
    "incidente": "incidente",
    "advogado": "advogado",
    "origem": "origem",
    "criado_em": "criado_em",
    "atualizado_em": "atualizado_em",
}

_ALLOWED_EMAILS = frozenset(
    {
        "guilherme.vitoriano@redprecatorios.com.br",
        "filipe.noberto@redprecatorios.com.br",
    }
)
_ALLOWED_NAMES = (
    "guilherme vitoriano",
    "filipe noberto",
)


def _fold_name(value: str) -> str:
    text = unicodedata.normalize("NFKD", value or "")
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return " ".join(text.casefold().split())


def user_can_manage_blacklist(user: dict | None) -> bool:
    """Só Guilherme Vitoriano e Filipe Noberto."""
    if not isinstance(user, dict):
        return False
    email = str(user.get("email") or "").strip().lower()
    if email in _ALLOWED_EMAILS:
        return True
    first = str(user.get("first_name") or "").strip()
    last = str(user.get("last_name") or "").strip()
    nome = _fold_name(" ".join(p for p in (first, last) if p))
    if not nome:
        nome = _fold_name(str(user.get("username") or ""))
    for allowed in _ALLOWED_NAMES:
        if nome == allowed or nome.startswith(allowed + " "):
            return True
    return False


def _env(name: str, fallback: str = "") -> str:
    raw = os.getenv(name)
    if raw is None or not str(raw).strip():
        raw = os.getenv(fallback) if fallback else ""
    return (raw or "").strip().strip("'\"")


def mysql_config() -> dict | None:
    database = _env("BLACKLIST_MYSQL_DATABASE", "FLASK_MYSQL_DATABASE")
    if not database:
        return None
    host = _env("BLACKLIST_MYSQL_HOST", "FLASK_MYSQL_HOST") or "127.0.0.1"
    user = _env("BLACKLIST_MYSQL_USER", "FLASK_MYSQL_USER") or "root"
    password = _env("BLACKLIST_MYSQL_PASSWORD", "FLASK_MYSQL_PASSWORD")
    try:
        port = int(_env("BLACKLIST_MYSQL_PORT", "FLASK_MYSQL_PORT") or "3306")
    except ValueError:
        port = 3306
    try:
        timeout = int(_env("BLACKLIST_MYSQL_CONNECT_TIMEOUT") or "10")
    except ValueError:
        timeout = 10
    timeout = max(1, min(timeout, 30))
    return {
        "host": host,
        "port": port,
        "database": database,
        "user": user,
        "password": password,
        "connection_timeout": timeout,
        "charset": "utf8mb4",
        "collation": "utf8mb4_unicode_ci",
    }


def _clip(value: object, limit: int) -> str:
    return " ".join(str(value or "").split())[:limit]


def normalizar_texto(value: object, limit: int = 255) -> str:
    """Maiúsculas, sem acento e sem caracteres especiais."""
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.upper()
    text = re.sub(r"[^A-Z0-9 ]+", " ", text)
    return " ".join(text.split())[:limit]


def normalizar_cpf(value: object) -> str:
    """Só dígitos. Com menos de 11, completa com zeros à esquerda."""
    digits = re.sub(r"\D+", "", str(value or ""))
    if not digits:
        return ""
    if len(digits) < 11:
        digits = digits.zfill(11)
    return digits[:14]


def _campos(payload: dict[str, Any]) -> dict[str, str]:
    tipo = _clip(payload.get("tipo"), 12).upper()
    if tipo not in _TIPOS:
        raise ValueError("Tipo inválido. Use CREDOR ou COMPLETO.")
    nome = normalizar_texto(payload.get("nome"), 255)
    cpf = normalizar_cpf(payload.get("cpf"))
    advogado = normalizar_texto(payload.get("advogado"), 255)
    processo_codigo = _clip(payload.get("processo_codigo"), 64)
    processo = _clip(payload.get("processo"), 64)
    incidente = _clip(payload.get("incidente"), 20)
    subtipo = _clip(payload.get("subtipo"), 12).upper()
    chave = build_chave(
        tipo,
        subtipo=subtipo,
        nome=nome,
        cpf=cpf,
        processo_codigo=processo_codigo,
        processo=processo,
        incidente=incidente,
        advogado=advogado,
    )
    if len(chave) > 700:
        raise ValueError("A chave gerada excede o limite de 700 caracteres.")
    return {
        "tipo": tipo,
        "subtipo": subtipo,
        "chave": chave,
        "nome": nome,
        "cpf": cpf,
        "processo_codigo": processo_codigo,
        "processo": processo,
        "incidente": incidente,
        "advogado": advogado,
    }


def _duplicate(exc: BaseException) -> bool:
    return getattr(exc, "errno", None) == 1062


def build_chave(
    tipo: str,
    *,
    subtipo: str = "",
    nome: str = "",
    cpf: str = "",
    processo_codigo: str = "",
    processo: str = "",
    incidente: str = "",
    advogado: str = "",
) -> str:
    tipo_u = (tipo or "").strip().upper()
    if tipo_u == "CREDOR":
        sub = (subtipo or "").strip().upper()
        if sub == "NOME":
            valor = nome.strip()
        elif sub == "CPF":
            valor = cpf.strip()
        else:
            raise ValueError("Para CREDOR, indique o subtipo NOME ou CPF.")
        if not valor:
            raise ValueError(f"Informe o campo correspondente ao subtipo {sub}.")
        return _SEP.join(("CREDOR", sub, valor))
    if tipo_u == "COMPLETO":
        parts = [
            nome.strip(),
            cpf.strip(),
            processo_codigo.strip(),
            processo.strip(),
            incidente.strip(),
            advogado.strip(),
        ]
        if not any(parts):
            raise ValueError("Informe ao menos um dado (nome, CPF, processo ou advogado).")
        return _SEP.join(["COMPLETO", *parts])
    raise ValueError("Tipo inválido. Use CREDOR ou COMPLETO.")


def to_sao_paulo_wall(value: datetime) -> str:
    """Converte um DATETIME ingênuo do banco (+03:00) para o relógio de São Paulo."""
    local = value.replace(tzinfo=_DB_TZ).astimezone(_SP_TZ)
    return local.strftime("%Y-%m-%d %H:%M:%S")


def _json_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return int(value) if value == int(value) else float(value)
    if isinstance(value, datetime):
        return to_sao_paulo_wall(value)
    if isinstance(value, date):
        return value.isoformat()
    return value


def _connect():
    cfg = mysql_config()
    if not cfg:
        raise RuntimeError("MySQL da blacklist não configurado (BLACKLIST_MYSQL_*).")
    return mysql.connector.connect(**cfg)


def _where(
    *,
    q: str,
    tipo: str,
    origem: str,
) -> tuple[str, list[Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    if tipo:
        clauses.append("tipo = %s")
        params.append(tipo)
    if origem:
        clauses.append("origem = %s")
        params.append(origem)
    term = q.strip()
    if term:
        like = "%" + term.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%"
        parts = [f"{col} LIKE %s ESCAPE '\\\\'" for col in _SEARCH_COLUMNS]
        clauses.append("(" + " OR ".join(parts) + ")")
        params.extend([like] * len(_SEARCH_COLUMNS))
    if not clauses:
        return "", params
    return " WHERE " + " AND ".join(clauses), params


def list_rows(
    *,
    page: int = 1,
    q: str = "",
    tipo: str = "",
    origem: str = "",
    sort: str = "id",
    direction: str = "desc",
) -> dict[str, Any]:
    page_n = page if page > 0 else 1
    tipo_u = (tipo or "").strip().upper()
    if tipo_u and tipo_u not in _TIPOS:
        raise ValueError("Filtro de tipo inválido.")
    origem_f = _clip(origem, 255)
    sort_col = SORT_COLUMNS.get((sort or "").strip().lower(), "id")
    direc = "ASC" if (direction or "").strip().lower() == "asc" else "DESC"
    where_sql, params = _where(q=q or "", tipo=tipo_u, origem=origem_f)
    offset = (page_n - 1) * PAGE_SIZE
    conn = _connect()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(f"SELECT COUNT(*) AS n FROM blacklist{where_sql}", tuple(params))
        total = int((cur.fetchone() or {}).get("n") or 0)
        cur.execute(
            f"""
            SELECT id, tipo, nome, cpf, processo_codigo, processo,
                   incidente, advogado, origem, criado_em, atualizado_em
            FROM blacklist
            {where_sql}
            ORDER BY {sort_col} {direc}, id DESC
            LIMIT %s OFFSET %s
            """,
            tuple(params) + (PAGE_SIZE, offset),
        )
        rows = [{k: _json_value(v) for k, v in row.items()} for row in (cur.fetchall() or [])]
        cur.close()
    finally:
        conn.close()
    pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE) if total else 1
    return {
        "ok": True,
        "rows": rows,
        "total": total,
        "page": page_n,
        "page_size": PAGE_SIZE,
        "pages": pages,
        "sort": sort_col,
        "dir": direc.lower(),
    }


def origem_da_plataforma(user: dict | None) -> str:
    """Nome e sobrenome do utilizador, seguidos de « - Plataforma»."""
    if not isinstance(user, dict):
        raise ValueError("Sessão inválida para registrar a origem.")
    first = str(user.get("first_name") or "").strip()
    last = str(user.get("last_name") or "").strip()
    nome = " ".join(p for p in (first, last) if p)
    if not nome:
        nome = str(user.get("username") or "").strip()
    if not nome:
        raise ValueError("Utilizador sem nome para registrar a origem.")
    origem = f"{nome} - Plataforma"
    if len(origem) > 255:
        raise ValueError("Nome do utilizador excede o limite da origem.")
    return origem


def insert_row(payload: dict[str, Any], *, origem: str) -> dict[str, Any]:
    campos = _campos(payload)
    origem = _clip(origem, 255)
    if not origem.endswith(" - Plataforma"):
        raise ValueError("Origem inválida.")
    conn = _connect()
    try:
        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO blacklist (
                    tipo, chave, nome, cpf, processo_codigo, processo,
                    incidente, advogado, origem
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    campos["tipo"],
                    campos["chave"],
                    campos["nome"],
                    campos["cpf"],
                    campos["processo_codigo"],
                    campos["processo"],
                    campos["incidente"],
                    campos["advogado"],
                    origem,
                ),
            )
            conn.commit()
        except mysql.connector.Error as exc:
            conn.rollback()
            if _duplicate(exc):
                raise ValueError("Já existe um registro com esta chave.") from exc
            raise
        new_id = cur.lastrowid
        cur.close()
    finally:
        conn.close()
    return {"ok": True, "id": new_id}


def update_row(row_id: int, payload: dict[str, Any]) -> dict[str, Any]:
    campos = _campos(payload)
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute("SELECT id FROM blacklist WHERE id = %s", (row_id,))
        if cur.fetchone() is None:
            cur.close()
            raise LookupError("Registro não encontrado.")
        try:
            cur.execute(
                """
                UPDATE blacklist
                   SET tipo = %s, chave = %s, nome = %s, cpf = %s,
                       processo_codigo = %s, processo = %s, incidente = %s,
                       advogado = %s
                 WHERE id = %s
                """,
                (
                    campos["tipo"],
                    campos["chave"],
                    campos["nome"],
                    campos["cpf"],
                    campos["processo_codigo"],
                    campos["processo"],
                    campos["incidente"],
                    campos["advogado"],
                    row_id,
                ),
            )
            conn.commit()
        except mysql.connector.Error as exc:
            conn.rollback()
            if _duplicate(exc):
                raise ValueError("Já existe um registro com esta chave.") from exc
            raise
        cur.close()
    finally:
        conn.close()
    return {"ok": True, "id": row_id}


def delete_row(row_id: int) -> dict[str, Any]:
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM blacklist WHERE id = %s", (row_id,))
        deleted = cur.rowcount
        conn.commit()
        cur.close()
    finally:
        conn.close()
    if not deleted:
        raise LookupError("Registro não encontrado.")
    return {"ok": True}


def _blacklist_api_request(method: str, path: str, payload: dict[str, Any] | None = None) -> tuple[int, Any]:
    base = (os.getenv("API_BLACKLIST_URL") or "").strip().rstrip("/")
    key = (os.getenv("API_BLACKLIST_KEY") or "").strip()
    if not base or not key:
        raise RuntimeError("API de casos excluídos não configurada.")
    headers = {"Authorization": "Bearer " + key, "Accept": "application/json"}
    data = None
    if payload is not None:
        data = json.dumps(payload).encode()
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(base + path, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read().decode()
            return resp.status, json.loads(raw) if raw else {}
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode()
        try:
            body = json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            body = {"erro": raw or str(exc.reason)}
        return int(exc.code or 500), body


def _blacklist_api_get(path: str) -> tuple[int, Any]:
    return _blacklist_api_request("GET", path)


def controle_ler() -> dict[str, Any]:
    """Estado da exclusão na API.

    Contrato para a API implementar:
    GET /controle
    Authorization: Bearer <API_BLACKLIST_KEY>
    200: {"ativa": true} ou {"ativa": false}
    outro status: {"erro": "..."}

    404 significa que a rota ainda não existe; a tela assume ligada.
    """
    status, body = _blacklist_api_request("GET", "/controle")
    if status == 404:
        return {"ok": True, "ativa": True, "disponivel": False}
    if status != 200 or not isinstance(body, dict) or "ativa" not in body:
        erro = body.get("erro") if isinstance(body, dict) else ""
        raise RuntimeError(str(erro or "Falha ao ler o estado da blacklist."))
    return {"ok": True, "ativa": bool(body.get("ativa")), "disponivel": True}


def controle_definir(ativa: bool) -> dict[str, Any]:
    """Liga ou pausa a exclusão.

    Contrato para a API implementar:
    POST /controle
    Authorization: Bearer <API_BLACKLIST_KEY>
    Content-Type: application/json
    {"ativa": true} liga; {"ativa": false} pausa.
    200: {"ativa": true|false}
    outro status: {"erro": "..."}
    """
    status, body = _blacklist_api_request("POST", "/controle", {"ativa": bool(ativa)})
    if status != 200 or not isinstance(body, dict):
        if status == 404:
            raise RuntimeError("A API ainda não aceita ligar ou pausar a blacklist.")
        erro = body.get("erro") if isinstance(body, dict) else ""
        raise RuntimeError(str(erro or "Falha ao atualizar o estado da blacklist."))
    valor = body.get("ativa") if "ativa" in body else ativa
    return {"ok": True, "ativa": bool(valor)}


def excluidos_health() -> dict[str, Any]:
    status, body = _blacklist_api_get("/health")
    if status != 200:
        raise RuntimeError(str((body or {}).get("erro") or "Falha ao consultar a API."))
    return {"ok": True, "status": (body or {}).get("status") or "ok"}


def excluidos_datas() -> dict[str, Any]:
    status, body = _blacklist_api_get("/excluidos/datas")
    if status != 200:
        raise RuntimeError(str((body or {}).get("erro") or "Falha ao listar as datas."))
    return {"ok": True, "dias": (body or {}).get("dias") or []}


def excluidos_listar(
    *,
    data: str = "",
    tabela: str = "",
    q: str = "",
    pagina: int = 1,
    limite: int = 15,
) -> dict[str, Any]:
    page = pagina if pagina > 0 else 1
    limit = limite if 1 <= limite <= 200 else 15
    params: dict[str, str] = {"pagina": str(page), "limite": str(limit)}
    if data.strip():
        params["data"] = data.strip()
    if tabela.strip():
        params["tabela"] = tabela.strip()
    if q.strip():
        params["q"] = q.strip()
    status, body = _blacklist_api_get("/excluidos?" + urllib.parse.urlencode(params))
    if status != 200:
        raise RuntimeError(str((body or {}).get("erro") or "Falha ao listar os casos excluídos."))
    total = int((body or {}).get("total") or 0)
    return {
        "ok": True,
        "total": total,
        "pagina": int((body or {}).get("pagina") or page),
        "limite": int((body or {}).get("limite") or limit),
        "itens": (body or {}).get("itens") or [],
        "tem_proxima": page * limit < total,
    }


_EXPORT_HEADERS = (
    "Tabela",
    "ID",
    "Requerente",
    "CPF",
    "Processo",
    "Incidente",
    "Processo código",
    "Advogado",
    "Removido em",
    "Dia",
    "Tempo restante",
)


def _parse_removido(value: object) -> datetime | None:
    text = str(value or "").strip().replace("T", " ")[:19]
    if len(text) < 19:
        return None
    try:
        parsed = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None
    return parsed.replace(tzinfo=_SP_TZ)


def texto_restante(removido_em: object, agora: datetime | None = None) -> str:
    removido = _parse_removido(removido_em)
    if removido is None:
        return ""
    agora = agora or datetime.now(_SP_TZ)
    restante = (removido + timedelta(days=30)) - agora
    segundos = int(restante.total_seconds())
    if segundos <= 0:
        return "Expirado"
    dias, resto = divmod(segundos, 86400)
    horas, resto = divmod(resto, 3600)
    minutos, _segs = divmod(resto, 60)
    if dias:
        return f"{dias}d {horas:02d}h {minutos:02d}min"
    return f"{horas:02d}h {minutos:02d}min"


def excluidos_coletar(*, data: str = "", tabela: str = "", q: str = "") -> list[dict[str, Any]]:
    """Busca todas as páginas do filtro até cobrir o total."""
    from concurrent.futures import ThreadPoolExecutor

    limite = 200
    primeiro = excluidos_listar(data=data, tabela=tabela, q=q, pagina=1, limite=limite)
    itens: list[dict[str, Any]] = list(primeiro.get("itens") or [])
    total = int(primeiro.get("total") or 0)
    paginas = min(400, (total + limite - 1) // limite) if total else 1
    if paginas <= 1:
        return itens

    def _pagina(numero: int) -> list[dict[str, Any]]:
        lote = excluidos_listar(data=data, tabela=tabela, q=q, pagina=numero, limite=limite)
        return list(lote.get("itens") or [])

    with ThreadPoolExecutor(max_workers=6) as pool:
        for extra in pool.map(_pagina, range(2, paginas + 1)):
            itens.extend(extra)
    return itens


_EX_SORTS: dict[str, Any] = {
    "id": lambda row: int(row.get("id") or 0),
    "requerente": lambda row: str(row.get("requerente") or "").casefold(),
    "cpf": lambda row: str(row.get("cpf") or ""),
    "processo": lambda row: str(row.get("processo") or "").casefold(),
    "incidente": lambda row: str(row.get("incidente") or "").casefold(),
    "processo_codigo": lambda row: str(row.get("processo_codigo") or "").casefold(),
    "advogado": lambda row: str(row.get("advogado") or "").casefold(),
    "removido_em": lambda row: str(row.get("removido_em") or ""),
    "expira": lambda row: str(row.get("removido_em") or ""),
}
_ex_cache: dict[tuple[str, str, str], tuple[float, list[dict[str, Any]]]] = {}
_EX_CACHE_TTL = 45.0


def _excluidos_filtrados(*, data: str, tabela: str, q: str) -> list[dict[str, Any]]:
    chave = (data.strip(), tabela.strip(), q.strip())
    agora = time.time()
    guardado = _ex_cache.get(chave)
    if guardado and agora - guardado[0] < _EX_CACHE_TTL:
        return guardado[1]
    linhas = excluidos_coletar(data=data, tabela=tabela, q=q)
    _ex_cache[chave] = (agora, linhas)
    if len(_ex_cache) > 8:
        antiga = min(_ex_cache, key=lambda item: _ex_cache[item][0])
        _ex_cache.pop(antiga, None)
    return linhas


def excluidos_consultar(
    *,
    data: str = "",
    tabela: str = "",
    q: str = "",
    pagina: int = 1,
    limite: int = 15,
    sort: str = "id",
    direction: str = "desc",
) -> dict[str, Any]:
    page = pagina if pagina > 0 else 1
    limit = limite if 1 <= limite <= 50 else 15
    coluna = sort if sort in _EX_SORTS else "id"
    desc = str(direction or "").lower() != "asc"
    linhas = sorted(_excluidos_filtrados(data=data, tabela=tabela, q=q), key=_EX_SORTS[coluna], reverse=desc)
    total = len(linhas)
    inicio = (page - 1) * limit
    return {
        "ok": True,
        "total": total,
        "pagina": page,
        "limite": limit,
        "sort": coluna,
        "dir": "desc" if desc else "asc",
        "itens": linhas[inicio : inicio + limit],
        "tem_proxima": page * limit < total,
    }


def excluidos_exportar(*, formato: str, escopo: str, data: str = "", tabela: str = "", q: str = "") -> tuple[bytes, str, str]:
    fmt = (formato or "").strip().lower()
    if fmt not in ("csv", "xlsx"):
        raise ValueError("Formato inválido. Use csv ou xlsx.")
    if (escopo or "").strip().lower() == "todos":
        data, tabela, q = "", "", ""
    rows = excluidos_coletar(data=data, tabela=tabela, q=q)
    agora = datetime.now(_SP_TZ)
    matriz: list[list[str]] = []
    for row in rows:
        matriz.append(
            [
                str(row.get("tabela") or ""),
                str(row.get("id") or ""),
                str(row.get("requerente") or ""),
                str(row.get("cpf") or ""),
                str(row.get("processo") or ""),
                str(row.get("incidente") or ""),
                str(row.get("processo_codigo") or ""),
                str(row.get("advogado") or ""),
                str(row.get("removido_em") or ""),
                str(row.get("data") or ""),
                texto_restante(row.get("removido_em"), agora),
            ]
        )
    stamp = agora.strftime("%Y%m%d-%H%M")
    if fmt == "csv":
        import csv
        import io

        buf = io.StringIO()
        writer = csv.writer(buf, delimiter=";", lineterminator="\n")
        writer.writerow(_EXPORT_HEADERS)
        writer.writerows(matriz)
        payload = buf.getvalue().encode("utf-8-sig")
        return payload, f"casos-excluidos-{stamp}.csv", "text/csv; charset=utf-8"
    import io

    from openpyxl import Workbook

    book = Workbook()
    sheet = book.active
    sheet.title = "Casos excluídos"
    sheet.append(list(_EXPORT_HEADERS))
    for line in matriz:
        sheet.append(line)
    out = io.BytesIO()
    book.save(out)
    return (
        out.getvalue(),
        f"casos-excluidos-{stamp}.xlsx",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
