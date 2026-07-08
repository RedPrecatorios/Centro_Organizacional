"""
PRÉ Análise Processual: proxy para API externa, cache em plataforma_central e lookup precainfos.
"""

from __future__ import annotations

import json
import os
import socket
from datetime import datetime, timezone
from http.client import RemoteDisconnected
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

import mysql.connector

_TERMINAL_STATUSES = frozenset(
    {"coleta_concluida", "erro", "falha", "cancelado", "cancelada"}
)
_SYNC_BATCH_LIMIT = 20
_DEFAULT_PAGE_SIZE = 15
_DISPLAY_TZ = ZoneInfo("America/Sao_Paulo")
_DATETIME_FIELDS = frozenset({"criado_em", "atualizado_em", "synced_at"})


def _source_tz() -> ZoneInfo:
    raw = (os.getenv("PRE_ANALISE_DB_TIMEZONE") or "UTC").strip() or "UTC"
    try:
        return ZoneInfo(raw)
    except Exception:
        return ZoneInfo("UTC")


def _to_display_iso(value: object) -> object:
    """Converte datetime (ou ISO string) para America/Sao_Paulo."""
    if value is None:
        return None
    dt: datetime | None = None
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            return value
        try:
            dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return value
    else:
        return value
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=_source_tz())
    return dt.astimezone(_DISPLAY_TZ).isoformat()


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


def _eda_mysql_config() -> dict | None:
    name = (os.getenv("EDA_MYSQL_DATABASE") or "plataforma_central").strip()
    if not name:
        return None
    raw_to = (os.getenv("EDA_MYSQL_CONNECT_TIMEOUT") or "10").strip()
    try:
        connect_timeout = int(raw_to)
    except ValueError:
        connect_timeout = 10
    connect_timeout = max(1, min(connect_timeout, 30))
    return {
        "host": (os.getenv("EDA_MYSQL_HOST") or "127.0.0.1").strip(),
        "port": int(os.getenv("EDA_MYSQL_PORT", "3306")),
        "database": name,
        "user": (os.getenv("EDA_MYSQL_USER") or "root").strip(),
        "password": os.getenv("EDA_MYSQL_PASSWORD", "") or "",
        "connection_timeout": connect_timeout,
    }


def _flask_mysql_config() -> dict | None:
    name = (os.getenv("FLASK_MYSQL_DATABASE") or "").strip().strip("'\"")
    if not name:
        return None
    host = (os.getenv("FLASK_MYSQL_HOST") or "127.0.0.1").strip().strip("'\"")
    user = (os.getenv("FLASK_MYSQL_USER") or "root").strip().strip("'\"")
    password = (os.getenv("FLASK_MYSQL_PASSWORD") or "").strip().strip("'\"")
    try:
        port = int(str(os.getenv("FLASK_MYSQL_PORT") or "3306").strip())
    except ValueError:
        port = 3306
    raw_to = (os.getenv("FLASK_MYSQL_CONNECT_TIMEOUT") or "10").strip()
    try:
        connection_timeout = int(raw_to)
    except ValueError:
        connection_timeout = 10
    connection_timeout = max(1, min(connection_timeout, 30))
    return {
        "host": host,
        "port": port,
        "database": name,
        "user": user,
        "password": password,
        "connection_timeout": connection_timeout,
    }


def pre_analise_api_base() -> str | None:
    base = (os.getenv("PRE_ANALISE_API_URL") or "").strip()
    return base or None


def pre_analise_api_token() -> str | None:
    token = (os.getenv("API_TOKEN") or "").strip()
    return token or None


def is_configured() -> bool:
    return bool(pre_analise_api_base() and pre_analise_api_token())


def api_health() -> tuple[dict, int]:
    """GET /api/pre-analise/health na API externa."""
    if not is_configured():
        return (
            {
                "ok": False,
                "healthy": False,
                "error": "API de pré-análise não configurada.",
            },
            503,
        )
    out, code = proxy_pre_analise_api_request(
        "GET",
        "/api/pre-analise/health",
        timeout=min(10, _api_timeout()),
    )
    healthy = code in (200, 202) and bool(out.get("ok", True))
    if "healthy" in out:
        healthy = bool(out.get("healthy"))
    return (
        {
            "ok": healthy,
            "healthy": healthy,
            "api": out,
        },
        200 if healthy else (code if code >= 400 else 503),
    )


def poll_interval_ms() -> int:
    try:
        value = int((os.getenv("PRE_ANALISE_POLL_INTERVAL_MS") or "5000").strip())
    except ValueError:
        value = 5000
    return max(2000, min(value, 60000))


def _api_timeout() -> int:
    try:
        timeout = int((os.getenv("PRE_ANALISE_API_TIMEOUT") or "30").strip())
    except ValueError:
        timeout = 30
    return max(5, min(timeout, 120))


def _api_headers() -> dict[str, str]:
    headers = {"Accept": "application/json"}
    token = pre_analise_api_token()
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def _api_is_timeout(exc: BaseException) -> bool:
    if isinstance(exc, (TimeoutError, socket.timeout)):
        return True
    if isinstance(exc, URLError):
        reason = getattr(exc, "reason", None)
        return isinstance(reason, (TimeoutError, socket.timeout))
    return False


def _api_connection_error(url: str, exc: BaseException) -> tuple[dict, int]:
    if _api_is_timeout(exc):
        return (
            {
                "ok": False,
                "timeout": True,
                "error": (
                    "Tempo esgotado à espera da API de pré-análise. "
                    "Tente novamente em instantes."
                ),
            },
            504,
        )
    return (
        {
            "ok": False,
            "error": (
                f"Não foi possível contactar a API de pré-análise ({url}). "
                f"Detalhe: {exc}"
            ),
        },
        502,
    )


def proxy_pre_analise_api_request(
    method: str,
    path: str,
    *,
    body: bytes | None = None,
    timeout: int | None = None,
) -> tuple[dict, int]:
    base = pre_analise_api_base()
    if not base:
        return (
            {
                "ok": False,
                "error": (
                    "API de pré-análise não configurada. "
                    "Defina PRE_ANALISE_API_URL e API_TOKEN no .env."
                ),
            },
            503,
        )
    if not pre_analise_api_token():
        return (
            {
                "ok": False,
                "error": "API_TOKEN não configurado no .env.",
            },
            503,
        )
    url = base.rstrip("/") + path
    headers = _api_headers()
    if body is not None:
        headers = {
            **headers,
            "Content-Type": "application/json; charset=utf-8",
        }
    req = Request(url, data=body, method=method, headers=headers)
    req_timeout = timeout if timeout is not None else _api_timeout()
    try:
        with urlopen(req, timeout=req_timeout) as resp:
            raw = resp.read()
            code = resp.getcode()
    except HTTPError as e:
        raw = e.read()
        code = e.code
    except (URLError, RemoteDisconnected, ConnectionResetError, TimeoutError, socket.timeout, OSError) as e:
        return _api_connection_error(url, e)
    try:
        out = json.loads(raw.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        return (
            {"ok": False, "error": "Resposta inválida da API de pré-análise (JSON esperado)."},
            502,
        )
    if not isinstance(out, dict):
        return ({"ok": False, "error": "Resposta inválida da API de pré-análise."}, 502)
    if code in (200, 201, 202, 400, 403, 404, 409, 422, 500):
        return out, code
    return out, 502


def _is_terminal_status(status: str | None, bloqueado: bool) -> bool:
    if bloqueado:
        return True
    norm = (status or "").strip().lower()
    return norm in _TERMINAL_STATUSES


def _serialize_row(row: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for k, v in row.items():
        if isinstance(v, datetime) or k in _DATETIME_FIELDS:
            out[k] = _to_display_iso(v)
        elif k == "status_payload" and isinstance(v, (str, bytes)):
            try:
                out[k] = json.loads(v) if v else None
            except (json.JSONDecodeError, TypeError):
                out[k] = None
        else:
            out[k] = v
    return out


def _ensure_table(cur) -> None:
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS pre_analise_casos (
            caso_id CHAR(36) PRIMARY KEY,
            id_externo VARCHAR(64) NULL,
            numero_cumprimento VARCHAR(200) NOT NULL,
            numero_incidente VARCHAR(50) NOT NULL,
            nome_credor VARCHAR(500) NULL,
            numero_depre_input VARCHAR(200) NULL,
            status VARCHAR(80) NOT NULL DEFAULT 'mapeamento_iniciado',
            fase_atual INT NULL,
            numero_depre VARCHAR(200) NULL,
            numero_processo_principal VARCHAR(200) NULL,
            motivo_blacklist TEXT NULL,
            blacklist_codigo VARCHAR(80) NULL,
            caminho_pasta VARCHAR(500) NULL,
            bloqueado TINYINT(1) NOT NULL DEFAULT 0,
            mensagem TEXT NULL,
            status_payload JSON NULL,
            polling_ativo TINYINT(1) NOT NULL DEFAULT 1,
            criado_em DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
            atualizado_em DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
                ON UPDATE CURRENT_TIMESTAMP(6),
            criado_por_user_id INT NULL,
            INDEX idx_polling (polling_ativo, atualizado_em),
            INDEX idx_criado_em (criado_em)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """
    )


def _db_connect():
    cfg = _eda_mysql_config()
    if not cfg:
        raise RuntimeError("MySQL da plataforma não configurado (EDA_MYSQL_*).")
    return mysql.connector.connect(**cfg, charset="utf8mb4", collation="utf8mb4_unicode_ci")


def resolve_id_externo(
    processo: str,
    incidente: str,
    override: str | None = None,
) -> tuple[str | None, str | None]:
    """
    Retorna (id_externo, aviso).
    Se override informado, usa-o. Senão busca em precainfosnew por processo+incidente.
    """
    if override:
        return override, None

    cfg = _flask_mysql_config()
    if not cfg:
        return None, None

    conn = None
    try:
        conn = mysql.connector.connect(**cfg, charset="utf8mb4", collation="utf8mb4_unicode_ci")
        cur = conn.cursor(dictionary=True)
        cur.execute("SHOW TABLES LIKE 'precainfosnew'")
        if not cur.fetchone():
            return None, None

        cur.execute("SHOW COLUMNS FROM `precainfosnew`")
        raw_cols = cur.fetchall() or []
        fields: set[str] = set()
        for r in raw_cols:
            if isinstance(r, dict):
                nm = r.get("Field") or r.get("field")
                if nm:
                    fields.add(str(nm))
            else:
                fields.add(str(r[0]))

        f_proc = _pick_field(
            fields,
            "numero_de_processo",
            "Numero_de_processo",
            "Numero_de_Processo",
            "processo",
            "Processo",
        )
        f_inc = _pick_field(
            fields,
            "numero_do_incidente",
            "Numero_do_incidente",
            "Numero_do_Incidente",
            "numero_de_incidente",
            "incidente",
            "Incidente",
        )
        if not f_proc:
            return None, None

        if f_inc:
            cur.execute(
                f"""
                SELECT id
                FROM precainfosnew
                WHERE TRIM(COALESCE(`{f_proc}`, '')) = %s
                  AND TRIM(COALESCE(`{f_inc}`, '')) = %s
                ORDER BY id DESC
                LIMIT 2
                """,
                (processo, incidente),
            )
        else:
            cur.execute(
                f"""
                SELECT id
                FROM precainfosnew
                WHERE TRIM(COALESCE(`{f_proc}`, '')) = %s
                ORDER BY id DESC
                LIMIT 2
                """,
                (processo,),
            )

        rows = cur.fetchall() or []
        if not rows:
            return None, None
        aviso = None
        if len(rows) > 1:
            aviso = (
                "Vários registos em precainfosnew para este processo/incidente; "
                "usado o id mais recente."
            )
        pid = rows[0].get("id")
        return (str(pid) if pid is not None else None), aviso
    except Exception:
        return None, None
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def _extract_nome_credor(payload: dict[str, Any]) -> str | None:
    for key in ("nome_credor", "nome", "credor", "requerente", "nome_requerente"):
        raw = payload.get(key)
        if raw is None:
            continue
        text = str(raw).strip()
        if text:
            return text
    return None


def _apply_status_snapshot(cur, caso_id: str, payload: dict[str, Any]) -> None:
    status = str(payload.get("status") or "").strip() or "desconhecido"
    bloqueado = bool(payload.get("bloqueado"))
    polling_ativo = 0 if _is_terminal_status(status, bloqueado) else 1
    fase = payload.get("fase_atual")
    try:
        fase_int = int(fase) if fase is not None else None
    except (TypeError, ValueError):
        fase_int = None
    nome_credor = _extract_nome_credor(payload)

    cur.execute(
        """
        UPDATE pre_analise_casos SET
            id_externo = COALESCE(%s, id_externo),
            nome_credor = COALESCE(%s, nome_credor),
            status = %s,
            fase_atual = %s,
            numero_depre = COALESCE(%s, numero_depre),
            numero_processo_principal = COALESCE(%s, numero_processo_principal),
            motivo_blacklist = %s,
            blacklist_codigo = %s,
            caminho_pasta = %s,
            bloqueado = %s,
            mensagem = %s,
            status_payload = %s,
            polling_ativo = %s
        WHERE caso_id = %s
        """,
        (
            payload.get("id_externo"),
            nome_credor,
            status,
            fase_int,
            payload.get("numero_depre"),
            payload.get("numero_processo_principal"),
            payload.get("motivo_blacklist"),
            payload.get("blacklist_codigo"),
            payload.get("caminho_pasta"),
            1 if bloqueado else 0,
            payload.get("mensagem"),
            json.dumps(payload, ensure_ascii=False),
            polling_ativo,
            caso_id,
        ),
    )


def _upsert_caso_from_inputs(
    cur,
    *,
    caso_id: str,
    id_externo: str | None,
    numero_cumprimento: str,
    numero_incidente: str,
    nome_credor: str | None,
    numero_depre_input: str | None,
    status: str,
    mensagem: str | None,
    user_id: int | None,
) -> None:
    """Insere ou actualiza caso local (suporta idempotência do iniciar na API externa)."""
    cur.execute(
        """
        INSERT INTO pre_analise_casos (
            caso_id, id_externo, numero_cumprimento, numero_incidente,
            nome_credor, numero_depre_input, status, mensagem,
            polling_ativo, criado_por_user_id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 1, %s)
        ON DUPLICATE KEY UPDATE
            id_externo = COALESCE(VALUES(id_externo), id_externo),
            numero_cumprimento = VALUES(numero_cumprimento),
            numero_incidente = VALUES(numero_incidente),
            nome_credor = COALESCE(VALUES(nome_credor), nome_credor),
            numero_depre_input = COALESCE(VALUES(numero_depre_input), numero_depre_input),
            status = VALUES(status),
            mensagem = VALUES(mensagem),
            polling_ativo = IF(
                polling_ativo = 0 AND VALUES(status) NOT IN (
                    'coleta_concluida', 'erro', 'falha', 'cancelado', 'cancelada'
                ),
                1,
                polling_ativo
            )
        """,
        (
            caso_id,
            id_externo,
            numero_cumprimento,
            numero_incidente,
            nome_credor or None,
            numero_depre_input or None,
            status,
            mensagem,
            user_id,
        ),
    )


def _upsert_caso_from_api_item(cur, item: dict[str, Any]) -> None:
    """Persiste item devolvido por GET /api/pre-analise/casos."""
    caso_id = str(item.get("caso_id") or "").strip()
    if not caso_id:
        return
    status = str(item.get("status") or "desconhecido").strip()
    bloqueado = bool(item.get("bloqueado"))
    polling_ativo = 0 if _is_terminal_status(status, bloqueado) else 1
    fase = item.get("fase_atual")
    try:
        fase_int = int(fase) if fase is not None else None
    except (TypeError, ValueError):
        fase_int = None

    cur.execute(
        """
        INSERT INTO pre_analise_casos (
            caso_id, id_externo, numero_cumprimento, numero_incidente,
            nome_credor, numero_depre_input, status, fase_atual,
            numero_depre, numero_processo_principal, motivo_blacklist,
            blacklist_codigo, caminho_pasta, bloqueado, mensagem,
            status_payload, polling_ativo
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON DUPLICATE KEY UPDATE
            id_externo = COALESCE(VALUES(id_externo), id_externo),
            numero_cumprimento = COALESCE(VALUES(numero_cumprimento), numero_cumprimento),
            numero_incidente = COALESCE(VALUES(numero_incidente), numero_incidente),
            nome_credor = COALESCE(VALUES(nome_credor), nome_credor),
            numero_depre_input = COALESCE(VALUES(numero_depre_input), numero_depre_input),
            status = VALUES(status),
            fase_atual = VALUES(fase_atual),
            numero_depre = VALUES(numero_depre),
            numero_processo_principal = VALUES(numero_processo_principal),
            motivo_blacklist = VALUES(motivo_blacklist),
            blacklist_codigo = VALUES(blacklist_codigo),
            caminho_pasta = VALUES(caminho_pasta),
            bloqueado = VALUES(bloqueado),
            mensagem = VALUES(mensagem),
            status_payload = VALUES(status_payload),
            polling_ativo = VALUES(polling_ativo)
        """,
        (
            caso_id,
            item.get("id_externo"),
            item.get("numero_cumprimento") or item.get("numero_de_processo"),
            item.get("numero_incidente") or item.get("numero_do_incidente"),
            _extract_nome_credor(item),
            item.get("numero_depre_input") or item.get("numero_depre"),
            status,
            fase_int,
            item.get("numero_depre"),
            item.get("numero_processo_principal"),
            item.get("motivo_blacklist"),
            item.get("blacklist_codigo"),
            item.get("caminho_pasta"),
            1 if bloqueado else 0,
            item.get("mensagem"),
            json.dumps(item, ensure_ascii=False),
            polling_ativo,
        ),
    )


def _fetch_status_lote(
    caso_ids: list[str],
) -> tuple[list[dict[str, Any]], list[dict[str, str]], list[str]]:
    """POST /api/pre-analise/status/lote; fallback para GET individual."""
    ids = [c for c in caso_ids if c]
    if not ids:
        return [], [], []

    body = json.dumps({"caso_ids": ids}, ensure_ascii=False).encode("utf-8")
    api_out, api_code = proxy_pre_analise_api_request(
        "POST",
        "/api/pre-analise/status/lote",
        body=body,
    )
    if api_code in (200, 202) and isinstance(api_out, dict):
        items = api_out.get("items")
        if isinstance(items, list):
            parsed = [i for i in items if isinstance(i, dict)]
            returned_ids = {
                str(i.get("caso_id") or "").strip()
                for i in parsed
                if str(i.get("caso_id") or "").strip()
            }
            not_found_ids = [cid for cid in ids if cid not in returned_ids]
            sync_errors = [
                {
                    "caso_id": cid,
                    "error": "Caso não encontrado na API (removido do cache local).",
                }
                for cid in not_found_ids
            ]
            return parsed, sync_errors, not_found_ids

    sync_errors: list[dict[str, str]] = []
    results: list[dict[str, Any]] = []
    not_found_ids: list[str] = []
    for caso_id in ids:
        one_out, one_code = proxy_pre_analise_api_request(
            "GET",
            f"/api/pre-analise/{caso_id}/status",
        )
        if one_code in (200, 202) and isinstance(one_out, dict):
            results.append(one_out)
        elif one_code == 404:
            not_found_ids.append(caso_id)
            sync_errors.append(
                {"caso_id": caso_id, "error": "Caso não encontrado na API (removido do cache local)."}
            )
        else:
            err = (
                one_out.get("mensagem")
                or one_out.get("detail")
                or one_out.get("error")
                or f"HTTP {one_code}"
            )
            sync_errors.append({"caso_id": caso_id, "error": str(err)})
    return results, sync_errors, not_found_ids


def _reconciliar_cache_da_api(
    cur,
    *,
    page: int = 1,
    limit: int = 100,
    prune: bool = False,
) -> bool:
    """
    GET /api/pre-analise/casos — alinha cache local com a API.
    Com ``prune=True``, remove do cache local casos que já não existem na API.
    """
    api_ids: set[str] = set()
    current_page = max(1, page)
    limit = max(1, min(limit, 200))
    got_any_page = False

    while True:
        qs = f"/api/pre-analise/casos?page={current_page}&limit={limit}"
        api_out, api_code = proxy_pre_analise_api_request("GET", qs)
        if api_code not in (200, 202) or not isinstance(api_out, dict):
            return got_any_page

        items = api_out.get("items")
        if not isinstance(items, list):
            items = api_out.get("casos")
        if not isinstance(items, list):
            return got_any_page

        got_any_page = True
        if not items:
            break

        for item in items:
            if not isinstance(item, dict):
                continue
            cid = str(item.get("caso_id") or "").strip()
            if cid:
                api_ids.add(cid)
            _upsert_caso_from_api_item(cur, item)

        if len(items) < limit:
            break
        current_page += 1

    if prune and got_any_page:
        if api_ids:
            placeholders = ", ".join(["%s"] * len(api_ids))
            cur.execute(
                f"DELETE FROM pre_analise_casos WHERE caso_id NOT IN ({placeholders})",
                tuple(api_ids),
            )
        else:
            cur.execute("DELETE FROM pre_analise_casos")

    return got_any_page


def _remover_caso_local(cur, caso_id: str) -> bool:
    cur.execute("DELETE FROM pre_analise_casos WHERE caso_id = %s", (caso_id,))
    return cur.rowcount > 0


def _count_active(cur) -> int:
    cur.execute("SELECT COUNT(*) AS n FROM pre_analise_casos WHERE polling_ativo = 1")
    row = cur.fetchone() or {}
    return int(row.get("n") or 0)


def _list_casos_page(cur, page: int, page_size: int) -> tuple[list[dict], int]:
    cur.execute("SELECT COUNT(*) AS n FROM pre_analise_casos")
    total = int((cur.fetchone() or {}).get("n") or 0)
    offset = (page - 1) * page_size
    cur.execute(
        """
        SELECT
            caso_id, id_externo, numero_cumprimento, numero_incidente,
            nome_credor, numero_depre_input, status, fase_atual,
            numero_depre, numero_processo_principal, motivo_blacklist,
            blacklist_codigo, caminho_pasta, bloqueado, mensagem,
            polling_ativo, criado_em, atualizado_em, criado_por_user_id
        FROM pre_analise_casos
        ORDER BY criado_em DESC
        LIMIT %s OFFSET %s
        """,
        (page_size, offset),
    )
    rows = [_serialize_row(r) for r in (cur.fetchall() or [])]
    return rows, total


def iniciar_caso(
    data: dict[str, Any],
    *,
    user_id: int | None = None,
) -> tuple[dict, int]:
    if not is_configured():
        return (
            {
                "ok": False,
                "error": (
                    "API de pré-análise não configurada. "
                    "Defina PRE_ANALISE_API_URL e API_TOKEN no .env."
                ),
            },
            503,
        )

    processo = str(
        data.get("numero_de_processo")
        or data.get("numero_cumprimento")
        or ""
    ).strip()
    incidente = str(
        data.get("numero_do_incidente")
        or data.get("numero_incidente")
        or ""
    ).strip()
    nome = str(data.get("nome") or data.get("nome_credor") or "").strip()
    depre_input = str(data.get("depre") or data.get("numero_depre") or "").strip()
    override_raw = str(data.get("id_externo") or "").strip()

    if not processo:
        return {"ok": False, "error": "numero_de_processo é obrigatório."}, 400
    if not incidente:
        return {"ok": False, "error": "numero_do_incidente é obrigatório."}, 400

    id_externo, aviso_lookup = resolve_id_externo(
        processo,
        incidente,
        override_raw or None,
    )

    payload = {
        "numero_cumprimento": processo,
        "numero_incidente": incidente,
        "nome_credor": nome or None,
        "numero_depre": depre_input or None,
        "id_externo": id_externo,
    }
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    api_out, api_code = proxy_pre_analise_api_request(
        "POST",
        "/api/pre-analise/iniciar",
        body=body,
    )

    if api_code not in (200, 201, 202):
        err = (
            api_out.get("mensagem")
            or api_out.get("detail")
            or api_out.get("error")
            or "Não foi possível iniciar a pré-análise."
        )
        return {"ok": False, "error": str(err), "api": api_out}, api_code if api_code >= 400 else 502

    caso_id = str(api_out.get("caso_id") or "").strip()
    if not caso_id:
        return (
            {"ok": False, "error": "API não retornou caso_id.", "api": api_out},
            502,
        )

    status = str(api_out.get("status") or "mapeamento_iniciado").strip()
    mensagem = str(api_out.get("mensagem") or "Processamento iniciado.").strip()

    conn = None
    try:
        conn = _db_connect()
        cur = conn.cursor(dictionary=True)
        _ensure_table(cur)
        _upsert_caso_from_inputs(
            cur,
            caso_id=caso_id,
            id_externo=id_externo,
            numero_cumprimento=processo,
            numero_incidente=incidente,
            nome_credor=nome or None,
            numero_depre_input=depre_input or None,
            status=status,
            mensagem=mensagem,
            user_id=user_id,
        )
        conn.commit()
    except Exception as e:
        return {"ok": False, "error": f"Falha ao gravar caso localmente: {e}"}, 500
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

    out: dict[str, Any] = {
        "ok": True,
        "caso_id": caso_id,
        "id_externo": id_externo,
        "status": status,
        "mensagem": mensagem,
    }
    if api_out.get("reutilizado") or api_out.get("ja_existia"):
        out["reutilizado"] = True
    if aviso_lookup:
        out["aviso"] = aviso_lookup
    return out, 200


def list_casos(
    page: int = 1,
    page_size: int = _DEFAULT_PAGE_SIZE,
    *,
    reconciliar: bool = False,
) -> tuple[dict, int]:
    page = max(1, page)
    page_size = max(1, min(page_size, 100))
    conn = None
    try:
        conn = _db_connect()
        cur = conn.cursor(dictionary=True)
        _ensure_table(cur)
        if reconciliar and is_configured():
            _reconciliar_cache_da_api(cur, prune=True)
            conn.commit()
        items, total = _list_casos_page(cur, page, page_size)
        active_count = _count_active(cur)
        pages = max(1, (total + page_size - 1) // page_size)
        return (
            {
                "ok": True,
                "items": items,
                "page": page,
                "page_size": page_size,
                "total": total,
                "pages": pages,
                "active_count": active_count,
            },
            200,
        )
    except Exception as e:
        return {"ok": False, "error": str(e)}, 500
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def get_caso_status(caso_id: str) -> tuple[dict, int]:
    caso_id = (caso_id or "").strip()
    if not caso_id:
        return {"ok": False, "error": "caso_id é obrigatório."}, 400

    api_out, api_code = proxy_pre_analise_api_request(
        "GET",
        f"/api/pre-analise/{caso_id}/status",
    )
    if api_code not in (200, 202):
        err = (
            api_out.get("mensagem")
            or api_out.get("detail")
            or api_out.get("error")
            or "Não foi possível obter o status."
        )
        return {"ok": False, "error": str(err), "api": api_out}, api_code if api_code >= 400 else 502

    conn = None
    try:
        conn = _db_connect()
        cur = conn.cursor(dictionary=True)
        _ensure_table(cur)
        _apply_status_snapshot(cur, caso_id, api_out)
        conn.commit()
    except Exception:
        pass
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

    return {"ok": True, **api_out}, 200


def get_status_por_externo(id_externo: str) -> tuple[dict, int]:
    id_externo = (id_externo or "").strip()
    if not id_externo:
        return {"ok": False, "error": "id_externo é obrigatório."}, 400

    api_out, api_code = proxy_pre_analise_api_request(
        "GET",
        f"/api/pre-analise/por-externo/{quote(id_externo, safe='')}",
    )
    if api_code not in (200, 202):
        err = (
            api_out.get("mensagem")
            or api_out.get("detail")
            or api_out.get("error")
            or "Não foi possível obter o status por id_externo."
        )
        return {"ok": False, "error": str(err), "api": api_out}, api_code if api_code >= 400 else 502

    caso_id = str(api_out.get("caso_id") or "").strip()
    conn = None
    try:
        if caso_id:
            conn = _db_connect()
            cur = conn.cursor(dictionary=True)
            _ensure_table(cur)
            _apply_status_snapshot(cur, caso_id, api_out)
            conn.commit()
    except Exception:
        pass
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

    return {"ok": True, **api_out}, 200


def cancelar_caso(caso_id: str) -> tuple[dict, int]:
    caso_id = (caso_id or "").strip()
    if not caso_id:
        return {"ok": False, "error": "caso_id é obrigatório."}, 400
    if not is_configured():
        return (
            {"ok": False, "error": "API de pré-análise não configurada."},
            503,
        )

    api_out, api_code = proxy_pre_analise_api_request(
        "POST",
        f"/api/pre-analise/{caso_id}/cancelar",
    )
    if api_code not in (200, 201, 202):
        err = (
            api_out.get("mensagem")
            or api_out.get("detail")
            or api_out.get("error")
            or "Não foi possível cancelar o caso."
        )
        return {"ok": False, "error": str(err), "api": api_out}, api_code if api_code >= 400 else 502

    conn = None
    try:
        conn = _db_connect()
        cur = conn.cursor(dictionary=True)
        _ensure_table(cur)
        status = str(api_out.get("status") or "cancelado").strip()
        mensagem = api_out.get("mensagem")
        cur.execute(
            """
            UPDATE pre_analise_casos
            SET status = %s,
                mensagem = %s,
                polling_ativo = 0,
                bloqueado = 0
            WHERE caso_id = %s
            """,
            (status, mensagem, caso_id),
        )
        if isinstance(api_out, dict) and api_out.get("caso_id"):
            _apply_status_snapshot(cur, caso_id, api_out)
        conn.commit()
    except Exception as e:
        return {"ok": False, "error": f"Cancelado na API, mas falha ao actualizar cache: {e}"}, 500
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

    return {"ok": True, **api_out}, 200


def reconciliar_casos(page: int = 1, limit: int = 100) -> tuple[dict, int]:
    if not is_configured():
        return (
            {"ok": False, "error": "API de pré-análise não configurada."},
            503,
        )
    conn = None
    try:
        conn = _db_connect()
        cur = conn.cursor(dictionary=True)
        _ensure_table(cur)
        ok = _reconciliar_cache_da_api(cur, prune=True)
        conn.commit()
        if not ok:
            return {"ok": False, "error": "Não foi possível reconciliar com a API externa."}, 502
        items, total = _list_casos_page(cur, 1, _DEFAULT_PAGE_SIZE)
        return {
            "ok": True,
            "reconciliado": True,
            "total": total,
            "active_count": _count_active(cur),
            "items": items,
        }, 200
    except Exception as e:
        return {"ok": False, "error": str(e)}, 500
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def sincronizar_casos(page: int = 1, page_size: int = _DEFAULT_PAGE_SIZE) -> tuple[dict, int]:
    if not is_configured():
        return (
            {
                "ok": False,
                "error": (
                    "API de pré-análise não configurada. "
                    "Defina PRE_ANALISE_API_URL e API_TOKEN no .env."
                ),
            },
            503,
        )

    page = max(1, page)
    page_size = max(1, min(page_size, 100))
    conn = None
    try:
        conn = _db_connect()
        cur = conn.cursor(dictionary=True)
        _ensure_table(cur)

        cur.execute(
            """
            SELECT caso_id
            FROM pre_analise_casos
            WHERE polling_ativo = 1
            ORDER BY atualizado_em ASC
            LIMIT %s
            """,
            (_SYNC_BATCH_LIMIT,),
        )
        active_rows = cur.fetchall() or []
        caso_ids = [
            str(row.get("caso_id") or "").strip()
            for row in active_rows
            if str(row.get("caso_id") or "").strip()
        ]
        sync_errors: list[dict[str, str]] = []

        if caso_ids:
            status_items, sync_errors, not_found_ids = _fetch_status_lote(caso_ids)
            for index, api_out in enumerate(status_items):
                cid = str(api_out.get("caso_id") or "").strip()
                if not cid and index < len(caso_ids):
                    cid = caso_ids[index]
                if cid:
                    _apply_status_snapshot(cur, cid, api_out)
            for cid in not_found_ids:
                _remover_caso_local(cur, cid)

        conn.commit()
        items, total = _list_casos_page(cur, page, page_size)
        active_count = _count_active(cur)
        pages = max(1, (total + page_size - 1) // page_size)
        out: dict[str, Any] = {
            "ok": True,
            "items": items,
            "page": page,
            "page_size": page_size,
            "total": total,
            "pages": pages,
            "active_count": active_count,
            "synced_at": _to_display_iso(datetime.now(timezone.utc)),
        }
        if sync_errors:
            out["sync_errors"] = sync_errors
        return out, 200
    except Exception as e:
        return {"ok": False, "error": str(e)}, 500
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def excluir_caso(caso_id: str) -> tuple[dict, int]:
    """Remove caso do cache local da plataforma (não apaga na API externa)."""
    caso_id = (caso_id or "").strip()
    if not caso_id:
        return {"ok": False, "error": "caso_id é obrigatório."}, 400
    conn = None
    try:
        conn = _db_connect()
        cur = conn.cursor(dictionary=True)
        _ensure_table(cur)
        removed = _remover_caso_local(cur, caso_id)
        conn.commit()
        if not removed:
            return {"ok": False, "error": "Caso não encontrado no cache local."}, 404
        return {"ok": True, "caso_id": caso_id, "mensagem": "Caso removido da lista da plataforma."}, 200
    except Exception as e:
        return {"ok": False, "error": str(e)}, 500
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
