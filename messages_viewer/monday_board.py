# -*- coding: utf-8 -*-
"""Consulta Monday sem Flask: já atribuído vs livre vs rejeitado."""

from __future__ import annotations

import json
import os
import re
import unicodedata
from pathlib import Path
from typing import Any

import requests

MONDAY_API_URL = "https://api.monday.com/v2"
_NON_DIGITS = re.compile(r"\D+")

_TITLE_PROCESSO = ("processo", "n processo", "nº processo", "numero processo")
_TITLE_INCIDENTE = ("incidente", "n incidente", "nº incidente", "numero incidente")
_TITLE_COMPRADOR = ("comprador", "compradores", "people", "pessoa")
_TITLE_PROPOSTA = ("proposta enviada", "proposta")


def _env(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


_MONDAY_ENV_KEYS = {
    "MONDAY_API_TOKEN",
    "MONDAY_BOARD_ID",
    "MONDAY_COL_PROCESSO",
    "MONDAY_COL_INCIDENTE",
    "MONDAY_COL_COMPRADOR",
    "MONDAY_COL_PROPOSTA_ENVIADA",
    "MONDAY_PEOPLE_EMAIL_MAP",
    "MONDAY_PEOPLE_ID",
    "MONDAY_PRECHECK_TIMEOUT",
    "PACOTE_CALCULO_PATH",
    "MONDAY_REQUESTS_API_URL",
    "API_KEY_MONDAY_REQUESTS",
}


def _read_env_file(env_path: Path) -> dict[str, str]:
    try:
        from dotenv import dotenv_values

        raw = dotenv_values(env_path) or {}
        return {str(k): str(v) for k, v in raw.items() if k and v}
    except Exception:
        out: dict[str, str] = {}
        try:
            for line in env_path.read_text(encoding="utf-8").splitlines():
                text = line.strip()
                if not text or text.startswith("#") or "=" not in text:
                    continue
                key, _, value = text.partition("=")
                key = key.strip()
                value = value.strip().strip("'").strip('"')
                if key and value:
                    out[key] = value
        except OSError:
            return {}
        return out


def _graphql_ready() -> bool:
    return bool(_env("MONDAY_API_TOKEN") and _env("MONDAY_BOARD_ID"))


def _si_lookup_ready() -> bool:
    return bool(_env("MONDAY_REQUESTS_API_URL") and _env("API_KEY_MONDAY_REQUESTS"))


def _ensure_monday_env() -> None:
    if _graphql_ready() and _si_lookup_ready():
        return
    pkg = _env("PACOTE_CALCULO_PATH")
    roots: list[Path] = []
    if pkg:
        roots.append(Path(pkg).expanduser())
    here = Path(__file__).resolve()
    roots.append(here.parents[1])  # Centro_Organizacional/.env
    roots.append(here.parents[1] / "pacote_calculo_pos_comprovante")
    roots.append(here.parents[2] / "pacote_calculo_pos_comprovante")
    for root in roots:
        env_path = root if root.suffix == ".env" else root / ".env"
        if not env_path.is_file():
            continue
        loaded = False
        for key, value in _read_env_file(env_path).items():
            if key in _MONDAY_ENV_KEYS and value and not _env(key):
                os.environ[key] = str(value)
                loaded = True
        if loaded and _graphql_ready() and _si_lookup_ready():
            return


def si_lookup_configured() -> bool:
    _ensure_monday_env()
    return _si_lookup_ready()


def is_configured() -> bool:
    _ensure_monday_env()
    return _graphql_ready() or _si_lookup_ready()


def _timeout() -> float:
    try:
        value = float(_env("MONDAY_PRECHECK_TIMEOUT", "30") or "30")
    except ValueError:
        value = 30.0
    return max(5.0, min(value, 90.0))


def _board_id() -> str:
    return _env("MONDAY_BOARD_ID")


def _norm_inc(value: Any) -> str:
    digits = _NON_DIGITS.sub("", str(value or ""))
    if not digits:
        return str(value or "").strip()
    return str(int(digits))


def _norm_proc(value: Any) -> str:
    return _NON_DIGITS.sub("", str(value or ""))


def _format_cnj(digits: str) -> str:
    d = _NON_DIGITS.sub("", str(digits or ""))
    if len(d) == 20:
        return f"{d[0:7]}-{d[7:9]}.{d[9:13]}.{d[13]}.{d[14:16]}.{d[16:20]}"
    return ""


def _proc_search_values(processo: str) -> list[str]:
    raw = str(processo or "").strip()
    digits = _norm_proc(raw)
    out: list[str] = []
    for value in (raw, digits, _format_cnj(digits)):
        text = str(value or "").strip()
        if text and text not in out:
            out.append(text)
    return out


def _inc_search_values(incidente: str) -> list[str]:
    raw = str(incidente or "").strip()
    norm = _norm_inc(incidente)
    out: list[str] = []
    for value in (raw, norm):
        text = str(value or "").strip()
        if text and text not in out:
            out.append(text)
    if norm.isdigit():
        padded = norm.zfill(2)
        if padded not in out:
            out.append(padded)
    return out


_MOTIVO_MANTEM_COMPRADOR = frozenset(
    {
        "cliente cedeu",
        "comprado",
        "pago/acordo",
        "pago acordo",
        "penhorado",
        "inapto p/ compra",
        "inapto p compra",
        "interditado",
        "blacklist",
        "em grau de recurso",
        "acordo assinado",
    }
)


def _as_flag(value: Any) -> bool | None:
    if value is True:
        return True
    if value is False:
        return False
    if value is None:
        return None
    text = str(value).strip().lower()
    if text in {"1", "true", "sim", "yes"}:
        return True
    if text in {"0", "false", "nao", "não", "no", ""}:
        return False
    return None


def _first_text(*bags: Any, key: str) -> str:
    for bag in bags:
        if not isinstance(bag, dict):
            continue
        text = str(bag.get(key) or "").strip()
        if text and text != "-":
            return text
    return ""


def _is_sem_comprador(name: str) -> bool:
    folded = _fold(name)
    return not folded or folded in {"desligados", "desligado"}


def _motivo_mantem_comprador(motivo: str) -> bool:
    folded = _fold(motivo)
    if not folded:
        return False
    compact = " ".join(folded.replace("/", " ").split())
    return folded in _MOTIVO_MANTEM_COMPRADOR or compact in _MOTIVO_MANTEM_COMPRADOR


def _is_recusado_justificar(label: str) -> bool:
    folded = _fold(label)
    return "recusado" in folded and "justific" in folded


def _infer_pode_trocar(proposta: str, motivo: str) -> bool:
    if _is_recusado_justificar(proposta) or _is_rejeitado(proposta):
        return not _motivo_mantem_comprador(motivo)
    return False


def _collect_compradores(*bags: Any) -> list[str]:
    names: list[str] = []
    seen: set[str] = set()
    for bag in bags:
        if not isinstance(bag, dict):
            continue
        one = str(bag.get("comprador") or "").strip()
        extra = bag.get("compradores")
        parts = [one] if one else []
        if isinstance(extra, list):
            parts.extend(str(n or "").strip() for n in extra)
        elif extra and not one:
            parts.append(str(extra).strip())
        for part in parts:
            if _is_sem_comprador(part):
                continue
            key = part.casefold()
            if not part or key in seen:
                continue
            seen.add(key)
            names.append(part)
    return names


def map_si_monday_payload(
    payload: dict[str, Any] | None,
    *,
    processo: str = "",
    incidente: str = "",
) -> dict[str, Any] | None:
    """Converte GET /monday/processo em outcome ja_atribuido/livre."""
    if not isinstance(payload, dict):
        return None
    itens = payload.get("itens") if isinstance(payload.get("itens"), list) else []
    want_inc = _norm_inc(incidente)
    item: dict[str, Any] | None = None
    if want_inc:
        for raw in itens:
            if isinstance(raw, dict) and _norm_inc(raw.get("incidente")) == want_inc:
                item = raw
                break
        if item is None and len(itens) == 1 and isinstance(itens[0], dict):
            item = itens[0]
    elif len(itens) == 1 and isinstance(itens[0], dict):
        item = itens[0]

    names = _collect_compradores(item, payload)
    who = ", ".join(names)
    inserido = bool(payload.get("inserido") or item)
    item_id = str((item or {}).get("item_id") or "") if item else ""
    proposta = _first_text(item, payload, key="proposta_enviada")
    motivo = _first_text(item, payload, key="motivo_retirada")
    mensagem = _first_text(item, payload, key="mensagem_usuario")
    atribuido = _as_flag((item or {}).get("atribuido") if item else None)
    if atribuido is None:
        atribuido = bool(_as_flag(payload.get("atribuido")))
    pode_trocar = _as_flag((item or {}).get("pode_trocar_comprador") if item else None)
    if pode_trocar is None:
        pode_trocar = _as_flag(payload.get("pode_trocar_comprador"))
    if pode_trocar is None:
        pode_trocar = _infer_pode_trocar(proposta, motivo)
    mapped = {
        "processo": str(processo or payload.get("processo") or "").strip(),
        "incidente": str(incidente or payload.get("incidente") or "").strip(),
        "monday_item_id": item_id or None,
        "proposta_enviada": proposta or None,
        "motivo_retirada": motivo or None,
        "mensagem_usuario": mensagem or None,
        "pode_trocar_comprador": bool(pode_trocar),
        "atribuido": bool(atribuido),
        "monday_configured": True,
        "monday_source": "si",
    }
    if pode_trocar:
        mapped.update(
            {
                "outcome": "rejeitado",
                "label": "Disponível",
                "comprador": who or None,
                "incluir": True,
            }
        )
        return mapped
    if atribuido:
        mapped.update(
            {
                "outcome": "ja_atribuido",
                "label": f"Em tratamento — {who}" if who else "Em tratamento",
                "comprador": who or None,
                "incluir": False,
            }
        )
        return mapped
    if inserido:
        mapped.update(
            {
                "outcome": "livre",
                "label": "Livre — na Monday sem comprador",
                "comprador": None,
                "incluir": True,
            }
        )
        return mapped
    mapped.update(
        {
            "outcome": "livre",
            "label": "Livre — não está na Monday",
            "comprador": None,
            "incluir": True,
        }
    )
    return mapped


def lookup_si_processo(processo: str, incidente: str = "") -> dict[str, Any] | None:
    """Consulta GET /monday/processo na API da 229 (IP liberado)."""
    if not si_lookup_configured():
        return None
    proc = str(processo or "").strip()
    if not proc:
        return None
    params: dict[str, str] = {"processo": proc}
    inc = str(incidente or "").strip()
    if inc:
        params["incidente"] = inc
    try:
        response = requests.get(
            _env("MONDAY_REQUESTS_API_URL").rstrip("/") + "/monday/processo",
            headers={
                "X-API-Key": _env("API_KEY_MONDAY_REQUESTS"),
                "Accept": "application/json",
            },
            params=params,
            timeout=_timeout(),
        )
    except requests.RequestException:
        return None
    if response.status_code >= 400:
        return None
    try:
        payload = response.json()
    except ValueError:
        return None
    return map_si_monday_payload(payload, processo=proc, incidente=inc)


def _fold(value: str) -> str:
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return " ".join(text.casefold().split())


def _people_map() -> dict[str, str]:
    """email -> monday people id (env MONDAY_PEOPLE_EMAIL_MAP=a@x.com:123,b@y.com:456)."""
    raw = _env("MONDAY_PEOPLE_EMAIL_MAP")
    out: dict[str, str] = {}
    for part in raw.split(","):
        part = part.strip()
        if ":" not in part:
            continue
        email, pid = part.rsplit(":", 1)
        email = email.strip().lower()
        pid = pid.strip()
        if email and pid:
            out[email] = pid
    return out


class MondayPrecheckError(RuntimeError):
    pass


def _json_dict(response: requests.Response) -> dict[str, Any] | None:
    try:
        payload = response.json()
    except ValueError:
        return None
    return payload if isinstance(payload, dict) else None


def monday_block_message(payload: dict[str, Any] | None, status_code: int = 0) -> str | None:
    parts: list[str] = []
    if isinstance(payload, dict):
        for err in payload.get("errors") or []:
            if isinstance(err, dict):
                parts.append(str(err.get("message") or ""))
                ext = err.get("extensions") or {}
                if isinstance(ext, dict):
                    parts.append(str(ext.get("code") or ""))
            else:
                parts.append(str(err))
    blob = " ".join(parts).casefold()
    if "ip is restricted" in blob or "request_blocked" in blob:
        return (
            "Monday bloqueou o IP deste servidor. "
            "Não foi possível verificar se o caso já está em tratamento."
        )
    if int(status_code or 0) == 401:
        return "Monday recusou a consulta (401)."
    return None


def _precheck_unavailable(base: dict[str, Any], reason: Any) -> dict[str, Any]:
    text = str(reason or "").strip()
    folded = text.casefold()
    ip_block = "bloqueou o ip" in folded or "ip is restricted" in folded or "request_blocked" in folded
    if ip_block:
        label = (
            "Monday bloqueou o IP deste servidor. "
            "Não foi possível verificar se o caso já está em tratamento."
        )
        outcome = "erro"
    else:
        label = "Pré-checagem Monday indisponível"
        if text and "https://api.monday.com" not in text:
            label = f"{label} ({text})"
        outcome = "livre"
    return {
        **base,
        "outcome": outcome,
        "label": label,
        "incluir": True,
        "monday_configured": True,
        "monday_error": True,
    }


def _execute(query: str, variables: dict[str, Any] | None = None) -> dict[str, Any]:
    token = _env("MONDAY_API_TOKEN")
    if not token:
        raise MondayPrecheckError("MONDAY_API_TOKEN não configurado.")
    try:
        response = requests.post(
            MONDAY_API_URL,
            headers={
                "Authorization": token,
                "Content-Type": "application/json",
                "API-Version": "2024-10",
            },
            json={"query": query, "variables": variables or {}},
            timeout=_timeout(),
        )
    except requests.RequestException as exc:
        raise MondayPrecheckError("Falha ao consultar a Monday.") from exc
    payload = _json_dict(response)
    blocked = monday_block_message(payload, response.status_code)
    if blocked:
        raise MondayPrecheckError(blocked)
    if response.status_code >= 400:
        raise MondayPrecheckError(f"Monday HTTP {response.status_code}.")
    if not payload:
        raise MondayPrecheckError("Resposta Monday inválida.")
    if payload.get("errors"):
        raise MondayPrecheckError("Monday API error.")
    data = payload.get("data")
    if not isinstance(data, dict):
        raise MondayPrecheckError("Resposta Monday inválida.")
    return data


def _list_columns() -> list[dict[str, Any]]:
    data = _execute(
        """
        query ($boardId: [ID!]) {
          boards(ids: $boardId) {
            columns { id title type }
          }
        }
        """,
        {"boardId": [_board_id()]},
    )
    boards = data.get("boards") or []
    if not boards:
        raise MondayPrecheckError(f"Board Monday não encontrado: {_board_id()}")
    return list(boards[0].get("columns") or [])


def _match_column(columns: list[dict[str, Any]], titles: tuple[str, ...], col_type: str = "") -> str | None:
    wanted = {_fold(t) for t in titles}
    typed: list[dict[str, Any]] = []
    for col in columns:
        if col_type and str(col.get("type") or "").casefold() != col_type:
            continue
        typed.append(col)
    pool = typed or columns
    for col in pool:
        title = _fold(str(col.get("title") or ""))
        if title in wanted:
            return str(col.get("id") or "") or None
    for col in pool:
        title = _fold(str(col.get("title") or ""))
        if any(w in title for w in wanted):
            return str(col.get("id") or "") or None
    return None


def resolve_column_ids() -> dict[str, str]:
    columns = _list_columns()
    ids = {
        "processo": _env("MONDAY_COL_PROCESSO")
        or _match_column(columns, _TITLE_PROCESSO)
        or "",
        "incidente": _env("MONDAY_COL_INCIDENTE")
        or _match_column(columns, _TITLE_INCIDENTE)
        or "",
        "comprador": _env("MONDAY_COL_COMPRADOR")
        or _match_column(columns, _TITLE_COMPRADOR, "people")
        or _match_column(columns, _TITLE_COMPRADOR)
        or "",
        "proposta_enviada": _env("MONDAY_COL_PROPOSTA_ENVIADA")
        or _match_column(columns, _TITLE_PROPOSTA, "status")
        or _match_column(columns, _TITLE_PROPOSTA)
        or "",
    }
    missing = [k for k, v in ids.items() if not v]
    if missing:
        raise MondayPrecheckError(
            "Colunas Monday em falta: "
            + ", ".join(missing)
            + ". Defina MONDAY_COL_PROCESSO, MONDAY_COL_INCIDENTE, "
            "MONDAY_COL_COMPRADOR e MONDAY_COL_PROPOSTA_ENVIADA."
        )
    return ids


def _find_items(col_ids: dict[str, str], processo: str, incidente: str) -> list[dict[str, Any]]:
    proc_vals = _proc_search_values(processo)
    if not proc_vals:
        return []
    filters = [{"column_id": col_ids["processo"], "column_values": proc_vals}]
    inc_vals = _inc_search_values(incidente)
    if inc_vals and col_ids.get("incidente"):
        filters.append({"column_id": col_ids["incidente"], "column_values": inc_vals})
    query = """
    query ($boardId: ID!, $columns: [ItemsPageByColumnValuesQuery!], $limit: Int!) {
      items_page_by_column_values(
        board_id: $boardId
        columns: $columns
        limit: $limit
      ) {
        items { id name }
      }
    }
    """
    data = _execute(
        query,
        {"boardId": _board_id(), "columns": filters, "limit": 25},
    )
    page = data.get("items_page_by_column_values") or {}
    items = [dict(it) for it in (page.get("items") or []) if isinstance(it, dict)]
    if items:
        return items
    # Fallback: só processo (incidente pode estar formatado diferente)
    data = _execute(
        query,
        {
            "boardId": _board_id(),
            "columns": [{"column_id": col_ids["processo"], "column_values": proc_vals}],
            "limit": 50,
        },
    )
    page = data.get("items_page_by_column_values") or {}
    return [dict(it) for it in (page.get("items") or []) if isinstance(it, dict)]


def _fetch_item_columns(item_ids: list[str], col_ids: dict[str, str]) -> list[dict[str, Any]]:
    if not item_ids:
        return []
    wanted = [col_ids["processo"], col_ids["incidente"], col_ids["comprador"], col_ids["proposta_enviada"]]
    data = _execute(
        """
        query ($ids: [ID!]) {
          items(ids: $ids) {
            id
            name
            column_values {
              id
              text
              value
              type
            }
          }
        }
        """,
        {"ids": item_ids},
    )
    items = []
    for item in data.get("items") or []:
        if not isinstance(item, dict):
            continue
        cols = {
            str(c.get("id")): c
            for c in (item.get("column_values") or [])
            if isinstance(c, dict) and str(c.get("id")) in wanted
        }
        item["by_id"] = cols
        items.append(item)
    return items


def _people_names(col: dict[str, Any] | None) -> list[str]:
    if not col:
        return []
    text = str(col.get("text") or "").strip()
    names = [p.strip() for p in text.split(",") if p.strip()]
    if names:
        return names
    try:
        raw = json.loads(col.get("value") or "{}")
    except (TypeError, json.JSONDecodeError):
        return []
    people = raw.get("personsAndTeams") or raw.get("persons_and_teams") or []
    out = []
    for p in people:
        if isinstance(p, dict):
            name = str(p.get("name") or p.get("id") or "").strip()
            if name:
                out.append(name)
    return out


def _status_label(col: dict[str, Any] | None) -> str:
    if not col:
        return ""
    text = str(col.get("text") or "").strip()
    if text:
        return text
    try:
        raw = json.loads(col.get("value") or "{}")
    except (TypeError, json.JSONDecodeError):
        return ""
    return str(raw.get("label") or "").strip()


def _is_rejeitado(label: str) -> bool:
    folded = _fold(label)
    return folded in {"rejeitado", "rejeitada", "rejected"} or "rejeit" in folded


def _col_text(col: dict[str, Any] | None) -> str:
    if not isinstance(col, dict):
        return ""
    text = str(col.get("text") or "").strip()
    if text:
        return text
    raw = col.get("value")
    if isinstance(raw, str) and raw.strip():
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return raw.strip().strip('"')
        if isinstance(parsed, str):
            return parsed.strip()
        if isinstance(parsed, dict):
            return str(parsed.get("text") or parsed.get("label") or "").strip()
    return ""


def _item_matches(item: dict[str, Any], col_ids: dict[str, str], processo: str, incidente: str) -> bool:
    cols = item.get("by_id") or {}
    proc_col = cols.get(col_ids["processo"]) or {}
    inc_col = cols.get(col_ids["incidente"]) or {}
    got_proc = _norm_proc(_col_text(proc_col) or proc_col.get("value") or "")
    got_inc = _norm_inc(_col_text(inc_col) or inc_col.get("value") or "")
    want_proc = _norm_proc(processo)
    want_inc = _norm_inc(incidente)
    if want_proc and got_proc and got_proc != want_proc:
        return False
    if want_inc and got_inc and got_inc != want_inc:
        return False
    return True


def _lookup_people_id(email: str) -> tuple[str | None, str | None]:
    email = (email or "").strip().lower()
    if not email:
        return None, None
    mapped = _people_map().get(email)
    if mapped:
        return mapped, None
    fallback = _env("MONDAY_PEOPLE_ID")
    try:
        data = _execute(
            """
            query ($emails: [String!]) {
              users(emails: $emails) { id name email }
            }
            """,
            {"emails": [email]},
        )
        users = data.get("users") or []
        if users and isinstance(users[0], dict):
            uid = str(users[0].get("id") or "").strip()
            name = str(users[0].get("name") or "").strip()
            if uid:
                return uid, name or email
    except Exception:
        pass
    if fallback:
        return fallback, None
    return None, None


def _assign_people(item_id: str, column_id: str, people_id: str) -> None:
    value = json.dumps(
        {"personsAndTeams": [{"id": int(people_id) if str(people_id).isdigit() else people_id, "kind": "person"}]},
        ensure_ascii=False,
    )
    _execute(
        """
        mutation ($boardId: ID!, $itemId: ID!, $columnId: String!, $value: JSON!) {
          change_column_value(
            board_id: $boardId
            item_id: $itemId
            column_id: $columnId
            value: $value
          ) { id }
        }
        """,
        {
            "boardId": _board_id(),
            "itemId": str(item_id),
            "columnId": column_id,
            "value": value,
        },
    )


def _evaluate_item(
    item: dict[str, Any],
    col_ids: dict[str, str],
    *,
    requester_email: str,
    requester_name: str,
    assign_on_rejeitado: bool = True,
) -> dict[str, Any]:
    cols = item.get("by_id") or {}
    people = _people_names(cols.get(col_ids["comprador"]))
    status = _status_label(cols.get(col_ids["proposta_enviada"]))
    item_id = str(item.get("id") or "")
    if people and not _is_rejeitado(status):
        comprador = ", ".join(people)
        return {
            "outcome": "ja_atribuido",
            "label": f"Em tratamento — {comprador}",
            "comprador": comprador,
            "monday_item_id": item_id,
            "proposta_enviada": status,
            "incluir": False,
        }
    if _is_rejeitado(status) and not assign_on_rejeitado:
        return {
            "outcome": "rejeitado",
            "label": "Proposta rejeitada na Monday — pode reatribuir na inclusão",
            "comprador": ", ".join(people) if people else None,
            "monday_item_id": item_id,
            "proposta_enviada": status,
            "incluir": True,
        }
    if _is_rejeitado(status):
        people_id, people_name = _lookup_people_id(requester_email)
        assigned_to = people_name or requester_name or requester_email
        if people_id:
            _assign_people(item_id, col_ids["comprador"], people_id)
            return {
                "outcome": "rejeitado_reatribuido",
                "label": f"Caso rejeitado atribuído a {assigned_to}",
                "comprador": assigned_to,
                "monday_item_id": item_id,
                "proposta_enviada": status,
                "incluir": False,
            }
        return {
            "outcome": "rejeitado_sem_people",
            "label": (
                "Proposta Enviada=Rejeitado, mas não foi possível identificar "
                "o utilizador Monday pelo e-mail. Defina MONDAY_PEOPLE_EMAIL_MAP "
                "ou MONDAY_PEOPLE_ID."
            ),
            "comprador": None,
            "monday_item_id": item_id,
            "proposta_enviada": status,
            "incluir": False,
        }
    return {
        "outcome": "livre",
        "label": "Livre — segue para inclusão",
        "comprador": ", ".join(people) if people else None,
        "monday_item_id": item_id,
        "proposta_enviada": status,
        "incluir": True,
    }


def precheck_caso(
    *,
    processo: str = "",
    incidente: str = "",
    numero_cumprimento: str = "",
    is_cumprimento: bool = False,
    user: dict | None = None,
    col_ids: dict[str, str] | None = None,
    mutate: bool = True,
    requester_email: str = "",
    requester_name: str = "",
) -> dict[str, Any]:
    if user and not requester_email:
        requester_email = str(user.get("email") or user.get("email_solicitante") or "")
        requester_name = str(user.get("name") or user.get("nome_solicitante") or "")
    proc = numero_cumprimento if is_cumprimento else processo
    inc = "" if is_cumprimento else incidente
    base = {
        "processo": str(processo or "").strip(),
        "incidente": str(incidente or "").strip(),
        "numero_cumprimento": str(numero_cumprimento or "").strip(),
        "is_cumprimento": bool(is_cumprimento),
    }
    if not is_configured():
        return {
            **base,
            "outcome": "livre",
            "label": "Monday não configurada — segue para inclusão",
            "incluir": True,
            "monday_configured": False,
        }
    if not _norm_proc(proc):
        return {**base, "outcome": "erro", "label": "Processo em falta.", "incluir": False}
    si = lookup_si_processo(proc, inc)
    if si:
        si.update(base)
        return si
    if not _graphql_ready():
        return _precheck_unavailable(base, "Consulta Monday via API SI indisponível.")
    try:
        ids = col_ids or resolve_column_ids()
        found = _find_items(ids, proc, inc)
        if not found:
            return {
                **base,
                "outcome": "livre",
                "label": "Livre — não está na Monday",
                "incluir": True,
                "monday_configured": True,
            }
        detailed = _fetch_item_columns([str(it.get("id")) for it in found], ids)
        matched = [
            it for it in detailed if _item_matches(it, ids, proc, inc)
        ] or detailed
        best = matched[0]
        result = _evaluate_item(
            best,
            ids,
            requester_email=requester_email,
            requester_name=requester_name,
            assign_on_rejeitado=mutate,
        )
        result.update(base)
        result["monday_configured"] = True
        return result
    except requests.RequestException as exc:
        return _precheck_unavailable(base, exc)
    except MondayPrecheckError as exc:
        return _precheck_unavailable(base, exc)


def _livre_fallback(raw: dict[str, Any], *, reason: str = "") -> dict[str, Any]:
    base = {
        "processo": str(raw.get("processo") or "").strip(),
        "incidente": str(raw.get("incidente") or "").strip(),
        "numero_cumprimento": str(raw.get("numero_cumprimento") or "").strip(),
        "is_cumprimento": bool(raw.get("is_cumprimento")),
    }
    if reason:
        return _precheck_unavailable(base, reason)
    return {
        **base,
        "outcome": "livre",
        "label": "Pré-checagem Monday indisponível",
        "incluir": True,
        "monday_configured": is_configured(),
    }


def precheck_casos(
    casos: list[dict[str, Any]],
    user: dict | None = None,
    *,
    mutate: bool = True,
    requester_email: str = "",
    requester_name: str = "",
) -> dict[str, Any]:
    items = []
    for raw in casos:
        if not isinstance(raw, dict):
            continue
        items.append(
            precheck_caso(
                processo=str(raw.get("processo") or ""),
                incidente=str(raw.get("incidente") or ""),
                numero_cumprimento=str(raw.get("numero_cumprimento") or ""),
                is_cumprimento=bool(raw.get("is_cumprimento")),
                user=user,
                col_ids=None,
                mutate=mutate,
                requester_email=requester_email,
                requester_name=requester_name,
            )
        )
    return {
        "ok": True,
        "monday_configured": is_configured(),
        "precheck_ok": bool(items) and all(not it.get("monday_error") for it in items),
        "items": items,
    }


