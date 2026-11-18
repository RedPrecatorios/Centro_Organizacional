from __future__ import annotations

import json
import os
import unicodedata
import urllib.error
import urllib.request

API_URL = "https://api.monday.com/v2"
API_VERSION = "2025-10"
BOARD_ID = "5079764927"
LABEL_IDS = [
    *range(0, 20),
    *range(101, 111),
    *range(151, 161),
]
STATUS_COLORS = [
    "working_orange",
    "done_green",
    "stuck_red",
    "dark_blue",
    "purple",
    "explosive",
    "grass_green",
    "bright_blue",
    "saladish",
    "egg_yolk",
    "blackish",
    "dark_red",
    "sofia_pink",
    "lipstick",
    "dark_purple",
    "bright_green",
    "chili_blue",
    "american_gray",
    "brown",
    "dark_orange",
    "sunset",
    "bubble",
    "peach",
    "berry",
    "winter",
    "river",
    "navy",
    "aquamarine",
    "indigo",
    "dark_indigo",
    "pecan",
    "lavender",
    "royal",
    "steel",
    "orchid",
    "lilac",
    "tan",
    "sky",
    "coffee",
    "teal",
]
COLOR_BY_ID = dict(zip(LABEL_IDS, STATUS_COLORS))
AREA_TEAMS = {
    "COMPRAS": "COMPRAS",
    "JURIDICO": "JURIDICO",
    "CALCULO": "CALCULO",
}
AREA_COLUMNS = {
    "COMPRAS": [
        ("5079764927", "status5", "Comprador", "SISTEMA"),
        ("7345244366", "status5", "Comprador", "SysCall - Inclusões"),
        ("5193758740", "status5", "Comprador", "INCLUSÕES"),
    ],
    "JURIDICO": [
        ("5079764927", "color0", "Responsável Jurídico", "SISTEMA"),
    ],
    "CALCULO": [
        ("5079764927", "status9", "Calculista", "SISTEMA"),
    ],
}


def _env(*names: str) -> str:
    for name in names:
        value = os.getenv(name)
        if value and value.strip():
            return value.strip().strip('"').strip("'")
    return ""


def monday_token() -> str:
    token = _env("MONDAY_api_key", "MONDAY_API_KEY", "Monday_api_key")
    if not token:
        raise RuntimeError("Token Monday ausente. Defina MONDAY_api_key no arquivo .env")
    return token


def _fold(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value or "")
    stripped = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    return " ".join(stripped.casefold().split())


def display_name(first_name: str, last_name: str) -> str:
    name = " ".join(part for part in ((first_name or "").strip(), (last_name or "").strip()) if part)
    return name[:30]


def _graphql(query: str, variables: dict | None = None) -> dict:
    payload = json.dumps({"query": query, "variables": variables or {}}).encode("utf-8")
    req = urllib.request.Request(
        API_URL,
        data=payload,
        headers={
            "Content-Type": "application/json",
            "Authorization": monday_token(),
            "API-Version": API_VERSION,
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        if "ip is restricted" in detail.lower() or "REQUEST_BLOCKED" in detail:
            raise RuntimeError(
                "A Monday bloqueou o IP deste servidor (68.183.23.91). "
                "Libere esse IP na conta Monday para o convite funcionar."
            ) from exc
        raise RuntimeError(f"Monday HTTP {exc.code}: {detail[:300]}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Falha ao falar com a Monday: {exc.reason}") from exc
    errors = body.get("errors") or []
    if errors:
        messages = []
        for err in errors:
            msg = str(err.get("message") or err)
            ext = err.get("extensions") or {}
            code = ext.get("code") or ""
            extra = ext.get("error_data") or ext.get("status_code") or ext.get("details") or ""
            if extra and extra != msg:
                msg = f"{msg} ({extra})"
            messages.append(f"{code}: {msg}" if code else msg)
        raise RuntimeError("Monday: " + " | ".join(messages))
    return body.get("data") or {}


def account_info() -> dict:
    data = _graphql(
        """
        query ($ids: [ID!]) {
          me { id name email }
          boards(ids: $ids) { id name }
        }
        """,
        {"ids": [BOARD_ID]},
    )
    me = data.get("me") or {}
    boards = data.get("boards") or []
    board = boards[0] if boards else {}
    if not me.get("id"):
        raise RuntimeError("Token Monday inválido.")
    if not board.get("id"):
        raise RuntimeError("Quadro 5079764927 não encontrado com este token.")
    return {
        "email": str(me.get("email") or ""),
        "name": str(me.get("name") or ""),
        "board_id": str(board.get("id") or BOARD_ID),
        "board_name": str(board.get("name") or ""),
    }


def _user_from_row(row: dict, email: str) -> dict:
    status = str(row.get("status") or "").upper()
    pending = status == "PENDING" or bool(row.get("is_pending"))
    return {
        "id": str(row.get("id") or ""),
        "name": str(row.get("name") or ""),
        "email": str(row.get("email") or email),
        "pending": pending,
    }


def _find_user(email: str) -> dict | None:
    wanted = (email or "").strip().casefold()

    def _match(rows: list) -> dict | None:
        for row in rows:
            if str(row.get("email") or "").strip().casefold() == wanted:
                return _user_from_row(row, email)
        return None

    try:
        data = _graphql(
            """
            query ($emails: [String]) {
              users(emails: $emails, kind: all, limit: 10) {
                id
                name
                email
                is_pending
                is_verified
              }
            }
            """,
            {"emails": [email]},
        )
        found = _match(data.get("users") or [])
        if found:
            return found
    except RuntimeError:
        pass
    data = _graphql(
        """
        query ($emails: [String]) {
          users(emails: $emails, limit: 10) {
            id
            name
            email
            is_pending
            is_verified
          }
        }
        """,
        {"emails": [email]},
    )
    return _match(data.get("users") or [])


def _ensure_board_invite(user_id: str) -> str:
    """invite_users cria o usuário pendente, mas o e-mail só sai ao assinar um quadro."""
    if not user_id:
        return "Monday criou o usuário, mas faltou o ID para enviar o e-mail do convite."
    variables = {"board_id": BOARD_ID, "user_ids": [user_id]}
    try:
        _graphql(
            """
            mutation ($board_id: ID!, $user_ids: [ID!]!) {
              add_subscribers_to_board(board_id: $board_id, user_ids: $user_ids) {
                id
                email
              }
            }
            """,
            variables,
        )
        return ""
    except RuntimeError:
        pass
    try:
        _graphql(
            """
            mutation ($board_id: ID!, $user_ids: [ID!]!) {
              add_users_to_board(board_id: $board_id, user_ids: $user_ids, kind: subscriber) {
                id
                email
              }
            }
            """,
            variables,
        )
        return ""
    except RuntimeError as exc:
        return f"Convite Monday criado, mas o e-mail pode não ter sido enviado: {exc}"


def _invite_user(email: str) -> dict:
    existing = _find_user(email)
    if existing and not existing.get("pending"):
        return {**existing, "existed": True, "invited": False}
    data = _graphql(
        """
        mutation ($emails: [String!]!) {
          invite_users(
            emails: $emails
            product: work_management
            user_role: MEMBER
          ) {
            invited_users { id name email }
            errors { email code message }
          }
        }
        """,
        {"emails": [email]},
    )
    result = data.get("invite_users") or {}
    errors = result.get("errors") or []
    invited = result.get("invited_users") or []
    if invited:
        row = invited[0]
        return {
            "id": str(row.get("id") or ""),
            "name": str(row.get("name") or ""),
            "email": str(row.get("email") or email),
            "existed": False,
            "invited": True,
        }
    if errors:
        message = "; ".join(
            f"{err.get('email') or email}: {err.get('message') or err.get('code') or 'erro'}"
            for err in errors
        )
        existing = _find_user(email)
        if existing:
            return {**existing, "existed": True, "invited": False, "warning": message}
        raise RuntimeError(f"Monday não convidou {email}: {message}")
    existing = _find_user(email)
    if existing:
        return {**existing, "existed": True, "invited": False}
    raise RuntimeError(f"Monday não retornou o convite de {email}")


def _list_teams() -> list[dict]:
    data = _graphql("query { teams { id name } }")
    return data.get("teams") or []


def _team_for_area(area: str) -> dict:
    wanted = AREA_TEAMS.get((area or "").strip().upper())
    if not wanted:
        raise RuntimeError("Área inválida. Use Comprador, Jurídico ou Calculista.")
    wanted_fold = _fold(wanted)
    for row in _list_teams():
        name = str(row.get("name") or "")
        if _fold(name) == wanted_fold:
            return {"id": str(row.get("id") or ""), "name": name}
    raise RuntimeError(f"Equipe Monday '{wanted}' não encontrada na conta.")


def _add_to_team(user_id: str, team: dict) -> str:
    if not user_id or not team.get("id"):
        return f"Usuário sem ID para entrar na equipe {team.get('name') or ''}".strip()
    data = _graphql(
        """
        mutation ($team_id: ID!, $user_ids: [ID!]!) {
          add_users_to_team(team_id: $team_id, user_ids: $user_ids) {
            successful_users { id email }
            failed_users { id email }
          }
        }
        """,
        {"team_id": team["id"], "user_ids": [user_id]},
    )
    result = data.get("add_users_to_team") or {}
    failed = result.get("failed_users") or []
    if failed:
        return (
            f"Convite ok, mas a Monday não colocou o usuário na equipe {team['name']} "
            "(pode ficar pendente até o convite ser aceito)."
        )
    return ""


def _column_state(board_id: str, column_id: str, column_title: str, board_name: str = "") -> dict:
    data = _graphql(
        """
        query ($ids: [ID!], $column_ids: [String]) {
          boards(ids: $ids) {
            id
            name
            columns(ids: $column_ids) {
              id
              title
              revision
              settings
            }
          }
        }
        """,
        {"ids": [board_id], "column_ids": [column_id]},
    )
    boards = data.get("boards") or []
    board = boards[0] if boards else {}
    columns = board.get("columns") if board else []
    column = (columns or [None])[0]
    where = board_name or str(board.get("name") or board_id)
    if not column or not column.get("revision"):
        raise RuntimeError(f"Não foi possível ler a coluna {column_title} no quadro {where}.")
    settings = column.get("settings") or {}
    if isinstance(settings, str):
        try:
            settings = json.loads(settings)
        except json.JSONDecodeError:
            settings = {}
    return {
        "id": str(column.get("id") or column_id),
        "title": str(column.get("title") or column_title),
        "revision": str(column.get("revision") or ""),
        "board_name": str(board.get("name") or where),
        "labels": list(settings.get("labels") or []),
    }


def _color_enum(value) -> str:
    if isinstance(value, str) and value in STATUS_COLORS:
        return value
    try:
        number = int(value)
    except (TypeError, ValueError):
        number = None
    if number in COLOR_BY_ID:
        return COLOR_BY_ID[number]
    return "working_orange"


def _label_input(row: dict, index: int) -> dict:
    payload = {
        "label": str(row.get("label") or "").strip() or "—",
        "color": _color_enum(row.get("color")),
        "index": index,
    }
    if row.get("id") is not None:
        try:
            label_id = int(row["id"])
        except (TypeError, ValueError):
            label_id = None
        if label_id in LABEL_IDS:
            payload["id"] = label_id
    if row.get("is_done"):
        payload["is_done"] = True
    if row.get("is_deactivated"):
        payload["is_deactivated"] = True
    return payload


def _next_index(used: set[int]) -> int:
    for i in range(40):
        if i not in used:
            used.add(i)
            return i
    raise RuntimeError("A coluna de status não tem posição livre para o novo rótulo.")


def _existing_label_inputs(labels: list[dict]) -> tuple[list[dict], set[int], set[int]]:
    used_index: set[int] = set()
    used_ids: set[int] = set()
    ready: list[dict] = []
    pending: list[dict] = []
    for row in labels:
        raw = row.get("index")
        if isinstance(raw, int) and 0 <= raw <= 39 and raw not in used_index:
            used_index.add(raw)
            item = _label_input(row, raw)
            ready.append(item)
            if "id" in item:
                used_ids.add(item["id"])
        else:
            pending.append(row)
    for row in pending:
        item = _label_input(row, _next_index(used_index))
        ready.append(item)
        if "id" in item:
            used_ids.add(item["id"])
    return ready, used_index, used_ids


def _ensure_status_label(
    board_id: str,
    column_id: str,
    column_title: str,
    full_name: str,
    board_name: str = "",
) -> dict:
    wanted = (full_name or "").strip()[:30]
    where = board_name or board_id
    if not wanted:
        raise RuntimeError(f"Nome e sobrenome são obrigatórios para o status {column_title}.")
    if len((full_name or "").strip()) > 30:
        raise RuntimeError(f"Nome + sobrenome passa de 30 caracteres, limite da coluna {column_title}.")

    state = _column_state(board_id, column_id, column_title, where)
    where = state.get("board_name") or where
    labels = state["labels"]
    for row in labels:
        if _fold(str(row.get("label") or "")) == _fold(wanted):
            return {
                "label": str(row.get("label") or wanted),
                "column": state["title"] or column_title,
                "board": where,
                "created": False,
            }

    next_labels, used_index, used_ids = _existing_label_inputs(labels)
    free_id = next((item for item in LABEL_IDS if item not in used_ids), None)
    if free_id is None:
        raise RuntimeError(f"A coluna {column_title} no quadro {where} já está no limite de 40 rótulos.")
    used_colors = {_color_enum(row.get("color")) for row in labels}
    free_color = next((color for color in STATUS_COLORS if color not in used_colors), COLOR_BY_ID[free_id])
    next_labels.append({
        "label": wanted,
        "color": free_color,
        "index": _next_index(used_index),
    })

    def _write(revision: str) -> None:
        _graphql(
            """
            mutation (
              $board_id: ID!
              $id: String!
              $revision: String!
              $settings: UpdateStatusColumnSettingsInput
            ) {
              update_status_column(
                board_id: $board_id
                id: $id
                revision: $revision
                settings: $settings
              ) { id }
            }
            """,
            {
                "board_id": board_id,
                "id": column_id,
                "revision": revision,
                "settings": {"labels": next_labels},
            },
        )

    try:
        _write(state["revision"])
    except RuntimeError as exc:
        if "revision" not in str(exc).casefold():
            raise
        _write(_column_state(board_id, column_id, column_title, where)["revision"])
    return {"label": wanted, "column": column_title, "board": where, "created": True}


def provision_user(email: str, first_name: str, last_name: str, area: str) -> dict:
    email = (email or "").strip()
    first_name = (first_name or "").strip()
    last_name = (last_name or "").strip()
    area = (area or "").strip().upper()
    if not first_name or not last_name:
        raise RuntimeError("Nome e sobrenome são obrigatórios.")
    if "@" not in email:
        raise RuntimeError("Informe um e-mail válido para a Monday.")
    if area not in AREA_TEAMS:
        raise RuntimeError("Área inválida. Use Comprador, Jurídico ou Calculista.")

    user = _invite_user(email)
    team = _team_for_area(area)
    warning = _add_to_team(user.get("id") or "", team)
    if user.get("invited") or user.get("pending") or not user.get("existed"):
        warning = " ".join(
            part for part in (warning, _ensure_board_invite(user.get("id") or "")) if part
        )
    full_name = display_name(first_name, last_name)
    created_any = False
    label_text = full_name
    column_title = ""
    boards_ok: list[str] = []
    label_warnings: list[str] = []
    for board_id, column_id, title, board_name in AREA_COLUMNS[area]:
        column_title = title
        try:
            result = _ensure_status_label(board_id, column_id, title, full_name, board_name)
            label_text = result.get("label") or label_text
            column_title = result.get("column") or title
            boards_ok.append(result.get("board") or board_name)
            created_any = created_any or bool(result.get("created"))
        except RuntimeError as exc:
            label_warnings.append(f"{board_name}: {exc}")
    if not boards_ok and label_warnings:
        raise RuntimeError(" ".join(label_warnings))
    column_summary = column_title
    if boards_ok:
        column_summary = f"{column_title} ({', '.join(boards_ok)})"
    extra = [warning, user.get("warning") or ""]
    extra.extend(label_warnings)
    return {
        "user_id": user.get("id") or "",
        "email": user.get("email") or email,
        "invited": bool(user.get("invited")),
        "existed": bool(user.get("existed")),
        "team": team.get("name") or "",
        "column": column_summary,
        "label": label_text,
        "label_created": created_any,
        "warning": " ".join(part for part in extra if part),
    }
