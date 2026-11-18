from __future__ import annotations

import base64
import json
import os
import time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request

ADMIN_BASE = "https://api.getgo.com/admin/rest/v1"
VOICE_BASE = "https://api.goto.com/voice-admin/v1"
USERS_BASE = "https://api.goto.com/users/v1"
OAUTH_URL = "https://authentication.logmeininc.com/oauth/token"

_cached_access_token = ""
_cached_access_expires = 0.0
_cached_users: list[dict] = []
_cached_users_at = 0.0


def _env(*names: str) -> str:
    for name in names:
        value = os.getenv(name)
        if value and value.strip():
            return value.strip().strip('"').strip("'")
    return ""


def goto_pat() -> str:
    token = _env("GoTo_Token", "GOTO_TOKEN")
    if not token:
        raise RuntimeError("Token GoTo ausente. Defina GoTo_Token no arquivo .env")
    return token


def _client_id() -> str:
    return _env("GoTo_Cliente_id", "GoTo_Client_ID", "GOTO_CLIENT_ID")


def _client_secret() -> str:
    return _env("GoTo_Secret_Key", "GoTo_Client_Secret", "GOTO_CLIENT_SECRET")


def _http_error_detail(exc: urllib.error.HTTPError) -> str:
    body = exc.read().decode("utf-8", errors="replace")
    try:
        parsed = json.loads(body)
    except json.JSONDecodeError:
        return body
    oauth_error = parsed.get("error") or ""
    oauth_desc = parsed.get("error_description") or ""
    if oauth_error == "unauthorized_client":
        return "OAuth Client sem o grant Personal Access Token. Ative esse grant no portal da GoTo."
    if oauth_error == "invalid_client":
        return "GoTo_Cliente_id ou GoTo_Secret_Key inválidos."
    if oauth_error == "invalid_grant":
        return "PAT recusado. Gere um novo token em myaccount.goto.com → Developer Tools."
    if oauth_error:
        return f"{oauth_error}: {oauth_desc}".strip(": ")
    code = parsed.get("int_err_code") or parsed.get("code") or ""
    message = parsed.get("msg") or parsed.get("message") or body
    if code == "InvalidToken" or "invalid token" in str(message).lower():
        return "Token GoTo inválido. Confira GoTo_Token, GoTo_Cliente_id e GoTo_Secret_Key no .env."
    return f"{code}: {message}" if code else str(message)


def _exchange_pat() -> tuple[str, int]:
    client_id = _client_id()
    secret = _client_secret()
    if not client_id or not secret:
        raise RuntimeError("Defina GoTo_Cliente_id e GoTo_Secret_Key no arquivo .env")
    basic = base64.b64encode(f"{client_id}:{secret}".encode("utf-8")).decode("ascii")
    data = urllib.parse.urlencode({
        "grant_type": "personal_access_token",
        "pat": goto_pat(),
    }).encode("utf-8")
    req = urllib.request.Request(
        OAUTH_URL,
        data=data,
        method="POST",
        headers={
            "Accept": "application/json",
            "Authorization": f"Basic {basic}",
            "Content-Type": "application/x-www-form-urlencoded",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=45) as res:
            payload = json.loads(res.read().decode("utf-8") or "{}")
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"GoTo HTTP {exc.code}: {_http_error_detail(exc)}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Falha de conexão com a GoTo: {exc.reason}") from exc
    token = str(payload.get("access_token") or "")
    if not token:
        raise RuntimeError("GoTo não retornou access_token")
    expires_in = int(payload.get("expires_in") or 3600)
    return token, expires_in


def access_token() -> str:
    global _cached_access_token, _cached_access_expires
    raw = goto_pat()
    if raw.startswith("eyJ") and raw.count(".") >= 2:
        return raw
    now = time.time()
    if _cached_access_token and now < _cached_access_expires:
        return _cached_access_token
    token, expires_in = _exchange_pat()
    _cached_access_token = token
    _cached_access_expires = now + max(expires_in - 60, 30)
    return token


def _request(
    method: str,
    url: str,
    payload: dict | None = None,
    params: dict | None = None,
    allow_codes: set[int] | None = None,
    retries: int | None = None,
) -> object:
    if params:
        query = urllib.parse.urlencode({k: v for k, v in params.items() if v is not None}, doseq=True)
        url = f"{url}?{query}"
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {access_token()}",
    }
    if data is not None:
        headers["Content-Type"] = "application/json"
    if retries is None:
        retries = 4 if method.upper() in {"GET", "PUT", "PATCH", "DELETE"} else 0
    last_error: Exception | None = None
    for attempt in range(retries + 1):
        req = urllib.request.Request(url, data=data, method=method, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=45) as res:
                raw = res.read().decode("utf-8")
                if not raw:
                    return None
                return json.loads(raw)
        except urllib.error.HTTPError as exc:
            if allow_codes and exc.code in allow_codes:
                try:
                    exc.read()
                except Exception:
                    pass
                return None
            detail = _http_error_detail(exc)
            last_error = RuntimeError(f"GoTo HTTP {exc.code}: {detail}")
            if exc.code in {502, 503, 504} and attempt < retries:
                time.sleep(min(2 ** attempt, 8))
                continue
            raise last_error from exc
        except urllib.error.URLError as exc:
            last_error = RuntimeError(f"Falha de conexão com a GoTo: {exc.reason}")
            if attempt < retries:
                time.sleep(min(2 ** attempt, 8))
                continue
            raise last_error from exc
    raise last_error or RuntimeError("Falha ao falar com a GoTo")


def me() -> dict:
    data = _request("GET", f"{ADMIN_BASE}/me")
    if not isinstance(data, dict):
        raise RuntimeError("Resposta inesperada em /me")
    return data


def account_info() -> dict:
    data = me()
    accounts = data.get("accounts") or []
    account = accounts[0] if accounts else {}
    key = str(account.get("key") or data.get("accountKey") or "")
    if not key:
        raise RuntimeError("Não foi possível obter o accountKey da conta GoTo")
    return {
        "email": str(data.get("email") or ""),
        "account_key": key,
        "account_name": str(account.get("name") or account.get("accountName") or ""),
    }


def account_key() -> str:
    return account_info()["account_key"]


def _has_licenses(row: dict) -> bool:
    keys = row.get("licenseKeys") or row.get("license_keys") or []
    if isinstance(keys, list) and any(str(item).strip() for item in keys):
        return True
    products = row.get("products") or []
    if isinstance(products, list) and any(str(item).strip() for item in products):
        return True
    return False


def _portal_status(row: dict) -> str:
    if not row.get("passwordCreated"):
        return "CONVITE"
    if not _has_licenses(row):
        return "SUSPENSO"
    raw = str(row.get("status") or "").upper()
    return {
        "ACTIVE": "ATIVO",
        "INACTIVE": "INATIVO",
        "SUSPENDED": "SUSPENSO",
    }.get(raw, raw or "ATIVO")


def _normalize_user(row: dict, account_key_value: str = "") -> dict:
    first = str(row.get("firstName") or row.get("first_name") or "").strip()
    last = str(row.get("lastName") or row.get("last_name") or "").strip()
    return {
        "user_key": str(row.get("key") or row.get("userKey") or ""),
        "email": str(row.get("email") or "").strip(),
        "first_name": first,
        "last_name": last,
        "status": _portal_status(row),
        "can_suspend": True,
        "account_key": account_key_value,
    }


def _rows_from_payload(payload: object) -> list:
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    if not isinstance(payload, dict):
        return []
    for key in ("results", "items", "users", "data"):
        value = payload.get(key)
        if isinstance(value, list):
            return [row for row in value if isinstance(row, dict)]
    return []


def _ascii_email(email: str) -> str:
    text = unicodedata.normalize("NFKD", (email or "").strip().lower())
    return text.encode("ascii", "ignore").decode("ascii")


def find_user_by_email(email: str) -> dict | None:
    target = _ascii_email(email)
    if "@" not in target:
        return None
    for user in list_users():
        if _ascii_email(user.get("email") or "") == target:
            return user
    return None


def list_users(force: bool = False) -> list[dict]:
    global _cached_users, _cached_users_at
    if not force and _cached_users and (time.time() - _cached_users_at) < 300:
        return _cached_users
    key = account_key()
    users: list[dict] = []
    offset = 0
    page_size = 100
    while True:
        payload = _request(
            "GET",
            f"{ADMIN_BASE}/accounts/{key}/users",
            params={
                "offset": offset,
                "pageSize": page_size,
            },
        )
        rows = _rows_from_payload(payload)
        users.extend(_normalize_user(row, key) for row in rows)
        if len(rows) < page_size:
            break
        offset += page_size
        if offset > 2000:
            break
    seen: set[str] = set()
    unique: list[dict] = []
    for user in users:
        marker = user["user_key"] or user["email"].lower()
        if not marker or marker in seen:
            continue
        seen.add(marker)
        unique.append(user)
    unique.sort(key=lambda item: (item["first_name"].lower(), item["last_name"].lower(), item["email"].lower()))
    _cached_users = unique
    _cached_users_at = time.time()
    return unique


def _connect_standard_license_keys(account_key_value: str) -> list[str]:
    payload = _request("GET", f"{ADMIN_BASE}/accounts/{account_key_value}/licenses")
    keys: list[str] = []
    seen: set[str] = set()
    for row in _rows_from_payload(payload):
        sku = str(row.get("sku") or "").upper()
        description = str(row.get("description") or "").lower().replace(" ", "")
        if sku.startswith("G2CSTANDARDU") or "gotoconnectstandard" in description:
            license_key = str(row.get("key") or "").strip()
            if license_key and license_key not in seen:
                seen.add(license_key)
                keys.append(license_key)
    if not keys:
        raise RuntimeError("Licença GoTo Connect Standard não encontrada na conta")
    return keys


def _group_key_by_name(account_key_value: str, name: str) -> str:
    target = (name or "").strip().lower()
    if not target:
        return ""
    payload = _request("GET", f"{ADMIN_BASE}/accounts/{account_key_value}/groups")
    for row in _rows_from_payload(payload):
        if str(row.get("name") or "").strip().lower() == target:
            return str(row.get("key") or "").strip()
    return ""


def _put_resource(url: str) -> object:
    try:
        return _request("PUT", url, allow_codes={409})
    except RuntimeError as exc:
        message = str(exc).lower()
        if "invalid.json" in message or "http 400" in message:
            return _request("PUT", url, payload={}, allow_codes={409})
        raise


def _assign_licenses(account_key_value: str, user_key: str, license_keys: list[str]) -> None:
    if not license_keys:
        raise RuntimeError("Nenhuma licença GoTo Connect Standard disponível")
    batch = f"{ADMIN_BASE}/accounts/{account_key_value}/licenses/{','.join(license_keys)}/users/{user_key}"
    try:
        _put_resource(batch)
        return
    except RuntimeError:
        last: RuntimeError | None = None
        for license_key in license_keys:
            try:
                _put_resource(
                    f"{ADMIN_BASE}/accounts/{account_key_value}/licenses/{license_key}/users/{user_key}"
                )
                return
            except RuntimeError as exc:
                last = exc
        if last:
            raise last


def _provision_user(account_key_value: str, user_key: str) -> None:
    license_keys = _connect_standard_license_keys(account_key_value)
    _assign_licenses(account_key_value, user_key, license_keys)
    group_key = _group_key_by_name(account_key_value, "Compradores")
    if group_key:
        _put_resource(
            f"{ADMIN_BASE}/accounts/{account_key_value}/groups/{group_key}/users/{user_key}"
        )
    _put_resource(
        f"{ADMIN_BASE}/accounts/{account_key_value}/profiles/default/users/{user_key}"
    )


def _refresh_user(account_key_value: str, user_key: str, fallback: dict) -> dict:
    detail = _request("GET", f"{ADMIN_BASE}/accounts/{account_key_value}/users/{user_key}")
    if isinstance(detail, dict):
        return _normalize_user(detail, account_key_value)
    return fallback


def create_user(email: str, first_name: str, last_name: str, send_welcome: bool) -> dict:
    global _cached_users, _cached_users_at
    key = account_key()
    payload = {
        "users": [
            {
                "email": email,
                "firstName": first_name,
                "lastName": last_name,
                "locale": "pt_BR",
                "timeZone": "America/Sao_Paulo",
            }
        ],
        "suppressEmail": not send_welcome,
    }
    result = _request("POST", f"{ADMIN_BASE}/accounts/{key}/users", payload)
    _cached_users = []
    _cached_users_at = 0.0
    rows = _rows_from_payload(result)
    created = None
    if rows:
        created = _normalize_user(rows[0], key)
    elif isinstance(result, dict):
        created = _normalize_user(result, key)
    if not created:
        raise RuntimeError("GoTo não retornou o usuário criado")
    created["email"] = created["email"] or email
    created["first_name"] = created["first_name"] or first_name
    created["last_name"] = created["last_name"] or last_name
    user_key = created.get("user_key") or ""
    if not user_key:
        raise RuntimeError("GoTo não retornou a chave do usuário")
    warning = ""
    try:
        time.sleep(2)
        _provision_user(key, user_key)
    except RuntimeError as exc:
        warning = (
            f"Conta GoTo criada, mas a licença não foi atribuída agora: {exc} "
            "Clique em Criar de novo para tentar a licença, ou atribua no portal."
        )
    refreshed = _refresh_user(key, user_key, created)
    refreshed["goto_warning"] = warning
    return refreshed


def send_welcome_email(user_key: str) -> str:
    user_key = str(user_key or "").strip()
    if not user_key:
        return "Não foi possível enviar o e-mail da GoTo: usuário sem chave."
    try:
        _request(
            "POST",
            f"{ADMIN_BASE}/accounts/{account_key()}/users/{user_key}/email",
            {},
            retries=4,
        )
        return ""
    except RuntimeError as exc:
        return f"E-mail de convite da GoTo não foi enviado agora: {exc}"


def _license_keys_for_user(account_key_value: str, user_key: str, detail: dict) -> list[str]:
    keys = detail.get("licenseKeys") or []
    if isinstance(keys, list) and any(str(item).strip() for item in keys):
        return [str(item) for item in keys if str(item).strip()]
    payload = _request("GET", f"{ADMIN_BASE}/accounts/{account_key_value}/licenses")
    found: list[str] = []
    for row in _rows_from_payload(payload):
        assigned = {str(item) for item in (row.get("userKeys") or [])}
        if user_key in assigned:
            license_key = str(row.get("key") or "").strip()
            if license_key:
                found.append(license_key)
    return found


def user_missing_error(exc: BaseException) -> bool:
    return "user.not.found" in str(exc)


def suspend_user(user_key: str) -> dict:
    global _cached_users, _cached_users_at
    key = account_key()
    user_key = str(user_key).strip()
    if not user_key:
        raise RuntimeError("Usuário GoTo inválido")
    try:
        detail = _request("GET", f"{ADMIN_BASE}/accounts/{key}/users/{user_key}")
    except RuntimeError as exc:
        if not user_missing_error(exc):
            raise
        _cached_users = []
        _cached_users_at = 0.0
        return {
            "user_key": user_key,
            "email": "",
            "first_name": "",
            "last_name": "",
            "status": "AUSENTE",
            "can_suspend": False,
            "account_key": key,
            "goto_warning": "Conta não encontrada na GoTo. A exclusão local foi concluída.",
        }
    if not isinstance(detail, dict):
        raise RuntimeError("Usuário GoTo não encontrado")
    _request(
        "DELETE",
        f"{ADMIN_BASE}/accounts/{key}/users/{user_key}/licenses",
        allow_codes={404},
    )
    refreshed = _request("GET", f"{ADMIN_BASE}/accounts/{key}/users/{user_key}")
    source = refreshed if isinstance(refreshed, dict) else detail
    for license_key in _license_keys_for_user(key, user_key, source):
        _request(
            "DELETE",
            f"{ADMIN_BASE}/accounts/{key}/licenses/{license_key}/users/{user_key}",
            allow_codes={404},
        )
    _cached_users = []
    _cached_users_at = 0.0
    updated = _request("GET", f"{ADMIN_BASE}/accounts/{key}/users/{user_key}")
    if isinstance(updated, dict):
        return _normalize_user(updated, key)
    return _normalize_user({**detail, "licenseKeys": [], "products": []}, key)


def reactivate_user(user_key: str, send_welcome: bool = False) -> dict:
    global _cached_users, _cached_users_at
    key = account_key()
    user_key = str(user_key).strip()
    if not user_key:
        raise RuntimeError("Usuário GoTo inválido")
    warning = ""
    _provision_user(key, user_key)
    if send_welcome:
        warning = send_welcome_email(user_key)
    _cached_users = []
    _cached_users_at = 0.0
    refreshed = _refresh_user(key, user_key, {"user_key": user_key})
    if warning:
        refreshed["goto_warning"] = warning
    return refreshed


def list_account_phones() -> list[dict]:
    key = account_key()
    phones: list[dict] = []
    payload = _request(
        "GET",
        f"{VOICE_BASE}/phone-numbers",
        params={"accountKey": key},
    )
    rows = []
    if isinstance(payload, dict):
        rows = payload.get("items") or _rows_from_payload(payload)
    elif isinstance(payload, list):
        rows = payload
    seen: set[str] = set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        number = str(row.get("number") or "").strip()
        phone_id = str(row.get("id") or "").strip()
        if not number or number in seen:
            continue
        seen.add(number)
        phones.append({
            "id": phone_id or number,
            "number": number,
            "name": str(row.get("name") or "").strip(),
            "status": str(row.get("status") or "").strip(),
        })
    phones.sort(key=lambda item: item["number"])
    return phones


def _digits(value: str) -> str:
    return "".join(ch for ch in (value or "") if ch.isdigit())


def _user_lines_index() -> dict[str, dict]:
    payload = _request(
        "GET",
        f"{USERS_BASE}/users",
        params={"accountKey": account_key()},
    )
    rows = []
    if isinstance(payload, dict):
        rows = payload.get("items") or _rows_from_payload(payload)
    elif isinstance(payload, list):
        rows = payload
    index: dict[str, dict] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        user_key = str(row.get("userKey") or "").strip()
        lines = row.get("lines") or []
        if not user_key or not isinstance(lines, list) or not lines:
            continue
        primary = next((item for item in lines if isinstance(item, dict) and item.get("primary")), None)
        if primary is None:
            primary = next((item for item in lines if isinstance(item, dict)), None)
        if not primary:
            continue
        line_id = str(primary.get("id") or "").strip()
        if not line_id:
            continue
        index[user_key] = {
            "id": line_id,
            "name": str(primary.get("name") or "").strip(),
            "number": str(primary.get("number") or "").strip(),
        }
    return index


def _line_for_user(user_key: str, attempts: int = 1) -> dict | None:
    user_key = str(user_key or "").strip()
    if not user_key:
        return None
    for attempt in range(max(attempts, 1)):
        line = _user_lines_index().get(user_key)
        if line:
            return line
        if attempt + 1 < attempts:
            time.sleep(1.5)
    return None


def _phone_by_number(number: str) -> dict | None:
    wanted = _digits(number)
    if not wanted:
        return None
    for row in list_account_phones():
        if _digits(row.get("number") or "") == wanted:
            return row
    return None


def _phone_detail(phone_id: str) -> dict:
    data = _request("GET", f"{VOICE_BASE}/phone-numbers/{phone_id}")
    return data if isinstance(data, dict) else {}


def _extension_detail(line_id: str) -> dict:
    data = _request("GET", f"{VOICE_BASE}/extensions/{line_id}")
    return data if isinstance(data, dict) else {}


def _reset_caller_id_if_phone(line_id: str, phone_id: str) -> None:
    if not line_id or not phone_id:
        return
    try:
        ext = _extension_detail(line_id)
    except RuntimeError:
        return
    caller = ext.get("callerId") if isinstance(ext.get("callerId"), dict) else {}
    if str(caller.get("phoneNumberId") or "") != phone_id:
        return
    _request("PATCH", f"{VOICE_BASE}/extensions/{line_id}", {"callerId": {"useDefault": True}})


def _unroute_phone_if_on_line(phone_id: str, line_id: str, restore_name: str = "") -> dict:
    if not phone_id or not line_id:
        return {}
    detail = _phone_detail(phone_id)
    route = detail.get("routeTo") if isinstance(detail.get("routeTo"), dict) else {}
    if str(route.get("id") or "") != line_id:
        return detail
    payload: dict = {"routeTo": None}
    if restore_name:
        payload["name"] = restore_name
    updated = _request("PATCH", f"{VOICE_BASE}/phone-numbers/{phone_id}", payload)
    return updated if isinstance(updated, dict) else detail


def _phone_permissions(phone_id: str) -> list[dict]:
    data = _request("GET", f"{VOICE_BASE}/phone-numbers/{phone_id}/permissions")
    items = data.get("items") if isinstance(data, dict) else data
    if not isinstance(items, list):
        return []
    return [item for item in items if isinstance(item, dict)]


def _grant_outbound_caller_id(phone_id: str, user_key: str) -> None:
    current: list[str] = []
    for item in _phone_permissions(phone_id):
        if str(item.get("userKey") or "") == user_key:
            current = [str(perm) for perm in (item.get("permissions") or []) if perm]
            break
    if "OUTBOUND_CALLER_ID" in current:
        return
    permissions = list(dict.fromkeys([*current, "OUTBOUND_CALLER_ID"]))
    _request(
        "POST",
        f"{VOICE_BASE}/phone-numbers/{phone_id}/permissions",
        {"userKey": user_key, "permissions": permissions},
    )


def _revoke_phone_permission(phone_id: str, user_key: str) -> None:
    if not phone_id or not user_key:
        return
    _request(
        "DELETE",
        f"{VOICE_BASE}/phone-numbers/{phone_id}/permissions/{user_key}",
        allow_codes={404},
    )


def _restore_phone_name(phone_id: str, display_name: str) -> None:
    if not phone_id or not display_name:
        return
    detail = _phone_detail(phone_id)
    if (detail.get("name") or "").strip() != display_name:
        return
    _request(
        "PATCH",
        f"{VOICE_BASE}/phone-numbers/{phone_id}",
        {"name": f"Livre - (Antiga {display_name})"},
    )


def _release_phone_from_user(phone_id: str, user_key: str, line_id: str, display_name: str) -> None:
    if not phone_id:
        return
    _revoke_phone_permission(phone_id, user_key)
    _reset_caller_id_if_phone(line_id, phone_id)
    restore_name = f"Livre - (Antiga {display_name})" if display_name else ""
    _unroute_phone_if_on_line(phone_id, line_id, restore_name)
    _restore_phone_name(phone_id, display_name)


def inherit_phone_number(
    user_key: str,
    number: str,
    display_name: str = "",
    previous_number: str = "",
    wait_for_line: bool = False,
) -> dict:
    user_key = str(user_key or "").strip()
    number = (number or "").strip()
    previous_number = (previous_number or "").strip()
    display_name = " ".join((display_name or "").split())
    if not user_key:
        raise RuntimeError("Usuário GoTo inválido")
    line = _line_for_user(user_key, attempts=4 if wait_for_line else 1)
    line_id = str((line or {}).get("id") or "")
    previous = _phone_by_number(previous_number) if previous_number else None
    if previous and _digits(previous.get("number") or "") != _digits(number):
        _release_phone_from_user(previous["id"], user_key, line_id, display_name)
    if not number:
        return {"number": "", "line_id": line_id}
    if not line_id:
        raise RuntimeError("Este usuário ainda não tem ramal na GoTo. Confira a licença de voz.")
    phone = _phone_by_number(number)
    if not phone:
        raise RuntimeError("Número não encontrado na lista telefônica da GoTo")
    phone_id = phone["id"]
    payload: dict = {
        "routeTo": {"id": line_id, "type": "EXTENSION"},
    }
    if display_name:
        payload["name"] = display_name
    updated = phone
    patched = _request("PATCH", f"{VOICE_BASE}/phone-numbers/{phone_id}", payload)
    if isinstance(patched, dict):
        updated = patched
    _grant_outbound_caller_id(phone_id, user_key)
    _request(
        "PATCH",
        f"{VOICE_BASE}/extensions/{line_id}",
        {"callerId": {"useDefault": False, "phoneNumberId": phone_id}},
    )
    return {
        "id": str(updated.get("id") or phone_id),
        "number": str(updated.get("number") or phone.get("number") or number),
        "name": str(updated.get("name") or display_name or phone.get("name") or ""),
        "line_id": line_id,
    }
