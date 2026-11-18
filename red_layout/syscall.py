from __future__ import annotations

from html import unescape
from http.cookiejar import CookieJar
import os
import re
import unicodedata
import urllib.error
import urllib.parse
import urllib.request

DEFAULT_URL = "http://redpre.syscall.com.br/crm"
USERS_TABLE = re.compile(
    r'<table class="table table-bordered table-striped data-table">(?P<body>.*?)</table>',
    re.I | re.S,
)
USER_TR = re.compile(r"<tr>(.*?)</tr>", re.I | re.S)
USER_TD = re.compile(r"<td>(.*?)</td>", re.I | re.S)
DELETE_ID = re.compile(r"usuarios/delete/(\d+)", re.I)
ALERT = re.compile(
    r'class="[^"]*alert[^"]*"[^>]*>(.*?)</div>',
    re.I | re.S,
)
SCRIPT = re.compile(r"<script\b[^>]*>.*?</script>", re.I | re.S)
STYLE = re.compile(r"<style\b[^>]*>.*?</style>", re.I | re.S)
HELP = re.compile(r'class="[^"]*help-block[^"]*"[^>]*>(.*?)</', re.I | re.S)
JS_NOISE = re.compile(
    r"(\+ '|res\.|===|xhr\.|function\s*\(|alert-success|total_linhas|blacklist)",
    re.I,
)


def _env(*names: str) -> str:
    for name in names:
        value = os.getenv(name)
        if value and value.strip():
            return value.strip().strip('"').strip("'")
    return ""


def _base() -> str:
    return (_env("SYSCALL_URL") or DEFAULT_URL).rstrip("/")


def _creds() -> tuple[str, str]:
    user = _env("SYSCALL_USER")
    password = _env("SYSCALL_PASS", "SYSCALL_PASSWORD")
    if not user or not password:
        raise RuntimeError("Credenciais SysCall ausentes. Defina SYSCALL_USER e SYSCALL_PASS no arquivo .env")
    return user, password


def login_name(email: str) -> str:
    local = (email or "").strip().split("@", 1)[0].strip()
    if not local:
        raise RuntimeError("E-mail inválido para o SysCall.")
    return local


def display_name(first_name: str, last_name: str) -> str:
    return " ".join(part for part in ((first_name or "").strip(), (last_name or "").strip()) if part)


def compact_login(first_name: str, last_name: str) -> str:
    raw = f"{first_name or ''}{last_name or ''}"
    raw = unicodedata.normalize("NFD", raw)
    raw = "".join(ch for ch in raw if unicodedata.category(ch) != "Mn")
    return re.sub(r"[^A-Za-z0-9]", "", raw)


def _clean(value: str) -> str:
    return unescape(re.sub(r"\s+", " ", value or "")).strip()


def _opener() -> urllib.request.OpenerDirector:
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(CookieJar()))
    opener.addheaders = [
        ("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) RED-Layout"),
        ("Accept", "text/html,application/xhtml+xml"),
    ]
    return opener


def _read(resp) -> str:
    raw = resp.read()
    charset = resp.headers.get_content_charset() or "utf-8"
    return raw.decode(charset, errors="replace")


def _open(opener: urllib.request.OpenerDirector, url: str, data: bytes | None = None, referer: str = "") -> tuple[str, str]:
    headers = {}
    if referer:
        headers["Referer"] = referer
    if data is not None:
        headers["Content-Type"] = "application/x-www-form-urlencoded"
    req = urllib.request.Request(url, data=data, headers=headers, method="POST" if data is not None else "GET")
    try:
        resp = opener.open(req, timeout=45)
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"SysCall HTTP {exc.code} em {url}: {body[:240]}") from exc
    except urllib.error.URLError as exc:
        reason = str(exc.reason or "")
        if "timed out" in reason.lower() or "timeout" in reason.lower():
            raise RuntimeError(
                "O SysCall não respondeu a partir deste servidor (68.183.23.91). "
                "O endereço redpre.syscall.com.br precisa aceitar conexão desta máquina."
            ) from exc
        raise RuntimeError(f"Falha ao falar com o SysCall: {exc.reason}") from exc
    return resp.geturl(), _read(resp)


def _login(opener: urllib.request.OpenerDirector) -> None:
    user, password = _creds()
    base = _base()
    payload = urllib.parse.urlencode({"username": user, "password": password}).encode("utf-8")
    url, html = _open(opener, f"{base}/login/login", data=payload, referer=f"{base}/login/")
    if "loginform" in html.lower() or "/login" in url.lower():
        raise RuntimeError("Falha no login do SysCall. Verifique SYSCALL_USER e SYSCALL_PASS.")


def _parse_users(html: str) -> list[dict]:
    match = USERS_TABLE.search(html or "")
    if not match:
        return []
    users: list[dict] = []
    for row in USER_TR.finditer(match.group("body")):
        inner = row.group(1)
        cells = [_clean(re.sub(r"<[^>]+>", " ", cell)) for cell in USER_TD.findall(inner)]
        if len(cells) < 2:
            continue
        nome = cells[0]
        usuario = cells[1]
        if not usuario or usuario.casefold() in {"usuário", "usuario"}:
            continue
        deleted = DELETE_ID.search(inner)
        users.append(
            {
                "id": deleted.group(1) if deleted else "",
                "nome": nome,
                "usuario": usuario,
                "tipo": cells[2] if len(cells) > 2 else "",
                "grupo": cells[3] if len(cells) > 3 else "",
                "equipe": cells[4] if len(cells) > 4 else "",
            }
        )
    return users


def _aliases(email: str, first_name: str = "", last_name: str = "") -> set[str]:
    usuario = login_name(email)
    aliases = {usuario.casefold(), usuario.replace(".", "").casefold(), (email or "").strip().casefold()}
    compact = compact_login(first_name, last_name)
    if compact:
        aliases.add(compact.casefold())
    return {item for item in aliases if item}


def _find_user(users: list[dict], email: str, first_name: str = "", last_name: str = "") -> dict | None:
    aliases = _aliases(email, first_name, last_name)
    nome = display_name(first_name, last_name).casefold()
    for row in users:
        usuario = (row.get("usuario") or "").casefold()
        if usuario in aliases:
            return row
        if nome and (row.get("nome") or "").casefold() == nome:
            return row
    return None


def _plain_html(html: str) -> str:
    html = SCRIPT.sub(" ", html or "")
    return STYLE.sub(" ", html)


def _useful_text(value: str) -> str:
    text = _clean(re.sub(r"<[^>]+>", " ", value or ""))
    if not text or JS_NOISE.search(text):
        return ""
    folded = text.casefold()
    if "importa" in folded and ("conclu" in folded or "blacklist" in folded):
        return ""
    if "erro de comunica" in folded:
        return ""
    return text


def _alert_text(html: str) -> str:
    html = _plain_html(html)
    parts = []
    for match in ALERT.finditer(html):
        text = _useful_text(match.group(1))
        if text:
            parts.append(text)
    for match in HELP.finditer(html):
        text = _useful_text(match.group(1))
        if text:
            parts.append(text)
    return " ".join(parts)[:300]


def account_info() -> dict:
    opener = _opener()
    _login(opener)
    _url, html = _open(opener, f"{_base()}/usuarios")
    if "usuarios/new" not in html:
        raise RuntimeError("Login SysCall ok, mas a aba Usuários não abriu.")
    users = _parse_users(html)
    return {
        "url": _base(),
        "users": len(users),
    }


def provision_user(email: str, first_name: str, last_name: str) -> dict:
    email = (email or "").strip()
    first_name = (first_name or "").strip()
    last_name = (last_name or "").strip()
    if not first_name or not last_name:
        raise RuntimeError("Nome e sobrenome são obrigatórios.")
    if "@" not in email:
        raise RuntimeError("Informe um e-mail válido para o SysCall.")

    usuario = login_name(email)
    nome = display_name(first_name, last_name)
    opener = _opener()
    _login(opener)
    base = _base()
    _url, lista = _open(opener, f"{base}/usuarios")
    existing = _find_user(_parse_users(lista), email, first_name, last_name)
    if existing:
        return {
            "created": False,
            "existed": True,
            "skipped": False,
            "username": existing.get("usuario") or usuario,
            "password": "",
            "nome": existing.get("nome") or nome,
            "warning": "",
        }

    _url, _form = _open(opener, f"{base}/usuarios/new")
    candidates = [usuario]
    compact = compact_login(first_name, last_name)
    if compact and compact.casefold() != usuario.casefold():
        candidates.append(compact)

    last_url = ""
    last_html = ""
    last_user = usuario
    for candidate in candidates:
        last_user = candidate
        payload = urllib.parse.urlencode(
            {
                "nome": nome,
                "email": email,
                "usuario": candidate,
                "senha": candidate,
                "conf_senha": candidate,
                "tipo": "Operador",
                "grupo": "2",
                "equipe": "1",
            }
        ).encode("utf-8")
        last_url, last_html = _open(
            opener,
            f"{base}/usuarios/new",
            data=payload,
            referer=f"{base}/usuarios/new",
        )
        _url, lista = _open(opener, f"{base}/usuarios")
        created = _find_user(_parse_users(lista), email, first_name, last_name)
        if created:
            return {
                "created": True,
                "existed": False,
                "skipped": False,
                "username": created.get("usuario") or candidate,
                "password": candidate,
                "nome": created.get("nome") or nome,
                "warning": "",
            }

    detail = _alert_text(last_html) or _alert_text(lista)
    if not detail:
        detail = "O formulário não salvou. Confira se o e-mail ou o usuário já existem no SysCall."
    raise RuntimeError(f"SysCall não confirmou o usuário {last_user}. {detail}")


def skipped_result() -> dict:
    return {
        "created": False,
        "existed": False,
        "skipped": True,
        "username": "",
        "password": "",
        "nome": "",
        "warning": "",
    }


def manual_result(email: str, first_name: str, last_name: str) -> dict:
    usuario = login_name(email)
    return {
        "created": False,
        "existed": False,
        "skipped": True,
        "username": usuario,
        "password": usuario,
        "nome": display_name(first_name, last_name),
        "warning": "",
    }


def delete_user(email: str) -> dict:
    email = (email or "").strip()
    if "@" not in email:
        return {
            "deleted": False,
            "missing": True,
            "username": "",
            "warning": "",
        }
    usuario = login_name(email)
    opener = _opener()
    _login(opener)
    base = _base()
    _url, lista = _open(opener, f"{base}/usuarios")
    existing = _find_user(_parse_users(lista), email)
    if not existing:
        return {
            "deleted": False,
            "missing": True,
            "username": usuario,
            "warning": "",
        }
    bot_user = _creds()[0].casefold()
    found_user = (existing.get("usuario") or "").casefold()
    if found_user and found_user == bot_user:
        return {
            "deleted": False,
            "missing": False,
            "username": existing.get("usuario") or usuario,
            "warning": "SysCall: não apaguei o usuário da automação.",
        }
    user_id = str(existing.get("id") or "").strip()
    if not user_id:
        raise RuntimeError(f"SysCall: achei {usuario} na lista, mas sem link de exclusão.")
    after_url, after_html = _open(
        opener,
        f"{base}/usuarios/delete/{user_id}",
        referer=f"{base}/usuarios",
    )
    _url, lista = _open(opener, f"{base}/usuarios")
    still = _find_user(_parse_users(lista), email)
    if still is None:
        return {
            "deleted": True,
            "missing": False,
            "username": existing.get("usuario") or usuario,
            "warning": "",
        }
    detail = _alert_text(after_html) or _alert_text(lista)
    raise RuntimeError(
        f"SysCall não removeu o usuário {usuario} (id={user_id})."
        + (f" {detail}" if detail else "")
        + (f" Página: {after_url}" if after_url else "")
    )
