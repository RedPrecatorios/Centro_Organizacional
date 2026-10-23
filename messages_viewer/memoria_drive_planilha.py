# -*- coding: utf-8 -*-
"""Localiza e descarrega a planilha de cálculo no Google Drive (pasta PLANS).

Árvore esperada (igual ao API_CALCULO):
``raiz / Ano / Mês / dd-mm-aaaa / processo[_incidente] / ficheiro.xlsx``
"""
from __future__ import annotations

import io
import json
import os
import re
import time
from datetime import date, datetime, timedelta
from typing import Any, Callable

# Mesmo escopo do token do API_CALCULO / ATUALIZACAO_CALCULO.
# Pedir drive.readonly no refresh com token emitido para ``drive`` → invalid_scope.
_SCOPES = ("https://www.googleapis.com/auth/drive",)
_DRIVE_RETRY_ATTEMPTS = 4
_DRIVE_RETRY_BASE_SEC = 0.7
_FOLDER_MIME = "application/vnd.google-apps.folder"
_XLSX_HINTS = (".xlsx", ".xlsm")
_MESES = (
    "Janeiro",
    "Fevereiro",
    "Março",
    "Abril",
    "Maio",
    "Junho",
    "Julho",
    "Agosto",
    "Setembro",
    "Outubro",
    "Novembro",
    "Dezembro",
)
_RE_DRIVE_FOLDER_ID = re.compile(
    r"(?:drive\.google\.com/(?:drive/)?(?:u/\d+/)?folders/|/folders/)([a-zA-Z0-9_-]+)"
)
# Árvore datada só como recurso (a pesquisa por nome é a via rápida).
_LOOKBACK_DAYS = 21


def _env(raw: str | None) -> str:
    return (raw or "").strip().split("#", 1)[0].strip().strip("'\"")


def parse_drive_folder_link(link_or_id: str) -> str | None:
    text = _env(link_or_id)
    if not text:
        return None
    if re.match(r"^[a-zA-Z0-9_-]+$", text):
        return text
    match = _RE_DRIVE_FOLDER_ID.search(text)
    return match.group(1) if match else None


def drive_folder_id() -> str | None:
    for key in (
        "MEMORIA_DRIVE_FOLDER_ID",
        "MEMORIA_CALCULO_DRIVE_FOLDER_ID",
        "DRIVE_SHARED_FOLDER_LINK",
        "GOOGLE_DRIVE_FOLDER_ID",
    ):
        fid = parse_drive_folder_link(os.getenv(key) or "")
        if fid:
            return fid
    return None


def _oauth_token_path() -> str | None:
    for key in (
        "MEMORIA_DRIVE_TOKEN_PATH",
        "GOOGLE_DRIVE_TOKEN_PATH",
        "DRIVE_TOKEN_PATH",
    ):
        p = _env(os.getenv(key))
        if p and os.path.isfile(p):
            return p
    return None


def _oauth_credentials_path() -> str | None:
    for key in (
        "MEMORIA_DRIVE_CREDENTIALS_PATH",
        "GOOGLE_DRIVE_CREDENTIALS_PATH",
        "DRIVE_CREDENTIALS_PATH",
    ):
        p = _env(os.getenv(key))
        if p and os.path.isfile(p):
            return p
    return None


def is_configured() -> bool:
    return bool(drive_folder_id() and _oauth_token_path())


def _pontos_tracos_para_sublinhado(value: object) -> str:
    text = str(value or "").strip().replace(".", "_").replace("-", "_")
    text = re.sub(r"_+", "_", text).strip("_")
    return text


def pasta_processo_incidente(processo: str, incidente: str = "") -> str:
    proc = _pontos_tracos_para_sublinhado(processo)
    inc = _pontos_tracos_para_sublinhado(incidente)
    if proc and inc:
        return f"{proc}_{inc}"
    return proc or inc or ""


def _digits(value: object) -> str:
    return re.sub(r"\D", "", str(value or ""))


def _scopes_from_token_file(token_path: str) -> list[str]:
    """Usa os scopes gravados no token; evita invalid_scope no refresh."""
    try:
        with open(token_path, encoding="utf-8") as fh:
            data = json.load(fh)
    except (OSError, json.JSONDecodeError, TypeError):
        return list(_SCOPES)
    scopes = data.get("scopes") or data.get("scope")
    if isinstance(scopes, str):
        scopes = [s for s in scopes.split() if s]
    if isinstance(scopes, (list, tuple)) and scopes:
        return [str(s) for s in scopes if str(s).strip()]
    return list(_SCOPES)


def _build_service() -> Any:
    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials
    from googleapiclient.discovery import build

    token_path = _oauth_token_path()
    if not token_path:
        raise RuntimeError(
            "Drive: falta token OAuth (MEMORIA_DRIVE_TOKEN_PATH ou GOOGLE_DRIVE_TOKEN_PATH)."
        )
    scopes = _scopes_from_token_file(token_path)
    creds = Credentials.from_authorized_user_file(token_path, scopes=scopes)
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
        try:
            with open(token_path, "w", encoding="utf-8") as fh:
                fh.write(creds.to_json())
        except OSError:
            pass
    if not creds or not creds.valid:
        raise RuntimeError("Drive: token OAuth inválido. Renove o token do API_CALCULO.")
    return build("drive", "v3", credentials=creds, cache_discovery=False)


def _list_kwargs(extra: dict[str, Any] | None = None) -> dict[str, Any]:
    kwargs: dict[str, Any] = {
        "supportsAllDrives": True,
        "includeItemsFromAllDrives": True,
        "pageSize": 100,
        "fields": "nextPageToken, files(id, name, mimeType, modifiedTime, size)",
    }
    if extra:
        kwargs.update(extra)
    return kwargs


def _http_status(err: BaseException) -> int | None:
    resp = getattr(err, "resp", None)
    status = getattr(resp, "status", None) if resp is not None else None
    try:
        return int(status) if status is not None else None
    except (TypeError, ValueError):
        return None


def _is_retryable_drive_error(err: BaseException) -> bool:
    status = _http_status(err)
    if status in (429, 500, 502, 503, 504):
        return True
    text = str(err).lower()
    return "internal error" in text or "internalerror" in text or "backend error" in text


def _execute_with_retry(request_factory: Callable[[], Any]) -> Any:
    """Executa ``.execute()`` com retry curto — o Drive devolve 500/internalError com frequência."""
    try:
        from googleapiclient.errors import HttpError
    except Exception:  # pragma: no cover
        HttpError = Exception  # type: ignore

    last_err: BaseException | None = None
    for attempt in range(_DRIVE_RETRY_ATTEMPTS):
        try:
            return request_factory().execute()
        except HttpError as e:  # type: ignore[misc]
            last_err = e
            if _is_retryable_drive_error(e) and attempt < _DRIVE_RETRY_ATTEMPTS - 1:
                time.sleep(_DRIVE_RETRY_BASE_SEC * (attempt + 1))
                continue
            raise
        except Exception as e:
            # Alguns wrappers não expõem HttpError tipado.
            if _is_retryable_drive_error(e) and attempt < _DRIVE_RETRY_ATTEMPTS - 1:
                last_err = e
                time.sleep(_DRIVE_RETRY_BASE_SEC * (attempt + 1))
                continue
            raise
    assert last_err is not None
    raise last_err


def _find_child_folder(service: Any, parent_id: str, name: str) -> str | None:
    if not parent_id or not name:
        return None
    safe = name.replace("\\", "\\\\").replace("'", "\\'")
    q = (
        f"'{parent_id}' in parents and mimeType='{_FOLDER_MIME}' "
        f"and name='{safe}' and trashed=false"
    )
    resp = _execute_with_retry(lambda: service.files().list(**_list_kwargs({"q": q})))
    files = resp.get("files") or []
    return files[0]["id"] if files else None


def _list_spreadsheets(service: Any, parent_id: str) -> list[dict[str, Any]]:
    q = f"'{parent_id}' in parents and trashed=false"
    out: list[dict[str, Any]] = []
    page_token: str | None = None
    while True:
        kwargs = _list_kwargs({"q": q})
        if page_token:
            kwargs["pageToken"] = page_token
        # Capture kwargs by value for the lambda default.
        resp = _execute_with_retry(
            lambda kw=dict(kwargs): service.files().list(**kw)
        )
        for f in resp.get("files") or []:
            name = str(f.get("name") or "")
            mime = str(f.get("mimeType") or "")
            if name.lower().endswith(_XLSX_HINTS) or "spreadsheet" in mime or "excel" in mime:
                out.append(f)
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return out


_REQ_STOPWORDS = frozenset(
    {"DE", "DA", "DO", "DAS", "DOS", "E", "DI", "DEL", "VAN", "VON"}
)


def _requerente_tokens(requerente: str) -> list[str]:
    """Tokens significativos do nome (ignora DE/DA/…); útil para casar no nome do .xlsx."""
    parts = re.split(r"[\s._]+", str(requerente or "").upper().strip())
    return [p for p in parts if len(p) >= 3 and p not in _REQ_STOPWORDS]


def _name_normalized(name: str) -> str:
    return re.sub(r"[\s._]+", " ", str(name or "").upper()).strip()


def _requerente_token_hits(file_meta: dict[str, Any], requerente: str) -> int:
    name_n = _name_normalized(str(file_meta.get("name") or ""))
    if not name_n:
        return 0
    return sum(1 for t in _requerente_tokens(requerente) if t in name_n)


def _score_file(file_meta: dict[str, Any], *, requerente: str, processo: str) -> tuple:
    name = str(file_meta.get("name") or "")
    name_u = name.upper()
    name_n = _name_normalized(name)
    req = " ".join(str(requerente or "").upper().split())
    tokens = _requerente_tokens(requerente)
    dig_full = _digits(processo)
    dig_name = _digits(name)
    score = 0
    if req and req in name_n:
        score += 120
    elif tokens:
        hits = sum(1 for t in tokens if t in name_n)
        if hits == len(tokens) and hits >= 2:
            score += 110
        elif hits >= 2:
            score += 80
        elif hits == 1:
            # Um só token (ex. só «MARCOS») é fraco — vários processos partilham o nº.
            score += 25
    if dig_full and dig_full in dig_name:
        score += 50
    elif len(dig_full) >= 15 and dig_full[:15] in dig_name:
        score += 45
    elif len(dig_full) >= 7 and dig_full[:7] in dig_name:
        # Só os 7 primeiros dígitos colidem entre partes do mesmo processo.
        score += 15
    if name_u.startswith("PRIORI"):
        score += 5
    modified = str(file_meta.get("modifiedTime") or "")
    return (score, modified)


def _pick_best(
    files: list[dict[str, Any]], *, requerente: str, processo: str
) -> dict[str, Any] | None:
    if not files:
        return None
    ranked = sorted(
        files,
        key=lambda f: _score_file(f, requerente=requerente, processo=processo),
        reverse=True,
    )
    if requerente and _requerente_tokens(requerente):
        # Com nome na pesquisa: nunca devolver planilha de outra pessoa só pelo nº.
        with_name = [
            f for f in ranked if _requerente_token_hits(f, requerente) >= 1
        ]
        if not with_name:
            return None
        # Preferir ≥2 tokens (nome+sobrenome) quando existirem.
        strong = [
            f for f in with_name if _requerente_token_hits(f, requerente) >= 2
        ]
        pool = strong or with_name
        return sorted(
            pool,
            key=lambda f: _score_file(f, requerente=requerente, processo=processo),
            reverse=True,
        )[0]
    return ranked[0]


def _iter_lookback_dates(days: int = _LOOKBACK_DAYS):
    today = date.today()
    for offset in range(days):
        yield today - timedelta(days=offset)


def _escape_drive_q(value: str) -> str:
    return str(value or "").replace("\\", "\\\\").replace("'", "\\'")


def _search_spreadsheets_by_name(
    service: Any,
    needles: list[str],
    *,
    requerente: str,
    processo: str,
    page_size: int = 40,
) -> dict[str, Any] | None:
    """1–N listagens por ``name contains`` — bem mais rápido que varrer 90 dias."""
    seen_q: set[str] = set()
    collected: list[dict[str, Any]] = []
    seen_ids: set[str] = set()

    def _consume(files: list[dict[str, Any]]) -> dict[str, Any] | None:
        for f in files:
            fid = str(f.get("id") or "")
            if fid and fid in seen_ids:
                continue
            if fid:
                seen_ids.add(fid)
            collected.append(f)
        best = _pick_best(collected, requerente=requerente, processo=processo)
        if not best:
            return None
        score, _ = _score_file(best, requerente=requerente, processo=processo)
        # Só corta cedo com match de nome (evita Sandra por só «0014301»).
        min_early = 80 if requerente else 40
        if score >= min_early:
            return {
                "id": best["id"],
                "name": best.get("name"),
                "modifiedTime": best.get("modifiedTime"),
                "folder": None,
                "date": None,
            }
        return None

    # Consultas compostas: sobrenome + dígitos do processo (mais precisas).
    dig = _digits(processo)
    tokens = _requerente_tokens(requerente)
    compound: list[list[str]] = []
    if tokens and len(dig) >= 7:
        last = tokens[-1]
        first = tokens[0]
        compound.append([last, dig[:7]])
        if first != last:
            compound.append([first, last, dig[:7]])
        if len(dig) >= 15:
            compound.append([last, dig[:15]])
    for parts in compound:
        clauses = " and ".join(f"name contains '{_escape_drive_q(p)}'" for p in parts)
        q = (
            f"{clauses} and trashed=false and "
            f"(mimeType contains 'spreadsheet' or mimeType contains 'excel' "
            f"or name contains '.xlsx' or name contains '.xlsm')"
        )
        if q in seen_q:
            continue
        seen_q.add(q)
        list_kw = _list_kwargs({"q": q, "pageSize": page_size})
        resp = _execute_with_retry(lambda kw=dict(list_kw): service.files().list(**kw))
        hit = _consume(list(resp.get("files") or []))
        if hit:
            return hit

    for needle in needles:
        raw = str(needle or "").strip()
        if len(raw) < 5:
            continue
        safe = _escape_drive_q(raw)
        q = (
            f"name contains '{safe}' and trashed=false and "
            f"(mimeType contains 'spreadsheet' or mimeType contains 'excel' "
            f"or name contains '.xlsx' or name contains '.xlsm')"
        )
        if q in seen_q:
            continue
        seen_q.add(q)
        list_kw = _list_kwargs({"q": q, "pageSize": page_size})
        resp = _execute_with_retry(lambda kw=dict(list_kw): service.files().list(**kw))
        hit = _consume(list(resp.get("files") or []))
        if hit:
            return hit

    if not collected:
        return None
    best = _pick_best(collected, requerente=requerente, processo=processo)
    if not best:
        return None
    return {
        "id": best["id"],
        "name": best.get("name"),
        "modifiedTime": best.get("modifiedTime"),
        "folder": None,
        "date": None,
    }


def _find_via_case_folders(
    service: Any,
    candidates: list[str],
    *,
    requerente: str,
    processo: str,
) -> dict[str, Any] | None:
    """Localiza pastas com o nome do processo e escolhe o .xlsx do requerente."""
    for nome in candidates:
        if not nome:
            continue
        safe = _escape_drive_q(nome)
        q = (
            f"name = '{safe}' and mimeType = '{_FOLDER_MIME}' and trashed=false"
        )
        list_kw = _list_kwargs({"q": q, "pageSize": 25})
        resp = _execute_with_retry(lambda kw=dict(list_kw): service.files().list(**kw))
        for folder in resp.get("files") or []:
            folder_id = str(folder.get("id") or "")
            if not folder_id:
                continue
            files = _list_spreadsheets(service, folder_id)
            best = _pick_best(files, requerente=requerente, processo=processo)
            if best:
                return {
                    "id": best["id"],
                    "name": best.get("name"),
                    "modifiedTime": best.get("modifiedTime"),
                    "folder": nome,
                    "date": None,
                }
    return None


def _find_in_dated_tree(
    service: Any,
    root: str,
    candidates: list[str],
    *,
    requerente: str,
    processo: str,
) -> dict[str, Any] | None:
    """Varre Ano/Mês/dd-mm-aaaa (lookback curto). Último recurso, sem requerente."""
    year_cache: dict[int, str | None] = {}
    month_cache: dict[tuple[int, int], str | None] = {}
    for day in _iter_lookback_dates():
        if day.year not in year_cache:
            year_cache[day.year] = _find_child_folder(service, root, str(day.year))
        year_id = year_cache[day.year]
        if not year_id:
            continue
        mkey = (day.year, day.month)
        if mkey not in month_cache:
            month_cache[mkey] = _find_child_folder(
                service, year_id, _MESES[day.month - 1]
            )
        month_id = month_cache[mkey]
        if not month_id:
            continue
        date_id = _find_child_folder(service, month_id, day.strftime("%d-%m-%Y"))
        if not date_id:
            continue
        for nome in candidates:
            case_id = _find_child_folder(service, date_id, nome)
            if not case_id:
                continue
            files = _list_spreadsheets(service, case_id)
            best = _pick_best(files, requerente=requerente, processo=processo)
            if best:
                return {
                    "id": best["id"],
                    "name": best.get("name"),
                    "modifiedTime": best.get("modifiedTime"),
                    "folder": nome,
                    "date": day.strftime("%d-%m-%Y"),
                }
    return None


def find_planilha(
    *,
    processo: str,
    incidente: str = "",
    requerente: str = "",
) -> dict[str, Any] | None:
    """Devolve metadados do ficheiro Drive ou None.

    Ordem: (1) nome+processo, (2) pastas do processo, (3) árvore datada só sem requerente.
    Com requerente, nunca devolve planilha de outra pessoa só pelo nº do processo.
    """
    root = drive_folder_id()
    if not root:
        raise RuntimeError("Drive: pasta raiz não configurada (MEMORIA_DRIVE_FOLDER_ID).")
    processo = str(processo or "").strip()
    if not processo:
        raise ValueError("Indique o número do processo.")
    pasta_nome = pasta_processo_incidente(processo, incidente)
    pasta_so_proc = _pontos_tracos_para_sublinhado(processo)
    candidates = [p for p in (pasta_nome, pasta_so_proc) if p]
    service = _build_service()

    dig = _digits(processo)
    needles: list[str] = []
    # Preferir trechos longos / pasta; os 7 dígitos só no fim (muitos falsos positivos).
    if len(dig) >= 15:
        needles.append(dig[:15])
    for n in candidates:
        if n and n not in needles:
            needles.append(n)
    if pasta_so_proc:
        head = "_".join(pasta_so_proc.split("_")[:2])
        if head and head not in needles:
            needles.append(head)
    if len(dig) >= 7 and dig[:7] not in needles:
        needles.append(dig[:7])

    found = _search_spreadsheets_by_name(
        service, needles, requerente=requerente, processo=processo
    )
    if found:
        return found

    found = _find_via_case_folders(
        service, candidates, requerente=requerente, processo=processo
    )
    if found:
        return found

    # Com requerente já tentámos casar o nome — não varrer 21 dias (lento e arriscado).
    if str(requerente or "").strip():
        return None

    return _find_in_dated_tree(
        service,
        root,
        candidates,
        requerente=requerente,
        processo=processo,
    )


def download_planilha_bytes(file_id: str) -> tuple[bytes, str]:
    """Descarrega o ficheiro; devolve (conteúdo, nome)."""
    from googleapiclient.http import MediaIoBaseDownload

    if not file_id:
        raise ValueError("file_id vazio.")
    service = _build_service()
    meta = _execute_with_retry(
        lambda: service.files().get(
            fileId=file_id, fields="id, name", supportsAllDrives=True
        )
    )
    name = str(meta.get("name") or "planilha.xlsx")
    buf = io.BytesIO()
    last_err: BaseException | None = None
    for attempt in range(_DRIVE_RETRY_ATTEMPTS):
        try:
            buf.seek(0)
            buf.truncate(0)
            request = service.files().get_media(
                fileId=file_id, supportsAllDrives=True
            )
            downloader = MediaIoBaseDownload(buf, request)
            done = False
            while not done:
                _, done = downloader.next_chunk()
            return buf.getvalue(), name
        except Exception as e:
            last_err = e
            if _is_retryable_drive_error(e) and attempt < _DRIVE_RETRY_ATTEMPTS - 1:
                time.sleep(_DRIVE_RETRY_BASE_SEC * (attempt + 1))
                continue
            raise
    assert last_err is not None
    raise last_err
