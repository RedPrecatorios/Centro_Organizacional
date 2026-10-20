# -*- coding: utf-8 -*-
"""Localiza e descarrega a planilha de cálculo no Google Drive (pasta PLANS).

Árvore esperada (igual ao API_CALCULO):
``raiz / Ano / Mês / dd-mm-aaaa / processo[_incidente] / ficheiro.xlsx``
"""
from __future__ import annotations

import io
import os
import re
from datetime import date, datetime, timedelta
from typing import Any

_SCOPES = ("https://www.googleapis.com/auth/drive.readonly",)
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
_LOOKBACK_DAYS = 90


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


def _build_service() -> Any:
    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials
    from googleapiclient.discovery import build

    token_path = _oauth_token_path()
    if not token_path:
        raise RuntimeError(
            "Drive: falta token OAuth (MEMORIA_DRIVE_TOKEN_PATH ou GOOGLE_DRIVE_TOKEN_PATH)."
        )
    creds = Credentials.from_authorized_user_file(token_path, scopes=list(_SCOPES))
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


def _find_child_folder(service: Any, parent_id: str, name: str) -> str | None:
    if not parent_id or not name:
        return None
    safe = name.replace("\\", "\\\\").replace("'", "\\'")
    q = (
        f"'{parent_id}' in parents and mimeType='{_FOLDER_MIME}' "
        f"and name='{safe}' and trashed=false"
    )
    resp = service.files().list(**_list_kwargs({"q": q})).execute()
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
        resp = service.files().list(**kwargs).execute()
        for f in resp.get("files") or []:
            name = str(f.get("name") or "")
            mime = str(f.get("mimeType") or "")
            if name.lower().endswith(_XLSX_HINTS) or "spreadsheet" in mime or "excel" in mime:
                out.append(f)
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return out


def _score_file(file_meta: dict[str, Any], *, requerente: str, processo: str) -> tuple:
    name = str(file_meta.get("name") or "")
    name_u = name.upper()
    req = " ".join(str(requerente or "").upper().split())
    dig = _digits(processo)[:7]
    score = 0
    if req and req in name_u:
        score += 100
    if dig and dig in _digits(name):
        score += 40
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
    best = ranked[0]
    score, _ = _score_file(best, requerente=requerente, processo=processo)
    if score <= 0 and requerente:
        # Sem match de nome/processo: ainda assim devolve o mais recente da pasta.
        ranked2 = sorted(
            files,
            key=lambda f: str(f.get("modifiedTime") or ""),
            reverse=True,
        )
        return ranked2[0]
    return best


def _iter_lookback_dates(days: int = _LOOKBACK_DAYS):
    today = date.today()
    for offset in range(days):
        yield today - timedelta(days=offset)


def find_planilha(
    *,
    processo: str,
    incidente: str = "",
    requerente: str = "",
) -> dict[str, Any] | None:
    """Devolve metadados do ficheiro Drive ou None."""
    root = drive_folder_id()
    if not root:
        raise RuntimeError("Drive: pasta raiz não configurada (MEMORIA_DRIVE_FOLDER_ID).")
    processo = str(processo or "").strip()
    if not processo:
        raise ValueError("Indique o número do processo.")
    pasta_nome = pasta_processo_incidente(processo, incidente)
    pasta_so_proc = _pontos_tracos_para_sublinhado(processo)
    candidates = [p for p in (pasta_nome, pasta_so_proc) if p]
    # Sem incidente: também tenta pastas que comecem pelo processo.
    service = _build_service()

    for day in _iter_lookback_dates():
        year_id = _find_child_folder(service, root, str(day.year))
        if not year_id:
            continue
        month_id = _find_child_folder(service, year_id, _MESES[day.month - 1])
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

    # Fallback: pesquisa global por nome contendo dígitos do processo (sob a conta).
    dig = _digits(processo)
    if len(dig) >= 7:
        needle = dig[:7]
        safe = needle.replace("\\", "\\\\").replace("'", "\\'")
        q = (
            f"name contains '{safe}' and trashed=false and "
            f"(mimeType contains 'spreadsheet' or mimeType contains 'excel' "
            f"or name contains '.xlsx' or name contains '.xlsm')"
        )
        resp = service.files().list(**_list_kwargs({"q": q, "pageSize": 25})).execute()
        files = resp.get("files") or []
        best = _pick_best(files, requerente=requerente, processo=processo)
        if best:
            return {
                "id": best["id"],
                "name": best.get("name"),
                "modifiedTime": best.get("modifiedTime"),
                "folder": None,
                "date": None,
            }
    return None


def download_planilha_bytes(file_id: str) -> tuple[bytes, str]:
    """Descarrega o ficheiro; devolve (conteúdo, nome)."""
    from googleapiclient.http import MediaIoBaseDownload

    if not file_id:
        raise ValueError("file_id vazio.")
    service = _build_service()
    meta = (
        service.files()
        .get(fileId=file_id, fields="id, name", supportsAllDrives=True)
        .execute()
    )
    name = str(meta.get("name") or "planilha.xlsx")
    buf = io.BytesIO()
    request = service.files().get_media(fileId=file_id, supportsAllDrives=True)
    downloader = MediaIoBaseDownload(buf, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    return buf.getvalue(), name
