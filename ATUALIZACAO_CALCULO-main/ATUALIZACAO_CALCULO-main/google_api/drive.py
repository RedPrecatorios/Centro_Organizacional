# -*- coding: utf-8 -*-
"""
Upload de planilhas para o **Google Drive** (API v3), via **conta de serviço** (headless / servidor).

Configuração no ``.env`` (mesma pasta do projecto, carregada pelo arranque):

- ``GOOGLE_DRIVE_ENABLED=1`` — liga o envio após gravar o ``.xlsm`` em ``calculation_automation/OUTPUT/``.
- ``GOOGLE_DRIVE_FOLDER_ID`` — ID da pasta de destino (URL do Drive: ``.../folders/ESTE_ID``).
- ``GOOGLE_DRIVE_SERVICE_ACCOUNT_JSON`` — caminho absoluto (recomendado) ao JSON da conta de
  serviço (GCP: IAM & Admin → Service Accounts → chave). Alternativa: variável
  ``GOOGLE_APPLICATION_CREDENTIALS`` (mesmo ficheiro).
- Opcional: ``GOOGLE_DRIVE_SHARED_DRIVE_ID`` — ID de *Shared drive* (Unidade partilhada) se
  a pasta viver nela; a API usa ``supportsAllDrives=True`` na mesma.

**No Google:** criar o projecto, activar a API "Google Drive", criar a conta de serviço, descarregar
a chave JSON. **Abrir a pasta de destino no Drive** (ou a unidade partilhada) e **adicionar o
e-mail** ``xxx@...iam.gserviceaccount.com`` como *Editor*.

Se ``GOOGLE_DRIVE_ENABLED`` for 0 ou faltar ID/pasta, nada é enviado; a planilha local continua
guardada.
"""
from __future__ import annotations

import mimetypes
import os
from pathlib import Path
from typing import Any, Optional

# Carrega .env do projecto (para chamadas via import).
try:
    from dotenv import load_dotenv

    _ROOT = Path(__file__).resolve().parent.parent
    load_dotenv(_ROOT / ".env")
    load_dotenv()
except Exception:
    pass

# Escopo: criar/gerir ficheiros que a aplicação usa (upload para pasta partilhada)
_SCOPES = ("https://www.googleapis.com/auth/drive",)

_GUESS_MIMES: dict[str, str] = {
    ".xlsm": "application/vnd.ms-excel.sheet.macroEnabled.12",
    ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    ".xls": "application/vnd.ms-excel",
    ".zip": "application/zip",
}


def _service_account_path() -> str:
    p = (os.getenv("GOOGLE_DRIVE_SERVICE_ACCOUNT_JSON") or "").strip()
    if p and os.path.isfile(p):
        return p
    p2 = (os.getenv("GOOGLE_APPLICATION_CREDENTIALS") or "").strip()
    if p2 and os.path.isfile(p2):
        return p2
    return ""


def _drive_enabled() -> bool:
    # python-dotenv trata "#" como comentário só quando no começo do token; em `.env`
    # deste projecto usamos `VAR=1 # comentário`, então normalizamos para o 1º token.
    raw = (os.getenv("GOOGLE_DRIVE_ENABLED") or "").strip()
    v = (raw.split("#", 1)[0].strip().split() or [""])[0].lower()
    return v in ("1", "true", "yes", "on", "sim")


def _folder_id() -> str:
    raw = (os.getenv("GOOGLE_DRIVE_FOLDER_ID") or "").strip()
    return (raw.split("#", 1)[0].strip().strip("'\""))

def _use_dated_subfolders() -> bool:
    """
    Quando activo, cria/usa uma subpasta com a data actual (dd-mm-aaaa) dentro de
    GOOGLE_DRIVE_FOLDER_ID.

    Env:
    - GOOGLE_DRIVE_DATED_SUBFOLDERS=1/0 (padrão 1)
    - GOOGLE_DRIVE_DATE_FORMAT (strftime; padrão %d-%m-%Y)
    - GOOGLE_DRIVE_TZ (padrão America/Sao_Paulo)
    """
    raw = (os.getenv("GOOGLE_DRIVE_DATED_SUBFOLDERS") or "1").strip()
    v = (raw.split("#", 1)[0].strip().split() or ["1"])[0].lower()
    return v not in ("0", "false", "no", "off", "disabled", "nao", "não")


def _date_folder_name() -> str:
    from datetime import datetime

    try:
        import pytz

        tz = pytz.timezone((os.getenv("GOOGLE_DRIVE_TZ") or "America/Sao_Paulo").strip())
        now = datetime.now(tz)
    except Exception:
        now = datetime.now()
    fmt = (os.getenv("GOOGLE_DRIVE_DATE_FORMAT") or "%d-%m-%Y").strip() or "%d-%m-%Y"
    try:
        return now.strftime(fmt)
    except Exception:
        return now.strftime("%d-%m-%Y")


def _ensure_child_folder(service: Any, *, parent_id: str, name: str) -> str:
    """
    Garante uma subpasta (folder) com `name` dentro de `parent_id`.
    Retorna o ID da pasta (primeira encontrada ou recém-criada).
    """
    import time

    try:
        from googleapiclient.errors import HttpError
    except Exception:  # pragma: no cover
        HttpError = Exception  # type: ignore

    shared_drive_id = _shared_drive_id_from_env()
    safe_name = name.replace("'", "\\'")
    q = (
        "mimeType='application/vnd.google-apps.folder' "
        f"and name='{safe_name}' "
        f"and '{parent_id}' in parents and trashed=false"
    )
    list_kwargs: dict[str, Any] = {
        "q": q,
        "fields": "files(id, name)",
        "pageSize": 10,
        "supportsAllDrives": True,
        "includeItemsFromAllDrives": True,
    }
    if shared_drive_id:
        list_kwargs["driveId"] = shared_drive_id
        list_kwargs["corpora"] = "drive"

    body: dict[str, Any] = {
        "name": name,
        "mimeType": "application/vnd.google-apps.folder",
        "parents": [parent_id],
    }

    # O Drive às vezes responde "internalError" (500) em list/create; retry curto resolve.
    last_err: Exception | None = None
    for attempt in range(3):
        try:
            resp = service.files().list(**list_kwargs).execute()
            files = resp.get("files") or []
            if files:
                return str(files[0].get("id"))
            created = (
                service.files()
                .create(body=body, fields="id", supportsAllDrives=True)
                .execute()
            )
            return str(created.get("id"))
        except HttpError as e:  # type: ignore[misc]
            last_err = e
            status = getattr(getattr(e, "resp", None), "status", None)
            if status and int(status) >= 500 and attempt < 2:
                time.sleep(0.8 * (attempt + 1))
                continue
            raise
        except Exception as e:
            last_err = e
            raise
    assert last_err is not None
    raise last_err


def _build_service() -> Any:
    from google.oauth2 import service_account
    from googleapiclient.discovery import build

    key_path = _service_account_path()
    if not key_path:
        raise RuntimeError(
            "Falta o ficheiro JSON da conta de serviço. Defina "
            "GOOGLE_DRIVE_SERVICE_ACCOUNT_JSON (caminho absoluto) ou "
            "GOOGLE_APPLICATION_CREDENTIALS."
        )
    creds = service_account.Credentials.from_service_account_file(
        key_path, scopes=_SCOPES
    )
    return build("drive", "v3", credentials=creds, cache_discovery=False)


def _mimetype(path: str) -> str:
    ext = os.path.splitext(path)[1].lower()
    g = _GUESS_MIMES.get(ext)
    if g:
        return g
    t, _ = mimetypes.guess_type(path)
    return t or "application/octet-stream"


def upload_saved_spreadsheet(file_path: str) -> Optional[str]:
    """
    Sobe o ficheiro (ex. ``.xlsm``) para a pasta configurada. Devolve ``webViewLink`` ou
    ``id`` se não houver link, ou ``None`` se o envio estiver desligado / falhar silenciosamente
    (ver mensagens no stdout).

    Não re-lança excepções: quem chama trata; para falhas fatais, propagar a partir de ``Drive``.
    """
    if not _drive_enabled():
        return None
    fid = _folder_id()
    if not fid:
        print(
            "\n[google_drive] GOOGLE_DRIVE_ENABLED=1 mas falta GOOGLE_DRIVE_FOLDER_ID no .env.\n"
        )
        return None
    if not file_path or not os.path.isfile(file_path):
        print(f"\n[google_drive] Ficheiro inexistente: {file_path!r}\n")
        return None
    try:
        return _upload_impl(file_path, fid)
    except Exception as e:
        print(f"\n[google_drive] Falha no upload: {e}\n")
        return None


def _upload_impl(file_path: str, parent_folder_id: str) -> str:
    from googleapiclient.http import MediaFileUpload

    name = os.path.basename(file_path)
    service = _build_service()
    dest_parent = parent_folder_id
    if _use_dated_subfolders():
        try:
            date_name = _date_folder_name()
            dest_parent = _ensure_child_folder(
                service, parent_id=parent_folder_id, name=date_name
            )
            print(
                f"\n[google_drive] Pasta do dia activa: {date_name!r} (dest_parent={dest_parent})\n"
            )
        except Exception as e:
            print(f"\n[google_drive] Aviso: não criou/achou pasta do dia: {e}\n")
            dest_parent = parent_folder_id
    else:
        print("\n[google_drive] Pasta do dia desactivada; a enviar para parent.\n")
    body: dict[str, Any] = {
        "name": name,
        "parents": [dest_parent],
    }
    media = MediaFileUpload(
        file_path, mimetype=_mimetype(file_path), resumable=True
    )
    created = (
        service.files()
        .create(
            body=body,
            media_body=media,
            fields="id, name, webViewLink, webContentLink",
            supportsAllDrives=True,
        )
        .execute()
    )
    link = created.get("webViewLink") or created.get("webContentLink")
    out = f"{created.get('id')}" if not link else link
    print(
        f"\n[google_drive] Enviado: {name!r} → {out}\n"
    )
    return out


class Drive:
    """
    Fachada mínima (compatibilidade com código antigo) — delega em
    :func:`upload_saved_spreadsheet`.
    """

    def __init__(self, shared_drive_id: str = "") -> None:
        self._shared_drive_id = (shared_drive_id or "").strip() or _shared_drive_id_from_env()

    def authenticate_service(self) -> Any:
        try:
            return _build_service()
        except Exception:
            return None

    def create_folder_if_not_exists(self, folder_name: str) -> None:
        return None

    def upload_file_to_drive(self, file_path: str) -> Optional[str]:
        return upload_saved_spreadsheet(file_path)

    def create_folder(self, folder_name: str, parent_id: Optional[str] = None) -> None:
        return None


def _shared_drive_id_from_env() -> str:
    return (os.getenv("GOOGLE_DRIVE_SHARED_DRIVE_ID") or "").strip()
