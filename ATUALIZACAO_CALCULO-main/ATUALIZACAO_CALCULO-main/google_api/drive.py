# -*- coding: utf-8 -*-
"""
Upload de planilhas para o **Google Drive** (API v3).

Configuração no ``.env`` (mesma pasta do projecto, carregada pelo arranque):

- ``GOOGLE_DRIVE_ENABLED=1`` — liga o envio após gravar o ``.xlsx`` em ``calculation_automation/OUTPUT/``.
- ``GOOGLE_DRIVE_FOLDER_ID`` — ID da pasta raiz **PLANS** (URL do Drive: ``.../folders/ESTE_ID``).
  Com ``GOOGLE_DRIVE_DATED_SUBFOLDERS=1`` (padrão), a árvore fica igual ao API_CALCULO:
  ``PLANS/ANO/Mês/dd-mm-aaaa/processo_incidente/planilha.xlsx``.
- **Pasta PLANS no My Drive (igual ao API_CALCULO):** use **OAuth** — ``GOOGLE_DRIVE_AUTH=oauth``,
  ``GOOGLE_DRIVE_TOKEN_PATH`` e ``GOOGLE_DRIVE_CREDENTIALS_PATH`` (mesmos ficheiros do
  ``API_CALCULO/secret/`` ou ``token.json`` na raiz do projecto). Contas de serviço **não**
  conseguem criar ficheiros em pastas do My Drive (erro 403 storageQuotaExceeded).
- **Unidade partilhada (Shared drive):** pode usar conta de serviço com
  ``GOOGLE_DRIVE_SERVICE_ACCOUNT_JSON`` + ``GOOGLE_DRIVE_SHARED_DRIVE_ID``.

Se ``GOOGLE_DRIVE_ENABLED`` for 0 ou faltar ID/pasta, nada é enviado; a planilha local continua
guardada em ``PLANS_ARCHIVED/``.
"""
from __future__ import annotations

import mimetypes
import os
import re
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


def _service_account_path() -> str:
    p = (os.getenv("GOOGLE_DRIVE_SERVICE_ACCOUNT_JSON") or "").strip()
    if p and os.path.isfile(p):
        return p
    p2 = (os.getenv("GOOGLE_APPLICATION_CREDENTIALS") or "").strip()
    if p2 and os.path.isfile(p2):
        return p2
    return ""


def _oauth_token_path() -> Path | None:
    raw = (os.getenv("GOOGLE_DRIVE_TOKEN_PATH") or "").strip()
    candidates: list[Path] = []
    if raw:
        candidates.append(Path(raw))
    candidates.extend(
        [
            _ROOT / "token.json",
            Path("/mnt/volume_nyc1_1778499395037/API_CALCULO/secret/token.json"),
        ]
    )
    for p in candidates:
        if p.is_file():
            return p
    return None


def _oauth_credentials_path() -> Path | None:
    raw = (os.getenv("GOOGLE_DRIVE_CREDENTIALS_PATH") or "").strip()
    candidates: list[Path] = []
    if raw:
        candidates.append(Path(raw))
    candidates.extend(
        [
            _ROOT / "credentials.json",
            Path("/mnt/volume_nyc1_1778499395037/API_CALCULO/secret/credentials.json"),
        ]
    )
    for p in candidates:
        if p.is_file():
            return p
    return None


def _drive_auth_mode() -> str:
    """
    ``oauth`` — My Drive / pasta PLANS (igual API_CALCULO).
    ``service_account`` — Shared drives com quota da unidade.
    """
    raw = (os.getenv("GOOGLE_DRIVE_AUTH") or "").strip().lower()
    if raw in ("oauth", "user", "installed"):
        return "oauth"
    if raw in ("service_account", "service-account", "sa"):
        return "service_account"
    if _oauth_token_path() is not None:
        return "oauth"
    if _service_account_path():
        return "service_account"
    return "oauth"


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
    Quando activo (padrao), usa a mesma arvore do API_CALCULO:

    PLANS/ANO/Mes/dd-mm-aaaa/processo_incidente/planilha.xlsx

    Env:
    - GOOGLE_DRIVE_DATED_SUBFOLDERS=1/0 (padrao 1)
    - GOOGLE_DRIVE_DATE_FORMAT (strftime; padrao %d-%m-%Y)
    - GOOGLE_DRIVE_TZ (padrao America/Sao_Paulo)
    """
    raw = (os.getenv("GOOGLE_DRIVE_DATED_SUBFOLDERS") or "1").strip()
    v = (raw.split("#", 1)[0].strip().split() or ["1"])[0].lower()
    return v not in ("0", "false", "no", "off", "disabled", "nao", "não")


def _date_folder_name() -> str:
    return _now_in_drive_tz().strftime(_date_format())


def _date_format() -> str:
    fmt = (os.getenv("GOOGLE_DRIVE_DATE_FORMAT") or "%d-%m-%Y").strip() or "%d-%m-%Y"
    return fmt


def _now_in_drive_tz():
    from datetime import datetime

    try:
        import pytz

        tz = pytz.timezone((os.getenv("GOOGLE_DRIVE_TZ") or "America/Sao_Paulo").strip())
        return datetime.now(tz)
    except Exception:
        return datetime.now()


def _texto_para_planilha(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _pontos_tracos_para_sublinhado(value: str) -> str:
    text = str(value).strip().replace(".", "_").replace("-", "_")
    text = re.sub(r"_+", "_", text).strip("_")
    return text


def _sanitize_filename_part(value: str, default: str) -> str:
    text = " ".join(str(value).strip().split())
    text = text.translate(str.maketrans("", "", r'\/:*?"<>|()\\'))
    text = re.sub(r"[^\w.\- ]+", "_", text).strip("._- ")
    return text or default


def _sanitize_folder_name(name: str) -> str:
    folder_name = name.strip()
    for char in '\\/:*?"<>|':
        if char in folder_name:
            folder_name = folder_name.replace(char, "_")
    return folder_name.strip() or "sem_nome"


def _primeiros_7_digitos_processo(value: Any) -> str:
    digits = re.sub(r"\D+", "", str(value or ""))
    return digits[:7] if digits else "sem_proc"


def pasta_processo_incidente_drive(main_dict: dict[str, Any]) -> str:
    processo = _texto_para_planilha(main_dict.get("Processo"))
    incidente = _texto_para_planilha(main_dict.get("Incidente"))
    proc = _pontos_tracos_para_sublinhado(processo) or "sem_processo"
    inc = _pontos_tracos_para_sublinhado(incidente) or "sem_incidente"
    return f"{proc}_{inc}"


def build_drive_file_name(
    main_dict: dict[str, Any],
    *,
    prioridade: bool = False,
) -> str:
    credor = _sanitize_filename_part(
        _texto_para_planilha(main_dict.get("Requerente")),
        "sem_credor",
    )
    proc7 = _primeiros_7_digitos_processo(main_dict.get("Processo"))
    incidente = (
        _pontos_tracos_para_sublinhado(_texto_para_planilha(main_dict.get("Incidente")))
        or "sem_incidente"
    )
    corpo = f"{credor} {proc7}_{incidente}.xlsx"
    if prioridade:
        return f"PRIORI {corpo}"
    return corpo


def _resolve_upload_destination(
    service: Any,
    *,
    root_folder_id: str,
    main_dict: dict[str, Any] | None,
) -> tuple[str, str]:
    if not _use_dated_subfolders():
        processo_depre = (
            pasta_processo_incidente_drive(main_dict)
            if main_dict
            else "sem_processo_sem_incidente"
        )
        return root_folder_id, processo_depre

    now = _now_in_drive_tz()
    year_name = str(now.year)
    month_name = _MESES[now.month - 1]
    date_name = _date_folder_name()
    processo_depre = (
        pasta_processo_incidente_drive(main_dict)
        if main_dict
        else "sem_processo_sem_incidente"
    )

    folder_year_id = _ensure_child_folder(
        service, parent_id=root_folder_id, name=_sanitize_folder_name(year_name)
    )
    folder_month_id = _ensure_child_folder(
        service, parent_id=folder_year_id, name=_sanitize_folder_name(month_name)
    )
    folder_date_id = _ensure_child_folder(
        service, parent_id=folder_month_id, name=_sanitize_folder_name(date_name)
    )
    folder_depre_id = _ensure_child_folder(
        service,
        parent_id=folder_date_id,
        name=_sanitize_folder_name(processo_depre),
    )
    print(
        f"\n[google_drive] Destino: {year_name}/{month_name}/{date_name}/{processo_depre}\n",
        flush=True,
    )
    return folder_depre_id, processo_depre


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
    name = _sanitize_folder_name(name)
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
    from googleapiclient.discovery import build

    mode = _drive_auth_mode()
    if mode == "oauth":
        from google.auth.transport.requests import Request
        from google.oauth2.credentials import Credentials

        token_path = _oauth_token_path()
        if not token_path:
            raise RuntimeError(
                "Drive OAuth: falta token.json. Defina GOOGLE_DRIVE_TOKEN_PATH ou copie o "
                "token do API_CALCULO (secret/token.json). Execute authorize_drive.py no "
                "API_CALCULO se necessário."
            )
        creds = Credentials.from_authorized_user_file(str(token_path), list(_SCOPES))
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                cred_path = _oauth_credentials_path()
                if not cred_path:
                    raise RuntimeError(
                        "Drive OAuth: token inválido e falta credentials.json para renovar."
                    )
                from google_auth_oauthlib.flow import InstalledAppFlow

                flow = InstalledAppFlow.from_client_secrets_file(
                    str(cred_path), list(_SCOPES)
                )
                creds = flow.run_local_server(port=0)
            token_path.write_text(creds.to_json(), encoding="utf-8")
        print(
            f"\n[google_drive] Autenticação OAuth ({token_path.name})\n",
            flush=True,
        )
        return build("drive", "v3", credentials=creds, cache_discovery=False)

    from google.oauth2 import service_account

    key_path = _service_account_path()
    if not key_path:
        raise RuntimeError(
            "Falta o ficheiro JSON da conta de serviço. Defina "
            "GOOGLE_DRIVE_SERVICE_ACCOUNT_JSON (caminho absoluto) ou "
            "GOOGLE_APPLICATION_CREDENTIALS, ou use GOOGLE_DRIVE_AUTH=oauth."
        )
    creds = service_account.Credentials.from_service_account_file(
        key_path, scopes=_SCOPES
    )
    print("\n[google_drive] Autenticação conta de serviço\n", flush=True)
    return build("drive", "v3", credentials=creds, cache_discovery=False)


def _mimetype(path: str) -> str:
    ext = os.path.splitext(path)[1].lower()
    g = _GUESS_MIMES.get(ext)
    if g:
        return g
    t, _ = mimetypes.guess_type(path)
    return t or "application/octet-stream"


def _update_existing_enabled() -> bool:
    """
    Se activo, quando já existir um ficheiro com o mesmo nome na pasta destino,
    faz overwrite via `files.update` em vez de criar um novo (evita duplicados).
    """
    raw = (os.getenv("GOOGLE_DRIVE_UPDATE_EXISTING") or "1").strip()
    v = (raw.split("#", 1)[0].strip().split() or ["1"])[0].lower()
    return v not in ("0", "false", "no", "off", "disabled", "nao", "não")


def _find_existing_file_id(service: Any, *, parent_id: str, name: str) -> str | None:
    shared_drive_id = _shared_drive_id_from_env()
    safe_name = name.replace("'", "\\'")
    q = f"'{parent_id}' in parents and name='{safe_name}' and trashed=false"
    kwargs: dict[str, Any] = {
        "q": q,
        "fields": "files(id, name)",
        "pageSize": 5,
        "supportsAllDrives": True,
        "includeItemsFromAllDrives": True,
    }
    if shared_drive_id:
        kwargs["driveId"] = shared_drive_id
        kwargs["corpora"] = "drive"
    resp = service.files().list(**kwargs).execute()
    files = resp.get("files") or []
    if not files:
        return None
    return str(files[0].get("id"))


def upload_saved_spreadsheet(
    file_path: str,
    *,
    main_dict: dict[str, Any] | None = None,
    prioridade: bool = False,
    drive_file_name: str | None = None,
) -> Optional[str]:
    """
    Sobe o ficheiro para o Google Drive na mesma arvore do API_CALCULO quando
    ``GOOGLE_DRIVE_DATED_SUBFOLDERS=1`` (padrao):

    PLANS/ANO/Mes/dd-mm-aaaa/processo_incidente/nome_planilha.xlsx
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
        return _upload_impl(
            file_path,
            fid,
            main_dict=main_dict,
            prioridade=prioridade,
            drive_file_name=drive_file_name,
        )
    except Exception as e:
        print(f"\n[google_drive] Falha no upload: {e}\n", flush=True)
        return None


def _upload_impl(
    file_path: str,
    parent_folder_id: str,
    *,
    main_dict: dict[str, Any] | None = None,
    prioridade: bool = False,
    drive_file_name: str | None = None,
) -> str:
    from googleapiclient.http import MediaFileUpload

    service = _build_service()
    dest_parent, processo_depre = _resolve_upload_destination(
        service,
        root_folder_id=parent_folder_id,
        main_dict=main_dict,
    )

    if drive_file_name and str(drive_file_name).strip():
        name = str(drive_file_name).strip()
    elif main_dict:
        name = build_drive_file_name(main_dict, prioridade=prioridade)
    else:
        name = os.path.basename(file_path)

    if not name.lower().endswith((".xlsx", ".xlsm", ".xls")):
        ext = os.path.splitext(file_path)[1].lower() or ".xlsx"
        name = f"{name}{ext}"

    body: dict[str, Any] = {"name": name, "parents": [dest_parent]}
    media = MediaFileUpload(file_path, mimetype=_mimetype(file_path), resumable=True)

    existing_id = None
    if _update_existing_enabled():
        try:
            existing_id = _find_existing_file_id(service, parent_id=dest_parent, name=name)
        except Exception:
            existing_id = None

    if existing_id:
        created = (
            service.files()
            .update(
                fileId=existing_id,
                media_body=media,
                fields="id, name, webViewLink, webContentLink",
                supportsAllDrives=True,
            )
            .execute()
        )
        print(f"\n[google_drive] Overwrite: {name!r} (id={existing_id})\n", flush=True)
    else:
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
        f"\n[google_drive] Enviado: {name!r} → {out}\n",
        flush=True,
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

    def upload_file_to_drive(
        self,
        file_path: str,
        *,
        main_dict: dict[str, Any] | None = None,
        prioridade: bool = False,
        drive_file_name: str | None = None,
    ) -> Optional[str]:
        return upload_saved_spreadsheet(
            file_path,
            main_dict=main_dict,
            prioridade=prioridade,
            drive_file_name=drive_file_name,
        )

    def create_folder(self, folder_name: str, parent_id: Optional[str] = None) -> None:
        return None


def _shared_drive_id_from_env() -> str:
    return (os.getenv("GOOGLE_DRIVE_SHARED_DRIVE_ID") or "").strip()
