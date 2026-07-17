"""
Anexos PDF da PRÉ Análise Processual via Google Drive.

Lista e faz proxy de PDFs numa pasta configurada
(``PRE_ANALISE_DRIVE_FOLDER_ID``), reutilizando credenciais
``GOOGLE_DRIVE_*`` (OAuth ou conta de serviço).
"""

from __future__ import annotations

import io
import os
import re
from typing import Any

_SCOPES = ("https://www.googleapis.com/auth/drive.readonly",)
_PDF_MIME = "application/pdf"
_FOLDER_MIME = "application/vnd.google-apps.folder"
_MAX_DEPTH = 2
_MAX_FILES = 50
_NOME_PERMITIDO = "redator"


def _nome_permitido_download(filename: str) -> bool:
    """Só ficheiros com «redator» no nome podem ser listados/baixados."""
    return _NOME_PERMITIDO in str(filename or "").lower()



def _env_token(raw: str | None) -> str:
    return (raw or "").strip().split("#", 1)[0].strip().strip("'\"")


def drive_folder_id() -> str | None:
    fid = _env_token(os.getenv("PRE_ANALISE_DRIVE_FOLDER_ID"))
    if fid:
        return fid
    # fallback opcional
    fid = _env_token(os.getenv("GOOGLE_DRIVE_FOLDER_ID"))
    return fid or None


def is_configured() -> bool:
    return bool(drive_folder_id() and (_oauth_token_path() or _service_account_path()))


def _oauth_token_path() -> str | None:
    for key in ("PRE_ANALISE_DRIVE_TOKEN_PATH", "GOOGLE_DRIVE_TOKEN_PATH"):
        p = _env_token(os.getenv(key))
        if p and os.path.isfile(p):
            return p
    return None


def _oauth_credentials_path() -> str | None:
    for key in ("PRE_ANALISE_DRIVE_CREDENTIALS_PATH", "GOOGLE_DRIVE_CREDENTIALS_PATH"):
        p = _env_token(os.getenv(key))
        if p and os.path.isfile(p):
            return p
    return None


def _service_account_path() -> str | None:
    for key in (
        "PRE_ANALISE_DRIVE_SERVICE_ACCOUNT_JSON",
        "GOOGLE_DRIVE_SERVICE_ACCOUNT_JSON",
        "GOOGLE_APPLICATION_CREDENTIALS",
    ):
        p = _env_token(os.getenv(key))
        if p and os.path.isfile(p):
            return p
    return None


def _shared_drive_id() -> str:
    return _env_token(
        os.getenv("PRE_ANALISE_DRIVE_SHARED_DRIVE_ID")
        or os.getenv("GOOGLE_DRIVE_SHARED_DRIVE_ID")
    )


def _auth_mode() -> str:
    raw = _env_token(
        os.getenv("PRE_ANALISE_DRIVE_AUTH") or os.getenv("GOOGLE_DRIVE_AUTH")
    ).lower()
    if raw in ("oauth", "user", "installed"):
        return "oauth"
    if raw in ("service_account", "service-account", "sa"):
        return "service_account"
    if _oauth_token_path():
        return "oauth"
    if _service_account_path():
        return "service_account"
    return "oauth"


def _build_service() -> Any:
    from googleapiclient.discovery import build

    mode = _auth_mode()
    if mode == "oauth":
        from google.auth.transport.requests import Request
        from google.oauth2.credentials import Credentials

        token_path = _oauth_token_path()
        if not token_path:
            raise RuntimeError(
                "Drive OAuth: falta token. Defina PRE_ANALISE_DRIVE_TOKEN_PATH "
                "ou GOOGLE_DRIVE_TOKEN_PATH."
            )
        # Token existente pode ter sido emitido com drive (full); readonly cabe nele.
        creds = Credentials.from_authorized_user_file(token_path)
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
            try:
                with open(token_path, "w", encoding="utf-8") as fh:
                    fh.write(creds.to_json())
            except OSError:
                pass
        if not creds or not creds.valid:
            raise RuntimeError(
                "Drive OAuth: token inválido. Renove o token (authorize_drive) "
                "ou verifique credentials.json."
            )
        return build("drive", "v3", credentials=creds, cache_discovery=False)

    from google.oauth2 import service_account

    key_path = _service_account_path()
    if not key_path:
        raise RuntimeError(
            "Falta conta de serviço. Defina PRE_ANALISE_DRIVE_SERVICE_ACCOUNT_JSON "
            "ou GOOGLE_DRIVE_SERVICE_ACCOUNT_JSON."
        )
    creds = service_account.Credentials.from_service_account_file(
        key_path, scopes=list(_SCOPES)
    )
    return build("drive", "v3", credentials=creds, cache_discovery=False)


def _list_kwargs(extra: dict[str, Any] | None = None) -> dict[str, Any]:
    kwargs: dict[str, Any] = {
        "supportsAllDrives": True,
        "includeItemsFromAllDrives": True,
        "pageSize": 100,
        "fields": "nextPageToken, files(id, name, mimeType, size, modifiedTime, parents, webViewLink)",
    }
    shared = _shared_drive_id()
    if shared:
        kwargs["driveId"] = shared
        kwargs["corpora"] = "drive"
    if extra:
        kwargs.update(extra)
    return kwargs


def _list_children(service: Any, parent_id: str) -> list[dict[str, Any]]:
    q = f"'{parent_id}' in parents and trashed=false"
    out: list[dict[str, Any]] = []
    page_token: str | None = None
    while True:
        kwargs = _list_kwargs({"q": q, "pageToken": page_token} if page_token else {"q": q})
        resp = service.files().list(**kwargs).execute()
        out.extend(resp.get("files") or [])
        page_token = resp.get("nextPageToken")
        if not page_token or len(out) >= _MAX_FILES:
            break
    return out


def _sanitize_match_name(value: str) -> str:
    text = " ".join(str(value or "").strip().split())
    text = text.translate(str.maketrans("", "", r'\/:*?"<>|'))
    return text.strip()


def _candidate_folder_names(
    *,
    numero_cumprimento: str | None,
    numero_incidente: str | None,
    caminho_pasta: str | None,
) -> list[str]:
    """
    Nome da pasta no Drive: ``{Numero_De_Processo}_{Incidente}``
    Ex.: ``0015376-03.2019.8.26.0053_56``
    """
    names: list[str] = []
    proc = _sanitize_match_name(numero_cumprimento or "")
    inc = _sanitize_match_name(numero_incidente or "")
    # Formato canónico (underscore) — igual ao exemplo informado.
    if proc and inc:
        names.append(f"{proc}_{inc}")
        names.append(f"{proc}-{inc}")
    if caminho_pasta:
        base = os.path.basename(str(caminho_pasta).rstrip("/\\"))
        if base:
            names.append(base)
    seen: set[str] = set()
    out: list[str] = []
    for n in names:
        key = n.lower()
        if key and key not in seen:
            seen.add(key)
            out.append(n)
    return out


def _find_case_folder_id(
    service: Any,
    root_id: str,
    *,
    numero_cumprimento: str | None,
    numero_incidente: str | None,
    caminho_pasta: str | None,
) -> tuple[str | None, str | None]:
    """
    Procura a subpasta do caso sob a pasta raiz.
    Retorna (folder_id, nome) ou (None, None) se não existir.
    """
    candidates = _candidate_folder_names(
        numero_cumprimento=numero_cumprimento,
        numero_incidente=numero_incidente,
        caminho_pasta=caminho_pasta,
    )
    if not candidates:
        return None, None

    children = _list_children(service, root_id)
    folders = [f for f in children if f.get("mimeType") == _FOLDER_MIME]
    by_lower = {str(f.get("name") or "").strip().lower(): f for f in folders}

    for cand in candidates:
        hit = by_lower.get(cand.lower())
        if hit and hit.get("id"):
            return str(hit["id"]), str(hit.get("name") or cand)

    # Match exacto processo_incidente ignorando maiúsculas; sem fallback genérico.
    return None, None


def _stem_lower(filename: str) -> str:
    name = str(filename or "").strip()
    if "." in name:
        name = name.rsplit(".", 1)[0]
    return name.lower().strip()


def _collect_caso_files(service: Any, folder_id: str) -> list[dict[str, Any]]:
    """Lista ficheiros directamente na pasta do caso (sem recursão)."""
    items = _list_children(service, folder_id)
    files: list[dict[str, Any]] = []
    for item in items:
        mime = str(item.get("mimeType") or "")
        if mime == _FOLDER_MIME:
            continue
        name = str(item.get("name") or "")
        fid = str(item.get("id") or "")
        if not fid or not name:
            continue
        files.append(
            {
                "id": fid,
                "name": name,
                "mime_type": mime,
                "size": item.get("size"),
                "modified_time": item.get("modifiedTime"),
                "web_view_link": item.get("webViewLink"),
            }
        )
    return files


def _montar_anexos_redator(files: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Apenas ficheiros cujo nome contém «redator» ficam disponíveis para download."""
    out: list[dict[str, Any]] = []
    for f in files:
        name = str(f.get("name") or "")
        if not _nome_permitido_download(name):
            continue
        fid = f.get("id")
        if not fid:
            continue
        out.append(
            {
                "tipo": "redator",
                "label": "Redator",
                "available": True,
                "id": fid,
                "name": name,
                "size": f.get("size"),
                "modified_time": f.get("modified_time"),
                "web_view_link": f.get("web_view_link"),
                "download_url": f"/api/pre-analise-processual/anexos/{fid}?download=1",
                "view_url": f"/api/pre-analise-processual/anexos/{fid}",
            }
        )
    out.sort(key=lambda x: str(x.get("name") or "").lower())
    return out[:_MAX_FILES]


def _file_under_root(service: Any, file_id: str, root_id: str) -> bool:
    """Garante que o ficheiro pertence à árvore da pasta configurada."""
    seen: set[str] = set()
    current = file_id
    for _ in range(20):
        if current in seen:
            return False
        seen.add(current)
        meta = (
            service.files()
            .get(
                fileId=current,
                fields="id, parents",
                supportsAllDrives=True,
            )
            .execute()
        )
        if current == root_id:
            return True
        parents = meta.get("parents") or []
        if root_id in parents:
            return True
        if not parents:
            return current == root_id
        current = str(parents[0])
    return False


def listar_anexos_pdf(
    *,
    numero_cumprimento: str | None = None,
    numero_incidente: str | None = None,
    caminho_pasta: str | None = None,
    folder_id_override: str | None = None,
) -> tuple[dict, int]:
    root = (folder_id_override or "").strip() or drive_folder_id()
    if not root:
        return (
            {
                "ok": False,
                "error": (
                    "Pasta do Google Drive não configurada. "
                    "Defina PRE_ANALISE_DRIVE_FOLDER_ID no .env."
                ),
            },
            503,
        )

    proc = (numero_cumprimento or "").strip()
    inc = (numero_incidente or "").strip()
    if not proc or not inc:
        return (
            {
                "ok": False,
                "error": "numero_cumprimento e numero_incidente são obrigatórios para localizar a pasta.",
            },
            400,
        )

    expected_folder = f"{_sanitize_match_name(proc)}_{_sanitize_match_name(inc)}"

    try:
        service = _build_service()
    except Exception as e:
        return {"ok": False, "error": f"Falha na autenticação Google Drive: {e}"}, 503

    try:
        case_folder_id, matched_name = _find_case_folder_id(
            service,
            root,
            numero_cumprimento=proc,
            numero_incidente=inc,
            caminho_pasta=caminho_pasta,
        )
        if not case_folder_id:
            return (
                {
                    "ok": True,
                    "folder_id": None,
                    "root_folder_id": root,
                    "matched_folder": None,
                    "expected_folder": expected_folder,
                    "total": 0,
                    "available_count": 0,
                    "items": [],
                    "aviso": f"Pasta «{expected_folder}» não encontrada no Google Drive.",
                },
                200,
            )

        files = _collect_caso_files(service, case_folder_id)
        items = _montar_anexos_redator(files)
        available_count = len(items)
        aviso = None
        if available_count == 0:
            aviso = (
                "Nenhum ficheiro com «redator» no nome foi encontrado nesta pasta. "
                "Cumprimento, DEPRE e Incidente não estão disponíveis para download."
            )
        return (
            {
                "ok": True,
                "folder_id": case_folder_id,
                "root_folder_id": root,
                "matched_folder": matched_name or expected_folder,
                "expected_folder": expected_folder,
                "total": available_count,
                "available_count": available_count,
                "items": items,
                "aviso": aviso,
            },
            200,
        )
    except Exception as e:
        return {"ok": False, "error": f"Falha ao listar anexos no Drive: {e}"}, 502


def baixar_anexo_pdf(
    file_id: str,
    *,
    as_attachment: bool = False,
) -> tuple[dict | bytes, int, dict[str, str]]:
    """
    Retorna (body, status, headers).
    Em sucesso body=bytes PDF; em erro body=dict JSON.
    """
    file_id = (file_id or "").strip()
    headers_err = {"Content-Type": "application/json; charset=utf-8"}
    if not file_id or not re.fullmatch(r"[\w-]{10,128}", file_id):
        return {"ok": False, "error": "file_id inválido."}, 400, headers_err

    root = drive_folder_id()
    if not root:
        return (
            {
                "ok": False,
                "error": "Pasta do Google Drive não configurada.",
            },
            503,
            headers_err,
        )

    try:
        service = _build_service()
    except Exception as e:
        return (
            {"ok": False, "error": f"Falha na autenticação Google Drive: {e}"},
            503,
            headers_err,
        )

    try:
        if not _file_under_root(service, file_id, root):
            return (
                {"ok": False, "error": "Ficheiro fora da pasta configurada."},
                403,
                headers_err,
            )
        meta = (
            service.files()
            .get(
                fileId=file_id,
                fields="id, name, mimeType",
                supportsAllDrives=True,
            )
            .execute()
        )
        name = str(meta.get("name") or "anexo.pdf")
        if not _nome_permitido_download(name):
            return (
                {
                    "ok": False,
                    "error": (
                        "Download permitido apenas para ficheiros com «redator» no nome."
                    ),
                },
                403,
                headers_err,
            )
        mime = str(meta.get("mimeType") or "")
        is_pdf = mime == _PDF_MIME or name.lower().endswith(".pdf")
        if not is_pdf and mime.startswith("application/vnd.google-apps."):
            return (
                {"ok": False, "error": "Ficheiro Google nativo — exporte como PDF no Drive."},
                415,
                headers_err,
            )

        request = service.files().get_media(fileId=file_id, supportsAllDrives=True)
        buf = io.BytesIO()
        from googleapiclient.http import MediaIoBaseDownload

        downloader = MediaIoBaseDownload(buf, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        data = buf.getvalue()
        safe_name = re.sub(r"[^\w.\- ]+", "_", name).strip() or "anexo.pdf"
        if not safe_name.lower().endswith(".pdf") and is_pdf:
            safe_name = f"{safe_name}.pdf"
        disposition = "attachment" if as_attachment else "inline"
        headers = {
            "Content-Type": "application/pdf" if is_pdf else (mime or "application/octet-stream"),
            "Content-Disposition": f'{disposition}; filename="{safe_name}"',
            "Content-Length": str(len(data)),
            "Cache-Control": "private, max-age=60",
        }
        return data, 200, headers
    except Exception as e:
        return {"ok": False, "error": f"Falha ao baixar anexo: {e}"}, 502, headers_err
