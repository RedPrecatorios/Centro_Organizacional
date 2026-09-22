# -*- coding: utf-8 -*-
"""
Solicitação de Inclusão: proxy BFF para a API auto_monday_requests.

A UI da plataforma não fala com a API remota. A chave fica no servidor.

Env:
  MONDAY_REQUESTS_API_URL          — ex. http://127.0.0.1:8080
  API_KEY_MONDAY_REQUESTS          — header X-API-Key
  MONDAY_REQUESTS_API_TIMEOUT      — segundos (default 90)
  MONDAY_REQUESTS_API_VERIFY_SSL   — true/false (default true)
  MONDAY_REQUESTS_POLL_INTERVAL_MS — intervalo sugerido ao frontend (default 15000)
  MONDAY_REQUESTS_CREATE_COOLDOWN_SECONDS — pausa entre criações por utilizador (default 60)
"""
from __future__ import annotations

import json
import os
import re
import threading
import unicodedata
from datetime import date, datetime, timedelta, timezone

from mysql.connector.errors import IntegrityError
from pathlib import Path
from typing import Any
from urllib.parse import quote

import requests

from messages_viewer.plataforma_auth import solicitante_from_user
from messages_viewer.plataforma_auth_store import auth_connection, auth_cursor

_OPEN_STATUSES = frozenset(
    {
        "RECEBIDO",
        "PROCESSANDO",
        "ANALISE_IA",
        "INCIDENTES_EM_PROCESSAMENTO",
        "AGUARDANDO_IMPUGNACAO",
        "AGUARDANDO_ANALISE_CALCULO",
    }
)
_DUP_ALWAYS_STATUSES = frozenset(
    {
        "RECEBIDO",
        "PROCESSANDO",
        "EM_TRATAMENTO",
        "ANALISE_IA",
        "AGUARDAR_CONCLUSAO_JURIDICA",
        "INCIDENTES_EM_PROCESSAMENTO",
        "AGUARDANDO_IMPUGNACAO",
        "AGUARDANDO_ANALISE_CALCULO",
    }
)
_DUP_RECENT_STATUSES = frozenset({"INAPTO"})
_INAPTO_DUP_HOURS = 24
_NON_DIGITS_RE = re.compile(r"\D+")
_STATUS_LOCK = threading.Lock()
_STATUS_FILE = (
    Path(__file__).resolve().parent / "data" / "solicitacao_inclusao_status.json"
)
_KNOWN_STATUS_LABELS = {
    "RECEBIDO": "Recebido",
    "PROCESSANDO": "Em processamento",
    "INAPTO": "Inapto",
    "SEM_PRECATORIO": "Sem precatório",
    "EM_TRATAMENTO": "Em tratamento",
    "ATRIBUIDO": "Atribuído",
    "CRIADO": "Criado",
    "APTO": "Apto",
    "ANALISE_IA": "Análise da IA",
    "AGUARDAR_CONCLUSAO_JURIDICA": "Aguardar conclusão jurídica",
    "INCIDENTES_EM_PROCESSAMENTO": "Incidentes em processamento",
    "AGUARDANDO_IMPUGNACAO": "Aguardando impugnação",
    "AGUARDANDO_ANALISE_CALCULO": "Aguardando análise do cálculo",
    "ATUALIZACAO_CALCULO_CONCLUIDA": "Atualização do cálculo concluída",
    "ATUALIZACAO_CALCULO_ERRO": "Erro na atualização do cálculo",
    "ANALISE_CALCULO_CONCLUIDA": "Análise do cálculo concluída",
    "CALCULO_CONCLUIDO": "Cálculo concluído",
    "SEM_COMPRADOR": "Sem comprador",
    "ERRO": "Erro",
    "NAO_ENCONTRADO_MONDAY": "Não encontrado na Monday",
}
_PT_WORD_MAP = {
    "analise": "análise",
    "analises": "análises",
    "aposentadoria": "aposentadoria",
    "assistencia": "assistência",
    "ate": "até",
    "atualizacao": "atualização",
    "atualizacoes": "atualizações",
    "audiencia": "audiência",
    "avaliacao": "avaliação",
    "beneficio": "benefício",
    "beneficios": "benefícios",
    "calculo": "cálculo",
    "calculos": "cálculos",
    "certidao": "certidão",
    "codigo": "código",
    "concluida": "concluída",
    "concluidas": "concluídas",
    "concluido": "concluído",
    "concluidos": "concluídos",
    "concordancia": "concordância",
    "configuracao": "configuração",
    "criacao": "criação",
    "decisao": "decisão",
    "disponivel": "disponível",
    "encontrada": "encontrada",
    "encontrado": "encontrado",
    "excecao": "exceção",
    "exclusao": "exclusão",
    "execucao": "execução",
    "historico": "histórico",
    "homologacao": "homologação",
    "homologacoes": "homologações",
    "impugnacao": "impugnação",
    "impugnacoes": "impugnações",
    "impossivel": "impossível",
    "inclusao": "inclusão",
    "indisponivel": "indisponível",
    "indice": "índice",
    "informacao": "informação",
    "informacoes": "informações",
    "intimacao": "intimação",
    "invalida": "inválida",
    "invalido": "inválido",
    "ja": "já",
    "juridica": "jurídica",
    "juridico": "jurídico",
    "liquida": "líquida",
    "liquido": "líquido",
    "maximo": "máximo",
    "medica": "médica",
    "medico": "médico",
    "mes": "mês",
    "minimo": "mínimo",
    "movimentacao": "movimentação",
    "movimentacoes": "movimentações",
    "nao": "não",
    "necessaria": "necessária",
    "necessario": "necessário",
    "numero": "número",
    "numeros": "números",
    "obrigatoria": "obrigatória",
    "obrigatorio": "obrigatório",
    "operacao": "operação",
    "periodo": "período",
    "periodos": "períodos",
    "peticao": "petição",
    "possivel": "possível",
    "precatorio": "precatório",
    "precatorios": "precatórios",
    "previdenciaria": "previdenciária",
    "previdenciario": "previdenciário",
    "publica": "pública",
    "publico": "público",
    "relatorio": "relatório",
    "relatorios": "relatórios",
    "remocao": "remoção",
    "reuniao": "reunião",
    "sao": "são",
    "sentenca": "sentença",
    "situacao": "situação",
    "solicitacao": "solicitação",
    "solicitacoes": "solicitações",
    "tambem": "também",
    "titulo": "título",
    "transito": "trânsito",
    "ultima": "última",
    "ultimo": "último",
    "usuario": "usuário",
    "usuarios": "usuários",
    "validacao": "validação",
    "voce": "você",
}
_PT_PHRASE_MAP = {
    "analise calculo concluida": "Análise do cálculo concluída",
    "analise da ia": "Análise da IA",
    "analise do calculo concluida": "Análise do cálculo concluída",
    "analise gemini nao executada": "Análise Gemini não executada",
    "aguardando analise calculo": "Aguardando análise do cálculo",
    "aguardando analise do calculo": "Aguardando análise do cálculo",
    "aguardando impugnacao": "Aguardando impugnação",
    "atualizacao calculo concluida": "Atualização do cálculo concluída",
    "atualizacao de calculo concluida": "Atualização de cálculo concluída",
    "atualizacao do calculo concluida": "Atualização do cálculo concluída",
    "calculo concluido": "Cálculo concluído",
    "credor nao encontrado": "Credor não encontrado",
    "credor nao encontrado no ocr": "Credor não encontrado no OCR",
    "nao encontrada": "Não encontrada",
    "nao encontrado": "Não encontrado",
    "nao encontrado na monday": "Não encontrado na Monday",
    "nao encontrado no monday": "Não encontrado na Monday",
    "pendente de homologacao": "Pendente de homologação",
    "pendente homologacao": "Pendente de homologação",
}
_PT_TOKEN_RE = re.compile(r"[A-Za-zÀ-ÿ]+")
_AUTOS_ALLOWED_EMAILS = frozenset(
    {
        "guilherme.vitoriano@redprecatorios.com.br",
    }
)
_AUTOS_ALLOWED_NAMES = (
    "guilherme vitoriano",
)
_GEMINI_DETAILS_ALLOWED_EMAILS = frozenset(
    {
        "guilherme.vitoriano@redprecatorios.com.br",
        "filipe.noberto@redprecatorios.com.br",
    }
)
_GEMINI_DETAILS_ALLOWED_NAMES = (
    "guilherme vitoriano",
    "filipe noberto",
)
_AUTOS_TEXTO_TIPOS = ("completo", "filtrado")
_AUTOS_TEXTO_TITULOS = {
    "completo": "Documentos completos (OCR)",
    "filtrado": "Texto filtrado pelo credor",
}
_SNAPSHOT_KEYS = (
    "id",
    "status",
    "processo_label",
    "processo",
    "incidente",
    "mensagem",
    "motivo",
    "compradores",
    "nome_solicitante",
    "email_solicitante",
    "nome_credor",
    "requerente",
    "cpf_credor",
    "cpf",
    "created_at",
    "updated_at",
    "detalhe",
    "casos",
    "casos_count",
    "casos_aptos_count",
    "casos_inaptos_count",
    "desfechos",
    "is_cumprimento",
    "numero_cumprimento",
    "fila_casos_a_frente",
    "fila_em_processamento",
    "eta_seconds_estimado",
    "eta_confianca",
    "avaliacao_cumprimento",
    "movimentacoes",
    "movimentacoes_resumo",
    "autos",
    "autos_texto",
    "analise_gemini",
    "analise_gemini_resultado",
    "analise_gemini_aviso",
    "calculo_atualizado",
)


def api_base() -> str | None:
    base = (os.getenv("MONDAY_REQUESTS_API_URL") or "").strip().rstrip("/")
    return base or None


def api_token() -> str | None:
    token = (os.getenv("API_KEY_MONDAY_REQUESTS") or "").strip()
    return token or None


def is_configured() -> bool:
    return bool(api_base() and api_token())


def poll_interval_ms() -> int:
    try:
        value = int((os.getenv("MONDAY_REQUESTS_POLL_INTERVAL_MS") or "15000").strip())
    except ValueError:
        value = 15000
    return max(2000, min(value, 60000))


def create_cooldown_seconds() -> int:
    try:
        value = int((os.getenv("MONDAY_REQUESTS_CREATE_COOLDOWN_SECONDS") or "60").strip())
    except ValueError:
        value = 60
    return max(5, min(value, 3600))


def create_cooldown_ms() -> int:
    return create_cooldown_seconds() * 1000


def _fold_pt(value: str) -> str:
    text = unicodedata.normalize("NFKD", value or "")
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return " ".join(text.casefold().split())


def _status_code_key(value: str) -> str:
    return re.sub(r"[\s\-]+", "_", str(value or "").strip()).upper()


def _apply_pt_words(text: str) -> str:
    def repl(match: re.Match[str]) -> str:
        word = match.group(0)
        mapped = _PT_WORD_MAP.get(_fold_pt(word))
        if not mapped:
            return word
        if word[:1].isupper():
            return mapped[:1].upper() + mapped[1:]
        return mapped

    return _PT_TOKEN_RE.sub(repl, text)


def pretty_pt_text(value: Any, *, as_label: bool = False) -> str:
    """Acentua status/motivos vindos da API sem ç/ã/é."""
    text = str(value or "").strip()
    if not text:
        return ""
    if text[0] in "{[":
        return text
    spaced = text.replace("_", " ").replace("-", " ")
    spaced = re.sub(r"\s+", " ", spaced).strip()
    folded = _fold_pt(spaced)
    if folded in _PT_PHRASE_MAP:
        return _PT_PHRASE_MAP[folded]
    code = _status_code_key(text)
    if re.fullmatch(r"[A-Z0-9_]+", code) and code in _KNOWN_STATUS_LABELS:
        return _KNOWN_STATUS_LABELS[code]
    pretty = _apply_pt_words(spaced)
    pretty = re.sub(r"\s+", " ", pretty).strip()
    if as_label and pretty:
        pretty = pretty[:1].upper() + pretty[1:]
    return pretty


def pt_display_config() -> dict[str, Any]:
    return {
        "status_labels": dict(_KNOWN_STATUS_LABELS),
        "phrases": dict(_PT_PHRASE_MAP),
        "words": dict(_PT_WORD_MAP),
    }


def _label_from_code(code: str) -> str:
    key = _status_code_key(code)
    if key in _KNOWN_STATUS_LABELS:
        return _KNOWN_STATUS_LABELS[key]
    return pretty_pt_text(code, as_label=True) or str(code or "").strip()


def _default_status_catalog() -> list[dict[str, str]]:
    return [{"code": code, "label": label} for code, label in _KNOWN_STATUS_LABELS.items()]


def _normalize_status_item(raw: Any) -> dict[str, str] | None:
    if isinstance(raw, dict):
        code = str(raw.get("code") or raw.get("status") or "").strip().upper()
        label = str(raw.get("label") or "").strip()
    else:
        code = str(raw or "").strip().upper()
        label = ""
    if not code:
        return None
    return {"code": code, "label": label or _label_from_code(code)}


def _parse_status_catalog(raw: Any) -> list[dict[str, str]]:
    items: list[dict[str, str]] = []
    seen: set[str] = set()

    def add(entry: Any) -> None:
        item = _normalize_status_item(entry)
        if not item or item["code"] in seen:
            return
        seen.add(item["code"])
        items.append(item)

    if isinstance(raw, dict):
        listed = raw.get("statuses") or raw.get("status") or raw.get("items")
        if isinstance(listed, list):
            for entry in listed:
                add(entry)
        else:
            for key, value in raw.items():
                if str(key) in {"statuses", "status", "items"}:
                    continue
                if isinstance(value, str):
                    add({"code": key, "label": value})
                else:
                    add(key)
    elif isinstance(raw, list):
        for entry in raw:
            add(entry)
    return items


def _refresh_status_labels(items: list[dict[str, str]]) -> tuple[list[dict[str, str]], bool]:
    changed = False
    for item in items:
        code = str(item.get("code") or "").strip().upper()
        if not code:
            continue
        pretty = _label_from_code(code)
        if item.get("label") != pretty:
            item["label"] = pretty
            changed = True
    return items, changed


def _read_status_catalog_unlocked() -> list[dict[str, str]]:
    try:
        raw = json.loads(_STATUS_FILE.read_text(encoding="utf-8"))
    except Exception:
        return _default_status_catalog()
    return _parse_status_catalog(raw) or _default_status_catalog()


def _write_status_catalog_unlocked(items: list[dict[str, str]]) -> None:
    _STATUS_FILE.parent.mkdir(parents=True, exist_ok=True)
    payload = {"statuses": items}
    tmp_path = _STATUS_FILE.with_suffix(".json.tmp")
    tmp_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    tmp_path.replace(_STATUS_FILE)


def status_catalog() -> list[dict[str, str]]:
    with _STATUS_LOCK:
        items = _read_status_catalog_unlocked()
        items, changed = _refresh_status_labels(items)
        if changed or not _STATUS_FILE.exists():
            try:
                _write_status_catalog_unlocked(items)
            except OSError:
                pass
        return items


def remember_statuses(*values: Any) -> list[dict[str, str]]:
    codes: list[str] = []
    for value in values:
        if isinstance(value, dict):
            code = str(value.get("status") or "").strip()
            if code:
                codes.append(code)
        elif isinstance(value, (list, tuple, set)):
            for item in value:
                if isinstance(item, dict):
                    code = str(item.get("status") or "").strip()
                    if code:
                        codes.append(code)
                elif item:
                    codes.append(str(item).strip())
        elif value:
            codes.append(str(value).strip())
    codes = [code.upper() for code in codes if code]
    with _STATUS_LOCK:
        items = _read_status_catalog_unlocked()
        items, changed = _refresh_status_labels(items)
        have = {str(item.get("code") or "").upper() for item in items}
        for code in codes:
            if code in have:
                continue
            items.append({"code": code, "label": _label_from_code(code)})
            have.add(code)
            changed = True
        if changed or not _STATUS_FILE.exists():
            try:
                _write_status_catalog_unlocked(items)
            except OSError:
                pass
        return items


def _unique_item_statuses(items: list[Any]) -> list[dict[str, str]]:
    """Estados da coluna Estado (status da solicitação), não dos casos internos."""
    order = {code: i for i, code in enumerate(_KNOWN_STATUS_LABELS)}
    seen: set[str] = set()
    codes: list[str] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        code = str(item.get("status") or "").strip().upper()
        if not code or code in seen:
            continue
        seen.add(code)
        codes.append(code)
    codes.sort(key=lambda code: (order.get(code, 1000), code))
    return [{"code": code, "label": _label_from_code(code)} for code in codes]


def _timeout() -> float:
    try:
        value = float((os.getenv("MONDAY_REQUESTS_API_TIMEOUT") or "90").strip())
    except ValueError:
        value = 90.0
    return max(5.0, min(value, 180.0))


def _verify_ssl() -> bool:
    raw = (os.getenv("MONDAY_REQUESTS_API_VERIFY_SSL") or "true").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def _headers(email_solicitante: str | None = None) -> dict[str, str]:
    key = api_token() or ""
    headers = {
        "X-API-Key": key,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    email = (email_solicitante or "").strip()
    if email:
        headers["X-Solicitante-Email"] = email
    return headers


def _not_configured() -> tuple[dict[str, Any], int]:
    return (
        {
            "ok": False,
            "error": (
                "API de solicitações não configurada. "
                "Defina MONDAY_REQUESTS_API_URL e API_KEY_MONDAY_REQUESTS no .env."
            ),
        },
        503,
    )


def _profile_incomplete() -> tuple[dict[str, Any], int]:
    return (
        {
            "ok": False,
            "error": (
                "Perfil incompleto: peça a um administrador para preencher "
                "nome, sobrenome e e-mail em Utilizadores."
            ),
        },
        400,
    )


def _map_http_error(response: requests.Response) -> tuple[dict[str, Any], int]:
    code = int(response.status_code)
    if code == 401:
        return {"ok": False, "error": "API key rejeitada pela API remota (401)."}, 401
    if code == 403:
        return {"ok": False, "error": "Acesso negado pela API remota (403)."}, 403
    if code == 404:
        return {"ok": False, "error": "Solicitação não encontrada."}, 404
    detail: Any
    try:
        detail = response.json()
    except Exception:
        detail = (response.text or "")[:500]
    msg = f"API retornou HTTP {code}."
    if isinstance(detail, dict):
        for key in ("error", "mensagem", "message", "detail"):
            val = detail.get(key)
            if isinstance(val, str) and val.strip():
                msg = val.strip()
                break
            if isinstance(val, list) and val:
                first = val[0]
                if isinstance(first, dict):
                    msg = str(first.get("msg") or first.get("message") or first)
                else:
                    msg = str(first)
                break
    return (
        {"ok": False, "error": msg, "detail": detail},
        code if code >= 400 else 502,
    )


def _request_error(exc: BaseException) -> tuple[dict[str, Any], int]:
    if isinstance(exc, requests.Timeout):
        return {"ok": False, "timeout": True, "error": "Timeout ao contactar a API."}, 504
    if isinstance(exc, requests.ConnectionError):
        return {
            "ok": False,
            "error": f"Não foi possível ligar à API ({api_base()}).",
        }, 503
    return {"ok": False, "error": str(exc) or "Erro ao contactar a API."}, 502


def _clip(value: Any, max_len: int) -> str:
    return str(value or "").strip()[:max_len]


def _digits(value: Any, max_len: int = 14) -> str:
    return _NON_DIGITS_RE.sub("", str(value or ""))[:max_len]


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return bool(value)
    text = str(value or "").strip().lower()
    return text in {"1", "true", "t", "yes", "y", "sim", "on"}


def _nome_credor(value: Any, max_len: int = 500) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    nfd = unicodedata.normalize("NFD", text)
    sem_acento = "".join(ch for ch in nfd if unicodedata.category(ch) != "Mn")
    ascii_text = sem_acento.encode("ascii", "ignore").decode("ascii")
    compacto = re.sub(r"\s+", " ", ascii_text).strip().upper()
    return compacto[:max_len]


def _ensure_cooldown_table(cur) -> None:
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS plataforma_solicitacao_cooldown (
            user_key VARCHAR(190) PRIMARY KEY,
            last_at DATETIME NOT NULL,
            in_flight TINYINT(1) NOT NULL DEFAULT 0
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """
    )


def _cooldown_user_key(email: str) -> str:
    return (email or "").strip().lower()[:190]


def _cooldown_blocked(remaining: int) -> tuple[dict[str, Any], int]:
    wait = max(1, int(remaining))
    cd = create_cooldown_seconds()
    unidade = "segundo" if wait == 1 else "segundos"
    return (
        {
            "ok": False,
            "error": (
                "Para evitar envios duplicados, aguarde antes de criar outra "
                f"solicitação. Nova requisição disponível em {wait} {unidade}."
            ),
            "code": "cooldown",
            "retry_after_seconds": wait,
            "cooldown_seconds": cd,
        },
        429,
    )


def _claim_create_slot(email: str) -> int:
    """0 se reservado; senão segundos de espera."""
    key = _cooldown_user_key(email)
    if not key:
        return 0
    cooldown = create_cooldown_seconds()
    stale = max(cooldown, int(_timeout()) + 10)
    try:
        with auth_connection() as conn:
            cur = auth_cursor(conn)
            _ensure_cooldown_table(cur)
            conn.commit()
            conn.start_transaction()
            cur.execute(
                """
                SELECT in_flight,
                       TIMESTAMPDIFF(SECOND, last_at, NOW()) AS elapsed
                FROM plataforma_solicitacao_cooldown
                WHERE user_key = %s
                FOR UPDATE
                """,
                (key,),
            )
            row = cur.fetchone() or None
            if row:
                elapsed = int(row.get("elapsed") or 0)
                in_flight = int(row.get("in_flight") or 0)
                if in_flight and elapsed < stale:
                    conn.rollback()
                    if elapsed < cooldown:
                        return max(1, cooldown - elapsed)
                    return max(1, stale - elapsed)
                if elapsed < cooldown:
                    conn.rollback()
                    return max(1, cooldown - elapsed)
                cur.execute(
                    """
                    UPDATE plataforma_solicitacao_cooldown
                    SET last_at = NOW(), in_flight = 1
                    WHERE user_key = %s
                    """,
                    (key,),
                )
            else:
                try:
                    cur.execute(
                        """
                        INSERT INTO plataforma_solicitacao_cooldown
                            (user_key, last_at, in_flight)
                        VALUES (%s, NOW(), 1)
                        """,
                        (key,),
                    )
                except IntegrityError:
                    conn.rollback()
                    return cooldown
            conn.commit()
            return 0
    except Exception:
        return 0


def _finish_create_slot(email: str, *, success: bool) -> None:
    key = _cooldown_user_key(email)
    if not key:
        return
    cooldown = create_cooldown_seconds()
    try:
        with auth_connection() as conn:
            cur = auth_cursor(conn)
            if success:
                cur.execute(
                    """
                    UPDATE plataforma_solicitacao_cooldown
                    SET in_flight = 0
                    WHERE user_key = %s
                    """,
                    (key,),
                )
            else:
                cur.execute(
                    """
                    UPDATE plataforma_solicitacao_cooldown
                    SET in_flight = 0,
                        last_at = DATE_SUB(NOW(), INTERVAL %s SECOND)
                    WHERE user_key = %s
                    """,
                    (cooldown, key),
                )
            conn.commit()
    except Exception:
        return


def _ensure_archive_table(cur) -> None:
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS plataforma_solicitacao_arquivo (
            sol_id VARCHAR(80) PRIMARY KEY,
            archived_by_user_id INT NULL,
            archived_by VARCHAR(100) NULL,
            archived_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            snapshot JSON NULL,
            INDEX idx_sol_arq_at (archived_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """
    )


def _json_safe(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe(v) for v in value]
    return value


def _snapshot_from_item(item: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key in _SNAPSHOT_KEYS:
        if key in item and item[key] is not None:
            out[key] = _json_safe(item[key])
    return out


def _copy_casos_counts(item: dict[str, Any]) -> None:
    det = item.get("detalhe") if isinstance(item.get("detalhe"), dict) else {}
    for key in (
        "casos_count",
        "casos_aptos_count",
        "casos_inaptos_count",
        "fila_casos_a_frente",
        "fila_em_processamento",
        "eta_seconds_estimado",
        "eta_confianca",
        "is_cumprimento",
        "numero_cumprimento",
        "avaliacao_cumprimento",
        "houve_homologacao",
        "houve_impugnacao",
        "houve_intimacao_impugnacao",
        "houve_concordancia_calculo",
        "movimentacoes",
        "movimentacoes_resumo",
        "autos",
        "autos_texto",
        "analise_gemini",
        "analise_gemini_resultado",
        "analise_gemini_aviso",
        "calculo_atualizado",
    ):
        if item.get(key) is None and det.get(key) is not None:
            item[key] = det[key]
    if not isinstance(item.get("casos"), list) and isinstance(det.get("casos"), list):
        item["casos"] = det["casos"]


def _item_is_cumprimento(item: dict[str, Any]) -> bool:
    det = item.get("detalhe") if isinstance(item.get("detalhe"), dict) else {}
    if item.get("is_cumprimento") is not None:
        return _as_bool(item.get("is_cumprimento"))
    if det.get("is_cumprimento") is not None:
        return _as_bool(det.get("is_cumprimento"))
    num = str(
        item.get("numero_cumprimento") or det.get("numero_cumprimento") or ""
    ).strip()
    return bool(num)


def _pick_nested_field(item: dict[str, Any], key: str) -> Any:
    if key in item and item.get(key) is not None:
        return item.get(key)
    det = item.get("detalhe") if isinstance(item.get("detalhe"), dict) else {}
    if key in det:
        return det.get(key)
    return None


def _calculo_atualizado_from_obj(obj: dict[str, Any] | None) -> Any:
    if not isinstance(obj, dict):
        return None
    for key in ("calculo_atualizado", "calculo_atualizado_formatado"):
        val = obj.get(key)
        if val is None:
            continue
        if isinstance(val, bool):
            continue
        if isinstance(val, (int, float)):
            return val
        text = str(val).strip()
        if text and text.lower() not in {"null", "none"}:
            return val
    return None


def _pick_calculo_atualizado(item: dict[str, Any]) -> Any:
    """Valor do cálculo atualizado no polling (item, detalhe, resultado Gemini ou casos)."""
    resultado = item.get("analise_gemini_resultado")
    det = item.get("detalhe") if isinstance(item.get("detalhe"), dict) else {}
    det_resultado = det.get("analise_gemini_resultado") if isinstance(det, dict) else None
    hit = _calculo_atualizado_from_obj(item)
    if hit is not None:
        return hit
    hit = _calculo_atualizado_from_obj(det)
    if hit is not None:
        return hit
    hit = _calculo_atualizado_from_obj(resultado if isinstance(resultado, dict) else None)
    if hit is not None:
        return hit
    hit = _calculo_atualizado_from_obj(
        det_resultado if isinstance(det_resultado, dict) else None
    )
    if hit is not None:
        return hit
    for caso in _casos_dicts(item):
        hit = _calculo_atualizado_from_obj(caso)
        if hit is not None:
            return hit
        caso_res = caso.get("analise_gemini_resultado")
        hit = _calculo_atualizado_from_obj(
            caso_res if isinstance(caso_res, dict) else None
        )
        if hit is not None:
            return hit
    return None


def _read_analise_gemini(obj: dict[str, Any] | None) -> bool | None:
    """None = campo ausente; True/False = valor enviado pelo server no polling."""
    if not isinstance(obj, dict) or "analise_gemini" not in obj:
        return None
    return _as_bool(obj.get("analise_gemini"))


def _annotate_analise_gemini(item: dict[str, Any]) -> None:
    """Propaga analise_gemini + resultado/aviso do polling para a UI."""
    det = item.get("detalhe") if isinstance(item.get("detalhe"), dict) else {}
    hit = False
    found = False
    for src in (item, det):
        val = _read_analise_gemini(src)
        if val is None:
            continue
        found = True
        if val:
            hit = True
            break
    for caso in _casos_dicts(item):
        val = _read_analise_gemini(caso)
        if val is None:
            continue
        found = True
        caso["analise_gemini"] = val
        if val:
            hit = True
    if found:
        item["analise_gemini"] = hit
    elif "analise_gemini" in item:
        item["analise_gemini"] = _as_bool(item.get("analise_gemini"))
    else:
        item["analise_gemini"] = False

    resultado = _pick_nested_field(item, "analise_gemini_resultado")
    if isinstance(resultado, dict):
        item["analise_gemini_resultado"] = _json_safe(resultado)
    elif resultado is None and "analise_gemini_resultado" in item:
        item["analise_gemini_resultado"] = None
    elif resultado is None and "analise_gemini_resultado" in det:
        item["analise_gemini_resultado"] = None
    elif resultado is not None and not isinstance(resultado, dict):
        # payload inesperado: não inventar objeto
        item["analise_gemini_resultado"] = None

    aviso = _pick_nested_field(item, "analise_gemini_aviso")
    if aviso is None:
        item["analise_gemini_aviso"] = None
    else:
        text = str(aviso).strip()
        item["analise_gemini_aviso"] = text or None

    try:
        calculo = _pick_calculo_atualizado(item)
        if calculo is None:
            item["calculo_atualizado"] = None
        else:
            item["calculo_atualizado"] = _json_safe(calculo)
    except Exception:
        pass


def _pretty_obj_status_motivo(obj: dict[str, Any]) -> None:
    status = obj.get("status")
    if status not in (None, ""):
        obj["status_label"] = _label_from_code(str(status))
    elif isinstance(obj.get("status_label"), str) and obj.get("status_label").strip():
        obj["status_label"] = pretty_pt_text(obj["status_label"], as_label=True)
    for key in ("motivo", "mensagem", "analise_gemini_aviso"):
        val = obj.get(key)
        if isinstance(val, str) and val.strip() and val.lstrip()[:1] not in "{[":
            obj[key] = pretty_pt_text(val)
    resultado = obj.get("analise_gemini_resultado")
    if isinstance(resultado, dict):
        for key, val in list(resultado.items()):
            if isinstance(val, str) and val.strip() and ("_" in val or _fold_pt(val) in _PT_PHRASE_MAP):
                resultado[key] = pretty_pt_text(val, as_label=True)


def _annotate_pt_display(item: dict[str, Any]) -> None:
    """Normaliza acentos de status, motivo e valores enumerados do polling."""
    _pretty_obj_status_motivo(item)
    det = item.get("detalhe")
    if isinstance(det, dict):
        _pretty_obj_status_motivo(det)
    for caso in _casos_dicts(item):
        _pretty_obj_status_motivo(caso)


def _annotate_tipo(item: dict[str, Any]) -> None:
    is_c = _item_is_cumprimento(item)
    item["is_cumprimento"] = is_c
    item["tipo"] = "cumprimento" if is_c else "incidente"
    try:
        _annotate_analise_gemini(item)
    except Exception:
        pass
    try:
        _annotate_pt_display(item)
    except Exception:
        pass
    if not is_c:
        return
    _annotate_movimentacoes(item)
    for fn in (_annotate_autos, _annotate_autos_texto, _annotate_avaliacao_cumprimento):
        try:
            fn(item)
        except Exception:
            continue


def _annotate_movimentacoes(item: dict[str, Any]) -> None:
    det = item.get("detalhe") if isinstance(item.get("detalhe"), dict) else {}
    mov = item.get("movimentacoes")
    if not isinstance(mov, dict):
        nested = det.get("movimentacoes")
        if isinstance(nested, dict):
            item["movimentacoes"] = nested
            mov = nested
    resumo = item.get("movimentacoes_resumo")
    if not isinstance(resumo, list):
        nested_resumo = det.get("movimentacoes_resumo")
        if isinstance(nested_resumo, list):
            item["movimentacoes_resumo"] = nested_resumo
            resumo = nested_resumo
    if isinstance(mov, dict) and not isinstance(resumo, list):
        built: list[str] = []
        for key in ("intimacao", "impugnacao", "calculo_homologado", "certidao"):
            row = mov.get(key) if isinstance(mov.get(key), dict) else {}
            text = str(row.get("resumo") or "").strip()
            if text:
                built.append(text)
        if built:
            item["movimentacoes_resumo"] = built


def _fold_person_name(value: str) -> str:
    return _fold_pt(value)


def _user_matches_person_allowlist(
    user: dict | None,
    emails: frozenset[str],
    names: tuple[str, ...],
) -> bool:
    if not isinstance(user, dict):
        return False
    email = str(user.get("email") or "").strip().lower()
    if email in emails:
        return True
    first = str(user.get("first_name") or "").strip()
    last = str(user.get("last_name") or "").strip()
    nome = _fold_person_name(" ".join(p for p in (first, last) if p))
    if not nome:
        nome = _fold_person_name(str(user.get("username") or ""))
    for allowed in names:
        if nome == allowed or nome.startswith(allowed + " "):
            return True
    return False


def user_can_view_autos(user: dict | None) -> bool:
    """Autos e TXTs baixados só para Guilherme Vitoriano."""
    return _user_matches_person_allowlist(
        user, _AUTOS_ALLOWED_EMAILS, _AUTOS_ALLOWED_NAMES
    )


def user_can_view_gemini_details(user: dict | None) -> bool:
    """Detalhe completo da análise Gemini: Guilherme Vitoriano ou Filipe Noberto."""
    return _user_matches_person_allowlist(
        user, _GEMINI_DETAILS_ALLOWED_EMAILS, _GEMINI_DETAILS_ALLOWED_NAMES
    )


def user_can_view_other_solicitacoes(user: dict | None) -> bool:
    """Listar/abrir solicitações de outros usuários: Guilherme Vitoriano ou Filipe Noberto."""
    return user_can_view_gemini_details(user)


def _redact_autos_texto_downloads(texto: Any) -> None:
    """Remove URLs/documentos de download; mantém aviso/credor para a UI."""
    if not isinstance(texto, dict):
        return
    texto.pop("url", None)
    texto["documentos"] = []
    texto["documentos_count"] = 0
    texto["disponivel"] = False


def _strip_autos(item: dict[str, Any] | None) -> None:
    if not isinstance(item, dict):
        return
    item.pop("autos", None)
    if "autos_texto" in item:
        _redact_autos_texto_downloads(item.get("autos_texto"))
    det = item.get("detalhe")
    if isinstance(det, dict):
        det.pop("autos", None)
        if "autos_texto" in det:
            _redact_autos_texto_downloads(det.get("autos_texto"))


def _redact_autos_unless_allowed(payload: dict[str, Any], user: dict | None) -> None:
    if user_can_view_autos(user):
        return
    _strip_autos(payload)
    items = payload.get("items")
    if isinstance(items, list):
        for it in items:
            _strip_autos(it)


def _has_calculo_atualizado(item: dict[str, Any]) -> bool:
    return _pick_calculo_atualizado(item) is not None


def _pop_calculo_atualizado(obj: dict[str, Any] | None) -> None:
    if not isinstance(obj, dict):
        return
    obj.pop("calculo_atualizado", None)
    obj.pop("calculo_atualizado_formatado", None)
    nested = obj.get("analise_gemini_resultado")
    if isinstance(nested, dict):
        nested.pop("calculo_atualizado", None)
        nested.pop("calculo_atualizado_formatado", None)


def _strip_gemini_resultado(item: dict[str, Any] | None) -> None:
    """Remove o JSON completo; marca concluída se havia resultado estruturado."""
    if not isinstance(item, dict):
        return
    resultado = item.get("analise_gemini_resultado")
    det = item.get("detalhe")
    det_resultado = (
        det.get("analise_gemini_resultado") if isinstance(det, dict) else None
    )
    has_calc = _has_calculo_atualizado(item)
    if isinstance(resultado, dict) or isinstance(det_resultado, dict) or has_calc:
        item["analise_gemini_concluida"] = True
        if isinstance(det, dict):
            det["analise_gemini_concluida"] = True
    item.pop("analise_gemini_resultado", None)
    _pop_calculo_atualizado(item)
    if isinstance(det, dict):
        det.pop("analise_gemini_resultado", None)
        _pop_calculo_atualizado(det)
    for caso in _casos_dicts(item):
        caso.pop("analise_gemini_resultado", None)
        _pop_calculo_atualizado(caso)


def _redact_gemini_unless_allowed(payload: dict[str, Any], user: dict | None) -> None:
    """Usuários comuns não recebem o JSON completo da análise Gemini."""
    if user_can_view_gemini_details(user):
        return
    _strip_gemini_resultado(payload)
    items = payload.get("items")
    if isinstance(items, list):
        for it in items:
            _strip_gemini_resultado(it)


def _redact_sensitive_fields(payload: dict[str, Any], user: dict | None) -> None:
    _redact_autos_unless_allowed(payload, user)
    _redact_gemini_unless_allowed(payload, user)


def _bff_autos_path(sol_id: str, indice: int | None = None) -> str:
    base = f"/api/solicitacao-inclusao/{quote(sol_id, safe='')}/autos"
    if indice is None:
        return base
    return f"{base}/{int(indice)}"


def _annotate_autos(item: dict[str, Any]) -> None:
    det = item.get("detalhe") if isinstance(item.get("detalhe"), dict) else {}
    autos = item.get("autos")
    if not isinstance(autos, dict):
        nested = det.get("autos")
        if isinstance(nested, dict):
            item["autos"] = nested
            autos = nested
    if not isinstance(autos, dict):
        return
    sid = _item_id(item)
    if not sid:
        return
    disponivel = _as_bool(autos.get("disponivel"))
    autos["disponivel"] = disponivel
    autos["url"] = _bff_autos_path(sid) if disponivel else None
    docs = autos.get("documentos")
    if not isinstance(docs, list):
        autos["documentos"] = []
        return
    cleaned: list[dict[str, Any]] = []
    for row in docs:
        if not isinstance(row, dict):
            continue
        try:
            indice = int(row.get("indice"))
        except (TypeError, ValueError):
            continue
        if indice < 0 or indice > 9999:
            continue
        doc_ok = row.get("disponivel")
        doc_ok = disponivel if doc_ok is None else _as_bool(doc_ok)
        cleaned.append(
            {
                "indice": indice,
                "titulo": str(row.get("titulo") or "Documento").strip() or "Documento",
                "tipo": str(row.get("tipo") or "Documento").strip() or "Documento",
                "ordem": row.get("ordem"),
                "disponivel": doc_ok,
                "url": _bff_autos_path(sid, indice) if doc_ok else None,
            }
        )
    autos["documentos"] = cleaned
    if autos.get("documentos_count") is None:
        autos["documentos_count"] = len(cleaned)


def _bff_autos_texto_path(sol_id: str, tipo: str) -> str:
    return f"/api/solicitacao-inclusao/{quote(sol_id, safe='')}/autos/texto/{tipo}"


def _autos_texto_tipo(row: dict[str, Any]) -> str | None:
    tipo = str(row.get("tipo") or "").strip().lower()
    if tipo in _AUTOS_TEXTO_TITULOS:
        return tipo
    url = str(row.get("url") or "").rstrip("/").lower()
    for key in _AUTOS_TEXTO_TIPOS:
        if url.endswith(f"/texto/{key}"):
            return key
    return None


def _annotate_autos_texto(item: dict[str, Any]) -> None:
    det = item.get("detalhe") if isinstance(item.get("detalhe"), dict) else {}
    texto = item.get("autos_texto")
    if not isinstance(texto, dict):
        nested = det.get("autos_texto")
        if isinstance(nested, dict):
            item["autos_texto"] = nested
            texto = nested
    if not isinstance(texto, dict):
        return
    sid = _item_id(item)
    if not sid:
        return
    # Metadados do OCR (preservar; o client usa para aviso de credor).
    if "ok" in texto:
        texto["ok"] = _as_bool(texto.get("ok"))
    if "credor_encontrado" in texto:
        texto["credor_encontrado"] = _as_bool(texto.get("credor_encontrado"))
    if texto.get("nome_credor") is not None:
        texto["nome_credor"] = str(texto.get("nome_credor") or "").strip() or None
    if texto.get("aviso") is not None:
        aviso = str(texto.get("aviso") or "").strip()
        texto["aviso"] = aviso or None
    for meta_key in ("pagina_credor", "paginas_filtradas", "paginas_total"):
        if meta_key in texto and texto.get(meta_key) is not None:
            try:
                texto[meta_key] = int(texto.get(meta_key))
            except (TypeError, ValueError):
                pass
    if "disponivel" not in texto and "ok" in texto:
        texto["disponivel"] = bool(texto.get("ok"))
    disponivel = _as_bool(texto.get("disponivel"))
    texto["disponivel"] = disponivel
    docs = texto.get("documentos")
    by_tipo: dict[str, dict[str, Any]] = {}
    if isinstance(docs, list):
        for row in docs:
            if not isinstance(row, dict):
                continue
            tipo = _autos_texto_tipo(row)
            if not tipo:
                continue
            doc_ok = row.get("disponivel")
            doc_ok = disponivel if doc_ok is None else _as_bool(doc_ok)
            titulo = str(row.get("titulo") or _AUTOS_TEXTO_TITULOS[tipo]).strip()
            by_tipo[tipo] = {
                "tipo": tipo,
                "titulo": titulo or _AUTOS_TEXTO_TITULOS[tipo],
                "disponivel": doc_ok,
                "url": _bff_autos_texto_path(sid, tipo) if doc_ok else None,
            }
    if disponivel:
        for tipo in _AUTOS_TEXTO_TIPOS:
            if tipo not in by_tipo:
                by_tipo[tipo] = {
                    "tipo": tipo,
                    "titulo": _AUTOS_TEXTO_TITULOS[tipo],
                    "disponivel": True,
                    "url": _bff_autos_texto_path(sid, tipo),
                }
    texto["documentos"] = [by_tipo[t] for t in _AUTOS_TEXTO_TIPOS if t in by_tipo]
    if texto.get("documentos_count") is None:
        texto["documentos_count"] = len(texto["documentos"])


def _tri_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if value == 1:
            return True
        if value == 0:
            return False
        return None
    text = str(value).strip().lower()
    if text in {"", "null", "none", "n/a", "-"}:
        return None
    if text in {"1", "true", "t", "yes", "y", "sim", "on"}:
        return True
    if text in {"0", "false", "f", "no", "n", "nao", "não", "off"}:
        return False
    return None


def _first_flag(obj: dict[str, Any], keys: tuple[str, ...]) -> bool | None:
    det = obj.get("detalhe") if isinstance(obj.get("detalhe"), dict) else {}
    av = obj.get("avaliacao_cumprimento") if isinstance(obj.get("avaliacao_cumprimento"), dict) else {}
    for key in keys:
        for src in (obj, det, av, av.get("flags") if isinstance(av.get("flags"), dict) else {}):
            if not isinstance(src, dict) or key not in src:
                continue
            tri = _tri_bool(src.get(key))
            if tri is not None:
                return tri
    return None


def _flags_from_obj(obj: dict[str, Any]) -> dict[str, bool | None]:
    return {
        "houve_homologacao": _first_flag(
            obj, ("houve_homologacao", "homologacao", "homologado")
        ),
        "houve_impugnacao": _first_flag(
            obj, ("houve_impugnacao", "impugnacao", "impugnado")
        ),
        "houve_intimacao_impugnacao": _first_flag(
            obj,
            (
                "houve_intimacao_impugnacao",
                "intimacao_impugnacao",
                "intimacao_para_impugnacao",
                "intimacao_impug",
            ),
        ),
        "houve_concordancia_calculo": _first_flag(
            obj,
            (
                "houve_concordancia_calculo",
                "concordancia_calculo",
                "concordancia_calcs",
                "concordou_calculos",
            ),
        ),
    }


def _casos_dicts(item: dict[str, Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    det = item.get("detalhe") if isinstance(item.get("detalhe"), dict) else {}
    for src in (
        item.get("casos"),
        det.get("casos"),
        item.get("desfechos"),
        det.get("desfechos"),
    ):
        if not isinstance(src, list):
            continue
        for row in src:
            if isinstance(row, dict):
                out.append(row)
    return out


def _cumprimento_flags(item: dict[str, Any]) -> dict[str, bool | None]:
    flags = _flags_from_obj(item)
    if any(v is not None for v in flags.values()):
        return flags
    for caso in _casos_dicts(item):
        nested = _flags_from_obj(caso)
        if any(v is not None for v in nested.values()):
            return nested
    return flags


def classificar_coleta_cumprimento(item: dict[str, Any]) -> dict[str, Any] | None:
    """Fluxo Coleta Cumprimento: homologação → impugnação → intimação → concordância."""
    flags = _cumprimento_flags(item)
    if all(v is None for v in flags.values()):
        return None
    if flags["houve_homologacao"] is True:
        return {
            "status": "APTO",
            "status_label": "Apto",
            "motivo": "Houve homologação. Apto — o valor fica a cargo do comprador.",
            "passo": "homologacao",
            "flags": flags,
        }
    if flags["houve_impugnacao"] is True:
        return {
            "status": "ANALISE_IA",
            "status_label": "Análise da IA",
            "motivo": "Houve impugnação. Documentos separados para a IA analisar.",
            "passo": "impugnacao",
            "flags": flags,
        }
    if flags["houve_intimacao_impugnacao"] is False:
        return {
            "status": "INAPTO",
            "status_label": "Inapto",
            "motivo": "Não houve intimação para impugnação.",
            "passo": "intimacao",
            "flags": flags,
        }
    if flags["houve_intimacao_impugnacao"] is True:
        if flags["houve_concordancia_calculo"] is True:
            return {
                "status": "APTO",
                "status_label": "Apto",
                "motivo": "Houve concordância com os cálculos. Apto — o valor fica a cargo do comprador.",
                "passo": "concordancia",
                "flags": flags,
            }
        if flags["houve_concordancia_calculo"] is False:
            return {
                "status": "AGUARDAR_CONCLUSAO_JURIDICA",
                "status_label": "Aguardar conclusão jurídica",
                "motivo": "Não houve concordância com os cálculos. Aguardar conclusão jurídica.",
                "passo": "concordancia",
                "flags": flags,
            }
    return {"status": None, "status_label": None, "motivo": None, "passo": None, "flags": flags}


def _annotate_avaliacao_cumprimento(item: dict[str, Any]) -> None:
    av = classificar_coleta_cumprimento(item)
    if not av:
        return
    item["avaliacao_cumprimento"] = av
    for key, val in (av.get("flags") or {}).items():
        if item.get(key) is None and val is not None:
            item[key] = val
    classified = str(av.get("status") or "").strip().upper()
    current = str(item.get("status") or "").strip().upper()
    if classified and current in {"RECEBIDO", "PROCESSANDO", ""}:
        item["status_api"] = current
        item["status"] = classified
        item["status_label"] = av.get("status_label") or _KNOWN_STATUS_LABELS.get(
            classified, classified
        )


def _normalize_tipo(tipo: str) -> str:
    want = (tipo or "").strip().lower()
    if want in {"cumprimento", "cump"}:
        return "cumprimento"
    if want in {"incidente", "inc"}:
        return "incidente"
    return ""


def _filter_by_tipo(items: list[dict[str, Any]], tipo: str) -> list[dict[str, Any]]:
    want = _normalize_tipo(tipo)
    if want == "cumprimento":
        return [it for it in items if _item_is_cumprimento(it)]
    if want == "incidente":
        return [it for it in items if not _item_is_cumprimento(it)]
    return items


def _parse_snapshot(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8", "replace")
    if isinstance(raw, str) and raw.strip():
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            return {}
        return data if isinstance(data, dict) else {}
    return {}


def _archived_ids() -> set[str]:
    with auth_connection() as conn:
        cur = auth_cursor(conn)
        _ensure_archive_table(cur)
        conn.commit()
        cur.execute("SELECT sol_id FROM plataforma_solicitacao_arquivo")
        return {str(r["sol_id"]) for r in cur.fetchall() if r.get("sol_id")}


def _is_archived(sol_id: str) -> bool:
    with auth_connection() as conn:
        cur = auth_cursor(conn)
        _ensure_archive_table(cur)
        conn.commit()
        cur.execute(
            "SELECT 1 FROM plataforma_solicitacao_arquivo WHERE sol_id = %s",
            (sol_id,),
        )
        return bool(cur.fetchone())


def _actor(user: dict | None) -> tuple[int | None, str]:
    if not user:
        return None, ""
    try:
        uid = int(user["id"]) if user.get("id") is not None else None
    except (TypeError, ValueError):
        uid = None
    name = str(user.get("username") or "").strip()[:100]
    return uid, name


def _item_id(item: Any) -> str:
    if not isinstance(item, dict):
        return ""
    return str(item.get("id") or "").strip()


def _remote_list(
    params: dict[str, Any],
    *,
    email_solicitante: str | None = None,
) -> tuple[dict[str, Any], int]:
    try:
        response = requests.get(
            f"{api_base()}/solicitacoes",
            params=params,
            headers=_headers(email_solicitante),
            timeout=_timeout(),
            verify=_verify_ssl(),
        )
    except Exception as exc:  # noqa: BLE001
        return _request_error(exc)
    if response.status_code >= 400:
        return _map_http_error(response)
    try:
        payload = response.json()
    except Exception:
        payload = {}
    if not isinstance(payload, dict):
        payload = {"items": payload if isinstance(payload, list) else []}
    items = payload.get("items")
    if not isinstance(items, list):
        items = []
    total = payload.get("total")
    try:
        total_n = int(total) if total is not None else len(items)
    except (TypeError, ValueError):
        total_n = len(items)
    return (
        {
            "ok": True,
            "items": items,
            "total": total_n,
            "limit": int(payload.get("limit") or params.get("limit") or 50),
            "offset": int(payload.get("offset") or params.get("offset") or 0),
        },
        200,
    )


def _remote_list_solicitantes(q: str = "") -> tuple[dict[str, Any], int]:
    """GET /solicitantes na API remota (emails distintos com ≥1 solicitação)."""
    params: dict[str, Any] = {}
    q = (q or "").strip()[:200]
    if q:
        params["q"] = q
    try:
        response = requests.get(
            f"{api_base()}/solicitantes",
            params=params or None,
            headers=_headers(),
            timeout=_timeout(),
            verify=_verify_ssl(),
        )
    except Exception as exc:  # noqa: BLE001
        return _request_error(exc)
    if response.status_code >= 400:
        return _map_http_error(response)
    try:
        payload = response.json()
    except Exception:
        payload = {}
    if not isinstance(payload, dict):
        payload = {"items": payload if isinstance(payload, list) else []}
    raw_items = payload.get("items")
    if not isinstance(raw_items, list):
        raw_items = []
    items: list[dict[str, Any]] = []
    for it in raw_items:
        if not isinstance(it, dict):
            continue
        email = str(it.get("email_solicitante") or "").strip().lower()
        if not email:
            continue
        nome = str(it.get("nome_solicitante") or "").strip()
        try:
            total = int(it.get("total") or 0)
        except (TypeError, ValueError):
            total = 0
        items.append(
            {
                "email_solicitante": email,
                "nome_solicitante": nome or email,
                "total": max(0, total),
            }
        )
    items.sort(
        key=lambda row: (
            str(row.get("nome_solicitante") or "").casefold(),
            str(row.get("email_solicitante") or ""),
        )
    )
    return {"ok": True, "items": items, "total": len(items)}, 200


def listar_solicitantes(
    user: dict | None, *, q: str = ""
) -> tuple[dict[str, Any], int]:
    if not user_can_view_other_solicitacoes(user):
        return {"ok": False, "error": "Sem permissão para listar solicitantes."}, 403
    if not is_configured():
        return _not_configured()
    sol = solicitante_from_user(user)
    if not sol:
        return _profile_incomplete()
    return _remote_list_solicitantes(q=q)


def build_create_payload(data: dict[str, Any], user: dict | None) -> tuple[dict[str, Any] | None, str | None]:
    sol = solicitante_from_user(user)
    if not sol:
        return None, (
            "Perfil incompleto: peça a um administrador para preencher "
            "nome, sobrenome e e-mail em Utilizadores."
        )

    processo = _clip(data.get("processo"), 80)
    incidente = _clip(data.get("incidente"), 50)
    numero_cumprimento = _clip(data.get("numero_cumprimento"), 80)
    cpf = _digits(data.get("cpf_credor") or data.get("cpf"), 14)
    requerente = _nome_credor(
        data.get("requerente") or data.get("nome_credor") or data.get("nome"),
        500,
    )
    is_cumprimento = _as_bool(
        data.get("is_cumprimento")
        if data.get("is_cumprimento") is not None
        else data.get("switch_cumprimento")
    )

    if is_cumprimento:
        if not numero_cumprimento:
            return None, "Informe o número do processo de cumprimento."
        if not requerente and not cpf:
            return None, "Informe o nome do credor ou o CPF."
    else:
        if not processo:
            return None, "Informe o número do processo."
        if not incidente:
            return None, "Informe o incidente."
        if not requerente and not cpf:
            return None, "Informe o nome do credor ou o CPF."
    if cpf and len(cpf) not in (11, 14):
        return None, "CPF/CNPJ deve ter 11 ou 14 dígitos."

    body: dict[str, Any] = {
        "nome_solicitante": sol["nome_solicitante"],
        "email_solicitante": sol["email_solicitante"],
        "is_cumprimento": is_cumprimento,
    }
    if is_cumprimento:
        body["numero_cumprimento"] = numero_cumprimento
    else:
        body["processo"] = processo
        body["incidente"] = incidente
    if cpf:
        body["cpf_credor"] = cpf
    if requerente:
        body["requerente"] = requerente
        body["nome_credor"] = requerente
    return body, None


def _norm_cnj(value: Any) -> str:
    return _NON_DIGITS_RE.sub("", str(value or ""))


def _norm_incidente(value: Any) -> str:
    digits = _NON_DIGITS_RE.sub("", str(value or ""))
    if not digits:
        return ""
    return digits.lstrip("0") or "0"


def _split_processo_label(label: str) -> tuple[str, str]:
    text = str(label or "").strip()
    if "/" in text:
        left, right = text.rsplit("/", 1)
        return left.strip(), right.strip()
    return text, ""


def _credor_pair(obj: dict[str, Any]) -> tuple[str, str]:
    det = obj.get("detalhe") if isinstance(obj.get("detalhe"), dict) else {}
    nome = _nome_credor(
        obj.get("requerente")
        or obj.get("nome_credor")
        or obj.get("nome")
        or det.get("requerente")
        or det.get("nome_credor")
        or det.get("nome")
        or ""
    )
    cpf = _digits(
        obj.get("cpf_credor") or obj.get("cpf") or det.get("cpf_credor") or det.get("cpf"),
        14,
    )
    return nome, cpf


def _same_credor(a: tuple[str, str], b: tuple[str, str]) -> bool:
    nome_a, cpf_a = a
    nome_b, cpf_b = b
    if cpf_a and cpf_b:
        return cpf_a == cpf_b
    if nome_a and nome_b:
        return nome_a == nome_b
    return False


def _item_proc_inc(item: dict[str, Any]) -> tuple[str, str]:
    det = item.get("detalhe") if isinstance(item.get("detalhe"), dict) else {}
    proc = str(item.get("processo") or det.get("processo") or "")
    inc = str(item.get("incidente") or det.get("incidente") or "")
    label = str(item.get("processo_label") or det.get("processo_label") or "")
    if label:
        left, right = _split_processo_label(label)
        proc = proc or left
        inc = inc or right
    return _norm_cnj(proc), _norm_incidente(inc)


def _item_cumprimento_num(item: dict[str, Any]) -> str:
    det = item.get("detalhe") if isinstance(item.get("detalhe"), dict) else {}
    raw = (
        item.get("numero_cumprimento")
        or det.get("numero_cumprimento")
        or item.get("cumprimento")
        or det.get("cumprimento")
        or ""
    )
    digits = _norm_cnj(raw)
    if digits:
        return digits
    if _item_is_cumprimento(item):
        proc, _inc = _item_proc_inc(item)
        return proc
    return ""


def _parse_item_dt(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, date):
        dt = datetime.combine(value, datetime.min.time())
    else:
        text = str(value or "").strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(text)
        except ValueError:
            dt = None
            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%d/%m/%Y %H:%M"):
                try:
                    dt = datetime.strptime(text[:19], fmt)
                    break
                except ValueError:
                    continue
            if dt is None:
                return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _item_updated_at(obj: dict[str, Any]) -> datetime | None:
    det = obj.get("detalhe") if isinstance(obj.get("detalhe"), dict) else {}
    for key in ("updated_at", "atualizado_em", "updatedAt", "created_at", "criado_em"):
        dt = _parse_item_dt(obj.get(key))
        if dt:
            return dt
        dt = _parse_item_dt(det.get(key))
        if dt:
            return dt
    return None


def _updated_within_hours(obj: dict[str, Any], hours: int) -> bool:
    dt = _item_updated_at(obj)
    if not dt:
        return False
    age = datetime.now(timezone.utc) - dt
    return age < timedelta(hours=max(1, hours))


def _iter_status_objs(item: dict[str, Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = [item]
    det = item.get("detalhe") if isinstance(item.get("detalhe"), dict) else {}
    for key in ("casos", "desfechos"):
        for lst in (item.get(key), det.get(key)):
            if not isinstance(lst, list):
                continue
            for row in lst:
                if isinstance(row, dict):
                    out.append(row)
    return out


def _status_needs_duplicate_confirm(
    status: str, obj: dict[str, Any], fallback: dict[str, Any] | None = None
) -> bool:
    st = str(status or "").upper()
    if st in _DUP_ALWAYS_STATUSES:
        return True
    if st in _DUP_RECENT_STATUSES:
        probe = obj if _item_updated_at(obj) else (fallback or obj)
        return _updated_within_hours(probe, _INAPTO_DUP_HOURS)
    return False


def _duplicate_reason_status(item: dict[str, Any]) -> str:
    for obj in _iter_status_objs(item):
        st = str(obj.get("status") or "").upper()
        if _status_needs_duplicate_confirm(st, obj, item):
            return st
    return str(item.get("status") or "").upper()


def _item_needs_duplicate_confirm(item: dict[str, Any]) -> bool:
    return any(
        _status_needs_duplicate_confirm(obj.get("status"), obj, item)
        for obj in _iter_status_objs(item)
    )


def _same_open_case(body: dict[str, Any], item: dict[str, Any]) -> bool:
    if not _item_needs_duplicate_confirm(item):
        return False
    body_cump = _as_bool(body.get("is_cumprimento"))
    item_cump = _item_is_cumprimento(item)
    if body_cump != item_cump:
        return False
    if not _same_credor(_credor_pair(body), _credor_pair(item)):
        return False
    if body_cump:
        want = _norm_cnj(body.get("numero_cumprimento"))
        got = _item_cumprimento_num(item)
        return bool(want) and want == got
    want_p, want_i = _norm_cnj(body.get("processo")), _norm_incidente(body.get("incidente"))
    got_p, got_i = _item_proc_inc(item)
    return bool(want_p and want_i) and want_p == got_p and want_i == got_i


def _match_public(item: dict[str, Any]) -> dict[str, Any]:
    status = _duplicate_reason_status(item) or str(item.get("status") or "").upper()
    nome, cpf = _credor_pair(item)
    proc, inc = _item_proc_inc(item)
    updated = _item_updated_at(item)
    out = {
        "id": _item_id(item),
        "status": status,
        "status_label": _KNOWN_STATUS_LABELS.get(status) or status,
        "is_cumprimento": _item_is_cumprimento(item),
        "requerente": nome,
        "cpf_credor": cpf,
        "updated_at": updated.isoformat() if updated else (item.get("updated_at") or ""),
    }
    if out["is_cumprimento"]:
        out["numero_cumprimento"] = item.get("numero_cumprimento") or _item_cumprimento_num(item)
    else:
        out["processo"] = item.get("processo_label") or item.get("processo") or proc
        out["incidente"] = item.get("incidente") or inc
    return out


def _find_open_duplicate(
    body: dict[str, Any], email: str
) -> dict[str, Any] | None:
    remote, code = _remote_list(
        {
            "limit": 200,
            "offset": 0,
            "email_solicitante": email,
        },
        email_solicitante=email,
    )
    if code >= 400 or not remote.get("ok"):
        return None
    hidden = _archived_ids()
    for it in remote.get("items") or []:
        if not isinstance(it, dict):
            continue
        if _item_id(it) in hidden:
            continue
        _copy_casos_counts(it)
        _annotate_tipo(it)
        if _same_open_case(body, it):
            return _match_public(it)
    return None


def _duplicate_open_blocked(match: dict[str, Any]) -> tuple[dict[str, Any], int]:
    status = str(match.get("status") or "").upper()
    if status == "INAPTO":
        error = "Já existe uma solicitação semelhante feita nas últimas 24h."
    else:
        status_label = str(match.get("status_label") or status).strip()
        where = {
            "RECEBIDO": "na fila",
            "PROCESSANDO": "em processamento",
            "EM_TRATAMENTO": "em tratamento",
            "ANALISE_IA": "em análise da IA",
            "AGUARDAR_CONCLUSAO_JURIDICA": "aguardando conclusão jurídica",
            "INCIDENTES_EM_PROCESSAMENTO": "com incidentes em processamento",
            "AGUARDANDO_IMPUGNACAO": "aguardando impugnação",
            "AGUARDANDO_ANALISE_CALCULO": "aguardando análise do cálculo",
        }.get(status, "já registrada")
        error = (
            "Já existe uma solicitação igual "
            f"{where}"
            + (f" ({status_label})" if status_label else "")
            + ". Confirme se deseja enviar outra requisição igual."
        )
    return (
        {
            "ok": False,
            "code": "duplicate_open",
            "error": error,
            "match": match,
        },
        409,
    )


def criar(data: dict[str, Any], user: dict | None) -> tuple[dict[str, Any], int]:
    if not is_configured():
        return _not_configured()
    body, err = build_create_payload(data if isinstance(data, dict) else {}, user)
    if err or body is None:
        return {"ok": False, "error": err or "Payload inválido."}, 400
    email = str(body.get("email_solicitante") or "")
    confirm_dup = _as_bool(
        data.get("confirmar_duplicado")
        if data.get("confirmar_duplicado") is not None
        else data.get("force")
    )
    if not confirm_dup:
        match = _find_open_duplicate(body, email)
        if match:
            return _duplicate_open_blocked(match)
    wait = _claim_create_slot(email)
    if wait:
        return _cooldown_blocked(wait)
    success = False
    try:
        try:
            response = requests.post(
                f"{api_base()}/solicitacoes",
                json=body,
                headers=_headers(body.get("email_solicitante")),
                timeout=_timeout(),
                verify=_verify_ssl(),
            )
        except Exception as exc:  # noqa: BLE001
            return _request_error(exc)
        if response.status_code >= 400:
            return _map_http_error(response)
        try:
            payload = response.json()
        except Exception:
            payload = {}
        if not isinstance(payload, dict):
            payload = {"result": payload}
        out = dict(payload)
        out["ok"] = True
        out["cooldown_seconds"] = create_cooldown_seconds()
        _copy_casos_counts(out)
        _annotate_tipo(out)
        remember_statuses(out)
        _redact_sensitive_fields(out, user)
        success = True
        return out, int(response.status_code or 202)
    finally:
        _finish_create_slot(email, success=success)


def _listar_arquivados(
    *,
    q: str,
    limit: int,
    offset: int,
    email_solicitante: str | None = None,
    tipo: str = "",
    user: dict | None = None,
) -> tuple[dict[str, Any], int]:
    like = f"%{q}%" if q else None
    email = (email_solicitante or "").strip().lower() or None
    where: list[str] = []
    params: list[Any] = []
    if email:
        where.append(
            "LOWER(JSON_UNQUOTE(JSON_EXTRACT(snapshot, '$.email_solicitante'))) = %s"
        )
        params.append(email)
    if like:
        where.append("(sol_id LIKE %s OR CAST(snapshot AS CHAR) LIKE %s)")
        params.extend([like, like])
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    with auth_connection() as conn:
        cur = auth_cursor(conn)
        _ensure_archive_table(cur)
        conn.commit()
        cur.execute(
            f"""
            SELECT sol_id, archived_by, archived_at, snapshot
            FROM plataforma_solicitacao_arquivo
            {where_sql}
            ORDER BY archived_at DESC
            """,
            params,
        )
        rows = cur.fetchall() or []
    items: list[dict[str, Any]] = []
    for row in rows:
        item = _parse_snapshot(row.get("snapshot"))
        item["id"] = str(row.get("sol_id") or item.get("id") or "")
        item["arquivado"] = True
        archived_at = row.get("archived_at")
        if isinstance(archived_at, datetime):
            item["arquivado_em"] = archived_at.isoformat()
        elif archived_at:
            item["arquivado_em"] = str(archived_at)
        if row.get("archived_by"):
            item["arquivado_por"] = row.get("archived_by")
        _copy_casos_counts(item)
        _annotate_tipo(item)
        items.append(item)
    remember_statuses(items)
    items = _filter_by_tipo(items, tipo)
    total = len(items)
    page = items[offset : offset + limit]
    out = {
        "ok": True,
        "items": page,
        "total": total,
        "limit": limit,
        "offset": offset,
        "has_open": False,
        "arquivado": True,
        "status_options": _unique_item_statuses(items),
    }
    _redact_sensitive_fields(out, user)
    return out, 200


def listar(
    user: dict | None,
    *,
    status: str = "",
    q: str = "",
    limit: int = 50,
    offset: int = 0,
    updated_since: str = "",
    arquivado: bool = False,
    tipo: str = "",
    email_solicitante: str = "",
) -> tuple[dict[str, Any], int]:
    if not is_configured():
        return _not_configured()
    sol = solicitante_from_user(user)
    if not sol:
        return _profile_incomplete()

    limit = max(1, min(int(limit or 50), 200))
    offset = max(0, int(offset or 0))
    q = (q or "").strip()
    target_email = sol["email_solicitante"]
    as_email = (email_solicitante or "").strip().lower()
    viewing_other = False
    if as_email and as_email != target_email:
        if not user_can_view_other_solicitacoes(user):
            return {
                "ok": False,
                "error": "Sem permissão para ver solicitações de outros usuários.",
            }, 403
        target_email = as_email
        viewing_other = True

    if arquivado:
        if viewing_other:
            return {
                "ok": False,
                "error": "Arquivo de outros usuários não está disponível.",
            }, 400
        return _listar_arquivados(
            q=q,
            limit=limit,
            offset=offset,
            email_solicitante=sol["email_solicitante"],
            tipo=tipo,
            user=user,
        )

    params: dict[str, Any] = {
        "limit": 200,
        "offset": 0,
        "email_solicitante": target_email,
    }
    if q:
        params["q"] = q[:200]
    updated_since = (updated_since or "").strip()
    if updated_since:
        params["updated_since"] = updated_since[:64]

    remote, code = _remote_list(params, email_solicitante=target_email)
    if code >= 400 or not remote.get("ok"):
        return remote, code

    hidden = _archived_ids()
    visible = [
        it
        for it in (remote.get("items") or [])
        if isinstance(it, dict) and _item_id(it) and _item_id(it) not in hidden
    ]
    for it in visible:
        it["arquivado"] = False
        _copy_casos_counts(it)
        _annotate_tipo(it)
    remember_statuses(visible)
    status_options = _unique_item_statuses(visible)
    has_open = any(
        str(it.get("status_api") or it.get("status") or "").upper() in _OPEN_STATUSES
        for it in visible
    )
    visible = _filter_by_tipo(visible, tipo)
    want = (status or "").strip().upper()
    if want and want not in {"OPEN", "DONE", "ALL"}:
        visible = [
            it
            for it in visible
            if str(it.get("status") or "").strip().upper() == want
        ]
    total_n = len(visible)
    page = visible[offset : offset + limit]
    out = {
        "ok": True,
        "items": page,
        "total": total_n,
        "limit": limit,
        "offset": offset,
        "has_open": has_open,
        "arquivado": False,
        "status_options": status_options,
        "email_solicitante": target_email,
        "viewing_other": viewing_other,
    }
    _redact_sensitive_fields(out, user)
    return out, 200


def detalhe(
    sol_id: str,
    user: dict | None,
    *,
    redact_autos: bool = True,
    email_solicitante: str = "",
) -> tuple[dict[str, Any], int]:
    if not is_configured():
        return _not_configured()
    sol = solicitante_from_user(user)
    if not sol:
        return _profile_incomplete()
    sid = _clip(sol_id, 80)
    if not sid:
        return {"ok": False, "error": "Identificador inválido."}, 400
    header_email = sol["email_solicitante"]
    as_email = (email_solicitante or "").strip().lower()
    if as_email and as_email != header_email:
        if not user_can_view_other_solicitacoes(user):
            return {
                "ok": False,
                "error": "Sem permissão para ver solicitações de outros usuários.",
            }, 403
        header_email = as_email
    archived = _is_archived(sid)
    try:
        response = requests.get(
            f"{api_base()}/solicitacoes/{quote(sid, safe='')}",
            headers=_headers(header_email),
            timeout=_timeout(),
            verify=_verify_ssl(),
        )
    except Exception as exc:  # noqa: BLE001
        if archived:
            return _detalhe_from_archive(
                sid, user, redact_autos=redact_autos, email_solicitante=as_email
            )
        return _request_error(exc)
    if response.status_code >= 400:
        if archived:
            return _detalhe_from_archive(
                sid, user, redact_autos=redact_autos, email_solicitante=as_email
            )
        return _map_http_error(response)
    try:
        payload = response.json()
    except Exception:
        payload = {}
    if not isinstance(payload, dict):
        payload = {"result": payload}
    owner = str(payload.get("email_solicitante") or "").strip().lower()
    if owner and owner != sol["email_solicitante"]:
        if not user_can_view_other_solicitacoes(user):
            return {"ok": False, "error": "Solicitação não encontrada."}, 404
        if as_email and owner != as_email:
            return {"ok": False, "error": "Solicitação não encontrada."}, 404
    out = dict(payload)
    out["ok"] = True
    out["arquivado"] = archived
    _copy_casos_counts(out)
    _annotate_tipo(out)
    remember_statuses(out)
    if redact_autos:
        _redact_sensitive_fields(out, user)
    return out, 200


def _detalhe_from_archive(
    sol_id: str,
    user: dict | None = None,
    *,
    redact_autos: bool = True,
    email_solicitante: str = "",
) -> tuple[dict[str, Any], int]:
    with auth_connection() as conn:
        cur = auth_cursor(conn)
        _ensure_archive_table(cur)
        conn.commit()
        cur.execute(
            """
            SELECT sol_id, archived_by, archived_at, snapshot
            FROM plataforma_solicitacao_arquivo
            WHERE sol_id = %s
            """,
            (sol_id,),
        )
        row = cur.fetchone()
    if not row:
        return {"ok": False, "error": "Solicitação não encontrada."}, 404
    item = _parse_snapshot(row.get("snapshot"))
    owner = str(item.get("email_solicitante") or "").strip().lower()
    sol = solicitante_from_user(user) if user else None
    self_email = (sol or {}).get("email_solicitante") or ""
    as_email = (email_solicitante or "").strip().lower()
    if owner and self_email and owner != self_email:
        if not user_can_view_other_solicitacoes(user):
            return {"ok": False, "error": "Solicitação não encontrada."}, 404
        if as_email and owner != as_email:
            return {"ok": False, "error": "Solicitação não encontrada."}, 404
    item["id"] = str(row.get("sol_id") or sol_id)
    item["ok"] = True
    item["arquivado"] = True
    archived_at = row.get("archived_at")
    if isinstance(archived_at, datetime):
        item["arquivado_em"] = archived_at.isoformat()
    elif archived_at:
        item["arquivado_em"] = str(archived_at)
    if row.get("archived_by"):
        item["arquivado_por"] = row.get("archived_by")
    _copy_casos_counts(item)
    _annotate_tipo(item)
    remember_statuses(item)
    if redact_autos:
        _redact_sensitive_fields(item, user)
    return item, 200


def arquivar(sol_id: str, user: dict | None) -> tuple[dict[str, Any], int]:
    sid = _clip(sol_id, 80)
    if not sid:
        return {"ok": False, "error": "Identificador inválido."}, 400
    detail, code = detalhe(sid, user, redact_autos=False)
    if code >= 400 or not detail.get("ok"):
        return detail, code
    uid, uname = _actor(user)
    snap = _snapshot_from_item(detail)
    snap["id"] = sid
    with auth_connection() as conn:
        cur = auth_cursor(conn)
        _ensure_archive_table(cur)
        cur.execute(
            """
            INSERT INTO plataforma_solicitacao_arquivo
                (sol_id, archived_by_user_id, archived_by, snapshot)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                archived_by_user_id = VALUES(archived_by_user_id),
                archived_by = VALUES(archived_by),
                snapshot = VALUES(snapshot),
                archived_at = CURRENT_TIMESTAMP
            """,
            (sid, uid, uname or None, json.dumps(snap, ensure_ascii=False)),
        )
        conn.commit()
    return {"ok": True, "id": sid, "arquivado": True}, 200


def desarquivar(sol_id: str, user: dict | None) -> tuple[dict[str, Any], int]:
    sid = _clip(sol_id, 80)
    if not sid:
        return {"ok": False, "error": "Identificador inválido."}, 400
    sol = solicitante_from_user(user)
    if not sol:
        return _profile_incomplete()
    if not _is_archived(sid):
        return {"ok": True, "id": sid, "arquivado": False}, 200
    if user and user.get("role") != "admin":
        with auth_connection() as conn:
            cur = auth_cursor(conn)
            cur.execute(
                "SELECT snapshot FROM plataforma_solicitacao_arquivo WHERE sol_id = %s",
                (sid,),
            )
            row = cur.fetchone()
        snap = _parse_snapshot((row or {}).get("snapshot"))
        email = str(snap.get("email_solicitante") or "").strip().lower()
        if email and email != sol["email_solicitante"]:
            return {"ok": False, "error": "Solicitação não encontrada."}, 404
    with auth_connection() as conn:
        cur = auth_cursor(conn)
        _ensure_archive_table(cur)
        cur.execute(
            "DELETE FROM plataforma_solicitacao_arquivo WHERE sol_id = %s",
            (sid,),
        )
        conn.commit()
    return {"ok": True, "id": sid, "arquivado": False}, 200


def limpar_fila(user: dict | None) -> tuple[dict[str, Any], int]:
    if not is_configured():
        return _not_configured()
    if not user or user.get("role") != "admin":
        return {"ok": False, "error": "Apenas administradores podem limpar a fila."}, 403
    sol = solicitante_from_user(user)
    if not sol:
        return _profile_incomplete()
    email = sol["email_solicitante"]
    headers = _headers(email)
    headers["X-Admin-Email"] = email
    try:
        response = requests.post(
            f"{api_base()}/admin/fila/limpar",
            json={"admin_email": email},
            headers=headers,
            timeout=max(_timeout(), 120.0),
            verify=_verify_ssl(),
        )
    except Exception as exc:  # noqa: BLE001
        return _request_error(exc)
    if response.status_code >= 400:
        if response.status_code == 400:
            return {"ok": False, "error": "E-mail de administrador não informado."}, 400
        if response.status_code == 403:
            return {
                "ok": False,
                "error": "Este e-mail não está autorizado a limpar a fila na API remota.",
            }, 403
        if response.status_code == 503:
            return {
                "ok": False,
                "error": "A lista de administradores não está configurada na API remota.",
            }, 503
        return _map_http_error(response)
    try:
        payload = response.json()
    except Exception:
        payload = {}
    if not isinstance(payload, dict):
        payload = {"result": payload}
    out = dict(payload)
    out["ok"] = True
    try:
        deleted = int(out.get("deleted") or 0)
    except (TypeError, ValueError):
        deleted = 0
    if deleted == 1:
        out["mensagem"] = "Fila limpa: 1 solicitação removida."
    else:
        out["mensagem"] = f"Fila limpa: {deleted} solicitações removidas."
    return out, int(response.status_code or 200)


def _proxy_autos_file(
    sol_id: str,
    user: dict | None,
    remote_path: str,
    *,
    download: bool = False,
    not_found: str = "Autos ainda não disponíveis.",
    email_solicitante: str = "",
) -> tuple[dict[str, Any] | None, int, Any]:
    if not is_configured():
        err, code = _not_configured()
        return err, code, None
    if not user_can_view_autos(user):
        return (
            {"ok": False, "error": "Você não tem permissão para acessar os autos."},
            403,
            None,
        )
    sol = solicitante_from_user(user)
    if not sol:
        err, code = _profile_incomplete()
        return err, code, None
    sid = _clip(sol_id, 80)
    if not sid:
        return {"ok": False, "error": "Identificador inválido."}, 400, None
    header_email = sol["email_solicitante"]
    as_email = (email_solicitante or "").strip().lower()
    if as_email and as_email != header_email:
        if not user_can_view_other_solicitacoes(user):
            return (
                {
                    "ok": False,
                    "error": "Sem permissão para ver solicitações de outros usuários.",
                },
                403,
                None,
            )
        header_email = as_email
    params: dict[str, str] = {}
    if download:
        params["download"] = "1"
    try:
        response = requests.get(
            f"{api_base()}{remote_path}",
            params=params or None,
            headers=_headers(header_email),
            timeout=max(_timeout(), 180.0),
            verify=_verify_ssl(),
            stream=True,
        )
    except Exception as exc:  # noqa: BLE001
        err, code = _request_error(exc)
        return err, code, None
    if response.status_code == 410:
        response.close()
        return (
            {
                "ok": False,
                "code": "expired",
                "error": "Documentos expirados. Solicite reprocessamento se necessário.",
            },
            410,
            None,
        )
    if response.status_code == 404:
        response.close()
        return {"ok": False, "error": not_found}, 404, None
    if response.status_code >= 400:
        mapped, code = _map_http_error(response)
        response.close()
        return mapped, code, None
    return None, int(response.status_code or 200), response


def baixar_autos(
    sol_id: str,
    user: dict | None,
    *,
    indice: int | None = None,
    download: bool = False,
    email_solicitante: str = "",
) -> tuple[dict[str, Any] | None, int, Any]:
    """Proxy do PDF de autos. Sucesso: (None, status, requests.Response). Erro: (dict, code, None)."""
    sid = _clip(sol_id, 80)
    if not sid:
        return {"ok": False, "error": "Identificador inválido."}, 400, None
    if indice is not None:
        try:
            indice = int(indice)
        except (TypeError, ValueError):
            return {"ok": False, "error": "Índice de auto inválido."}, 400, None
        if indice < 0 or indice > 9999:
            return {"ok": False, "error": "Índice de auto inválido."}, 400, None
        remote_path = f"/solicitacoes/{quote(sid, safe='')}/autos/{indice}"
    else:
        remote_path = f"/solicitacoes/{quote(sid, safe='')}/autos"
    return _proxy_autos_file(
        sid,
        user,
        remote_path,
        download=download,
        email_solicitante=email_solicitante,
    )


def baixar_autos_texto(
    sol_id: str,
    user: dict | None,
    *,
    tipo: str,
    download: bool = False,
    email_solicitante: str = "",
) -> tuple[dict[str, Any] | None, int, Any]:
    """Proxy dos TXTs de autos (OCR completo / filtrado pelo credor)."""
    kind = str(tipo or "").strip().lower()
    if kind not in _AUTOS_TEXTO_TITULOS:
        return {"ok": False, "error": "Tipo de texto inválido."}, 400, None
    sid = _clip(sol_id, 80)
    if not sid:
        return {"ok": False, "error": "Identificador inválido."}, 400, None
    remote_path = f"/solicitacoes/{quote(sid, safe='')}/autos/texto/{kind}"
    return _proxy_autos_file(
        sid,
        user,
        remote_path,
        download=download,
        not_found="Textos ainda não disponíveis.",
        email_solicitante=email_solicitante,
    )
