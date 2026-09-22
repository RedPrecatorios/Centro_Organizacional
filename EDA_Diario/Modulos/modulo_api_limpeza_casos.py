"""
API de limpeza pré-Etapa 0: busca casos a remover antes de enriquecer contatos.

Configuração (env):
  EDA_LIMPEZA_API_URL      — URL completa do endpoint (obrigatória para ativar)
  EDA_LIMPEZA_API_TOKEN    — token enviado na autenticação
  EDA_LIMPEZA_API_TIMEOUT  — segundos (padrão 60)
  EDA_LIMPEZA_API_HEADER   — nome do header (padrão Authorization)
  EDA_LIMPEZA_API_AUTH     — formato: bearer (padrão) | raw | query
  EDA_LIMPEZA_API_ENABLED  — 0/false desliga mesmo com URL (padrão: ligado se URL+token)

A máquina remota ainda está em construção: com URL/token ausentes a Etapa 0
apenas registra aviso e segue sem filtrar.
"""
from __future__ import annotations

import json
import os
import re
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

import pandas as pd

from modulo_blacklist import _normalizar_cpf_cmp


class LimpezaApiErro(Exception):
    """Falha ao consultar ou interpretar a API de limpeza."""


def _env(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return default if v is None else str(v).strip()


# Modelos EDA Diário em que a limpeza via API se aplica.
MODELOS_LIMPEZA_API = frozenset({"prc_tjsp", "prc_cmp"})


def limpeza_api_aplica_ao_modelo(modelo: str | None) -> bool:
    return (modelo or "").strip().lower() in MODELOS_LIMPEZA_API


def limpeza_api_habilitada() -> bool:
    flag = _env("EDA_LIMPEZA_API_ENABLED", "").lower()
    if flag in ("0", "false", "no", "off", "nao", "não"):
        return False
    if flag in ("1", "true", "yes", "on", "sim"):
        return bool(_env("EDA_LIMPEZA_API_URL") and _env("EDA_LIMPEZA_API_TOKEN"))
    # Padrão: ativa só se URL e token estiverem configurados
    return bool(_env("EDA_LIMPEZA_API_URL") and _env("EDA_LIMPEZA_API_TOKEN"))


def _normalizar_texto_cmp(valor: Any) -> str:
    if valor is None or (isinstance(valor, float) and pd.isna(valor)):
        return ""
    s = unicodedata.normalize("NFD", str(valor).strip())
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    s = re.sub(r"\s+", " ", s.upper()).strip()
    return "" if s in ("NAN", "NONE", "NULL") else s


def _normalizar_proc_inc(valor: Any) -> str:
    return _normalizar_texto_cmp(valor)


def chave_caso_limpeza(
    *,
    requerente: Any = "",
    cpf: Any = "",
    processo: Any = "",
    incidente: Any = "",
) -> tuple[str, str, str, str]:
    """Chave canônica para cruzar planilha × API."""
    return (
        _normalizar_texto_cmp(requerente),
        _normalizar_cpf_cmp(cpf),
        _normalizar_proc_inc(processo),
        _normalizar_proc_inc(incidente),
    )


def _pick(d: dict[str, Any], *aliases: str) -> Any:
    lower = {str(k).strip().lower(): v for k, v in d.items()}
    for a in aliases:
        if a.lower() in lower:
            return lower[a.lower()]
    return ""


def normalizar_caso_api(item: Any) -> tuple[str, str, str, str] | None:
    """Aceita dict (vários nomes de campo) ou lista/tupla [req, cpf, proc, inc]."""
    if item is None:
        return None
    if isinstance(item, (list, tuple)) and len(item) >= 4:
        return chave_caso_limpeza(
            requerente=item[0],
            cpf=item[1],
            processo=item[2],
            incidente=item[3],
        )
    if not isinstance(item, dict):
        return None
    return chave_caso_limpeza(
        requerente=_pick(
            item,
            "requerente",
            "Requerente",
            "nome",
            "NOME",
            "Nome",
        ),
        cpf=_pick(item, "cpf", "CPF", "cpf_cnpj", "CPF_CNPJ"),
        processo=_pick(
            item,
            "numero_de_processo",
            "Numero_de_Processo",
            "numero_processo",
            "processo",
            "Processo",
        ),
        incidente=_pick(
            item,
            "numero_do_incidente",
            "Numero_do_Incidente",
            "numero_incidente",
            "incidente",
            "Incidente",
        ),
    )


def extrair_lista_casos(payload: Any) -> list[tuple[str, str, str, str]]:
    """Extrai lista de casos de formatos JSON comuns."""
    bruto = payload
    if isinstance(payload, dict):
        for key in (
            "casos",
            "cases",
            "data",
            "items",
            "resultados",
            "results",
            "remover",
            "to_remove",
        ):
            if key in payload and isinstance(payload[key], list):
                bruto = payload[key]
                break
        else:
            # objeto único
            um = normalizar_caso_api(payload)
            return [um] if um and any(um) else []

    if not isinstance(bruto, list):
        raise LimpezaApiErro(
            f"Resposta da API de limpeza não é uma lista de casos (tipo={type(payload).__name__})."
        )

    out: list[tuple[str, str, str, str]] = []
    vistos: set[tuple[str, str, str, str]] = set()
    for item in bruto:
        chave = normalizar_caso_api(item)
        if not chave or not any(chave):
            continue
        if chave in vistos:
            continue
        vistos.add(chave)
        out.append(chave)
    return out


def _montar_request(url: str, token: str) -> urllib.request.Request:
    header_name = _env("EDA_LIMPEZA_API_HEADER", "Authorization") or "Authorization"
    # API atual: Authorization com token cru (sem Bearer) + POST
    auth_mode = _env("EDA_LIMPEZA_API_AUTH", "raw").lower() or "raw"
    method = (_env("EDA_LIMPEZA_API_METHOD", "POST") or "POST").upper()

    final_url = url
    headers = {
        "Accept": "application/json",
        "User-Agent": "EDA-Diario-Etapa0/1.0",
    }
    data = None
    if auth_mode == "query":
        sep = "&" if "?" in url else "?"
        final_url = f"{url}{sep}token={urllib.parse.quote(token)}"
    elif auth_mode == "bearer":
        headers[header_name] = f"Bearer {token}" if not token.lower().startswith("bearer ") else token
    else:
        # raw (padrão desta API)
        headers[header_name] = token

    if method in ("POST", "PUT", "PATCH"):
        # Corpo vazio: autenticação só no header
        data = b""
        headers.setdefault("Content-Length", "0")

    return urllib.request.Request(final_url, data=data, method=method, headers=headers)


def consultar_casos_para_remover(
    *,
    url: str | None = None,
    token: str | None = None,
    timeout: float | None = None,
) -> list[tuple[str, str, str, str]]:
    """
    Chama a API remota autenticando só com o token (POST + Authorization raw por padrão).
    Retorna lista de chaves (requerente, cpf, processo, incidente).
    """
    url = (url if url is not None else _env("EDA_LIMPEZA_API_URL")).strip()
    token = (token if token is not None else _env("EDA_LIMPEZA_API_TOKEN")).strip()
    if timeout is None:
        try:
            timeout = float(_env("EDA_LIMPEZA_API_TIMEOUT", "120") or "120")
        except ValueError:
            timeout = 120.0

    if not url:
        raise LimpezaApiErro("EDA_LIMPEZA_API_URL não configurada.")
    if not token:
        raise LimpezaApiErro("EDA_LIMPEZA_API_TOKEN não configurado.")

    req = _montar_request(url, token)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
            status = getattr(resp, "status", None) or resp.getcode()
    except urllib.error.HTTPError as exc:
        body = ""
        try:
            body = exc.read().decode("utf-8", errors="replace")[:500]
        except Exception:
            pass
        raise LimpezaApiErro(f"API limpeza HTTP {exc.code}: {body or exc.reason}") from exc
    except urllib.error.URLError as exc:
        raise LimpezaApiErro(f"API limpeza inacessível: {exc.reason}") from exc

    if status and int(status) >= 400:
        raise LimpezaApiErro(f"API limpeza retornou status {status}.")

    try:
        texto = raw.decode("utf-8")
        payload = json.loads(texto) if texto.strip() else []
    except json.JSONDecodeError as exc:
        raise LimpezaApiErro(f"API limpeza não retornou JSON válido: {exc}") from exc

    return extrair_lista_casos(payload)


def filtrar_planilha_remover_casos(
    df: pd.DataFrame,
    casos_remover: list[tuple[str, str, str, str]] | set[tuple[str, str, str, str]],
    *,
    col_requerente: str | None,
    col_cpf: str,
    col_processo: str | None,
    col_incidente: str | None,
) -> tuple[pd.DataFrame, int]:
    """
    Remove linhas da planilha que batem com a lista da API
    (Requerente + CPF + Número de Processo + Número do Incidente).
    """
    if df.empty or not casos_remover:
        return df.reset_index(drop=True), 0

    alvo = set(casos_remover)
    keep: list[bool] = []
    removidos = 0
    for _, row in df.iterrows():
        chave = chave_caso_limpeza(
            requerente=row.get(col_requerente) if col_requerente else "",
            cpf=row.get(col_cpf),
            processo=row.get(col_processo) if col_processo else "",
            incidente=row.get(col_incidente) if col_incidente else "",
        )
        if chave in alvo:
            keep.append(False)
            removidos += 1
        else:
            keep.append(True)

    out = df.loc[keep].reset_index(drop=True)
    return out, removidos


def aplicar_limpeza_api_na_planilha(
    df: pd.DataFrame,
    *,
    col_requerente: str | None,
    col_cpf: str,
    col_processo: str | None,
    col_incidente: str | None,
    modelo: str | None = None,
    falhar_se_erro: bool = False,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """
    Consulta a API (se habilitada e modelo elegível) e filtra a planilha.
    Só PRC TJSP e PRC CMP. Se desabilitada/sem config/modelo fora: df intacto.
    """
    stats: dict[str, Any] = {
        "habilitada": False,
        "consultada": False,
        "casos_api": 0,
        "linhas_removidas": 0,
        "aviso": "",
        "modelo": (modelo or "").strip().lower(),
    }
    if not limpeza_api_aplica_ao_modelo(modelo):
        stats["aviso"] = (
            f"Limpeza API não se aplica ao modelo {modelo!r} "
            f"(somente PRC TJSP e PRC CMP)."
        )
        print(f"     [limpeza API] {stats['aviso']}")
        return df, stats
    if not limpeza_api_habilitada():
        stats["aviso"] = "API de limpeza não configurada (EDA_LIMPEZA_API_URL / TOKEN) — pulando."
        print(f"     [limpeza API] {stats['aviso']}")
        return df, stats

    stats["habilitada"] = True
    try:
        casos = consultar_casos_para_remover()
        stats["consultada"] = True
        stats["casos_api"] = len(casos)
        print(f"     [limpeza API] {len(casos)} caso(s) recebidos para remoção.")
    except LimpezaApiErro as exc:
        stats["aviso"] = str(exc)
        print(f"     [limpeza API][AVISO] {exc}")
        if falhar_se_erro:
            raise
        return df, stats

    df_out, n_rem = filtrar_planilha_remover_casos(
        df,
        casos,
        col_requerente=col_requerente,
        col_cpf=col_cpf,
        col_processo=col_processo,
        col_incidente=col_incidente,
    )
    stats["linhas_removidas"] = n_rem
    print(
        f"     [limpeza API] Removidas {n_rem} linha(s) da planilha "
        f"({len(df)} → {len(df_out)}) antes do enriquecimento."
    )
    return df_out, stats
