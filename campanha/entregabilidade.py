"""Prazo de reenvio, descadastro, eventos do Elastic Email e ritmo do disparo.

O link de descadastro é um token HMAC do e-mail. O webhook só grava evento
quando o token da URL confere com CAMPANHA_WEBHOOK_TOKEN.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import math
import os
import urllib.parse
from datetime import date, timedelta
from typing import Any, Callable

_RODAPE_HTML = (
    '<p style="margin-top:28px;font-size:12px;color:#666666;">'
    "Se não quiser mais receber estas mensagens, "
    '<a href="{{unsubscribe_url}}">descadastre-se</a>.'
    "</p>"
)
_RODAPE_TEXTO = "\nSe não quiser mais receber estas mensagens, acesse: {{unsubscribe_url}}\n"

_STATUS_EVENTO = {
    "open": "open",
    "opened": "open",
    "click": "click",
    "clicked": "click",
    "unsubscribe": "unsubscribe",
    "unsubscribed": "unsubscribe",
    "complaint": "complaint",
    "complaints": "complaint",
    "abuse": "complaint",
    "abusereport": "complaint",
    "abuse_report": "complaint",
    "spam": "complaint",
    "bounce": "bounce",
    "bounced": "bounce",
    "hardbounce": "bounce",
    "hard_bounce": "bounce",
    "error": "bounce",
}

_MOTIVO_BLACKLIST = {
    "unsubscribe": "Descadastro campanha",
    "complaint": "Reclamacao de spam",
    "bounce": "Devolucao de e-mail",
}


_ANCORA_SEMANA = date(2026, 1, 5)


def prazo_envio_segundos() -> int:
    bruto = (os.getenv("CAMPANHA_PRAZO_MINUTOS") or "20").strip()
    try:
        minutos = int(bruto)
    except ValueError:
        minutos = 20
    return max(1, minutos) * 60


def paralelismo_para_prazo(
    quantidade: int,
    segundos_por_envio: float = 1.0,
    prazo: float | None = None,
) -> int:
    """Envios simultâneos para a quantidade caber no prazo, qualquer que seja o tamanho da lista."""
    prazo_s = float(prazo_envio_segundos() if prazo is None else prazo)
    qtd = max(1, int(quantidade))
    latencia = max(0.05, float(segundos_por_envio))
    if prazo_s <= 0:
        return qtd
    if qtd * latencia <= prazo_s:
        return 1
    return max(1, math.ceil((qtd * latencia) / prazo_s))


def selecionar_par_da_semana(itens: list, chave: Callable[[Any], str]) -> tuple[list, dict]:
    """Usa um par por semana, na ordem estável da chave. Na segunda-feira entra o próximo par."""
    ordenados = sorted(itens, key=lambda item: (chave(item) or "").lower())
    if len(ordenados) <= 2:
        pares = [ordenados] if ordenados else []
    else:
        pares = [ordenados[i:i + 2] for i in range(0, len(ordenados), 2)]
    if not pares:
        return [], {"indice": 0, "total_pares": 0, "inicio": "", "fim": "", "chaves": []}
    semana = (date.today() - _ANCORA_SEMANA).days // 7
    if semana < 0:
        semana = 0
    indice = semana % len(pares)
    inicio = _ANCORA_SEMANA + timedelta(days=semana * 7)
    fim = inicio + timedelta(days=6)
    escolhidos = pares[indice]
    return escolhidos, {
        "indice": indice + 1,
        "total_pares": len(pares),
        "inicio": inicio.isoformat(),
        "fim": fim.isoformat(),
        "chaves": [chave(item) for item in escolhidos],
    }


def base_publica() -> str:
    bruto = (os.getenv("CAMPANHA_PUBLIC_BASE_URL") or "http://68.183.23.91:8000").strip()
    return bruto.rstrip("/")


def _segredo_link() -> bytes:
    segredo = (os.getenv("CAMPANHA_LINK_SECRET") or os.getenv("SECRET_KEY") or "").strip()
    if not segredo:
        raise RuntimeError("CAMPANHA_LINK_SECRET não configurado.")
    return segredo.encode("utf-8")


def _assinatura(email_norm: str) -> str:
    return hmac.new(_segredo_link(), email_norm.encode("utf-8"), hashlib.sha256).hexdigest()[:32]


def token_descadastro(email: str) -> str:
    norm = (email or "").strip().upper()
    bruto = f"{norm}|{_assinatura(norm)}".encode("utf-8")
    return base64.urlsafe_b64encode(bruto).decode("ascii").rstrip("=")


def email_do_token(token: str) -> str | None:
    if not token:
        return None
    try:
        padded = token + ("=" * (-len(token) % 4))
        bruto = base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8")
        email, assinatura = bruto.rsplit("|", 1)
    except (ValueError, UnicodeError):
        return None
    email = email.strip().upper()
    if not email or "@" not in email:
        return None
    esperado = _assinatura(email)
    if not hmac.compare_digest(assinatura, esperado):
        return None
    return email


def url_descadastro(email: str) -> str:
    return f"{base_publica()}/campanha/descadastrar/{token_descadastro(email)}"


def aplicar_rodape(html: str, texto: str, url: str) -> tuple[str, str]:
    html = html or ""
    texto = texto or ""
    if "descadastre" not in html.lower():
        bloco = _RODAPE_HTML.replace("{{unsubscribe_url}}", url)
        if "</body>" in html.lower():
            idx = html.lower().rfind("</body>")
            html = html[:idx] + bloco + html[idx:]
        else:
            html = html + bloco
    else:
        html = html.replace("{{unsubscribe_url}}", url)
    if "descadastre" not in texto.lower():
        texto = texto + _RODAPE_TEXTO.replace("{{unsubscribe_url}}", url)
    else:
        texto = texto.replace("{{unsubscribe_url}}", url)
    return html, texto


def headers_descadastro(url: str, campaign_id: str, idempotency_key: str) -> dict[str, str]:
    return {
        "X-Campaign-Id": campaign_id,
        "X-Idempotency-Key": idempotency_key,
        "List-Unsubscribe": f"<{url}>",
        "List-Unsubscribe-Post": "List-Unsubscribe=One-Click",
    }


def garantir_esquema_campanha(conn) -> None:
    """Cria as colunas usadas pelo prazo, pelas aberturas e pelas recusas."""
    from campanha.core import _ensure_emails_campanha_columns_mysql

    _ensure_emails_campanha_columns_mysql(conn)
    cur = conn.cursor()
    try:
        extras = [
            ("reenvio_skip", "INT NOT NULL DEFAULT 0"),
            ("aberturas", "INT NOT NULL DEFAULT 0"),
            ("recusas", "INT NOT NULL DEFAULT 0"),
            ("bounces", "INT NOT NULL DEFAULT 0"),
        ]
        for nome, tipo in extras:
            cur.execute(
                """
                SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE()
                  AND TABLE_NAME = 'campanha_disparos'
                  AND COLUMN_NAME = %s
                """,
                (nome,),
            )
            if cur.fetchone()[0] == 0:
                cur.execute(f"ALTER TABLE campanha_disparos ADD COLUMN `{nome}` {tipo}")
        conn.commit()
    finally:
        cur.close()


def emails_recebidos_na_janela(conn, dias: int = 30) -> set[str]:
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT DISTINCT UPPER(TRIM(email))
            FROM emails
            WHERE campanha_disparo_ultimo_sucesso IS NOT NULL
              AND campanha_disparo_ultimo_sucesso >= (NOW() - INTERVAL %s DAY)
              AND email IS NOT NULL
              AND TRIM(email) <> ''
            """,
            (int(dias),),
        )
        return {row[0] for row in cur.fetchall() if row and row[0]}
    finally:
        cur.close()


def token_webhook_confere(apresentado: str | None) -> bool:
    esperado = (os.getenv("CAMPANHA_WEBHOOK_TOKEN") or "").strip()
    if not esperado or not apresentado:
        return False
    return hmac.compare_digest(str(apresentado).strip(), esperado)


def classificar_evento(payload: dict) -> tuple[str, str] | None:
    """Devolve (open|click|unsubscribe|complaint|bounce, email) ou None."""
    if not isinstance(payload, dict):
        return None
    plano = {str(chave).lower(): valor for chave, valor in payload.items()}
    status = ""
    for chave in ("status", "eventtype", "event", "type", "notificationtype"):
        if plano.get(chave):
            status = str(plano[chave]).strip().lower().replace(" ", "").replace("-", "")
            break
    tipo = _STATUS_EVENTO.get(status)
    if not tipo:
        return None
    email = ""
    for chave in ("to", "recipient", "email", "recipientemail", "address"):
        if plano.get(chave):
            email = str(plano[chave]).strip()
            break
    if "<" in email and ">" in email:
        email = email.split("<", 1)[1].split(">", 1)[0].strip()
    if not email or "@" not in email:
        return None
    return tipo, email.strip().upper()


def _campanha_do_email(cur, email_norm: str) -> str | None:
    cur.execute(
        """
        SELECT campanha_disparo_campaign_id
        FROM emails
        WHERE email IS NOT NULL AND UPPER(TRIM(email)) = %s
          AND campanha_disparo_campaign_id IS NOT NULL
        ORDER BY campanha_disparo_ultimo_sucesso DESC, campanha_disparo_ultimo DESC
        LIMIT 1
        """,
        (email_norm[:300],),
    )
    row = cur.fetchone()
    if not row or not row[0]:
        return None
    return str(row[0])


def registrar_evento(conn, tipo: str, email_norm: str) -> None:
    """Conta abertura, clique, descadastro, reclamação ou devolução e bloqueia o contato."""
    cur = conn.cursor()
    try:
        if tipo == "open":
            cur.execute(
                """
                UPDATE emails
                   SET campanha_aberturas = campanha_aberturas + 1,
                       campanha_ultima_abertura = CURRENT_TIMESTAMP(3)
                 WHERE email IS NOT NULL AND UPPER(TRIM(email)) = %s
                """,
                (email_norm[:300],),
            )
            campanha = _campanha_do_email(cur, email_norm)
            if campanha:
                cur.execute(
                    "UPDATE campanha_disparos SET aberturas = aberturas + 1 WHERE campaign_id = %s",
                    (campanha,),
                )
        elif tipo == "click":
            cur.execute(
                """
                UPDATE emails
                   SET campanha_cliques = campanha_cliques + 1
                 WHERE email IS NOT NULL AND UPPER(TRIM(email)) = %s
                """,
                (email_norm[:300],),
            )
        elif tipo in ("unsubscribe", "complaint", "bounce"):
            coluna = {
                "unsubscribe": "campanha_descadastro",
                "complaint": "campanha_reclamacao",
                "bounce": "campanha_bounce",
            }[tipo]
            cur.execute(
                f"""
                SELECT COUNT(*) FROM emails
                 WHERE email IS NOT NULL AND UPPER(TRIM(email)) = %s
                   AND `{coluna}` IS NULL
                """,
                (email_norm[:300],),
            )
            primeira = int(cur.fetchone()[0] or 0) > 0
            cur.execute(
                f"""
                UPDATE emails
                   SET `{coluna}` = COALESCE(`{coluna}`, CURRENT_TIMESTAMP(3))
                 WHERE email IS NOT NULL AND UPPER(TRIM(email)) = %s
                """,
                (email_norm[:300],),
            )
            if primeira:
                campanha = _campanha_do_email(cur, email_norm)
                contador = "bounces" if tipo == "bounce" else "recusas"
                if campanha:
                    cur.execute(
                        f"UPDATE campanha_disparos SET `{contador}` = `{contador}` + 1 WHERE campaign_id = %s",
                        (campanha,),
                    )
            cur.execute(
                """
                INSERT INTO blacklist (tipo, valor, motivo, ativo)
                VALUES ('EMAIL', %s, %s, 1)
                ON DUPLICATE KEY UPDATE ativo = 1
                """,
                (email_norm[:300], _MOTIVO_BLACKLIST[tipo]),
            )
        conn.commit()
    finally:
        cur.close()


def ler_payload_notificacao(corpo: bytes, tipo_conteudo: str, formulario: dict | None) -> dict:
    if formulario:
        return dict(formulario)
    texto = (corpo or b"").decode("utf-8", errors="replace").strip()
    if not texto:
        return {}
    if "json" in (tipo_conteudo or "").lower() or texto[:1] in "{[":
        try:
            dado = json.loads(texto)
        except json.JSONDecodeError:
            dado = None
        if isinstance(dado, dict):
            return dado
        if isinstance(dado, list) and dado and isinstance(dado[0], dict):
            return dado[0]
    return dict(urllib.parse.parse_qsl(texto))


def registrar_webhook_elasticemail(url: str) -> dict[str, Any]:
    """Cadastra a URL de notificação. A chave v4 não tem permissão de webhook."""
    import urllib.request
    import urllib.error

    chave = (os.getenv("ELASTICEMAIL_API_KEY") or "").strip()
    if not chave or not url:
        return {"ok": False, "motivo": "sem_chave_ou_url"}
    consulta = urllib.parse.urlencode(
        {
            "apikey": chave,
            "name": "centro-campanha",
            "webNotificationUrl": url,
            "notificationForAbuseReport": "true",
            "notificationForClicked": "true",
            "notificationForError": "true",
            "notificationForOpened": "true",
            "notificationForUnsubscribed": "true",
            "notificationForSent": "false",
            "notifyOncePerEmail": "false",
        }
    )
    req = urllib.request.Request(
        "https://api.elasticemail.com/v2/account/addwebhook?" + consulta,
        method="GET",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            bruto = resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        detalhe = exc.read().decode("utf-8", errors="replace")[:300]
        return {"ok": False, "motivo": "http", "status": exc.code, "detalhe": _redigir(detalhe)}
    try:
        dado = json.loads(bruto)
    except json.JSONDecodeError:
        dado = {"raw": bruto[:300]}
    sucesso = bool(isinstance(dado, dict) and dado.get("success"))
    erro = ""
    if isinstance(dado, dict):
        erro = _redigir(str(dado.get("error") or ""))
    return {"ok": sucesso, "motivo": "cadastrado" if sucesso else "recusado", "erro": erro}


def _redigir(texto: str) -> str:
    for marca in ("token=", "apikey=", "apiKey="):
        while marca in texto:
            antes, _, resto = texto.partition(marca)
            corte = resto.find("&")
            resto = "***" + (resto[corte:] if corte >= 0 else "")
            texto = antes + marca + resto
            break
    return texto[:300]
