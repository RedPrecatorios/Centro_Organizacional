# -*- coding: utf-8 -*-
"""
API HTTP **interna** (localhost) para actualização de cálculo por ``id_precainfosnew``.

- ``GET /health`` — verificação (systemd, balanceador)
- ``GET /fila`` — estado da fila (operador actual, casos à espera, média e estimativa em segundos)
- ``POST /atualizar`` — corpo JSON ``{"id_precainfosnew": <int>, "feito_por": "<opcional>"}`` (ou ``"id"``)
  Pedidos entram na fila e aguardam a vez (um cálculo de cada vez).
  Se ``feito_por`` vier vazio ou omitido, assume-se **automação** (robô / chamada sem utilizador).

Ambiente (``.env`` na mesma pasta que este ficheiro ou no cwd):
- ``CALCULO_ATUALIZACAO_API_HOST`` (padrão ``127.0.0.1``)
- ``CALCULO_ATUALIZACAO_API_PORT`` (padrão ``5099``)
- ``CALCULO_ATUALIZACAO_API_KEY`` (opcional; se definido, exige cabeçalho ``X-API-Key``)

Arranque: ``python3 api_atualizacao_calculo.py`` ou serviço systemd (ver ``deploy/``).
"""
from __future__ import annotations

import json
import os
import sys
import traceback
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from dotenv import load_dotenv

load_dotenv(_ROOT / ".env")
load_dotenv()

from calculo_job_queue import (
    configure_runner,
    enqueue,
    get_fila_status,
    get_prec_id_status,
    submit_and_wait,
)


def _read_json_body(handler: BaseHTTPRequestHandler) -> dict[str, Any] | None:
    try:
        n = int(handler.headers.get("Content-Length", "0") or "0")
    except ValueError:
        n = 0
    if n <= 0:
        return None
    raw = handler.rfile.read(n)
    if not raw:
        return None
    try:
        return json.loads(raw.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None


def _json_truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return value != 0
    s = str(value or "").strip().lower()
    return s in ("1", "true", "yes", "sim", "on")


def _parse_honorarios_percent(value: Any) -> float | None:
    if value is None or value == "":
        return None
    text = str(value).strip().replace("%", "").replace(" ", "").replace(",", ".")
    if not text:
        return None
    try:
        number = float(text)
    except ValueError:
        return None
    if number < 0:
        return None
    return round(min(number, 100.0), 2)


def _run_atualizacao(
    prec_id: int,
    feito_por: str | None = None,
    prioridade: bool = False,
    form_payload: dict[str, Any] | None = None,
    percentual_honorarios: float | None = None,
) -> dict[str, Any]:
    from datetime import datetime

    from manager.manager import Manager

    m = Manager(datetime.now())
    if form_payload:
        if percentual_honorarios is not None:
            form_payload = dict(form_payload)
            form_payload["Percentual_Honorarios"] = percentual_honorarios
        return m.run_atualizacao_calculo_from_form(
            form_payload, feito_por=feito_por, prioridade=bool(prioridade)
        )
    return m.run_atualizacao_calculo(
        prec_id,
        feito_por=feito_por,
        prioridade=bool(prioridade),
        percentual_honorarios=percentual_honorarios,
    )


configure_runner(_run_atualizacao)


class _Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, format: str, *args) -> None:
        print(
            f"[api_atualizacao_calculo] {self.client_address[0]} - {format % args}",
            file=sys.stderr,
        )

    def _send(self, code: int, body: dict[str, Any]) -> None:
        b = json.dumps(body, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(b)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(b)

    def _check_api_key(self) -> bool:
        expected = (os.getenv("CALCULO_ATUALIZACAO_API_KEY") or "").strip()
        if not expected:
            return True
        got = (self.headers.get("X-API-Key") or self.headers.get("X-Api-Key") or "").strip()
        return got == expected

    def do_GET(self) -> None:
        path = (self.path or "").split("?", 1)[0].rstrip("/") or "/"
        if path in ("/health", "/healthz"):
            self._send(200, {"ok": True, "service": "atualizacao_calculo"})
            return
        if path in ("/fila", "/status"):
            if not self._check_api_key():
                self._send(403, {"ok": False, "error": "X-API-Key inválida ou em falta."})
                return
            fila = get_fila_status()
            self._send(200, {"ok": True, "fila": fila})
            return
        if path.startswith("/status/"):
            if not self._check_api_key():
                self._send(403, {"ok": False, "error": "X-API-Key inválida ou em falta."})
                return
            try:
                prec_id = int(path.split("/", 2)[2])
            except (IndexError, ValueError):
                self._send(400, {"ok": False, "error": "id_precainfosnew inválido na URL."})
                return
            self._send(200, get_prec_id_status(prec_id))
            return
        self._send(404, {"ok": False, "error": "not found"})

    def do_POST(self) -> None:
        path = (self.path or "").split("?", 1)[0].rstrip("/") or "/"
        if path != "/atualizar":
            self._send(404, {"ok": False, "error": "not found"})
            return
        if not self._check_api_key():
            self._send(403, {"ok": False, "error": "X-API-Key inválida ou em falta."})
            return

        data = _read_json_body(self)
        if not isinstance(data, dict):
            self._send(400, {"ok": False, "error": "JSON inválido ou vazio."})
            return
        pid = data.get("id_precainfosnew")
        if pid is None and data.get("id") is not None:
            pid = data.get("id")
        if pid is None:
            self._send(400, {"ok": False, "error": "Obrigatório: id_precainfosnew (número)."})
            return
        try:
            prec_id = int(pid)
        except (TypeError, ValueError):
            self._send(400, {"ok": False, "error": "id_precainfosnew inválido."})
            return

        raw_fp = data.get("feito_por")
        if raw_fp is not None and raw_fp != "" and not isinstance(raw_fp, str):
            raw_fp = str(raw_fp)
        feito_por = (raw_fp.strip()[:200] if isinstance(raw_fp, str) and raw_fp.strip() else None)

        try:
            timeout_raw = (os.getenv("CALCULO_ATUALIZACAO_API_TIMEOUT") or "600").strip()
            timeout = float(timeout_raw.replace(",", "."))
        except ValueError:
            timeout = 600.0
        if timeout < 60:
            timeout = 600.0

        wait_raw = data.get("wait", True)
        wait_sync = wait_raw not in (False, "false", "False", "0", 0)
        prioridade = _json_truthy(data.get("prioridade"))
        percentual_honorarios = _parse_honorarios_percent(
            data.get("percentual_honorarios")
            if data.get("percentual_honorarios") is not None
            else data.get("Percentual_Honorarios")
        )
        form_payload = data.get("form_payload")
        if form_payload is not None and not isinstance(form_payload, dict):
            self._send(400, {"ok": False, "error": "form_payload inválido."})
            return

        try:
            if wait_sync:
                out, fila_ao_entrar = submit_and_wait(
                    prec_id,
                    feito_por=feito_por,
                    timeout=timeout,
                    prioridade=prioridade,
                    form_payload=form_payload,
                    percentual_honorarios=percentual_honorarios,
                )
                if fila_ao_entrar and fila_ao_entrar.get("em_execucao"):
                    out = dict(out)
                    out["fila_ao_entrar"] = fila_ao_entrar
                if not out.get("ok"):
                    self._send(400, out)
                else:
                    self._send(200, out)
                return
            out, _fila = enqueue(
                prec_id,
                feito_por=feito_por,
                prioridade=prioridade,
                form_payload=form_payload,
                percentual_honorarios=percentual_honorarios,
            )
        except Exception as e:
            traceback.print_exc()
            self._send(500, {"ok": False, "error": str(e)})
            return

        self._send(202, out)


def main() -> None:
    host = (os.getenv("CALCULO_ATUALIZACAO_API_HOST") or "127.0.0.1").strip()
    try:
        port = int((os.getenv("CALCULO_ATUALIZACAO_API_PORT") or "5099").strip())
    except ValueError:
        port = 5099
    if port < 1 or port > 65535:
        port = 5099
    print(
        f"[api_atualizacao_calculo] a escutar em http://{host}:{port}/ "
        f"(POST /atualizar, GET /health, GET /fila, GET /status/<id>)",
        file=sys.stderr,
    )
    httpd = ThreadingHTTPServer((host, port), _Handler)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()


if __name__ == "__main__":
    main()
