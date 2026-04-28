# -*- coding: utf-8 -*-
"""
API HTTP **interna** (localhost) para actualização de cálculo por ``id_precainfosnew``.

- ``GET /health`` — verificação (systemd, balanceador)
- ``POST /atualizar`` — corpo JSON ``{{"id_precainfosnew": <int>}}`` (ou ``"id"``)

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
import threading
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

_job_lock = threading.Lock()


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


def _run_atualizacao(prec_id: int) -> dict[str, Any]:
    from datetime import datetime

    from manager.manager import Manager

    m = Manager(datetime.now())
    return m.run_atualizacao_calculo(prec_id)


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
        if self.path in ("/health", "/healthz"):
            self._send(200, {"ok": True, "service": "atualizacao_calculo"})
            return
        self._send(404, {"ok": False, "error": "not found"})

    def do_POST(self) -> None:
        if self.path != "/atualizar" and self.path != "/atualizar/":
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

        if not _job_lock.acquire(blocking=False):
            self._send(
                409,
                {
                    "ok": False,
                    "error": "Já existe uma actualização de cálculo em curso. Tente de novo após o fim.",
                },
            )
            return
        try:
            out = _run_atualizacao(prec_id)
        except Exception as e:
            traceback.print_exc()
            self._send(500, {"ok": False, "error": str(e)})
            return
        finally:
            _job_lock.release()

        if not out.get("ok"):
            self._send(400, out)
        else:
            self._send(200, out)


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
        f"(POST /atualizar, GET /health)",
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
