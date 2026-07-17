#!/usr/bin/env python3
"""
Cliente HTTP standalone para a API do dashboard-backend (uso na cloud / plataforma).

Nao importa o pacote `api` nem o pipeline — depende so de `requests` + env.

Env (ou argumentos):
  TJSP_API_BASE_URL   ex.: http://127.0.0.1:8003
  TJSP_API_TOKEN      mesmo valor de API_TOKEN no servidor

  Exemplos:
  python clients/tjsp_api_client.py health
  python clients/tjsp_api_client.py search --nome "Heloisa Maria Fernandes Queiroz"
  python clients/tjsp_api_client.py search --cpf 12345678901 --wait
  python clients/tjsp_api_client.py search --processo "0017669-72.2021.8.26.0053" --nome "Jose Silva"
  python clients/tjsp_api_client.py status <job_id>
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from typing import Any

import requests


DEFAULT_BASE_URL = "http://127.0.0.1:8003"
DEFAULT_POLL_SECONDS = 5.0


class TjspApiClient:
    def __init__(
        self,
        base_url: str,
        token: str,
        *,
        timeout: float = 60.0,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.token = token.strip()
        self.timeout = timeout
        if not self.token:
            raise ValueError("Token ausente. Defina TJSP_API_TOKEN ou --token.")

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def health(self) -> dict[str, Any]:
        """Health e publico (sem token)."""
        response = requests.get(
            f"{self.base_url}/api/v1/health",
            timeout=self.timeout,
        )
        response.raise_for_status()
        return response.json()

    def create_search(
        self,
        *,
        nome: str | None = None,
        cpf: str | None = None,
        processo: str | None = None,
    ) -> dict[str, Any]:
        body: dict[str, str] = {}
        if nome and str(nome).strip():
            body["nome"] = str(nome).strip()
        if cpf and str(cpf).strip():
            body["cpf"] = str(cpf).strip()
        if processo and str(processo).strip():
            body["processo"] = str(processo).strip()
        if "cpf" in body:
            if len(body) != 1:
                raise ValueError("CPF deve ser informado sozinho.")
        elif "processo" in body:
            if "nome" not in body:
                raise ValueError(
                    "Ao pesquisar por processo, informe também --nome (filtro na capa)."
                )
            if len(body) != 2:
                raise ValueError("Use processo + nome (sem CPF).")
        elif "nome" not in body or len(body) != 1:
            raise ValueError("Informe --nome, --cpf, ou --processo + --nome.")
        response = requests.post(
            f"{self.base_url}/api/v1/searches",
            headers=self._headers(),
            json=body,
            timeout=self.timeout,
        )
        if response.status_code == 401:
            raise PermissionError("Token rejeitado pela API (401).")
        response.raise_for_status()
        return response.json()

    def get_search(self, job_id: str) -> dict[str, Any]:
        response = requests.get(
            f"{self.base_url}/api/v1/searches/{job_id}",
            headers=self._headers(),
            timeout=self.timeout,
        )
        if response.status_code == 401:
            raise PermissionError("Token rejeitado pela API (401).")
        if response.status_code == 404:
            raise LookupError(f"Job nao encontrado: {job_id}")
        response.raise_for_status()
        return response.json()

    def search_and_wait(
        self,
        *,
        nome: str | None = None,
        cpf: str | None = None,
        processo: str | None = None,
        poll_seconds: float = DEFAULT_POLL_SECONDS,
        max_wait_seconds: float = 3600.0,
    ) -> dict[str, Any]:
        created = self.create_search(nome=nome, cpf=cpf, processo=processo)
        job_id = created["job_id"]
        deadline = time.monotonic() + max_wait_seconds
        while True:
            payload = self.get_search(job_id)
            status = payload.get("status")
            if status in {"done", "failed"}:
                return payload
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"Timeout aguardando job {job_id} (ultimo status={status})"
                )
            time.sleep(poll_seconds)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Cliente da API TJSP dashboard-backend (cloud/plataforma)."
    )
    parser.add_argument(
        "--base-url",
        default=os.getenv("TJSP_API_BASE_URL", DEFAULT_BASE_URL),
        help="URL base da API (default: TJSP_API_BASE_URL ou http://127.0.0.1:8003)",
    )
    parser.add_argument(
        "--token",
        default=os.getenv("TJSP_API_TOKEN") or os.getenv("API_TOKEN", ""),
        help="Bearer token (default: TJSP_API_TOKEN ou API_TOKEN)",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("health", help="GET /api/v1/health (sem token)")

    p_search = sub.add_parser("search", help="POST /api/v1/searches")
    crit = p_search.add_mutually_exclusive_group(required=False)
    crit.add_argument("--nome", help="Nome da parte (ou filtro na capa com --processo)")
    crit.add_argument("--cpf", help="CPF (11 dígitos)")
    crit.add_argument("--processo", help="Número CNJ (exige também --nome)")
    p_search.add_argument(
        "nome_posicional",
        nargs="?",
        default=None,
        help="Compat: nome posicional (mesmo que --nome)",
    )
    p_search.add_argument(
        "--wait",
        action="store_true",
        help="Fica em poll ate done/failed",
    )
    p_search.add_argument(
        "--timeout",
        type=float,
        default=3600.0,
        help="Timeout total em segundos quando --wait (default 3600)",
    )
    p_search.add_argument(
        "--poll",
        type=float,
        default=DEFAULT_POLL_SECONDS,
        help="Intervalo de poll em segundos (default 5)",
    )

    p_status = sub.add_parser("status", help="GET /api/v1/searches/{job_id}")
    p_status.add_argument("job_id", help="ID do job")

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command == "health":
        response = requests.get(
            f"{args.base_url.rstrip('/')}/api/v1/health",
            timeout=30,
        )
        response.raise_for_status()
        print(json.dumps(response.json(), ensure_ascii=False, indent=2))
        return 0

    client = TjspApiClient(args.base_url, args.token)

    if args.command == "search":
        nome = args.nome or args.nome_posicional
        if not any([nome, args.cpf, args.processo]):
            parser.error("Informe --nome, --cpf, ou --processo + --nome.")
        if args.processo and not nome:
            parser.error("Com --processo, informe também --nome (filtro na capa).")
        kwargs = {"nome": nome, "cpf": args.cpf, "processo": args.processo}
        if args.wait:
            result = client.search_and_wait(
                **kwargs,
                poll_seconds=args.poll,
                max_wait_seconds=args.timeout,
            )
        else:
            result = client.create_search(**kwargs)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0 if result.get("status") != "failed" else 1

    if args.command == "status":
        result = client.get_search(args.job_id)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0 if result.get("status") != "failed" else 1

    parser.error(f"Comando desconhecido: {args.command}")
    return 2


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:  # noqa: BLE001 — CLI surface
        print(f"ERRO: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
