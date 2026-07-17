"""Testes de token + envio/recebimento via cliente cloud (sem scrape real)."""

from __future__ import annotations

import os
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

# Token fixo so para este arquivo de teste.
TEST_TOKEN = "test-token-dashboard-backend-ci"


@pytest.fixture()
def api_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("API_TOKEN", TEST_TOKEN)
    # Evita worker real: job fica queued e o teste injeta resultado no store.
    monkeypatch.setenv("LOG_DIR", str(tmp_path / "logs"))
    monkeypatch.setenv("DEBUG_HTML_DIR", str(tmp_path / "logs" / "debug_html"))
    monkeypatch.setenv("FINAL_OUTPUT_DIR", str(tmp_path / "output"))
    yield


@pytest.fixture()
def client(api_env, monkeypatch: pytest.MonkeyPatch):
    # Import late so env is already set.
    from api.jobs.worker import SearchJobWorker

    # No-op worker: nao dispara Chrome/REFACTOR.
    monkeypatch.setattr(SearchJobWorker, "start", lambda self: None)
    monkeypatch.setattr(SearchJobWorker, "stop", lambda self, timeout=5.0: None)

    from api.app import app

    with TestClient(app) as test_client:
        yield test_client


def test_health_is_public(client: TestClient):
    response = client.get("/api/v1/health")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_search_rejects_missing_token(client: TestClient):
    response = client.post("/api/v1/searches", json={"nome": "Fulano de Tal"})
    assert response.status_code == 401


def test_search_rejects_wrong_token(client: TestClient):
    response = client.post(
        "/api/v1/searches",
        json={"nome": "Fulano de Tal"},
        headers={"Authorization": "Bearer wrong-token"},
    )
    assert response.status_code == 401


def test_search_send_and_receive_with_token(client: TestClient):
    headers = {"Authorization": f"Bearer {TEST_TOKEN}"}

    created = client.post(
        "/api/v1/searches",
        json={"nome": "Heloisa Maria Fernandes Queiroz"},
        headers=headers,
    )
    assert created.status_code == 202, created.text
    body = created.json()
    assert body["status"] == "queued"
    job_id = body["job_id"]
    assert job_id

    # Simula conclusao do worker (envio/recebimento sem pipeline).
    from api.app import app

    store = app.state.job_store
    store.update(
        job_id,
        status="done",
        progress="done",
        result_json={
            "nome": "Heloisa Maria Fernandes Queiroz",
            "processos_aptos": [
                {
                    "numero_de_processo": "0017618-95.2020.8.26.0053",
                    "numero_do_incidente": "24",
                    "source": "database",
                    "record": {"Requerente": "HELOISA MARIA FERNANDES QUEIROZ"},
                    "json_path": None,
                    "fanout_json_path": None,
                }
            ],
            "skipped": [],
            "artifacts": {"final_output_dir": str(Path("output"))},
            "errors": [],
        },
    )

    received = client.get(f"/api/v1/searches/{job_id}", headers=headers)
    assert received.status_code == 200, received.text
    payload = received.json()
    assert payload["status"] == "done"
    assert payload["job_id"] == job_id
    assert len(payload["processos_aptos"]) == 1
    assert (
        payload["processos_aptos"][0]["numero_de_processo"]
        == "0017618-95.2020.8.26.0053"
    )


def test_cloud_client_send_receive(client: TestClient, monkeypatch: pytest.MonkeyPatch):
    """Valida o script clients/tjsp_api_client.py contra a API com token."""
    import clients.tjsp_api_client as cloud_client

    headers_ok = {"Authorization": f"Bearer {TEST_TOKEN}"}

    # Stub HTTP do cliente para usar TestClient (sem rede real).
    class _Resp:
        def __init__(self, status_code: int, data: dict):
            self.status_code = status_code
            self._data = data
            self.text = str(data)

        def json(self):
            return self._data

        def raise_for_status(self):
            if self.status_code >= 400:
                raise RuntimeError(f"HTTP {self.status_code}")

    def fake_request(method, url, headers=None, json=None, timeout=None):
        path = url.replace("http://test", "")
        if method == "GET" and path.endswith("/api/v1/health"):
            r = client.get(path)
            return _Resp(r.status_code, r.json())
        if method == "POST" and path.endswith("/api/v1/searches"):
            r = client.post(path, headers=headers or {}, json=json or {})
            return _Resp(r.status_code, r.json())
        if method == "GET" and "/api/v1/searches/" in path:
            r = client.get(path, headers=headers or {})
            return _Resp(r.status_code, r.json())
        raise AssertionError(f"URL inesperada: {method} {url}")

    monkeypatch.setattr(
        cloud_client.requests,
        "get",
        lambda url, headers=None, timeout=None: fake_request(
            "GET", url, headers=headers, timeout=timeout
        ),
    )
    monkeypatch.setattr(
        cloud_client.requests,
        "post",
        lambda url, headers=None, json=None, timeout=None: fake_request(
            "POST", url, headers=headers, json=json, timeout=timeout
        ),
    )

    api = cloud_client.TjspApiClient("http://test", TEST_TOKEN)
    health = api.health()
    assert health["status"] == "ok"

    created = api.create_search("Nadir Costa de Oliveira")
    assert created["status"] == "queued"
    job_id = created["job_id"]

    from api.app import app

    app.state.job_store.update(
        job_id,
        status="done",
        progress="done",
        result_json={
            "nome": "Nadir Costa de Oliveira",
            "processos_aptos": [],
            "skipped": [],
            "artifacts": {},
            "errors": [],
        },
    )

    got = api.get_search(job_id)
    assert got["status"] == "done"
    assert got["job_id"] == job_id

    # Token errado
    bad = cloud_client.TjspApiClient("http://test", "token-errado")
    with pytest.raises(PermissionError):
        bad.create_search("Alguem")
