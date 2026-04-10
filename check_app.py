"""
Verificação rápida de rotas e templates (execute: python check_app.py).
"""
from __future__ import annotations

from app import app


def main() -> int:
    c = app.test_client()
    checks = [
        ("GET", "/", [302], False),
        ("GET", "/planilhas", [302], False),
        ("GET", "/eda/", [200], False),
        ("GET", "/whatsapp", [200], False),
        ("GET", "/static/css/platform.css", [200], False),
        ("GET", "/static/css/whatsapp_module.css", [200], False),
        ("GET", "/eda/api/status", [200], False),
        ("GET", "/eda/progresso", [200], False),
        ("GET", "/api/instances", [200, 500], False),
        ("GET", "/eda/blacklist", [200, 500], False),
        ("GET", "/eda/historico", [200, 500], False),
        ("GET", "/eda/exportar", [200, 500], False),
    ]
    bad: list[tuple] = []
    for method, path, ok_codes, follow in checks:
        r = c.get(path, follow_redirects=follow)
        if r.status_code not in ok_codes:
            bad.append((path, r.status_code, ok_codes))

    r_home = c.get("/", follow_redirects=True)
    body = r_home.get_data(as_text=True)
    if r_home.status_code != 200 or "Painel" not in body:
        bad.append(("/", f"follow home: {r_home.status_code}", [200]))

    if bad:
        print("Falhou:", bad)
        return 1
    print("OK: todas as verificacoes passaram.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
