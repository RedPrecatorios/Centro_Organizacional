"""
Monta a app Flask do EDA Diário (pasta separada) em /eda/ via WSGI.
Configure EDA_DIARIO_PATH no .env; se vazio, usa o caminho padrão do desenvolvedor.
"""

from __future__ import annotations

import importlib.util
import os
import sys
from pathlib import Path
from typing import Any


def _base_projeto() -> Path:
    """Pasta onde estão `app.py` e `eda_integracao.py` (raiz do repositório)."""
    return Path(__file__).resolve().parent


def _caminho_eda() -> Path | None:
    raw = (os.getenv("EDA_DIARIO_PATH") or "").strip()
    if not raw:
        p = _base_projeto() / "EDA_Diario"
    else:
        p = Path(raw).expanduser().resolve()
    if not (p / "app.py").is_file():
        return None
    return p


def tentar_montar_eda(app) -> bool:
    """
    Anexa a aplicação EDA em /eda. Retorna True se montou com sucesso.
    """
    app.config["HAS_EDIARIO"] = False
    root = _caminho_eda()
    if root is None:
        return False
    ap = root / "app.py"
    try:
        spec = importlib.util.spec_from_file_location("eda_diario_montada", ap)
        if spec is None or spec.loader is None:
            return False
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        eda_flask: Any = getattr(mod, "app", None)
        if eda_flask is None:
            return False
    except Exception as e:
        print(f"[EDA Diário] Não foi possível carregar {ap}: {e}", file=sys.stderr)
        return False

    from werkzeug.middleware.dispatcher import DispatcherMiddleware

    inner = app.wsgi_app
    app.wsgi_app = DispatcherMiddleware(inner, {"/eda": eda_flask})
    app.config["HAS_EDIARIO"] = True
    return True
