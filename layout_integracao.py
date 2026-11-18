# -*- coding: utf-8 -*-
"""Monta o RED Layout (mapa de baias) em /layout-app, dentro da aba Layout."""
from __future__ import annotations

import sys


def tentar_montar_layout(app) -> bool:
    app.config["HAS_LAYOUT"] = False
    try:
        from red_layout.main import app as layout_app
        from red_layout.main import on_startup

        on_startup()
    except Exception as exc:
        print(f"[Layout] Não foi possível carregar o mapa: {exc}", file=sys.stderr)
        return False

    from a2wsgi import ASGIMiddleware
    from werkzeug.middleware.dispatcher import DispatcherMiddleware

    app.wsgi_app = DispatcherMiddleware(
        app.wsgi_app,
        {"/layout-app": ASGIMiddleware(layout_app)},
    )
    app.config["HAS_LAYOUT"] = True
    return True
