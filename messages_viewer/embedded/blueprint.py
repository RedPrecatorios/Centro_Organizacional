"""
Segundo projeto da plataforma.

- Rotas ficam neste blueprint (prefixo URL /embedded).
- Templates: pasta embedded/templates/
- Estáticos: pasta embedded/static/  → URL /embedded/static/...

Na app principal: já está registrado em app.py. Para novas rotas, use
@embedded_bp.route(...) abaixo.
"""

from flask import Blueprint, render_template

embedded_bp = Blueprint(
    "embedded",
    __name__,
    url_prefix="/embedded",
    template_folder="templates",
    static_folder="static",
    static_url_path="/embedded/static",
)


@embedded_bp.route("/")
def index():
    return render_template("embedded_index.html")
