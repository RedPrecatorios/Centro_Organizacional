"""
Módulo EDA Diário — planilhas, blacklist, histórico e exportação.
Dados e arquivos ficam em EDA_Diario/ (Entrada, Resultados, Modulos).
"""
from __future__ import annotations

import io
import sys
import threading
from datetime import datetime
from pathlib import Path

from flask import (
    Blueprint,
    flash,
    jsonify,
    redirect,
    render_template,
    request,
    send_file,
    url_for,
)

# Raiz do subprojeto EDA (irmão deste arquivo: blueprints/../EDA_Diario)
EDA_ROOT = Path(__file__).resolve().parent.parent / "EDA_Diario"
MODULOS = EDA_ROOT / "Modulos"
ENTRADA = EDA_ROOT / "Entrada"
RESULTADOS = EDA_ROOT / "Resultados"

sys.path.insert(0, str(MODULOS))

ENTRADA.mkdir(parents=True, exist_ok=True)
RESULTADOS.mkdir(parents=True, exist_ok=True)

from modulo_banco import (
    adicionar_blacklist,
    conectar,
    criar_banco_e_tabelas,
    exportar_por_periodo,
)

eda_bp = Blueprint(
    "eda",
    __name__,
    url_prefix="/eda",
    template_folder=str(EDA_ROOT / "templates"),
)

estado: dict = {
    "rodando": False,
    "log": [],
    "etapa": None,
}


def init_eda_db() -> None:
    """Cria banco/tabelas MySQL se possível (mesmo comportamento do app original)."""
    try:
        criar_banco_e_tabelas()
    except Exception as exc:
        print(f"[EDA] Aviso ao inicializar banco: {exc}")


def _nome_arquivo_final() -> str:
    data = datetime.now().strftime("%d-%m-%Y")
    return f"{data} PRC TJSP FINAL.xlsx"


def _caminho_final() -> Path:
    return RESULTADOS / _nome_arquivo_final()


def _log(msg: str) -> None:
    print(msg)
    estado["log"].append(msg)


def _data_arquivo(caminho: Path) -> str | None:
    if caminho and caminho.exists():
        ts = caminho.stat().st_mtime
        return datetime.fromtimestamp(ts).strftime("%d/%m/%Y %H:%M")
    return None


def _arquivo_entrada(tipo: str):
    nomes = {
        "principal": "principal.xlsx",
        "p2": "enriquecimento_lemitti.csv",
        "p3": "enriquecimento_assertiva.csv",
    }
    p = ENTRADA / nomes[tipo]
    return p if p.exists() else None


def _listar_blacklist(
    busca: str = "", limite: int = 20, offset: int = 0
) -> tuple[list[dict], int]:
    try:
        conn = conectar()
        cur = conn.cursor(dictionary=True)
        filtro = f"%{busca}%" if busca else "%"
        cur.execute(
            """
            SELECT id, tipo, valor, motivo, data_inclusao
            FROM blacklist
            WHERE ativo = 1 AND valor LIKE %s
            ORDER BY data_inclusao DESC
            LIMIT %s OFFSET %s
        """,
            (filtro, limite, offset),
        )
        rows = cur.fetchall()

        cur.execute(
            """
            SELECT COUNT(*) as total FROM blacklist
            WHERE ativo = 1 AND valor LIKE %s
        """,
            (filtro,),
        )
        total = cur.fetchone()["total"]

        cur.close()
        conn.close()
        for r in rows:
            if r.get("data_inclusao"):
                r["data_inclusao"] = r["data_inclusao"].strftime("%d/%m/%Y %H:%M")
        return rows, total
    except Exception as e:
        print(f"[ERRO blacklist] {e}")
        return [], 0


def _listar_execucoes() -> list[dict]:
    try:
        conn = conectar()
        cur = conn.cursor(dictionary=True)
        cur.execute(
            """
            SELECT id, etapa, data_execucao,
                   total_registros, total_enriquecidos_p2,
                   total_enriquecidos_p3, total_sem_contato
            FROM execucoes
            ORDER BY data_execucao DESC
            LIMIT 50
        """
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()
        for r in rows:
            if r.get("data_execucao"):
                r["data_execucao"] = r["data_execucao"].strftime("%d/%m/%Y %H:%M")
        return rows
    except Exception as e:
        print(f"[ERRO execucoes] {e}")
        return []


@eda_bp.route("/")
def index():
    arquivos = {
        "principal": _arquivo_entrada("principal"),
        "p2": _arquivo_entrada("p2"),
        "p3": _arquivo_entrada("p3"),
        "intermediaria": (RESULTADOS / "INTERMEDIARIA.xlsx").exists(),
        "final": _caminho_final().exists(),
        "nome_final": _nome_arquivo_final(),
        "nao_encontrados": (RESULTADOS / "cpfs_nao_encontrados_p2.csv").exists(),
        "data_intermediaria": _data_arquivo(RESULTADOS / "INTERMEDIARIA.xlsx"),
        "data_nao_encontrados": _data_arquivo(RESULTADOS / "cpfs_nao_encontrados_p2.csv"),
        "data_final": _data_arquivo(_caminho_final()),
        "blacklist_bloqueios": (RESULTADOS / "blacklist_bloqueios.csv").exists(),
        "data_blacklist_bloq": _data_arquivo(RESULTADOS / "blacklist_bloqueios.csv"),
    }
    return render_template("index.html", arquivos=arquivos, estado=estado)


@eda_bp.route("/upload/<tipo>", methods=["POST"])
def upload(tipo):
    nomes = {
        "principal": "principal.xlsx",
        "p2": "enriquecimento_lemitti.csv",
        "p3": "enriquecimento_assertiva.csv",
    }
    if tipo not in nomes:
        flash("Tipo de arquivo invalido.", "error")
        return redirect(url_for("eda.index"))

    arquivo = request.files.get("arquivo")
    if not arquivo or arquivo.filename == "":
        flash("Nenhum arquivo selecionado.", "error")
        return redirect(url_for("eda.index"))

    destino = ENTRADA / nomes[tipo]
    arquivo.save(str(destino))
    flash(f"Arquivo enviado com sucesso: {arquivo.filename}", "success")
    return redirect(url_for("eda.index"))


@eda_bp.route("/rodar/etapa1", methods=["POST"])
def rodar_etapa1():
    if estado["rodando"]:
        flash("Ja existe uma execucao em andamento.", "warning")
        return redirect(url_for("eda.index"))

    p_principal = _arquivo_entrada("principal")
    p_p2 = _arquivo_entrada("p2")
    if not p_principal or not p_p2:
        flash("Envie a planilha principal e a planilha Lemitti antes de rodar.", "error")
        return redirect(url_for("eda.index"))

    estado["rodando"] = True
    estado["log"] = []
    estado["etapa"] = 1

    def _executar():
        try:
            from modulo_merge import etapa1_enriquecer_com_p2

            etapa1_enriquecer_com_p2(
                caminho_principal=str(p_principal),
                caminho_p2=str(p_p2),
                caminho_saida_intermediaria=str(RESULTADOS / "INTERMEDIARIA.xlsx"),
                caminho_csv_nao_encontrados=str(RESULTADOS / "cpfs_nao_encontrados_p2.csv"),
                caminho_blacklist_txt=str(EDA_ROOT / "blacklist.txt"),
            )
            _log("[OK] Etapa 1 concluida com sucesso.")
        except Exception as exc:
            _log(f"[ERRO] {exc}")
        finally:
            estado["rodando"] = False

    threading.Thread(target=_executar, daemon=True).start()
    return redirect(url_for("eda.progresso"))


@eda_bp.route("/rodar/etapa2", methods=["POST"])
def rodar_etapa2():
    if estado["rodando"]:
        flash("Ja existe uma execucao em andamento.", "warning")
        return redirect(url_for("eda.index"))

    p_intermediaria = RESULTADOS / "INTERMEDIARIA.xlsx"
    p_p3 = _arquivo_entrada("p3")
    if not p_intermediaria.exists() or not p_p3:
        flash("Rode a Etapa 1 primeiro e envie a planilha Assertiva.", "error")
        return redirect(url_for("eda.index"))

    estado["rodando"] = True
    estado["log"] = []
    estado["etapa"] = 2

    def _executar():
        try:
            from modulo_merge import etapa2_enriquecer_com_p3

            etapa2_enriquecer_com_p3(
                caminho_intermediaria=str(p_intermediaria),
                caminho_p3=str(p_p3),
                caminho_saida_final=str(_caminho_final()),
                caminho_blacklist_txt=str(EDA_ROOT / "blacklist.txt"),
            )
            _log("[OK] Etapa 2 concluida com sucesso.")
        except Exception as exc:
            _log(f"[ERRO] {exc}")
        finally:
            estado["rodando"] = False

    threading.Thread(target=_executar, daemon=True).start()
    return redirect(url_for("eda.progresso"))


@eda_bp.route("/progresso")
def progresso():
    return render_template("progresso.html", estado=estado)


@eda_bp.route("/api/status")
def api_status():
    return jsonify(
        {
            "rodando": estado["rodando"],
            "etapa": estado["etapa"],
            "log": estado["log"][-50:],
        }
    )


@eda_bp.route("/download/<arquivo>")
def download(arquivo):
    mapa = {
        "final": _caminho_final(),
        "intermediaria": RESULTADOS / "INTERMEDIARIA.xlsx",
        "nao_encontrados": RESULTADOS / "cpfs_nao_encontrados_p2.csv",
        "blacklist_bloqueios": RESULTADOS / "blacklist_bloqueios.csv",
    }
    caminho = mapa.get(arquivo)
    if not caminho or not caminho.exists():
        flash("Arquivo nao encontrado.", "error")
        return redirect(url_for("eda.index"))
    return send_file(str(caminho), as_attachment=True)


@eda_bp.route("/blacklist")
def blacklist():
    criar_banco_e_tabelas()
    busca = request.args.get("q", "").strip()
    pagina = max(1, int(request.args.get("p", 1)))
    por_pag = 20
    offset = (pagina - 1) * por_pag

    registros, total = _listar_blacklist(busca, por_pag, offset)
    total_paginas = max(1, -(-total // por_pag))

    return render_template(
        "blacklist.html",
        registros=registros,
        busca=busca,
        pagina=pagina,
        total_paginas=total_paginas,
        total=total,
        por_pag=por_pag,
    )


@eda_bp.route("/blacklist/adicionar", methods=["POST"])
def blacklist_adicionar():
    tipo = request.form.get("tipo", "").upper().strip()
    valor = request.form.get("valor", "").strip()
    motivo = request.form.get("motivo", "").strip() or None

    if not tipo or not valor:
        flash("Tipo e valor sao obrigatorios.", "error")
        return redirect(url_for("eda.blacklist"))

    criar_banco_e_tabelas()
    adicionar_blacklist(tipo, valor, motivo)
    flash(f"Adicionado à blacklist: [{tipo}] {valor}", "success")
    return redirect(url_for("eda.blacklist"))


@eda_bp.route("/blacklist/remover/<int:id_registro>", methods=["POST"])
def blacklist_remover(id_registro: int):
    conn = conectar()
    cur = conn.cursor()
    cur.execute("UPDATE blacklist SET ativo = 0 WHERE id = %s", (id_registro,))
    conn.commit()
    cur.close()
    conn.close()
    flash("Entrada removida da blacklist.", "success")
    return redirect(url_for("eda.blacklist"))


@eda_bp.route("/api/blacklist")
def api_blacklist():
    rows, total = _listar_blacklist()
    return jsonify({"rows": rows, "total": total})


@eda_bp.route("/historico")
def historico():
    criar_banco_e_tabelas()
    registros = _listar_execucoes()
    return render_template("historico.html", registros=registros)


@eda_bp.route("/exportar")
def exportar():
    criar_banco_e_tabelas()
    return render_template("exportar.html")


@eda_bp.route("/exportar/gerar", methods=["POST"])
def exportar_gerar():
    disparo_inicio = request.form.get("disparo_inicio", "").strip() or None
    disparo_fim = request.form.get("disparo_fim", "").strip() or None
    entrada_inicio = request.form.get("entrada_inicio", "").strip() or None
    entrada_fim = request.form.get("entrada_fim", "").strip() or None

    if not any([disparo_inicio, disparo_fim, entrada_inicio, entrada_fim]):
        flash("Preencha ao menos um campo de data para exportar.", "error")
        return redirect(url_for("eda.exportar"))

    try:
        dados = exportar_por_periodo(
            disparo_inicio=disparo_inicio,
            disparo_fim=disparo_fim,
            entrada_inicio=entrada_inicio,
            entrada_fim=entrada_fim,
        )
    except Exception as exc:
        flash(f"Erro ao consultar o banco: {exc}", "error")
        return redirect(url_for("eda.exportar"))

    df_principal = dados["principal"]
    df_sms = dados["sms"]
    df_emails = dados["emails"]

    if df_principal.empty and df_sms.empty and df_emails.empty:
        flash("Nenhum registro encontrado para o periodo informado.", "warning")
        return redirect(url_for("eda.exportar"))

    import openpyxl

    def _gravar_aba(wb, df, titulo):
        ws = wb.create_sheet(title=titulo)
        ws.append(list(df.columns))
        for row in df.itertuples(index=False, name=None):
            ws.append(
                [
                    v.strftime("%d/%m/%Y %H:%M")
                    if hasattr(v, "strftime")
                    else (None if str(v) in ("nan", "NaT", "None", "") else v)
                    for v in row
                ]
            )

    wb = openpyxl.Workbook()
    wb.remove(wb.active)
    _gravar_aba(wb, df_principal, "Principal")
    _gravar_aba(wb, df_sms, "sms")
    _gravar_aba(wb, df_emails, "Emails")

    buffer = io.BytesIO()
    wb.save(buffer)

    buffer.seek(0)
    partes = []
    if disparo_inicio or disparo_fim:
        di = (
            datetime.strptime(disparo_inicio, "%Y-%m-%d").strftime("%d-%m-%Y")
            if disparo_inicio
            else "?"
        )
        df = (
            datetime.strptime(disparo_fim, "%Y-%m-%d").strftime("%d-%m-%Y")
            if disparo_fim
            else "?"
        )
        partes.append(f"disparo {di} a {df}")
    if entrada_inicio or entrada_fim:
        ei = (
            datetime.strptime(entrada_inicio, "%Y-%m-%d").strftime("%d-%m-%Y")
            if entrada_inicio
            else "?"
        )
        ef = (
            datetime.strptime(entrada_fim, "%Y-%m-%d").strftime("%d-%m-%Y")
            if entrada_fim
            else "?"
        )
        partes.append(f"entrada {ei} a {ef}")
    nome_arquivo = f"Exportacao {' + '.join(partes)}.xlsx"

    return send_file(
        buffer,
        as_attachment=True,
        download_name=nome_arquivo,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
