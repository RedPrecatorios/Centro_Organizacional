"""RPV: exibir sem REFACTOR, autos ou API de cálculo."""

from __future__ import annotations

from tjsp_pipeline.scraper.show_page import (
    PrecatorioRecord,
    extract_from_show_html,
    is_rpv_text,
    record_is_rpv,
)

from api.services.orchestrator import (
    _append_rpv,
    _format_blacklist_docs,
    _is_apto_from_json,
    _rpv_display_record,
)


def test_is_rpv_text_from_esaj_label():
    assert is_rpv_text("Requisição de Pequeno Valor (CNJ) (07)")
    assert is_rpv_text("RPV")
    assert not is_rpv_text("Precatório (CNJ) (12)")
    assert not is_rpv_text("database")
    assert not is_rpv_text("Cumprimento de Sentença")


def test_record_is_rpv_from_classe_or_label():
    rpv = PrecatorioRecord(
        numero_de_processo="0037419-89.2023.8.26.0053",
        numero_do_incidente="7",
        processo_codigo="X",
        url="http://example",
        label="Requisição de Pequeno Valor (CNJ) (07)",
        classe="RPV",
    )
    prec = PrecatorioRecord(
        numero_de_processo="0017669-72.2021.8.26.0053",
        numero_do_incidente="12",
        processo_codigo="Y",
        url="http://example",
        label="Precatório (CNJ) (12)",
        classe="Precatório (CNJ) (12)",
    )
    assert record_is_rpv(rpv)
    assert not record_is_rpv(prec)


def test_extract_from_show_html_marks_rpv_classe():
    html = """
    <html><body>
      <span class="unj-larger">Requisição de Pequeno Valor (CNJ) (07)</span>
      <a class="unj-larger-1 processoPrinc">0037419-89.2023.8.26.0053</a>
      <span>Número do processo: 0037419-89.2023.8.26.0053</span>
    </body></html>
    """
    rec = extract_from_show_html(
        html,
        url="http://esaj/show.do",
        processo_codigo="ABC",
        label="Requisição de Pequeno Valor (CNJ) (07)",
        require_precatorio_title=False,
    )
    assert rec is not None
    assert rec.numero_de_processo == "0037419-89.2023.8.26.0053"
    assert rec.numero_do_incidente == "7"
    assert rec.classe == "RPV"
    assert record_is_rpv(rec)


def test_rpv_display_record_has_no_calculo_fields():
    rec = PrecatorioRecord(
        numero_de_processo="0037419-89.2023.8.26.0053",
        numero_do_incidente="7",
        processo_codigo="X",
        url="",
        label="RPV",
        classe="RPV",
    )
    payload = _rpv_display_record(rec)
    aptos: list = []
    _append_rpv(aptos, rec)
    assert payload["is_rpv"] is True
    assert "Calculo_Atualizado" not in payload
    assert not payload.get("Motivo")
    assert aptos[0].status == "rpv"
    assert not aptos[0].motivo


def test_blacklist_motivo_uses_doc_name_encontrado():
    one = _format_blacklist_docs(
        [{"title": "Pedido de Habilitação", "status": "BLACKLISTED"}]
    )
    assert one == "Pedido de Habilitação encontrado"
    many = _format_blacklist_docs(
        [
            {"title": "Pedido de Habilitação", "status": "BLACKLISTED"},
            {"title": "Petição (Outras)", "status": "BLACKLISTED"},
        ]
    )
    assert many == "Pedido de Habilitação, Petição (Outras) encontrado(s)"


def test_depre_extinto_is_inapto_not_apto():
    apto, motivo = _is_apto_from_json(
        {
            "record": {"Numero_de_Meses": "72", "Valor_Requisitado": "15.626.33"},
            "context": {
                "extra_infos": {
                    "depre_context": {
                        "depre": "0085058-57.2022.8.26.0500",
                        "is_blacklisted": True,
                        "blocked_status": "extinto",
                    }
                }
            },
            "updated_fields": {
                "depre_context": {
                    "is_blacklisted": True,
                    "blocked_status": "extinto",
                }
            },
        }
    )
    assert apto is False
    assert motivo == "INAPTO: EXTINTO (DEPRE)"


def test_rpv_display_stamps_search_credor():
    rec = PrecatorioRecord(
        numero_de_processo="0037419-89.2023.8.26.0053",
        numero_do_incidente="7",
        processo_codigo="X",
        url="",
        label="RPV",
        classe="RPV",
    )
    payload = _rpv_display_record(
        rec,
        requerente="ARGEMIRO FERREIRA DA SILVA FILHO",
        cpf="41198891815",
    )
    assert payload["Requerente"] == "ARGEMIRO FERREIRA DA SILVA FILHO"
    assert payload["CPF"] == "41198891815"
