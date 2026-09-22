# -*- coding: utf-8 -*-
from datetime import date
from decimal import Decimal

from messages_viewer.calculo_manual import (
    _format_db_date,
    _format_money_br,
    _format_money_db,
    _parse_money,
    _persist_values,
    form_fields_from_row,
    memoria_valores_from_form,
    normalize_form,
    synthetic_memoria_id,
)


def test_parse_money_banco_precainfosnew():
    """Valores reais em precainfosnew usam ponto como milhar e como decimal."""
    assert _parse_money("199.922.30") == Decimal("199922.30")
    assert _parse_money("36.788.22") == Decimal("36788.22")
    assert _parse_money("0.00") == Decimal("0")
    assert _parse_money("150.000,50") == Decimal("150000.50")
    assert _parse_money("150000.50") == Decimal("150000.50")
    assert _parse_money("R$ 1.234,56") == Decimal("1234.56")


def test_format_money_br():
    assert _format_money_br(Decimal("199922.30")) == "199.922,30"
    assert _format_money_br(Decimal("0")) == "0,00"


def test_format_money_db_precainfosnew():
    assert _format_money_db(Decimal("107469.80")) == "107.469.80"
    assert _format_money_db(Decimal("0")) == "0.00"
    assert _format_db_date(date(2025, 8, 31)) == "31/08/2025"


def test_persist_values_update_formato_banco():
    payload, err = normalize_form(
        {
            "requerente": "Paulo Henrique Loureiro",
            "entidade_devedora": "FESP",
            "numero_de_processo": "0023563-87.2025.8.26.0053",
            "numero_do_incidente": "9",
            "data_base": "2025-08-31",
            "data_decisao": "2026-08-31",
            "principal_liquido": "107469,80",
            "juros_moratorio": "46055,82",
            "numero_de_meses": "65",
            "descontos": "13795,98",
        }
    )
    assert err is None
    colmap = {
        "requerente": "Requerente",
        "entidade_devedora": "Entidade_Devedora",
        "numero_de_processo": "Numero_de_Processo",
        "numero_do_incidente": "Numero_do_Incidente",
        "data_base": "Data_Base",
        "data_decisao": "Data_Decisao",
        "principal_liquido": "Principal_Liquido",
        "juros_moratorio": "Juros_Moratorio",
        "inst_prev": "INST_PREV",
        "numero_de_meses": "Numero_de_Meses",
    }
    fields = set(colmap.values()) | {"Script", "Data_de_Entrada", "Natureza", "Numero_de_Meses_TERMO"}
    values = _persist_values(payload, colmap, fields)
    assert values["Principal_Liquido"] == "107.469.80"
    assert values["Juros_Moratorio"] == "46.055.82"
    assert values["INST_PREV"] == "13.795.98"
    assert values["Data_Base"] == "31/08/2025"
    assert values["Data_Decisao"] == "31/08/2026"
    assert values["Numero_do_Incidente"] == "9"
    assert "Script" not in values
    assert values["Numero_de_Meses"] == "65"


def test_synthetic_memoria_id_negativo_e_estavel():
    a = synthetic_memoria_id("0023563-87.2025.8.26.0053", "9")
    b = synthetic_memoria_id("00235638720258260053", "09")
    assert a < 0
    assert a == b


def test_memoria_valores_from_form():
    payload, err = normalize_form(
        {
            "requerente": "Paulo Henrique Loureiro",
            "entidade_devedora": "FESP",
            "numero_de_processo": "0023563-87.2025.8.26.0053",
            "data_base": "2025-08-31",
            "data_decisao": "2026-08-31",
            "principal_liquido": "1000,00",
            "juros_moratorio": "200,00",
            "numero_de_meses": "12",
            "descontos": "50,00",
        }
    )
    assert err is None
    vals = memoria_valores_from_form(payload)
    assert vals["principal_bruto"] == Decimal("0.00")
    assert vals["juros"] == Decimal("0.00")
    assert vals["total_bruto"] == Decimal("0.00")
    assert vals["desc_saude_prev"] == Decimal("0.00")
    assert vals["reserva_honorarios"] == Decimal("0.00")
    assert vals["total_liquido"] == Decimal("0.00")
    assert vals["percentual_honorarios"] == Decimal("30.00")


def test_normalize_desconto_unico_zera_rubricas():
    payload, err = normalize_form(
        {
            "requerente": "Maria Silva",
            "entidade_devedora": "FAZENDA DO ESTADO DE SÃO PAULO",
            "numero_de_processo": "0001234-56.2020.8.26.0050",
            "numero_do_incidente": "12",
            "cumprimento": "0001111-00.2018.8.26.0050",
            "oc": "12345",
            "data_base": "2018-01-15",
            "data_decisao": "25/06/2020",
            "principal_liquido": "199.922.30",
            "juros_moratorio": "35.576.30",
            "numero_de_meses": "105",
            "descontos": "1.234,56",
            "spprev": "999",
            "id_precainfosnew": "88",
        }
    )
    assert err is None
    assert payload["principal_liquido"] == Decimal("199922.30")
    assert payload["juros_moratorio"] == Decimal("35576.30")
    assert payload["inst_prev"] == Decimal("1234.56")
    assert payload["spprev"] == 0
    assert payload["data_decisao"] == date(2020, 6, 25)
    assert payload["id_precainfosnew"] == 88
    assert payload["prioridade"] is False


def test_normalize_prioridade_sim():
    payload, err = normalize_form(
        {
            "requerente": "Maria Silva",
            "entidade_devedora": "FAZENDA DO ESTADO DE SÃO PAULO",
            "numero_de_processo": "0001234-56.2020.8.26.0050",
            "data_base": "2018-01-15",
            "data_decisao": "25/06/2020",
            "principal_liquido": "1",
            "juros_moratorio": "1",
            "numero_de_meses": "10",
            "prioridade": "sim",
        }
    )
    assert err is None
    assert payload["prioridade"] is True


def test_form_fields_from_row_preenche_calculo():
    row = {
        "id": 42,
        "Requerente": "Maria Silva",
        "Entidade_Devedora": "FAZENDA DO ESTADO DE SÃO PAULO",
        "Numero_de_Processo": "0001234-56.2020.8.26.0050",
        "Numero_do_Incidente": "12",
        "Ordem": "99",
        "Processo_Principal": "0001111-00.2018.8.26.0050",
        "Data_Base": date(2018, 1, 15),
        "Data_Decisao": "25/06/2020",
        "Principal_Liquido": "199.922.30",
        "Juros_Moratorio": "35.576.30",
        "Numero_de_Meses": 105,
        "INST_PREV": "1.234,56",
        "SPPREV": "0",
        "IAMSPE": 0,
        "IPESP": 0,
        "ASSIT_MED_HOSPITAL": 0,
        "INST_PREV_CAIXA_BENEF": 0,
        "ASSIST_MED_CAIXA_BENEF": 0,
    }
    form = form_fields_from_row(row, set(row.keys()))
    assert form["id_precainfosnew"] == 42
    assert form["requerente"] == "Maria Silva"
    assert form["data_base"] == "2018-01-15"
    assert form["data_decisao"] == "2020-06-25"
    assert form["principal_liquido"] == "199.922,30"
    assert form["juros_moratorio"] == "35.576,30"
    assert form["descontos"] == "1.234,56"
    assert form["numero_de_meses"] == "105"


def test_form_fields_nao_duplica_inst_prev():
    row = {
        "id": 1,
        "SPPREV": Decimal("1000.00"),
        "IAMSPE": Decimal("0"),
        "IPESP": Decimal("0"),
        "ASSIT_MED_HOSPITAL": Decimal("0"),
        "INST_PREV_CAIXA_BENEF": Decimal("0"),
        "ASSIST_MED_CAIXA_BENEF": Decimal("0"),
        "INST_PREV": Decimal("1000.00"),
        "Principal_Liquido": Decimal("0"),
        "Juros_Moratorio": Decimal("0"),
    }
    form = form_fields_from_row(row, set(row.keys()))
    assert form["descontos"] == "1.000,00"
