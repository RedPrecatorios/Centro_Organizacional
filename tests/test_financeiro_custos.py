from decimal import Decimal

from messages_viewer.financeiro_custos import (
    _SEED_INFORMATICA,
    normalizar_payload,
    parse_valor,
    resumir,
    valor_no_ano,
)


def test_parse_valor_brasileiro():
    assert parse_valor("4.100,00") == Decimal("4100.00")
    assert parse_valor("137,80") == Decimal("137.80")
    assert parse_valor("") is None
    assert parse_valor(None) is None


def test_vigencia_ano_multiplica_doze():
    assert valor_no_ano(Decimal("100.00"), "ano") == Decimal("1200.00")
    assert valor_no_ano(Decimal("100.00"), "mes") == Decimal("100.00")
    assert valor_no_ano(None, "ano") is None


def test_resumo_do_mes_soma_recorrente_e_pontual():
    rows = [
        {"valor": Decimal("10.00"), "vigencia": "ano", "moeda": "BRL"},
        {"valor": Decimal("3.50"), "vigencia": "mes", "moeda": "BRL"},
        {"valor": None, "vigencia": "ano", "moeda": "BRL"},
    ]
    resumo = resumir(rows, 10)[0]
    assert resumo["moeda"] == "BRL"
    assert resumo["recorrente_mensal"] == "10.00"
    assert resumo["pontual"] == "3.50"
    assert resumo["total_visivel"] == "13.50"


def test_resumo_do_ano_projeta_recorrente():
    rows = [
        {"valor": Decimal("10.00"), "vigencia": "ano", "moeda": "BRL"},
        {"valor": Decimal("5.00"), "vigencia": "mes", "moeda": "BRL"},
    ]
    resumo = resumir(rows, None)[0]
    assert resumo["projecao_ano"] == "125.00"


def test_mes_especifico_exige_competencia():
    try:
        normalizar_payload(
            {
                "departamento": "ti",
                "fornecedor": "Adobe",
                "valor": "190,00",
                "vigencia": "mes",
                "ano": "2026",
                "mes": "",
            }
        )
    except ValueError as exc:
        assert "mês" in str(exc).lower()
    else:
        raise AssertionError("mês vazio deveria falhar")


def test_custo_geral_nao_e_departamento():
    try:
        normalizar_payload(
            {
                "departamento": "geral",
                "fornecedor": "Adobe",
                "valor": "10,00",
                "vigencia": "ano",
                "ano": "2026",
            }
        )
    except ValueError as exc:
        assert "departamento" in str(exc).lower()
    else:
        raise AssertionError("Custo Geral não recebe lançamento")


def test_valor_que_muda_soma_os_meses():
    payload = normalizar_payload(
        {
            "departamento": "ti",
            "fornecedor": "Adobe",
            "vigencia": "variavel",
            "ano": "2026",
            "meses": {"1": "100,00", "2": "150,50", "3": ""},
        }
    )
    assert payload["valor"] is None
    assert payload["meses"][1] == Decimal("100.00")
    assert 3 not in payload["meses"]
    assert valor_no_ano(None, "variavel", payload["meses"]) == Decimal("250.50")
    resumo = resumir(
        [{"valor": Decimal("250.50"), "vigencia": "variavel", "moeda": "BRL"}],
        None,
    )[0]
    assert resumo["projecao_ano"] == "250.50"
    assert resumo["variavel"] == "250.50"


def test_exemplo_informatica_soma_linhas_preenchidas():
    total = sum(Decimal(v) for _, v in _SEED_INFORMATICA if v)
    assert total == Decimal("49398.77")
    assert len(_SEED_INFORMATICA) == 22
