from tjsp_pipeline.integration.refactor_log import parse_refactor_log_line


def test_parse_calculo_api_line():
    info = parse_refactor_log_line(
        "[CalculoApi] [300s] POST http://example/calculo/fila/ | "
        "processo=0037419-89.2023.8.26.0053 | incidente=7"
    )
    assert info is not None
    assert info["processo"] == "0037419-89.2023.8.26.0053"
    assert info["incidente"] == "7"
    assert info["event"] == "calculo"


def test_parse_incidente_start_and_blacklist():
    start = parse_refactor_log_line(
        "[INCIDENTE] Verificando processo=0009944-08.2000.8.26.0008 incidente=1"
    )
    assert start["event"] == "start"
    bl = parse_refactor_log_line(
        "[REPROCESSAMENTO] Autos blacklisted detectados | "
        "processo=0009944-08.2000.8.26.0008 | incidente=1 | quantidade=4"
    )
    assert bl["event"] == "blacklist"
    assert not bl.get("from_depre")


def test_parse_depre_extinto_extracts_numero():
    info = parse_refactor_log_line(
        "[DEPRE] Status bloqueado | "
        "processo=0420725-64.1992.8.26.0053 | incidente=27 | "
        "depre=0085058-57.2022.8.26.0500 | status=extinto"
    )
    assert info is not None
    assert info["event"] == "extinto"
    assert info["depre"] == "0085058-57.2022.8.26.0500"
    assert info["blocked_status"].lower() == "extinto"
    assert info["from_depre"] is True


def test_parse_ignores_unrelated():
    assert parse_refactor_log_line("Execucao iniciada | run_id=abc") is None
