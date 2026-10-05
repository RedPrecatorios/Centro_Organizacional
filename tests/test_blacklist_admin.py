from datetime import datetime

from messages_viewer.blacklist_admin import (
    SORT_COLUMNS,
    build_chave,
    normalizar_cpf,
    normalizar_texto,
    origem_da_plataforma,
    to_sao_paulo_wall,
    user_can_manage_blacklist,
)

_SEP = "\x1f"


def test_chave_credor_nome():
    chave = build_chave("CREDOR", subtipo="NOME", nome="Maria Silva")
    assert chave == _SEP.join(("CREDOR", "NOME", "Maria Silva"))


def test_chave_credor_cpf_exige_valor():
    try:
        build_chave("credor", subtipo="CPF", cpf="  ")
    except ValueError as exc:
        assert "CPF" in str(exc)
    else:
        raise AssertionError("esperava ValueError")


def test_chave_completo():
    chave = build_chave(
        "COMPLETO",
        nome="Ana",
        cpf="123",
        processo_codigo="COD",
        processo="0001",
        incidente="1",
        advogado="Dr",
    )
    assert chave == _SEP.join(("COMPLETO", "Ana", "123", "COD", "0001", "1", "Dr"))


def test_sort_whitelist_ignora_coluna_desconhecida():
    assert "id" in SORT_COLUMNS
    assert SORT_COLUMNS.get("nome") == "nome"
    assert SORT_COLUMNS.get("motivo") is None
    assert SORT_COLUMNS.get("1; drop") is None


def test_datetime_do_banco_vai_para_sao_paulo():
    # 21:08 em +03:00 é 15:08 em America/Sao_Paulo (UTC-3).
    assert to_sao_paulo_wall(datetime(2026, 9, 25, 21, 8, 6)) == "2026-09-25 15:08:06"


def test_origem_usa_nome_e_sobrenome():
    assert (
        origem_da_plataforma({"first_name": "Guilherme", "last_name": "Vitoriano"})
        == "Guilherme Vitoriano - Plataforma"
    )
    assert origem_da_plataforma({"username": "filipe"}) == "filipe - Plataforma"


def test_normaliza_nome_e_cpf():
    assert normalizar_texto(" João d'Ávila-Souza ") == "JOAO D AVILA SOUZA"
    assert normalizar_texto("Dra. Maria") == "DRA MARIA"
    assert normalizar_cpf("123.456.789-01") == "12345678901"
    assert normalizar_cpf("12345") == "00000012345"
    assert normalizar_cpf("") == ""
    assert normalizar_cpf("12345678901") == "12345678901"


def test_allowlist_nomes_e_emails():
    assert user_can_manage_blacklist(
        {"email": "Guilherme.Vitoriano@redprecatorios.com.br"}
    )
    assert user_can_manage_blacklist(
        {"first_name": "Filipe", "last_name": "Noberto da Silva"}
    )
    assert user_can_manage_blacklist({"username": "guilherme vitoriano"})
    assert user_can_manage_blacklist({"username": "outro", "role": "admin"})
    assert not user_can_manage_blacklist({"username": "outro", "role": "colaborador"})
    assert not user_can_manage_blacklist(None)
