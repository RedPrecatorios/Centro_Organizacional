from messages_viewer.blacklist_pagina import (
    _motivo_manual,
    caso_de_linha,
    criterios_busca,
    pares_de_contatos,
    registros_do_caso,
    valor_gravado,
    where_precainfos,
)
from messages_viewer.plataforma_auth import permission_ids_from_form, user_blacklist_caps


class _Form(dict):
    def get(self, key, default=None):
        return super().get(key, default)


def test_niveis_exigem_visualizacao():
    form = _Form({"tab_blacklist_inserir": "1", "tab_blacklist_editar": "1"})
    assert permission_ids_from_form(form, "tab_") == []


def test_niveis_junto_com_visualizacao():
    form = _Form(
        {
            "new_tab_blacklist": "1",
            "new_tab_blacklist_inserir": "1",
            "new_tab_blacklist_remover": "1",
            "new_tab_index": "1",
        }
    )
    ids = permission_ids_from_form(form, "new_tab_")
    assert ids[0] == "index" or "index" in ids
    assert "blacklist" in ids
    assert "blacklist_inserir" in ids
    assert "blacklist_remover" in ids
    assert "blacklist_editar" not in ids


def test_donos_tem_controle_total():
    caps = user_blacklist_caps(
        {
            "email": "filipe.noberto@redprecatorios.com.br",
            "role": "admin",
            "id": 1,
        }
    )
    assert caps == frozenset({"ver", "inserir", "editar", "remover"})


def test_admin_tem_controle_total_da_blacklist():
    caps = user_blacklist_caps(
        {
            "email": "outro@redprecatorios.com.br",
            "role": "admin",
            "first_name": "Outro",
            "last_name": "Admin",
            "username": "admin",
            "id": 2,
        }
    )
    assert caps == frozenset({"ver", "inserir", "editar", "remover"})


def test_valor_processo_incidente():
    tipo, valor = valor_gravado(
        {
            "tipo": "PROCESSO_INCIDENTE",
            "processo": " 0001234-56.2023.8.26.0100 ",
            "incidente": "99",
            "motivo": "Blacklist",
        }
    )
    assert tipo == "PROCESSO_INCIDENTE"
    assert valor == "0001234-56.2023.8.26.0100|99"


def test_processo_obrigatorio():
    try:
        valor_gravado({"tipo": "PROCESSO_INCIDENTE", "processo": " ", "incidente": ""})
    except ValueError as exc:
        assert "processo" in str(exc).lower()
    else:
        raise AssertionError("esperava ValueError")


def test_caso_completo_separa_um_registro_por_tipo():
    itens = registros_do_caso(
        {
            "processo": " 0001234-56.2023.8.26.0100 ",
            "incidente": "99",
            "nome": "maria silva",
            "cpf": "123.456.789-01",
            "telefone": "(11) 98888-7777",
            "email": "maria@example.com",
        }
    )
    assert itens == [
        ("PROCESSO_INCIDENTE", "0001234-56.2023.8.26.0100|99"),
        ("NOME", "MARIA SILVA"),
        ("CPF", "12345678901"),
        ("TELEFONE", "11988887777"),
        ("EMAIL", "MARIA@EXAMPLE.COM"),
    ]


def test_so_telefone_grava_um_registro():
    assert registros_do_caso({"telefone": "11988887777"}) == [("TELEFONE", "11988887777")]


def test_varios_telefones_viram_registros_separados():
    itens = registros_do_caso(
        {
            "cpf": "12345678901",
            "telefone": ["(11) 98888-7777", "11 97777-6666", "", "(11) 98888-7777", "11966665555"],
        }
    )
    assert itens == [
        ("CPF", "12345678901"),
        ("TELEFONE", "11988887777"),
        ("TELEFONE", "11977776666"),
        ("TELEFONE", "11966665555"),
    ]


class _FormLista(dict):
    def getlist(self, key):
        valor = dict.get(self, key, [])
        if isinstance(valor, str):
            return [valor]
        return list(valor)


def test_formulario_com_telefones_repetidos_no_nome():
    itens = registros_do_caso(_FormLista({"telefone": ["11988887777", "11977776666"]}))
    assert itens == [("TELEFONE", "11988887777"), ("TELEFONE", "11977776666")]


def test_telefone_invalido_entre_varios():
    _recusa({"telefone": ["11988887777", "abc"]}, "Telefone inválido")


def test_processo_com_cpf_sem_nome():
    itens = registros_do_caso(
        {"processo": "0001234-56.2023.8.26.0100", "cpf": "12345678901"}
    )
    assert itens == [
        ("PROCESSO_INCIDENTE", "0001234-56.2023.8.26.0100|"),
        ("CPF", "12345678901"),
    ]


def _recusa(form, trecho):
    try:
        registros_do_caso(form)
    except ValueError as exc:
        assert trecho in str(exc)
    else:
        raise AssertionError("esperava ValueError")


def test_nome_sem_cpf_recusado():
    _recusa({"nome": "Maria Silva"}, "CPF")


def test_processo_sem_pessoa_recusado():
    _recusa({"processo": "0001234-56.2023.8.26.0100"}, "nome ou o CPF")


def test_incidente_sem_processo_recusado():
    _recusa({"incidente": "99", "cpf": "12345678901"}, "processo")


def test_caso_vazio_recusado():
    _recusa({}, "ao menos um dado")


def test_busca_exige_cpf_ou_processo():
    _recusa_busca("", "", "", "CPF ou o número")


def test_incidente_sozinho_na_busca():
    _recusa_busca("", "", "12", "processo")


def test_where_precainfos_combina_cpf_e_processo():
    where, params = where_precainfos(
        {"cpf": "12345678901", "processo": "0001", "incidente": ""},
        {"cpf": "CPF", "processo": "Numero_de_Processo", "incidente": "Numero_do_Incidente"},
    )
    assert "CPF" in where and "Numero_de_Processo" in where
    assert "Incidente" not in where
    assert params == ["12345678901", "0001"]


def test_caso_de_linha_normaliza_cpf():
    caso = caso_de_linha(
        {"Requerente": "Maria Silva", "CPF": "123.456.789-01", "Numero_de_Processo": "0001", "Numero_do_Incidente": "9"},
        {"nome": "Requerente", "cpf": "CPF", "processo": "Numero_de_Processo", "incidente": "Numero_do_Incidente"},
    )
    assert caso == {
        "nome": "Maria Silva",
        "cpf": "12345678901",
        "processo": "0001",
        "incidente": "9",
    }


def test_pares_de_contatos_sem_repetir():
    pares = pares_de_contatos(
        ["(11) 98888-7777", "11988887777", "abc"],
        ["maria@example.com", "MARIA@example.com"],
    )
    assert pares == [
        ("TELEFONE", "11988887777"),
        ("EMAIL", "MARIA@EXAMPLE.COM"),
    ]


def _recusa_busca(cpf, processo, incidente, trecho):
    try:
        criterios_busca(cpf, processo, incidente)
    except ValueError as exc:
        assert trecho in str(exc)
    else:
        raise AssertionError("esperava ValueError")


def test_motivo_fora_da_lista():
    try:
        _motivo_manual("livre")
    except ValueError as exc:
        assert "Motivo" in str(exc)
    else:
        raise AssertionError("esperava ValueError")
