# -*- coding: utf-8 -*-
from messages_viewer.cumprimento_busca import (
    incidente_lista_ampla,
    incidente_match_variants,
    linha_cumprimento,
    mesclar_sem_duplicar,
    normalizar_incidente,
    normalizar_processo_incidente_chave,
)


def test_incidente_vazio_ou_zero_lista_todos():
    assert incidente_lista_ampla("") is True
    assert incidente_lista_ampla("0") is True
    assert incidente_lista_ampla("00") is True
    assert incidente_lista_ampla("13") is False


def test_normalizar_incidente_remove_zeros_a_esquerda():
    assert normalizar_incidente("01") == "1"
    assert normalizar_incidente("001") == "1"
    assert normalizar_incidente("13") == "13"
    assert normalizar_incidente("0") == ""
    assert normalizar_incidente("") == ""


def test_incidente_01_casa_com_1_nas_variantes():
    variants = incidente_match_variants("01")
    assert "1" in variants
    assert "01" in variants


def test_chave_blacklist_processo_incidente_01_igual_1():
    proc = "0018269-64.2019.8.26.0053"
    assert normalizar_processo_incidente_chave(proc, "01") == f"{proc}|1"
    assert normalizar_processo_incidente_chave(proc, "1") == f"{proc}|1"
    assert normalizar_processo_incidente_chave(proc, "001") == (
        normalizar_processo_incidente_chave(proc, "01")
    )


def test_linha_sem_nome_nem_cpf_fica_de_fora():
    assert (
        linha_cumprimento(
            {
                "id": 1,
                "requerente": "SO REQUERENTE",
                "nome": "",
                "cpf": "",
                "numero_do_cumprimento": "0001-00.2025.8.26.0053",
            }
        )
        is None
    )


def test_linha_com_nome_monta_formulario():
    item = linha_cumprimento(
        {
            "id": 489,
            "requerente": "IVAN DANGE",
            "nome": "Raimundo Jurandi de Freitas",
            "cpf": "032.742.918-62",
            "numero_do_cumprimento": "0000926-45.2025.8.26.0053",
            "processo_principal": "1009438-78.2017.8.26.0053",
            "entidade_devedora": "SAO PAULO PREVIDENCIA",
            "principal_liquido": "8.743,05",
            "juros": "871,56",
            "desconto_previdenciario": "1.105,44",
            "desconto_assistencia_medica": "200,99",
            "meses": 42,
            "data_base": "01/08/2023",
        }
    )
    assert item is not None
    assert item["source"] == "coleta_cumprimento"
    assert item["requerente"] == "Raimundo Jurandi de Freitas"
    assert item["cpf"] == "032.742.918-62"
    assert item["form"]["principal_liquido"] == "8.743,05"
    assert item["form"]["descontos"] == "1.306,43"
    assert item["form"]["numero_de_meses"] == "42"
    assert item["form"]["data_base"] == "2023-08-01"
    assert item["form"]["data_decisao"] == ""
    assert item["form"]["cumprimento"] == "1009438-78.2017.8.26.0053"
    assert item["form"]["id_precainfosnew"] == ""
    assert item["calculo_completo"] is True


def test_linha_completa_libera_atualizacao():
    item = linha_cumprimento(
        {
            "id": 7,
            "nome": "Raimundo Jurandi de Freitas",
            "cpf": "032.742.918-62",
            "numero_do_cumprimento": "0000926-45.2025.8.26.0053",
            "processo_principal": "1009438-78.2017.8.26.0053",
            "entidade_devedora": "SAO PAULO PREVIDENCIA",
            "principal_liquido": "8.743,05",
            "juros": "871,56",
            "meses": 42,
            "data_base": "01/08/2023",
            "transito_julgado": "17/06/2015",
        }
    )
    assert item["calculo_completo"] is True
    assert item["form"]["data_decisao"] == ""


def test_linha_sem_meses_ainda_libera_atualizacao():
    """Meses em falta não bloqueiam: robô (≤200 mil→1000) ou UI «Verificar Meses»."""
    item = linha_cumprimento(
        {
            "id": 9,
            "nome": "MARIA",
            "cpf": "1",
            "numero_do_cumprimento": "0001-00.2020.8.26.0053",
            "entidade_devedora": "SPPREV",
            "principal_liquido": "10.000,00",
            "juros": "1.000,00",
            "meses": None,
            "data_base": "01/08/2023",
        }
    )
    assert item is not None
    assert item["form"]["numero_de_meses"] == ""
    assert item["calculo_completo"] is True


def test_transito_nao_vira_data_decisao():
    base = {
        "id": 8,
        "nome": "MARIA",
        "cpf": "1",
        "numero_do_cumprimento": "0001-00.2020.8.26.0053",
        "entidade_devedora": "SPPREV",
        "principal_liquido": "10,00",
        "juros": "1,00",
        "meses": 3,
        "data_base": "01/08/2023",
    }
    sem_data = linha_cumprimento({**base, "transito_julgado": "nao_encontrada"})
    texto_null = linha_cumprimento({**base, "transito_julgado": "null"})
    assert sem_data["calculo_completo"] is True
    assert sem_data["form"]["data_decisao"] == ""
    assert texto_null["form"]["data_decisao"] == ""


def test_data_so_mes_ano_usa_ultimo_dia():
    setembro = linha_cumprimento(
        {
            "id": 2,
            "nome": "MARIA",
            "cpf": "1",
            "numero_do_cumprimento": "0001",
            "data_base": "09/2008",
        }
    )
    maio = linha_cumprimento(
        {
            "id": 3,
            "nome": "MARIA",
            "cpf": "1",
            "numero_do_cumprimento": "0001",
            "data_base": "05/2025",
        }
    )
    abril = linha_cumprimento(
        {
            "id": 4,
            "nome": "MARIA",
            "cpf": "1",
            "numero_do_cumprimento": "0001",
            "data_base": "4/2011",
        }
    )
    assert setembro["form"]["data_base"] == "2008-09-30"
    assert maio["form"]["data_base"] == "2025-05-31"
    assert abril["form"]["data_base"] == "2011-04-30"


def test_mescla_preserva_valores_da_memoria_e_anexa_coleta():
    existente = [
        {
            "requerente": "Maria Cristina",
            "numero_de_processo": "0000878-86.2025.8.26.0053",
            "source": "memoria_calculo",
            "id_precainfosnew": None,
            "id_coleta_cumprimento": 9,
            "total_liquido": 11973.91,
            "feito_por": "CMP_CALC",
        }
    ]
    novo = linha_cumprimento(
        {
            "id": 9,
            "nome": "Maria Cristina",
            "cpf": "923.558.468-04",
            "numero_do_cumprimento": "0000878-86.2025.8.26.0053",
            "entidade_devedora": "SPPREV",
            "principal_liquido": "10,00",
            "juros": "1,00",
            "data_base": "01/08/2023",
        }
    )
    outro = linha_cumprimento(
        {
            "id": 10,
            "nome": "Outra Pessoa",
            "cpf": "111.111.111-11",
            "numero_do_cumprimento": "0000878-86.2025.8.26.0053",
        }
    )
    merged = mesclar_sem_duplicar(existente, [novo, outro])
    assert len(merged) == 2
    assert merged[0]["source"] == "memoria_calculo"
    assert merged[0]["total_liquido"] == 11973.91
    assert merged[0]["memoria_cumprimento"] is True
    assert merged[0]["form"]["entidade_devedora"] == "SPPREV"
    assert merged[0]["form"]["principal_liquido"] == "10,00"
    assert merged[1]["source"] == "coleta_cumprimento"
    assert merged[1]["requerente"] == "Outra Pessoa"
    assert merged[1]["total_liquido"] == 0
