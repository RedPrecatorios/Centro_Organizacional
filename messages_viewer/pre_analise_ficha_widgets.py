# -*- coding: utf-8 -*-
"""Metadados de widgets da ficha PRÉ Análise (espelha contratos.php / contratos.js)."""
from __future__ import annotations

from typing import Any

_SIM_NAO = [
    {"value": "", "label": "Selecione"},
    {"value": "sim", "label": "Sim"},
    {"value": "não", "label": "Não"},
]

_HERDEIRO_HABILITADO = [
    {"value": "", "label": "Selecione"},
    {"value": "sim", "label": "SIM"},
    {"value": "não", "label": "NÃO"},
]

_ESTADO_CIVIL = [
    {"value": "", "label": "Selecione"},
    {"value": "solteiro(a)", "label": "Solteiro(a)"},
    {"value": "casado(a)", "label": "Casado(a)"},
    {"value": "divorciado(a)", "label": "Divorciado(a)"},
    {"value": "separado(a)", "label": "Separado(a)"},
    {"value": "viúvo(a)", "label": "Viúvo(a)"},
]

_REGIME_CONJUGE = [
    {"value": "", "label": "Selecione"},
    {"value": "União Estável", "label": "União Estável"},
    {"value": "Comunhão Universal de Bens", "label": "Comunhão Universal de Bens"},
    {"value": "Comunhão Parcial de Bens", "label": "Comunhão Parcial de Bens"},
    {"value": "Separação Total de Bens", "label": "Separação Total de Bens"},
    {"value": "Participação Final nos Aquestos", "label": "Participação Final nos Aquestos"},
    {"value": "Separação Obrigatória de Bens", "label": "Separação Obrigatória de Bens"},
    {"value": "Separação Convencional de Bens", "label": "Separação Convencional de Bens"},
]

_NACIONALIDADE = [
    {"value": "", "label": "Selecione"},
    {"value": "brasileiro(a)", "label": "Brasileiro(a)"},
    {"value": "americano(a)", "label": "Americano(a)"},
    {"value": "canadense", "label": "Canadense"},
    {"value": "mexicano(a)", "label": "Mexicano(a)"},
    {"value": "francês/francesa", "label": "Francês/Francesa"},
    {"value": "alemão/alemã", "label": "Alemão/Alemã"},
    {"value": "italiano(a)", "label": "Italiano(a)"},
    {"value": "espanhol(a)", "label": "Espanhol(a)"},
    {"value": "inglês/inglesa", "label": "Inglês/Inglesa"},
    {"value": "japonês/japonesa", "label": "Japonês/Japonesa"},
    {"value": "chinês/chinesa", "label": "Chinês/Chinesa"},
    {"value": "russo(a)", "label": "Russo(a)"},
    {"value": "indiano(a)", "label": "Indiano(a)"},
    {"value": "sul-africano(a)", "label": "Sul-africano(a)"},
    {"value": "australiano(a)", "label": "Australiano(a)"},
    {"value": "neozelandês/neozelandesa", "label": "Neozelandês/Neozelandesa"},
    {"value": "argentino(a)", "label": "Argentino(a)"},
    {"value": "colombiano(a)", "label": "Colombiano(a)"},
    {"value": "chileno(a)", "label": "Chileno(a)"},
    {"value": "português/portuguesa", "label": "Português/Portuguesa"},
]

_UF = [
    {"value": "", "label": "Selecione"},
    {"value": "AC", "label": "Acre"},
    {"value": "AL", "label": "Alagoas"},
    {"value": "AP", "label": "Amapá"},
    {"value": "AM", "label": "Amazonas"},
    {"value": "BA", "label": "Bahia"},
    {"value": "CE", "label": "Ceará"},
    {"value": "DF", "label": "Distrito Federal"},
    {"value": "ES", "label": "Espírito Santo"},
    {"value": "GO", "label": "Goiás"},
    {"value": "MA", "label": "Maranhão"},
    {"value": "MT", "label": "Mato Grosso"},
    {"value": "MS", "label": "Mato Grosso do Sul"},
    {"value": "MG", "label": "Minas Gerais"},
    {"value": "PA", "label": "Pará"},
    {"value": "PB", "label": "Paraíba"},
    {"value": "PR", "label": "Paraná"},
    {"value": "PE", "label": "Pernambuco"},
    {"value": "PI", "label": "Piauí"},
    {"value": "RJ", "label": "Rio de Janeiro"},
    {"value": "RN", "label": "Rio Grande do Norte"},
    {"value": "RS", "label": "Rio Grande do Sul"},
    {"value": "RO", "label": "Rondônia"},
    {"value": "RR", "label": "Roraima"},
    {"value": "SC", "label": "Santa Catarina"},
    {"value": "SP", "label": "São Paulo"},
    {"value": "SE", "label": "Sergipe"},
    {"value": "TO", "label": "Tocantins"},
]

_TIPO_CONTA = [
    {"value": "", "label": "Selecione"},
    {"value": "Corrente", "label": "Corrente"},
    {"value": "Poupança", "label": "Poupança"},
]

_PROCURADORIA_TIPO = [
    {"value": "", "label": "Selecione"},
    {"value": "2", "label": "Municipal"},
    {"value": "1", "label": "Estadual"},
    {"value": "3", "label": "Federal"},
]

_MENCIONAR = [
    {"value": "Sim", "label": "Sim"},
    {"value": "Não", "label": "Não"},
]

PROCURADORIA_BY_TIPO = {
    "1": "PROCURADORIA DO ESTADO DE SÃO PAULO (PGE/SP)",
    "2": "PROCURADORIA GERAL DO MUNICÍPIO (PGM/SP)",
    "3": "PROCURADORIA GERAL DA FAZENDA NACIONAL (PGFN)",
}

DATE_FIELDS = frozenset(
    {
        "expedicao_oficio_requisitorio",
        "data_de_nascimento_credor",
        "data_do_obito",
        "data_de_nascimento_conjuge_credor",
        "data_de_nascimento_anuente_credor",
        "data_base",
        "data_de_nascimento_herdeiro",
        "data_de_nascimento_conjuge_herdeiro",
    }
)

MONEY_FIELDS = frozenset(
    {
        "principal_liquido",
        "juros_moratorio",
        "juros_compensatorio",
        "descontos_previdenciarios",
        "descontos_de_assistencia_medica",
        "custas",
        "total_da_requisicao",
        "preco_de_compra",
        "valor_liquido_do_oficio",
        "valor_da_proposta_herdeiro",
    }
)

MONEY_SUM_FIELDS = frozenset(
    {
        "principal_liquido",
        "juros_moratorio",
        "juros_compensatorio",
        "descontos_previdenciarios",
        "descontos_de_assistencia_medica",
        "custas",
    }
)

PERCENT_FIELDS = frozenset(
    {
        "percentual_honorarios",
        "percentual_de_compra",
        "percentual_detido",
        "percentual_cedido",
        "percentual_honorarios_herdeiro",
        "percentual_detido_herdeiros",
        "sobra",
        "honorarios_reservados",
        "percentual_nao_cedido",
    }
)

METADATA_FIELDS = frozenset(
    {
        "qtd_herdeiros",
        "percentual_detido_herdeiros",
        "sobra",
        "recupere",
        "incluido_por",
        "incluido_em",
        "alterado_por",
        "alterado_em",
    }
)

# Campos com class="required" (borda amarela) em contratos_engine/pages/contratos.php
LEGACY_REQUIRED_FIELDS = frozenset(
    {
        "cumprimento_de_sentenca",
        "incidente",
        "nome_credor",
        "cpf_credor",
        "estado_civil_credor",
        "nacionalidade_credor",
        "credor_falecido",
        "anuente_credor",
        "percentual_honorarios",
        "cessionaria",
        "herdeiro_habilitado",
        "nome_herdeiro",
        "cpf_herdeiro",
        "parentesco_herdeiro",
        "nacionalidade_herdeiro",
        "estado_civil_herdeiro",
        "percentual_detido",
        "herdeiro_cedente",
    }
)

CPF_FIELDS = frozenset(
    {
        "cpf_credor",
        "cpf_conjuge_credor",
        "cpf_anuente_credor",
        "cpf_advogado",
        "cpf_herdeiro",
        "cpf_conjuge_herdeiro",
    }
)

CEP_LOOKUP_GROUPS: dict[str, dict[str, str]] = {
    "cep_credor": {
        "logradouro": "logradouro_credor",
        "complemento": "complemento_credor",
        "bairro": "bairro_credor",
        "cidade": "cidade_credor",
        "estado": "estado_credor",
        "numero": "numero_logradouro_credor",
    },
    "cep_conjuge_credor": {
        "logradouro": "logradouro_conjuge_credor",
        "bairro": "bairro_conjuge_credor",
        "cidade": "cidade_conjuge_credor",
        "estado": "estado_conjuge_credor",
        "numero": "numero_logradouro_conjuge_credor",
    },
    "cep_anuente_credor": {
        "logradouro": "logradouro_anuente_credor",
        "bairro": "bairro_anuente_credor",
        "cidade": "cidade_anuente_credor",
        "estado": "estado_anuente_credor",
        "numero": "numero_logradouro_anuente_credor",
    },
    "cep_advogado": {
        "logradouro": "logradouro_advogado",
        "bairro": "bairro_advogado",
        "cidade": "cidade_advogado",
        "estado": "estado_advogado",
        "numero": "numero_logradouro_advogado",
    },
    "cep_herdeiro": {
        "logradouro": "logradouro_herdeiro",
        "bairro": "bairro_herdeiro",
        "cidade": "cidade_herdeiro",
        "estado": "estado_herdeiro",
        "numero": "numero_logradouro_herdeiro",
    },
    "cep_conjuge_herdeiro": {
        "logradouro": "logradouro_conjuge_herdeiro",
        "bairro": "bairro_conjuge_herdeiro",
        "cidade": "cidade_conjuge_herdeiro",
        "estado": "estado_conjuge_herdeiro",
        "numero": "numero_logradouro_conjuge_herdeiro",
    },
}

VIACEP_URL = "https://viacep.com.br/ws/{cep}/json/"

CEP_FIELDS = frozenset(CEP_LOOKUP_GROUPS.keys())

PHONE_FIELDS = frozenset(
    {
        "telefone_credor",
        "telefone_conjuge_credor",
        "telefone_anuente_credor",
        "telefone_herdeiro",
        "telefone_conjuge_herdeiro",
    }
)

UF_SELECT_FIELDS = frozenset(
    {
        "estado_credor",
        "estado_conjuge_credor",
        "estado_anuente_credor",
        "estado_advogado",
        "estado_herdeiro",
        "estado_conjuge_herdeiro",
    }
)

SELECT_BY_KEY: dict[str, list[dict[str, str]]] = {
    "procuradoria_tipo": _PROCURADORIA_TIPO,
    "estado_civil_credor": _ESTADO_CIVIL,
    "regime_conjuge_credor": _REGIME_CONJUGE,
    "nacionalidade_credor": _NACIONALIDADE,
    "credor_falecido": _SIM_NAO,
    "anuente_credor": _SIM_NAO,
    "interveniente_anuente": _SIM_NAO,
    "tipo_conta_credor": _TIPO_CONTA,
    "tem_chave_pix_credor": _SIM_NAO,
    "nacionalidade_conjuge_credor": _NACIONALIDADE,
    "tipo_conta_conjuge_credor": _TIPO_CONTA,
    "tem_chave_pix_conjuge_credor": _SIM_NAO,
    "estado_civil_anuente_credor": _ESTADO_CIVIL,
    "nacionalidade_anuente_credor": _NACIONALIDADE,
    "herdeiro_habilitado": _HERDEIRO_HABILITADO,
    "nacionalidade_herdeiro": _NACIONALIDADE,
    "estado_civil_herdeiro": _ESTADO_CIVIL,
    "regime_herdeiro_conjuge": _REGIME_CONJUGE,
    "herdeiro_cedente": _SIM_NAO,
    "tipo_conta_herdeiro": _TIPO_CONTA,
    "tem_chave_pix_herdeiro": _SIM_NAO,
    "nacionalidade_conjuge_herdeiro": _NACIONALIDADE,
    "tipo_conta_conjuge_herdeiro": _TIPO_CONTA,
    "tem_chave_pix_conjuge_herdeiro": _SIM_NAO,
    "mencionar": _MENCIONAR,
}

for uf_key in UF_SELECT_FIELDS:
    SELECT_BY_KEY[uf_key] = _UF

# Campos espelhados com mesmo sufixo
for prefix in (
    "estado_civil_",
    "nacionalidade_",
    "tipo_conta_",
    "tem_chave_pix_",
    "regime_",
):
    pass  # chaves explícitas acima


def widget_for_field(key: str) -> dict[str, Any]:
    required = key in LEGACY_REQUIRED_FIELDS

    def _finalize(widget: dict[str, Any]) -> dict[str, Any]:
        out = dict(widget)
        if required:
            out["required"] = True
        return out

    if key in METADATA_FIELDS:
        if key in PERCENT_FIELDS:
            return _finalize({"type": "percent", "readonly": True})
        return _finalize({"type": "readonly"})
    if key == "procuradoria":
        return _finalize({"type": "readonly", "linked_from": "procuradoria_tipo"})
    if key == "template":
        return _finalize({"type": "readonly"})
    if key == "cessionaria":
        return _finalize({"type": "cessionaria_select"})
    if key in DATE_FIELDS:
        return _finalize({"type": "date"})
    if key in MONEY_FIELDS:
        widget: dict[str, Any] = {"type": "money"}
        if key in MONEY_SUM_FIELDS:
            widget["sum_into"] = "total_da_requisicao"
        if key == "total_da_requisicao":
            widget["readonly"] = True
        return _finalize(widget)
    if key in PERCENT_FIELDS:
        readonly_percent = {
            "honorarios_reservados",
            "percentual_nao_cedido",
            "percentual_de_compra",
        }
        return _finalize(
            {
                "type": "percent",
                "readonly": key in readonly_percent,
                "calculated_from": "percentual_honorarios"
                if key == "percentual_de_compra"
                else None,
            }
        )
    if key in CPF_FIELDS:
        return _finalize({"type": "cpf"})
    if key in CEP_FIELDS:
        return _finalize({"type": "cep"})
    if key in PHONE_FIELDS:
        return _finalize({"type": "phone"})
    if key in SELECT_BY_KEY:
        return _finalize({"type": "select", "options": SELECT_BY_KEY[key]})
    if key.endswith("_credor") and key.startswith("uf_"):
        return _finalize({"type": "text", "maxlength": 2})
    if key.endswith("_herdeiro") and key.startswith("uf_"):
        return _finalize({"type": "text", "maxlength": 2})
    if key == "numero_logradouro_credor" or key.endswith("_logradouro_") or "numero_logradouro" in key:
        return _finalize({"type": "number"})
    return _finalize({"type": "text"})


def attach_widgets_to_fields(fields: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for field in fields:
        item = dict(field)
        key = item.get("key") or ""
        item["widget"] = widget_for_field(str(key))
        out.append(item)
    return out


def widgets_payload() -> dict[str, Any]:
    return {
        "procuradoria_by_tipo": PROCURADORIA_BY_TIPO,
        "money_sum_fields": sorted(MONEY_SUM_FIELDS),
        "cep_lookup_groups": CEP_LOOKUP_GROUPS,
        "viacep_url": VIACEP_URL,
    }
