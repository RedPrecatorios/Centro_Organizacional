# -*- coding: utf-8 -*-
"""Testes de paridade (mapeamentos) — sem invocar motor PHP nem expor dados reais."""
from __future__ import annotations

import unittest
from unittest.mock import patch

from messages_viewer.contratos_adapter import _herdeiro_to_row, ficha_to_autopreca_row
from messages_viewer.contratos_tipos import listar_tipos, template_from_cessionaria
from messages_viewer.contratos_validacao import validar_contratos_geracao, validar_herdeiros_save


class ContratosParidadeTest(unittest.TestCase):
    def test_template_red_gt_nenhum(self):
        self.assertEqual(
            template_from_cessionaria("RED PRECATÓRIOS E INTERMEDIAÇÃO LTDA"), "RED"
        )
        self.assertEqual(
            template_from_cessionaria("GT PRECATÓRIOS E ASSESSORIA LTDA"), "GT"
        )
        self.assertEqual(template_from_cessionaria("ATOS SECURITIZADORA LTDA"), "NENHUM")

    def test_preca_contratos_ids(self):
        ids = {c["id"] for c in listar_tipos(tipo="preca", recupere=False)}
        self.assertIn("0", ids)
        self.assertIn("1", ids)
        self.assertIn("26", ids)

    def test_recupere_contratos(self):
        ids = {c["id"] for c in listar_tipos(tipo="preca", recupere=True)}
        self.assertEqual(ids, {"1", "2", "3", "4"})

    def test_rpv_contratos(self):
        ids = {c["id"] for c in listar_tipos(tipo="rpv", recupere=False)}
        self.assertIn("0", ids)
        self.assertIn("10", ids)

    def test_adapter_money_and_percent(self):
        row = ficha_to_autopreca_row(
            {
                "cessionaria": "RED PRECATÓRIOS E INTERMEDIAÇÃO LTDA",
                "principal_liquido": "1234.56",
                "percentual_honorarios": "25",
                "cep_anuente_credor": "01001-000",
                "interveniente_anuente": "sim",
            }
        )
        self.assertTrue(row["principal_liquido"].startswith("R$"))
        self.assertTrue(row["percentual_honorarios"].endswith("%"))
        self.assertEqual(row["cep_anuente"], "01001-000")
        self.assertEqual(row["interveniente_anuente"], 1)
        self.assertEqual(row["template"], "RED")

    @patch("messages_viewer.contratos_validacao.carregar_ficha")
    def test_validacao_bloqueia_contrato_unico_sem_cpf(self, mock_carregar):
        mock_carregar.return_value = (
            {
                "ok": True,
                "dados": {
                    "cumprimento_de_sentenca": "123",
                    "incidente": "1",
                    "nome_credor": "JOAO",
                    "cessionaria": "RED PRECATÓRIOS E INTERMEDIAÇÃO LTDA",
                    "percentual_honorarios": "10%",
                },
                "herdeiros": [],
                "herdeiros_abertos": False,
                "herdeiros_validado": False,
            },
            200,
        )
        out, code = validar_contratos_geracao(
            cumprimento_de_sentenca="123",
            incidente="1",
            nome_credor="JOAO",
            tipo="preca",
            contrato_id="24",
            cessionaria="RED PRECATÓRIOS E INTERMEDIAÇÃO LTDA",
        )
        self.assertEqual(code, 200)
        self.assertFalse(out["valido"])
        self.assertTrue(out["bloqueado"])
        self.assertEqual(out["contratos"][0]["label"], "DECLARAÇÃO DE ENDEREÇO")
        keys = {f["key"] for f in out["contratos"][0]["campos_faltantes"]}
        self.assertIn("cpf_credor", keys)

    @patch("messages_viewer.contratos_validacao.carregar_ficha")
    def test_validacao_usa_dados_override_do_formulario(self, mock_carregar):
        mock_carregar.return_value = (
            {
                "ok": True,
                "dados": {
                    "cumprimento_de_sentenca": "123",
                    "incidente": "1",
                    "nome_credor": "JOAO",
                },
                "herdeiros": [],
                "herdeiros_abertos": False,
                "herdeiros_validado": False,
            },
            200,
        )
        out, code = validar_contratos_geracao(
            cumprimento_de_sentenca="123",
            incidente="1",
            nome_credor="JOAO",
            tipo="preca",
            contrato_id="24",
            cessionaria="RED PRECATÓRIOS E INTERMEDIAÇÃO LTDA",
            dados_override={
                "cpf_credor": "12345678901",
                "percentual_honorarios": "30%",
                "credor_falecido": "não",
                "anuente_credor": "não",
                "procuradoria_tipo": "1",
                "estado_civil_credor": "solteiro(a)",
                "nacionalidade_credor": "brasileira",
            },
        )
        self.assertEqual(code, 200)
        self.assertTrue(out["valido"])
        endereco = next(c for c in out["contratos"] if c["label"] == "DECLARAÇÃO DE ENDEREÇO")
        self.assertTrue(endereco["ok"])

    @patch("messages_viewer.contratos_validacao.carregar_ficha")
    def test_validacao_nao_exige_campos_advogado(self, mock_carregar):
        mock_carregar.return_value = (
            {
                "ok": True,
                "dados": {
                    "cumprimento_de_sentenca": "123",
                    "incidente": "1",
                    "nome_credor": "JOAO",
                    "cpf_credor": "000",
                    "cessionaria": "RED PRECATÓRIOS E INTERMEDIAÇÃO LTDA",
                    "percentual_honorarios": "10%",
                    "estado_civil_credor": "solteiro(a)",
                    "nacionalidade_credor": "brasileira",
                    "credor_falecido": "não",
                    "anuente_credor": "não",
                },
                "herdeiros": [],
                "herdeiros_abertos": False,
                "herdeiros_validado": False,
            },
            200,
        )
        out, code = validar_contratos_geracao(
            cumprimento_de_sentenca="123",
            incidente="1",
            nome_credor="JOAO",
            tipo="preca",
            contrato_id="9",
            cessionaria="RED PRECATÓRIOS E INTERMEDIAÇÃO LTDA",
        )
        self.assertEqual(code, 200)
        self.assertTrue(out["valido"])
        keys = {f["key"] for f in out.get("campos_faltantes_agregados", [])}
        self.assertNotIn("nome_advogado", keys)
        self.assertNotIn("cpf_advogado", keys)

    def test_widgets_procuradoria_and_money(self):
        from messages_viewer.pre_analise_ficha_widgets import widget_for_field

        self.assertEqual(widget_for_field("procuradoria_tipo")["type"], "select")
        self.assertEqual(widget_for_field("procuradoria")["type"], "readonly")
        self.assertEqual(widget_for_field("data_de_nascimento_credor")["type"], "date")
        self.assertEqual(widget_for_field("principal_liquido")["type"], "money")
        self.assertEqual(widget_for_field("percentual_honorarios")["type"], "percent")
        self.assertTrue(widget_for_field("percentual_de_compra")["readonly"])
        self.assertTrue(widget_for_field("nome_credor")["required"])
        self.assertFalse(widget_for_field("nome_advogado").get("required"))

    def test_widgets_cep_lookup_groups(self):
        from messages_viewer.pre_analise_ficha_widgets import CEP_LOOKUP_GROUPS, widgets_payload

        self.assertIn("cep_credor", CEP_LOOKUP_GROUPS)
        self.assertEqual(CEP_LOOKUP_GROUPS["cep_credor"]["cidade"], "cidade_credor")
        payload = widgets_payload()
        self.assertIn("cep_lookup_groups", payload)
        self.assertIn("viacep_url", payload)

    def _herdeiro_base(self, **overrides):
        dados = {
            "herdeiro_habilitado": "sim",
            "nome_herdeiro": "MARIA SILVA",
            "cpf_herdeiro": "12345678901",
            "parentesco_herdeiro": "filho(a)",
            "nacionalidade_herdeiro": "brasileira",
            "estado_civil_herdeiro": "solteiro(a)",
            "percentual_detido": "100",
            "herdeiro_cedente": "não",
            "percentual_honorarios_herdeiro": "30",
            "percentual_cedido": "0",
        }
        dados.update(overrides)
        return {"dados": dados}

    def test_validar_herdeiros_soma_100(self):
        issues = validar_herdeiros_save(
            [
                self._herdeiro_base(percentual_detido="60"),
                self._herdeiro_base(
                    cpf_herdeiro="98765432100",
                    nome_herdeiro="JOSE SILVA",
                    percentual_detido="30",
                ),
            ]
        )
        self.assertTrue(any("100" in i for i in issues))

    def test_validar_herdeiros_cpf_duplicado(self):
        issues = validar_herdeiros_save(
            [
                self._herdeiro_base(percentual_detido="50"),
                self._herdeiro_base(
                    nome_herdeiro="OUTRO NOME",
                    percentual_detido="50",
                ),
            ]
        )
        self.assertTrue(any("duplicado" in i.lower() for i in issues))

    def test_validar_herdeiros_cedente_exige_banco(self):
        issues = validar_herdeiros_save(
            [
                self._herdeiro_base(
                    herdeiro_cedente="sim",
                    percentual_cedido="70",
                    valor_da_proposta_herdeiro="R$ 1.000,00",
                )
            ]
        )
        self.assertTrue(any("banco" in i.lower() for i in issues))

    def test_validar_herdeiros_ok_completo(self):
        issues = validar_herdeiros_save([self._herdeiro_base()])
        self.assertEqual(issues, [])

    def test_herdeiro_to_row_recalc_e_percentuais(self):
        row = _herdeiro_to_row(
            {
                "nome_herdeiro": "maria",
                "percentual_detido": "50",
                "percentual_honorarios_herdeiro": "30",
                "herdeiro_cedente": "sim",
                "percentual_cedido": "20",
                "mencionar": "sim",
            }
        )
        self.assertEqual(row["nome_herdeiro"], "MARIA")
        self.assertTrue(row["percentual_detido"].endswith("%"))
        self.assertTrue(row["honorarios_reservados"].endswith("%"))
        self.assertEqual(row["mencionar"], "sim")

    @patch("messages_viewer.contratos_validacao.carregar_ficha")
    def test_validacao_bloqueia_herdeiros_invalidos(self, mock_carregar):
        mock_carregar.return_value = (
            {
                "ok": True,
                "dados": {
                    "cumprimento_de_sentenca": "123",
                    "incidente": "1",
                    "nome_credor": "JOAO",
                    "cpf_credor": "000",
                    "cessionaria": "RED PRECATÓRIOS E INTERMEDIAÇÃO LTDA",
                    "percentual_honorarios": "10%",
                    "estado_civil_credor": "solteiro(a)",
                    "nacionalidade_credor": "brasileira",
                    "credor_falecido": "não",
                    "anuente_credor": "não",
                },
                "herdeiros": [self._herdeiro_base(percentual_detido="50")],
                "herdeiros_abertos": True,
                "herdeiros_validado": True,
            },
            200,
        )
        out, code = validar_contratos_geracao(
            cumprimento_de_sentenca="123",
            incidente="1",
            nome_credor="JOAO",
            tipo="preca",
            contrato_id="0",
            cessionaria="RED PRECATÓRIOS E INTERMEDIAÇÃO LTDA",
        )
        self.assertEqual(code, 200)
        self.assertFalse(out["valido"])
        self.assertTrue(out["bloqueado"])
        self.assertTrue(any("100" in b for b in out.get("bloqueios", [])))

    @patch("messages_viewer.contratos_validacao.carregar_ficha")
    def test_validacao_herdeiros_validados_completo(self, mock_carregar):
        mock_carregar.return_value = (
            {
                "ok": True,
                "dados": {
                    "cumprimento_de_sentenca": "123",
                    "incidente": "1",
                    "nome_credor": "JOAO",
                    "cpf_credor": "000",
                    "cessionaria": "RED PRECATÓRIOS E INTERMEDIAÇÃO LTDA",
                    "percentual_honorarios": "10%",
                    "estado_civil_credor": "solteiro(a)",
                    "nacionalidade_credor": "brasileira",
                    "credor_falecido": "não",
                    "anuente_credor": "não",
                },
                "herdeiros": [self._herdeiro_base()],
                "herdeiros_abertos": True,
                "herdeiros_validado": True,
            },
            200,
        )
        out, code = validar_contratos_geracao(
            cumprimento_de_sentenca="123",
            incidente="1",
            nome_credor="JOAO",
            tipo="preca",
            contrato_id="24",
            cessionaria="RED PRECATÓRIOS E INTERMEDIAÇÃO LTDA",
        )
        self.assertEqual(code, 200)
        self.assertTrue(out["valido"])


if __name__ == "__main__":
    unittest.main()
