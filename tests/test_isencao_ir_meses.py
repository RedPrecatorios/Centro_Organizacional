# -*- coding: utf-8 -*-
import sys
import unittest
from pathlib import Path

_ROOT = (
    Path(__file__).resolve().parents[1]
    / "ATUALIZACAO_CALCULO-main"
    / "ATUALIZACAO_CALCULO-main"
)
sys.path.insert(0, str(_ROOT))

from calculation_automation.sheet_constants import (  # noqa: E402
    LIMITE_ISENCAO_IR_TOTAL_LIQUIDO,
    NUMERO_MESES_ISENCAO_IR,
    deve_aplicar_meses_isencao_ir,
)


class TestIsencaoIrMeses(unittest.TestCase):
    def test_aplica_quando_total_liquido_ate_200_mil(self):
        self.assertTrue(deve_aplicar_meses_isencao_ir(199_999.99, 36))
        self.assertTrue(deve_aplicar_meses_isencao_ir(200_000, 12))
        self.assertTrue(deve_aplicar_meses_isencao_ir(1, 9))

    def test_nao_aplica_acima_de_200_mil_nem_zero(self):
        self.assertFalse(deve_aplicar_meses_isencao_ir(200_000.01, 36))
        self.assertFalse(deve_aplicar_meses_isencao_ir(0, 36))
        self.assertFalse(deve_aplicar_meses_isencao_ir(None, 36))

    def test_nao_reaplica_se_ja_tem_1000_meses(self):
        self.assertFalse(deve_aplicar_meses_isencao_ir(50_000, 1000))
        self.assertFalse(deve_aplicar_meses_isencao_ir(50_000, 1001))

    def test_constantes(self):
        self.assertEqual(LIMITE_ISENCAO_IR_TOTAL_LIQUIDO, 200_000.0)
        self.assertEqual(NUMERO_MESES_ISENCAO_IR, 1000)


if __name__ == "__main__":
    unittest.main()
