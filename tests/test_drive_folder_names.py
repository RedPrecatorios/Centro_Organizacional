# -*- coding: utf-8 -*-
import sys
from pathlib import Path

_ROOT = (
    Path(__file__).resolve().parents[1]
    / "ATUALIZACAO_CALCULO-main"
    / "ATUALIZACAO_CALCULO-main"
)
sys.path.insert(0, str(_ROOT))

from google_api.drive import (  # noqa: E402
    _filename_indicates_prioridade,
    _texto_para_planilha,
    build_drive_file_name,
    pasta_processo_incidente_drive,
)


def test_incidente_vazio_nao_vira_none_no_nome():
    d = {"Processo": "0001234-56.2020.8.26.0050", "Incidente": None}
    assert pasta_processo_incidente_drive(d) == "0001234_56_2020_8_26_0050_sem_incidente"
    d["Incidente"] = "None"
    assert pasta_processo_incidente_drive(d) == "0001234_56_2020_8_26_0050_sem_incidente"


def test_texto_none_como_vazio():
    assert _texto_para_planilha(None) == ""
    assert _texto_para_planilha("None") == ""
    assert _texto_para_planilha("Maria") == "Maria"


def test_nome_ficheiro_drive():
    name = build_drive_file_name(
        {"Requerente": "Maria Silva", "Processo": "0001234-56.2020.8.26.0050", "Incidente": "12"}
    )
    assert name == "Maria Silva 0001234_12.xlsx"


def test_nome_ficheiro_drive_priori():
    d = {
        "Requerente": "Maria Silva",
        "Processo": "0001234-56.2020.8.26.0050",
        "Incidente": "12",
    }
    assert build_drive_file_name(d, prioridade=False) == "Maria Silva 0001234_12.xlsx"
    assert build_drive_file_name(d, prioridade=True) == "PRIORI Maria Silva 0001234_12.xlsx"


def test_filename_indicates_prioridade_aceita_espaco_e_underscore():
    assert _filename_indicates_prioridade("PRIORI Maria Silva 0001234_12.xlsx")
    assert _filename_indicates_prioridade("PRIORI_Maria.xlsx")
    assert _filename_indicates_prioridade("PRIORI_AUTO_Maria.xlsx")
    assert not _filename_indicates_prioridade("Maria Silva 0001234_12.xlsx")
