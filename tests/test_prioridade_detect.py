# -*- coding: utf-8 -*-
import json
import sys
from pathlib import Path

_ROOT = (
    Path(__file__).resolve().parents[1]
    / "ATUALIZACAO_CALCULO-main"
    / "ATUALIZACAO_CALCULO-main"
)
sys.path.insert(0, str(_ROOT))

from manager.prioridade_detect import (  # noqa: E402
    detect_from_analise_outputs,
    detect_from_coleta_depre,
    resolve_planilha_prioridade,
)


def test_coleta_valor_pago_com_saldo_e_priori():
    assert detect_from_coleta_depre(1500.0, 800.0) is True
    assert detect_from_coleta_depre(0, 800.0) is False
    assert detect_from_coleta_depre(1500.0, 0) is False
    assert detect_from_coleta_depre(None, None) is False


def test_analise_fanout_prioridade(tmp_path, monkeypatch):
    monkeypatch.setenv("REFACTOR_TJSP_PATH", str(tmp_path))
    json_dir = tmp_path / "output" / "json"
    json_dir.mkdir(parents=True)
    payload = {
        "context": {
            "extra_infos": {
                "prioridade": True,
                "prioridade_status": "extraido",
            }
        }
    }
    (json_dir / "0001234-56.2020.8.26.0050_12_fanout.json").write_text(
        json.dumps(payload), encoding="utf-8"
    )
    assert detect_from_analise_outputs("0001234-56.2020.8.26.0050", "12") is True
    assert detect_from_analise_outputs("0001234-56.2020.8.26.0050", "99") is False


def test_analise_sem_saldo_nao_e_priori(tmp_path, monkeypatch):
    monkeypatch.setenv("REFACTOR_TJSP_PATH", str(tmp_path))
    extra_dir = tmp_path / "output" / "depre_prioridade"
    extra_dir.mkdir(parents=True)
    (extra_dir / "0001234-56.2020.8.26.0050_12_saldo_extracao.json").write_text(
        json.dumps({"status": "sem_saldo", "extracao": {"valor_principal": "1"}}),
        encoding="utf-8",
    )
    assert detect_from_analise_outputs("0001234-56.2020.8.26.0050", "12") is False


def test_analise_extracao_extraido(tmp_path, monkeypatch):
    monkeypatch.setenv("REFACTOR_TJSP_PATH", str(tmp_path))
    extra_dir = tmp_path / "output" / "depre_prioridade"
    extra_dir.mkdir(parents=True)
    (extra_dir / "0005283-73.2022.8.26.0053_15_saldo_extracao.json").write_text(
        json.dumps(
            {
                "status": "extraido",
                "extracao": {
                    "valor_principal": "119.165.44",
                    "juros_moratorios": "32.259.78",
                },
            }
        ),
        encoding="utf-8",
    )
    assert (
        resolve_planilha_prioridade(
            explicit=False,
            processo="0005283-73.2022.8.26.0053",
            incidente="15",
        )
        is True
    )
    assert (
        resolve_planilha_prioridade(
            explicit=False,
            valor_pago=100.0,
            saldo=50.0,
        )
        is True
    )
    assert resolve_planilha_prioridade(explicit=True) is True
    assert resolve_planilha_prioridade(explicit=False, valor_pago=0, saldo=10) is False
