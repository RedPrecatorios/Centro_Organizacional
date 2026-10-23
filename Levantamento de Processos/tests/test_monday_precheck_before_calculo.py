"""Pré-checagem Monday ocorre antes do REFACTOR / API de cálculo."""

from __future__ import annotations

from tjsp_pipeline.scraper.show_page import PrecatorioRecord

from api.services.orchestrator import _split_after_monday_precheck


def _rec(processo: str, incidente: str) -> PrecatorioRecord:
    return PrecatorioRecord(
        numero_de_processo=processo,
        numero_do_incidente=incidente,
        processo_codigo="X",
        url="",
        label="Precatório (CNJ)",
        classe="Precatório (CNJ)",
    )


def test_ja_atribuido_is_removed_from_enrich_and_marked_pronto():
    rec_lock = _rec("0002631-05.2024.8.26.0606", "1")
    rec_livre = _rec("0017669-72.2021.8.26.0053", "12")
    live = {
        "0002631-05.2024.8.26.0606|1": {
            "rec": rec_lock,
            "kind": "processando",
            "status": "processando",
            "stage": "encontrado",
            "record": {},
        },
        "0017669-72.2021.8.26.0053|12": {
            "rec": rec_livre,
            "kind": "processando",
            "status": "processando",
            "stage": "encontrado",
            "record": {},
        },
    }

    def lookup(processo: str, incidente: str):
        if "0002631" in processo:
            return {
                "outcome": "ja_atribuido",
                "comprador": "Fulano Comprador",
                "monday_item_id": "999",
                "label": "Em tratamento — Fulano Comprador",
            }
        return {"outcome": "livre", "incluir": True}

    remaining, skipped = _split_after_monday_precheck(
        [rec_lock, rec_livre],
        live,
        lookup=lookup,
        requerente="ERISVALDO",
        cpf="12345678901",
    )
    assert remaining == [rec_livre]
    assert skipped == [rec_lock]
    locked = live["0002631-05.2024.8.26.0606|1"]
    assert locked["kind"] == "apto"
    assert locked["stage"] == "pronto"
    recd = locked["record"]
    assert recd["monday_outcome"] == "ja_atribuido"
    assert recd["comprador"] == "Fulano Comprador"
    assert recd["skip_calculo"] is True
    assert recd["Requerente"] == "ERISVALDO"
    assert live["0017669-72.2021.8.26.0053|12"]["kind"] == "processando"


def test_lookup_error_keeps_case_for_calculo():
    rec = _rec("0017669-72.2021.8.26.0053", "4")
    live = {
        "0017669-72.2021.8.26.0053|4": {
            "rec": rec,
            "kind": "processando",
            "record": {},
        }
    }

    def boom(_p, _i):
        raise RuntimeError("monday down")

    remaining, skipped = _split_after_monday_precheck(
        [rec], live, lookup=boom
    )
    assert remaining == [rec]
    assert skipped == []
    assert live["0017669-72.2021.8.26.0053|4"]["kind"] == "processando"


def test_si_payload_atribuido_skips_calculo():
    mapped = {
        "outcome": "ja_atribuido",
        "comprador": "Telma Santana",
        "monday_item_id": "9",
        "label": "Em tratamento — Telma Santana",
        "incluir": False,
    }
    rec = _rec("0002631-05.2024.8.26.0606", "1")
    live = {
        "0002631-05.2024.8.26.0606|1": {
            "rec": rec,
            "kind": "processando",
            "record": {},
        }
    }
    remaining, skipped = _split_after_monday_precheck(
        [rec], live, lookup=lambda *_a, **_k: mapped
    )
    assert remaining == []
    assert skipped == [rec]
    assert live["0002631-05.2024.8.26.0606|1"]["kind"] == "apto"
    assert live["0002631-05.2024.8.26.0606|1"]["record"]["comprador"] == "Telma Santana"


def test_rejeitado_stays_in_enrich_and_keeps_buyer():
    mapped = {
        "outcome": "rejeitado",
        "comprador": "Telma Santana",
        "proposta_enviada": "Rejeitado",
        "label": "Com Telma Santana — Proposta Enviada = Rejeitado",
        "incluir": True,
    }
    rec = _rec("0002631-05.2024.8.26.0606", "1")
    live = {
        "0002631-05.2024.8.26.0606|1": {
            "rec": rec,
            "kind": "processando",
            "record": {},
        }
    }
    remaining, skipped = _split_after_monday_precheck(
        [rec], live, lookup=lambda *_a, **_k: mapped
    )
    assert remaining == [rec]
    assert skipped == []
    recd = live["0002631-05.2024.8.26.0606|1"]["record"]
    assert recd["monday_outcome"] == "rejeitado"
    assert recd["comprador"] == "Telma Santana"
    assert recd["proposta_enviada"] == "Rejeitado"


def test_ja_atribuido_keeps_mensagem_usuario():
    mapped = {
        "outcome": "ja_atribuido",
        "comprador": "Amanda Cristina",
        "proposta_enviada": "Recusado (Justificar)",
        "motivo_retirada": "Em grau de recurso",
        "mensagem_usuario": (
            "Motivo Retirada: Em grau de recurso. Confirme com o jurídico "
            "ou envie um e-mail para confirmar a atribuição do caso."
        ),
        "incluir": False,
        "label": "Em tratamento — Amanda Cristina",
    }
    rec = _rec("0001234-56.2020.8.26.0100", "1")
    live = {
        "0001234-56.2020.8.26.0100|1": {
            "rec": rec,
            "kind": "processando",
            "record": {},
        }
    }
    remaining, skipped = _split_after_monday_precheck(
        [rec], live, lookup=lambda *_a, **_k: mapped
    )
    assert remaining == []
    recd = live["0001234-56.2020.8.26.0100|1"]["record"]
    assert recd["skip_calculo"] is True
    assert recd["motivo_retirada"] == "Em grau de recurso"
    assert recd["mensagem_usuario"].startswith("Motivo Retirada: Em grau de recurso")
