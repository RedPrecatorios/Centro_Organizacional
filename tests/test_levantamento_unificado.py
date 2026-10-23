# -*- coding: utf-8 -*-
from datetime import datetime, timezone

from messages_viewer.levantamento_processual import _normalize_payload_body
from messages_viewer.monday_board import map_si_monday_payload
from messages_viewer.monday_precheck import (
    _evaluate_item,
    _format_cnj,
    _is_rejeitado,
    _norm_inc,
    _norm_proc,
    _proc_search_values,
)
from messages_viewer.solicitacao_inclusao import _item_after_cutoff


def test_normalize_credor_nome_e_cpf():
    body, err = _normalize_payload_body(nome="Jose Silva", cpf="123.456.789-01")
    assert err is None
    assert body == {"nome": "Jose Silva", "cpf": "12345678901"}


def test_normalize_processo_incidente_nome():
    body, err = _normalize_payload_body(
        nome="Maria",
        processo="0017669-72.2021.8.26.0053",
        incidente="12",
    )
    assert err is None
    assert body["processo"].startswith("0017669")
    assert body["incidente"] == "12"
    assert body["nome"] == "Maria"


def test_normalize_processo_incidente_cpf_sem_nome():
    body, err = _normalize_payload_body(
        cpf="12345678901",
        processo="0017669-72.2021.8.26.0053",
        incidente="4",
    )
    assert err is None
    assert "cpf" in body
    assert body["incidente"] == "4"


def test_normalize_rejeita_nome_sozinho():
    body, err = _normalize_payload_body(nome="Fulano de Tal")
    assert body is None
    assert err


def test_monday_norm_and_rejeitado():
    assert _norm_inc("012") == "12"
    assert _norm_proc("0017669-72.2021.8.26.0053") == "00176697220218260053"
    assert _is_rejeitado("Rejeitado")
    assert not _is_rejeitado("Enviado")
    assert _format_cnj("00026310520248260606") == "0002631-05.2024.8.26.0606"
    vals = _proc_search_values("0002631-05.2024.8.26.0606")
    assert "0002631-05.2024.8.26.0606" in vals
    assert "00026310520248260606" in vals


def test_evaluate_item_with_buyer_is_em_tratamento():
    col_ids = {
        "comprador": "c1",
        "proposta_enviada": "s1",
    }
    item = {
        "id": "123",
        "by_id": {
            "c1": {"text": "Fulano Comprador"},
            "s1": {"text": "Enviado"},
        },
    }
    out = _evaluate_item(
        item,
        col_ids,
        requester_email="a@x.com",
        requester_name="A",
        assign_on_rejeitado=False,
    )
    assert out["outcome"] == "ja_atribuido"
    assert out["incluir"] is False
    assert out["comprador"] == "Fulano Comprador"
    assert "Em tratamento" in out["label"]


def test_si_lookup_atribuido_is_em_tratamento():
    out = map_si_monday_payload(
        {
            "processo": "0002631-05.2024.8.26.0606",
            "incidente": "1",
            "inserido": True,
            "atribuido": True,
            "comprador": "Telma Santana",
            "compradores": ["Telma Santana"],
            "itens": [
                {
                    "item_id": "123",
                    "incidente": "1",
                    "atribuido": True,
                    "comprador": "Telma Santana",
                }
            ],
        },
        processo="0002631-05.2024.8.26.0606",
        incidente="1",
    )
    assert out is not None
    assert out["outcome"] == "ja_atribuido"
    assert out["incluir"] is False
    assert out["comprador"] == "Telma Santana"
    assert "Em tratamento" in out["label"]


def test_si_lookup_rejeitado_permite_inclusao():
    out = map_si_monday_payload(
        {
            "inserido": True,
            "atribuido": True,
            "comprador": "Telma Santana",
            "compradores": ["Telma Santana"],
            "proposta_enviada": "Rejeitado",
            "itens": [
                {
                    "item_id": "123",
                    "incidente": "1",
                    "atribuido": True,
                    "comprador": "Telma Santana",
                    "proposta_enviada": "Rejeitado",
                }
            ],
        },
        processo="0002631-05.2024.8.26.0606",
        incidente="1",
    )
    assert out is not None
    assert out["outcome"] == "rejeitado"
    assert out["incluir"] is True
    assert out["comprador"] == "Telma Santana"
    assert out["proposta_enviada"] == "Rejeitado"
    assert out["label"] == "Disponível"


def test_si_lookup_recusado_sem_motivo_pode_trocar():
    out = map_si_monday_payload(
        {
            "inserido": True,
            "atribuido": False,
            "comprador": "Clauber Souza",
            "proposta_enviada": "Recusado (Justificar)",
            "motivo_retirada": None,
            "pode_trocar_comprador": True,
            "mensagem_usuario": None,
            "itens": [
                {
                    "item_id": "1",
                    "incidente": "14",
                    "atribuido": False,
                    "comprador": "Clauber Souza",
                    "proposta_enviada": "Recusado (Justificar)",
                    "motivo_retirada": None,
                    "pode_trocar_comprador": True,
                }
            ],
        },
        processo="0021574-22.2020.8.26.0053",
        incidente="14",
    )
    assert out["outcome"] == "rejeitado"
    assert out["incluir"] is True
    assert out["label"] == "Disponível"
    assert out["pode_trocar_comprador"] is True
    assert out["mensagem_usuario"] is None


def test_si_lookup_recusado_penhorado_mantem_comprador():
    out = map_si_monday_payload(
        {
            "inserido": True,
            "atribuido": True,
            "comprador": "Amanda Cristina",
            "proposta_enviada": "Recusado (Justificar)",
            "motivo_retirada": "Penhorado",
            "pode_trocar_comprador": False,
            "mensagem_usuario": None,
            "itens": [
                {
                    "item_id": "1",
                    "incidente": "1",
                    "atribuido": True,
                    "comprador": "Amanda Cristina",
                    "proposta_enviada": "Recusado (Justificar)",
                    "motivo_retirada": "Penhorado",
                    "pode_trocar_comprador": False,
                }
            ],
        },
        processo="0001234-56.2020.8.26.0100",
        incidente="1",
    )
    assert out["outcome"] == "ja_atribuido"
    assert out["incluir"] is False
    assert out["motivo_retirada"] == "Penhorado"
    assert "Em tratamento" in out["label"]


def test_si_lookup_inapto_compra_mostra_mensagem():
    msg = (
        "Motivo Retirada: Inapto p/ Compra. Confirme com o jurídico "
        "ou envie um e-mail para confirmar a atribuição do caso."
    )
    out = map_si_monday_payload(
        {
            "inserido": True,
            "atribuido": True,
            "comprador": "Amanda Cristina",
            "proposta_enviada": "Recusado (Justificar)",
            "motivo_retirada": "Inapto p/ Compra",
            "pode_trocar_comprador": False,
            "mensagem_usuario": msg,
            "itens": [
                {
                    "item_id": "1",
                    "incidente": "1",
                    "atribuido": True,
                    "comprador": "Amanda Cristina",
                    "proposta_enviada": "Recusado (Justificar)",
                    "motivo_retirada": "Inapto p/ Compra",
                    "pode_trocar_comprador": False,
                }
            ],
        },
        processo="0001234-56.2020.8.26.0100",
        incidente="1",
    )
    assert out["outcome"] == "ja_atribuido"
    assert out["incluir"] is False
    assert out["mensagem_usuario"] == msg


def test_si_lookup_sem_comprador_segue_livre():
    out = map_si_monday_payload(
        {
            "inserido": True,
            "atribuido": False,
            "comprador": None,
            "compradores": [],
            "itens": [{"item_id": "1", "incidente": "1", "atribuido": False}],
        },
        processo="0001234-56.2020.8.26.0100",
        incidente="1",
    )
    assert out is not None
    assert out["outcome"] == "livre"
    assert out["incluir"] is True


def test_si_lookup_ausente_segue_livre():
    out = map_si_monday_payload(
        {"inserido": False, "atribuido": False, "comprador": None, "itens": []},
        processo="0001234-56.2020.8.26.0100",
        incidente="4",
    )
    assert out is not None
    assert out["outcome"] == "livre"
    assert out["incluir"] is True


def test_si_lookup_varios_compradores():
    out = map_si_monday_payload(
        {
            "inserido": True,
            "atribuido": True,
            "comprador": None,
            "compradores": ["Ana", "Bia"],
            "itens": [],
        },
        processo="0001234-56.2020.8.26.0100",
        incidente="",
    )
    assert out is not None
    assert out["outcome"] == "ja_atribuido"
    assert out["comprador"] == "Ana, Bia"


def test_item_after_cutoff_hides_legacy():
    cutoff = datetime(2026, 10, 20, 19, 0, tzinfo=timezone.utc)
    old = {"created_at": "2026-10-16T14:15:00+00:00"}
    new = {"created_at": "2026-10-20T20:00:00+00:00"}
    assert not _item_after_cutoff(old, cutoff)
    assert _item_after_cutoff(new, cutoff)
    assert _item_after_cutoff(new, None)


def test_precheck_graphql_failure_falls_through_to_livre(monkeypatch):
    from messages_viewer import monday_precheck as mp

    monkeypatch.setattr(mp, "is_configured", lambda: True)
    monkeypatch.setattr(mp, "lookup_si_processo", lambda *a, **k: None)
    monkeypatch.setattr(
        mp,
        "resolve_column_ids",
        lambda: (_ for _ in ()).throw(mp.MondayPrecheckError("401 Unauthorized")),
    )
    out = mp.precheck_casos(
        [{"processo": "0018269-64.2019.8.26.0053", "incidente": "1"}],
        user=None,
    )
    assert out["ok"] is True
    assert out["items"]
    assert out["items"][0]["incluir"] is True
    assert out["items"][0]["outcome"] == "livre"


def test_incluir_calls_criar_when_precheck_fails(monkeypatch):
    from messages_viewer import monday_precheck as mp

    monkeypatch.setattr(mp, "is_configured", lambda: True)
    monkeypatch.setattr(mp, "lookup_si_processo", lambda *a, **k: None)
    monkeypatch.setattr(
        mp,
        "resolve_column_ids",
        lambda: (_ for _ in ()).throw(mp.MondayPrecheckError("401 Unauthorized")),
    )
    called = {}

    def fake_criar(payload, user, skip_cooldown=False):
        called["payload"] = payload
        return {"ok": True, "id": "sol-1", "status": "EM_INCLUSAO"}, 202

    monkeypatch.setattr(
        "messages_viewer.solicitacao_inclusao.criar",
        fake_criar,
    )
    out, code = mp.incluir_apos_precheck(
        [{"processo": "0018269-64.2019.8.26.0053", "incidente": "1", "cpf_credor": "26521949845"}],
        user=None,
    )
    assert code == 202 or code == 200
    assert called["payload"]["processo"] == "0018269-64.2019.8.26.0053"
    assert out["items"][0]["outcome"] == "enviado"
    assert out["items"][0]["solicitacao"]["id"] == "sol-1"
