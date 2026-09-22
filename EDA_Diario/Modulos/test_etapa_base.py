"""
Testes da Etapa 0 (enriquecimento pela base) — sem MySQL.
Executar: python3 EDA_Diario/Modulos/test_etapa_base.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))

from modulo_etapa_base import (  # noqa: E402
    COL_BLACKLIST,
    COL_DATA_BL,
    COL_MOTIVO_BL,
    _contatos_para_linha,
    _indice_contatos_base,
    filtrar_exportacao_blacklist,
    motivo_exclui_caso,
)


def _assert(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)


def test_indice_e_match_processo() -> None:
    procs = [
        {
            "processo_id": 1,
            "cpf": "12345678909",
            "numero_processo": "PROC-1",
            "numero_incidente": "1",
            "telefones_sms": "11999990000",
            "telefones_hsm": "",
            "emails": "a@b.com",
        },
        {
            "processo_id": 2,
            "cpf": "12345678909",
            "numero_processo": "PROC-2",
            "numero_incidente": "2",
            "telefones_sms": "11888887777",
            "telefones_hsm": "11777776666",
            "emails": "",
        },
    ]
    by_cpf, by_chave = _indice_contatos_base(procs)
    _assert("12345678909" in by_cpf, by_cpf.keys())
    _assert(len(by_cpf["12345678909"]) == 2, by_cpf)

    t, e, h, achou = _contatos_para_linha(
        "12345678909", "PROC-1", "1", by_cpf, by_chave
    )
    _assert(achou, "deveria achar")
    _assert(t == ["11999990000"], t)
    _assert(e == ["a@b.com"], e)
    _assert(h == [], h)

    t2, e2, h2, achou2 = _contatos_para_linha(
        "12345678909", "PROC-X", "9", by_cpf, by_chave
    )
    _assert(achou2, "fallback CPF")
    _assert("11999990000" in t2 and "11888887777" in t2, t2)
    _assert("11777776666" in h2, h2)


def test_cpf_ausente() -> None:
    t, e, h, achou = _contatos_para_linha("00000000000", "P", "1", {}, {})
    _assert(not achou, "não deveria achar")
    _assert(t == [] and e == [] and h == [], (t, e, h))


def test_colunas_bl_constantes() -> None:
    _assert(COL_BLACKLIST == "Blacklist", COL_BLACKLIST)
    _assert(COL_MOTIVO_BL == "Motivo_blacklist", COL_MOTIVO_BL)
    _assert(COL_DATA_BL == "Data_inclusao_blacklist", COL_DATA_BL)


def test_motivo_exclui_caso() -> None:
    _assert(motivo_exclui_caso("Retirar do Mailing"), "retirar")
    _assert(motivo_exclui_caso("Remover do Mailing_SysCall"), "remover")
    _assert(motivo_exclui_caso("Inapto"), "inapto")
    _assert(motivo_exclui_caso("Cedido"), "cedido")
    _assert(motivo_exclui_caso("Blacklist"), "blacklist")
    _assert(motivo_exclui_caso("CPF; Blacklist; outro"), "multi")
    _assert(motivo_exclui_caso("Importado de BACKUP DELETED PROCESSES"), "backup")
    _assert(motivo_exclui_caso("Solicitou remoção"), "solicitou")
    _assert(motivo_exclui_caso("PF"), "pf")
    _assert(motivo_exclui_caso("Fez Acordo"), "fez acordo")
    _assert(motivo_exclui_caso("Acordo"), "acordo")
    _assert(motivo_exclui_caso("pediu pra retirar duas vezes"), "pediu")
    _assert(motivo_exclui_caso("Bounce"), "bounce")
    _assert(motivo_exclui_caso("Invalido"), "invalido")
    _assert(motivo_exclui_caso("Investidor"), "investidor")
    _assert(motivo_exclui_caso("Investidora"), "investidora")
    _assert(motivo_exclui_caso("Teste"), "teste")
    _assert(not motivo_exclui_caso("Engano"), "engano não exclui caso")
    _assert(not motivo_exclui_caso("Telefone Incorreto"), "TI não exclui caso")
    _assert(not motivo_exclui_caso("Deixou Recado_SysCall"), "recado não exclui caso")
    _assert(not motivo_exclui_caso("Sem Interesse"), "sem interesse fica")
    _assert(not motivo_exclui_caso(""), "vazio")


def test_filtrar_exportacao_blacklist() -> None:
    df = pd.DataFrame(
        {
            "CPF": ["111", "222", "333", "444"],
            COL_MOTIVO_BL: [
                "Telefone Incorreto",
                "Inapto",
                "Engano",
                "Solicitou remoção",
            ],
            COL_BLACKLIST: ["Sim", "Sim", "Sim", "Sim"],
        }
    )
    regs_t = [
        [("11999990000", False), ("11888887777", False)],
        [("11777776666", False)],
        [("11666665555", False)],
        [("11444443333", False)],
    ]
    regs_e = [
        [("a@x.com", False)],
        [("b@x.com", False)],
        [("engano@x.com", False), ("ok@x.com", False)],
        [("c@x.com", False)],
    ]
    regs_h = [[("11999990000", False)], [], [("11666665555", False)], []]

    bl = {
        "TELEFONE": {
            "11999990000",
            "11888887777",
            "11777776666",
            "11666665555",
            "11444443333",
        },
        "EMAIL": {
            "ENGANO@X.COM",
            "A@X.COM",
            "B@X.COM",
            "C@X.COM",
            "OK@X.COM",
        },
    }
    motivo_map = {
        ("TELEFONE", "11999990000"): "Telefone Incorreto",
        ("TELEFONE", "11888887777"): "Outro",
        ("TELEFONE", "11777776666"): "Inapto",
        ("EMAIL", "ENGANO@X.COM"): "Engano",
    }

    df_out, rt, re, rh, rsms, remail, stats = filtrar_exportacao_blacklist(
        df, regs_t, regs_e, regs_h, bl=bl, motivo_map=motivo_map
    )
    _assert(stats["casos_excluidos"] == 2, stats)  # Inapto + Solicitou remoção
    _assert(list(df_out["CPF"]) == ["111", "333"], list(df_out["CPF"]))
    _assert(stats["tels_incorretos_omitidos_sms"] == 1, stats)
    _assert(stats["emails_omitidos_aba"] == 1, stats)
    _assert(len(rt[0]) == 2, rt[0])
    _assert([t for t, _ in rsms[0]] == ["11888887777"], rsms[0])
    _assert([e for e, _ in re[1]] == ["engano@x.com", "ok@x.com"], re[1])
    _assert([e for e, _ in remail[1]] == ["ok@x.com"], remail[1])
    _assert(rh is not None and len(rh) == 2, rh)


if __name__ == "__main__":
    test_indice_e_match_processo()
    test_cpf_ausente()
    test_colunas_bl_constantes()
    test_motivo_exclui_caso()
    test_filtrar_exportacao_blacklist()
    print("OK test_etapa_base")
