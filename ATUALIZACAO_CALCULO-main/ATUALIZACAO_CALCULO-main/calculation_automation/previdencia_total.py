# -*- coding: utf-8 -*-
"""Total de descontos/previdência a partir de precainfosnew (main_dict)."""
from __future__ import annotations


def _f(main_dict: dict, key: str) -> float:
    try:
        return float(main_dict.get(key) or 0)
    except (TypeError, ValueError):
        return 0.0


def previdencia_total_from_main_dict(main_dict: dict) -> float:
    """
    Soma descontos **uma vez**, com a mesma regra de ``run_single_calculo`` / ``manager``:

    - Se ``INST_PREV`` coincide com uma das rubricas (SPPRE, IAMSPE, …), não somar
      ``INST_PREV`` outra vez (evita ~2× na planilha).
    - Se ``ASSIST_MED_CAIXA_BENEF`` == ``ASSIT_MED_HOSPITAL``, a assistência caixa fica 0.
    """
    spprev = _f(main_dict, "SPPREV")
    iamspe = _f(main_dict, "IAMSPE")
    ipesp = _f(main_dict, "IPESP")
    assit = _f(main_dict, "ASSIT_MED_HOSPITAL")
    inst_caixa = _f(main_dict, "INST_PREV_CAIXA_BENEF")
    assist = _f(main_dict, "ASSIST_MED_CAIXA_BENEF")
    inst_prev = _f(main_dict, "INST_PREV")

    if assit == assist:
        assist = 0.0

    componentes = [spprev, iamspe, ipesp, assit, inst_caixa, assist]

    if inst_prev == 0 or inst_prev not in componentes:
        return sum(componentes) + inst_prev
    return sum(componentes)


def apply_descontos_from_main_dict_to_merged(
    merged: dict[str, float | None],
    main_dict: dict,
    *,
    print_ok: bool = False,
    tolerance: float = 1.02,
) -> bool:
    """
    Se O310 (desc_saude_prev) da planilha vier inflado vs. soma da base, corrige
    ``merged`` antes do UPSERT em ``memoria_calculo``. Ajusta O311 (IR) na mesma
    proporção quando parecer derivado de O310 (ex.: ~10%).
    """
    expected = previdencia_total_from_main_dict(main_dict)
    if expected < 0.01:
        return False

    read_ds = merged.get("desc_saude_prev")
    if read_ds is None:
        return False
    try:
        read_abs = abs(float(read_ds))
    except (TypeError, ValueError):
        return False

    if read_abs <= expected * tolerance:
        return False

    merged["desc_saude_prev"] = expected

    read_di = merged.get("desc_ir")
    if read_di is not None and read_abs > 0:
        try:
            di_abs = abs(float(read_di))
            ratio = di_abs / read_abs
            if 0.05 < ratio < 0.20:
                merged["desc_ir"] = round(expected * ratio, 2)
        except (TypeError, ValueError):
            pass

    if print_ok:
        print(
            "\n\t[memoria_calculo] desc_saude_prev corrigido (soma base precainfosnew): "
            f"planilha {read_abs:,.2f} → {expected:,.2f}\n"
        )
    return True
