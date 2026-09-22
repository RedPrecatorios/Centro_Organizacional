# -*- coding: utf-8 -*-
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT))

from messages_viewer.pre_analise_processual import (  # noqa: E402
    _is_terminal_status,
    _status_is_terminal,
    _terminal_status_sql_in,
)


def test_reprocess_in_progress_is_not_terminal_even_if_blocked():
    assert _is_terminal_status("mapeamento_iniciado", True) is False
    assert _is_terminal_status("coleta_em_andamento", True) is False
    assert _status_is_terminal("mapeamento_iniciado") is False


def test_finished_and_error_statuses_stop_polling():
    assert _is_terminal_status("analise_gpt_concluida", False) is True
    assert _is_terminal_status("erro_processamento", True) is True
    assert _is_terminal_status("blacklist", True) is True
    assert _is_terminal_status("cancelado", False) is True


def test_terminal_sql_includes_gpt_concluded():
    sql = _terminal_status_sql_in()
    assert "'analise_gpt_concluida'" in sql
    assert "'erro_processamento'" in sql


if __name__ == "__main__":
    test_reprocess_in_progress_is_not_terminal_even_if_blocked()
    test_finished_and_error_statuses_stop_polling()
    test_terminal_sql_includes_gpt_concluded()
    print("ok")
