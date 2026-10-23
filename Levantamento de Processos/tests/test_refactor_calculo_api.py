"""REFACTOR CLI: --api-calculo e proteção dos pares dual-cloud no .env."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from tjsp_pipeline.config import parse_refactor_calculo_api
from tjsp_pipeline.integration.refactor_runner import (
    _is_protected_refactor_env_key,
    build_refactor_command,
    ensure_refactor_env,
)


def test_parse_refactor_calculo_api_default_2():
    assert parse_refactor_calculo_api(None) == 2
    assert parse_refactor_calculo_api("") == 2
    assert parse_refactor_calculo_api("x") == 2
    assert parse_refactor_calculo_api("3") == 2
    assert parse_refactor_calculo_api("1") == 1
    assert parse_refactor_calculo_api("2") == 2


def test_build_refactor_command_includes_api_calculo_2():
    settings = SimpleNamespace(
        refactor_path=Path("/tmp/refactor"),
        refactor_mode="reprocessamento",
        refactor_test_mode=False,
        refactor_calculo_api_id=2,
    )
    cmd = build_refactor_command(
        settings, "lote.txt", python_bin="/usr/bin/python3"
    )
    assert "--reprocessamento" in cmd
    assert cmd[cmd.index("--txt") + 1] == "lote.txt"
    assert cmd[cmd.index("--api-calculo") + 1] == "2"


def test_build_refactor_command_honors_api_1():
    settings = SimpleNamespace(
        refactor_path=Path("/tmp/refactor"),
        refactor_mode="preenchimento",
        refactor_test_mode=True,
        refactor_calculo_api_id=1,
    )
    cmd = build_refactor_command(settings, "a.txt", python_bin="python")
    assert "--preenchimento" in cmd
    assert "--test" in cmd
    assert cmd[cmd.index("--api-calculo") + 1] == "1"


def test_protected_dual_cloud_env_keys():
    assert _is_protected_refactor_env_key("CALCULO_API_2_URL")
    assert _is_protected_refactor_env_key("USE_CALCULO_API_1")
    assert not _is_protected_refactor_env_key("CALCULO_API_URL")
    assert not _is_protected_refactor_env_key("USE_CALCULO_API")


def test_ensure_refactor_env_does_not_overwrite_api_2():
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        refactor = Path(tmp) / "REFACTOR_TJSP"
        refactor.mkdir()
        env_path = refactor / ".env"
        env_path.write_text(
            "CALCULO_API_2_URL=http://api-2.example/\n"
            "CALCULO_API_2_TOKEN=keep-me\n"
            "USE_CALCULO_API_2=True\n",
            encoding="utf-8",
        )
        settings = SimpleNamespace(
            refactor_path=refactor,
            flask_db_host="h",
            flask_db_port="3306",
            flask_db_user="u",
            flask_db_password="p",
            flask_db_database="d",
            preenchimento_sql="SELECT 1",
            enable_persistence="true",
            calculo_api_url="http://legacy.example/",
            calculo_api_token="legacy",
            calculo_api_configured=True,
            use_calculo_api="true",
            gemini_api_key="",
            use_gemini_api="",
        )
        ensure_refactor_env(settings)
        text = env_path.read_text(encoding="utf-8")
        assert "CALCULO_API_2_URL=http://api-2.example/" in text
        assert "CALCULO_API_2_TOKEN=keep-me" in text
        assert "USE_CALCULO_API_2=True" in text
