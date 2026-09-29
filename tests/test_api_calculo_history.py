import json
from pathlib import Path

from messages_viewer import api_calculo_history as history
from messages_viewer.api_calculo_history import ingest_finished_jobs


def test_history_counts_each_job_once(tmp_path, monkeypatch):
    monkeypatch.setenv("API_CALCULO_HISTORY_DIR", str(tmp_path / "hist"))
    history._seen_cache = None
    history._seen_cache_size = -1
    log = tmp_path / "api.log"
    log.write_text(
        "Calculo job_id=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa concluido\n"
        "Calculo job_id=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb falhou http=500\n",
        encoding="utf-8",
    )
    first = ingest_finished_jobs(
        [
            {
                "job_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "status": "done",
                "source_ip": "1.1.1.1",
            },
            {
                "job_id": "cccccccccccccccccccccccccccccccc",
                "status": "done",
                "source_ip": "2.2.2.2",
            },
        ],
        log_path=log,
        now_iso="2026-09-24T14:00:00-03:00",
    )
    assert first["done"] == 2
    assert first["error"] == 1
    assert first["tracked"] == 3

    log.write_text(
        log.read_text(encoding="utf-8")
        + "Calculo job_id=dddddddddddddddddddddddddddddddd concluido\n",
        encoding="utf-8",
    )
    second = ingest_finished_jobs(
        [
            {
                "job_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "status": "done",
                "source_ip": "1.1.1.1",
            }
        ],
        log_path=log,
        now_iso="2026-09-24T14:01:00-03:00",
    )
    assert second["done"] == 3
    assert second["error"] == 1
    meta = json.loads((tmp_path / "hist" / "meta.json").read_text(encoding="utf-8"))
    assert meta["bootstrapped"] is True
