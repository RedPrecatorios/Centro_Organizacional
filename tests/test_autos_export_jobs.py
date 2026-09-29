import json
import time
from pathlib import Path

from messages_viewer.autos_export_jobs import (
    _refactor_subprocess_env,
    delete_job,
    list_jobs,
    resolve_job_file,
    resolve_job_zip,
    user_can_access_job,
)


def _job_dir(jobs: Path, job_id: str, *, created_at: float | None = None, user_id=None) -> Path:
    job = jobs / job_id
    files = job / "files" / "incidente"
    files.mkdir(parents=True)
    (files / "resultado_final.pdf").write_bytes(b"%PDF")
    (job / "autos_export.zip").write_bytes(b"PK")
    (job / "result.json").write_text(
        json.dumps({"ok": True, "outcome": {"file_count": 1, "files": []}}),
        encoding="utf-8",
    )
    (job / "meta.json").write_text(
        json.dumps(
            {
                "job_id": job_id,
                "processo": "0000001-11.2020.8.26.0053",
                "incidente": "2",
                "created_at": created_at if created_at is not None else time.time(),
                "status": "done",
                "user_id": user_id,
                "user_name": "Teste",
            }
        ),
        encoding="utf-8",
    )
    return job


def test_resolve_job_file_blocks_path_traversal(tmp_path, monkeypatch):
    jobs = tmp_path / "jobs"
    job = _job_dir(jobs, "abcd1234efgh5678")
    pdf = job / "files" / "incidente" / "resultado_final.pdf"
    (tmp_path / "secret.txt").write_text("nope", encoding="utf-8")
    monkeypatch.setattr(
        "messages_viewer.autos_export_jobs._jobs_dir",
        lambda: jobs,
    )
    assert resolve_job_file("abcd1234efgh5678", "incidente/resultado_final.pdf") == pdf.resolve()
    assert resolve_job_file("abcd1234efgh5678", "../secret.txt") is None
    assert resolve_job_file("abcd1234efgh5678", "/etc/passwd") is None
    assert resolve_job_zip("abcd1234efgh5678") == (job / "autos_export.zip")


def test_expired_job_hides_files_and_leaves_list(tmp_path, monkeypatch):
    jobs = tmp_path / "jobs"
    _job_dir(jobs, "abcd1234efgh5678", created_at=time.time() - 40 * 3600)
    monkeypatch.setattr("messages_viewer.autos_export_jobs._jobs_dir", lambda: jobs)
    monkeypatch.setattr("messages_viewer.autos_export_jobs.retain_hours", lambda: 24)
    assert resolve_job_file("abcd1234efgh5678", "incidente/resultado_final.pdf") is None
    assert resolve_job_zip("abcd1234efgh5678") is None
    assert list_jobs(user_id=1, is_admin=True) == []
    assert not (jobs / "abcd1234efgh5678").exists()


def test_list_and_delete_are_scoped_to_owner(tmp_path, monkeypatch):
    jobs = tmp_path / "jobs"
    _job_dir(jobs, "abcd1234efgh5678", user_id=7)
    _job_dir(jobs, "bbbb1234efgh5678", user_id=9)
    monkeypatch.setattr("messages_viewer.autos_export_jobs._jobs_dir", lambda: jobs)
    mine = list_jobs(user_id=7, is_admin=False)
    assert [row["job_id"] for row in mine] == ["abcd1234efgh5678"]
    denied, code = delete_job("bbbb1234efgh5678", user_id=7, is_admin=False)
    assert code == 404
    assert denied["ok"] is False
    ok, code = delete_job("abcd1234efgh5678", user_id=7, is_admin=False)
    assert code == 200
    assert ok["ok"] is True
    assert list_jobs(user_id=7, is_admin=False) == []


def test_autos_esaj_subprocess_disables_calculo():
    env = _refactor_subprocess_env()
    assert env.get("AUTOS_EXPORT_MODE") == "1"
    assert env.get("USE_CALCULO_API") == "0"
    assert env.get("ENABLE_MEMORIA_CALCULO_SYNC") == "0"


def test_user_can_access_legacy_job_without_owner():
    assert user_can_access_job({"user_id": None}, user_id=3, is_admin=False) is True
    assert user_can_access_job({"user_id": 3}, user_id=8, is_admin=False) is False
    assert user_can_access_job({"user_id": 3}, user_id=8, is_admin=True) is True
