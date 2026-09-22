from pathlib import Path

from messages_viewer.autos_export_jobs import resolve_job_file, resolve_job_zip


def test_resolve_job_file_blocks_path_traversal(tmp_path, monkeypatch):
    jobs = tmp_path / "jobs"
    job = jobs / "abcd1234efgh5678"
    files = job / "files" / "incidente"
    files.mkdir(parents=True)
    pdf = files / "resultado_final.pdf"
    pdf.write_bytes(b"%PDF")
    (tmp_path / "secret.txt").write_text("nope", encoding="utf-8")
    monkeypatch.setattr(
        "messages_viewer.autos_export_jobs._jobs_dir",
        lambda: jobs,
    )
    assert resolve_job_file("abcd1234efgh5678", "incidente/resultado_final.pdf") == pdf.resolve()
    assert resolve_job_file("abcd1234efgh5678", "../secret.txt") is None
    assert resolve_job_file("abcd1234efgh5678", "/etc/passwd") is None
    assert resolve_job_zip("abcd1234efgh5678") is None
    zip_path = job / "autos_export.zip"
    zip_path.write_bytes(b"PK")
    assert resolve_job_zip("abcd1234efgh5678") == zip_path
