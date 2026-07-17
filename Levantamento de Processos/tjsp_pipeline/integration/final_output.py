"""Publish REFACTOR artifacts into the workspace final output folder.

Target layout must match REFACTOR_TJSP-main/output exactly:

  output/
    json/
    parsing/
    depre_prioridade/
    requests/
    gemini/
    calculo/
    n_meses_gemini/
    test_persistence/
    non_persisted.csv   (optional)
"""

from __future__ import annotations

import logging
import re
import shutil
from datetime import datetime
from pathlib import Path

from tjsp_pipeline.config import Settings
from tjsp_pipeline.scraper.show_page import PrecatorioRecord

logger = logging.getLogger(__name__)

# Canonical REFACTOR output subdirectories (exact mirror).
FINAL_SUBDIRS = (
    "json",
    "parsing",
    "depre_prioridade",
    "requests",
    "gemini",
    "calculo",
    "n_meses_gemini",
    "test_persistence",
)

ROOT_FILES = (
    "non_persisted.csv",
    "non_persisted.xlsx",
)


def _slug(value: str) -> str:
    return re.sub(r"[^\w.-]+", "_", value).strip("_")[:60] or "record"


def record_prefixes(records: list[PrecatorioRecord]) -> list[str]:
    return [
        f"{_slug(r.numero_de_processo)}_{_slug(r.numero_do_incidente)}"
        for r in records
    ]


def record_codigos(records: list[PrecatorioRecord]) -> list[str]:
    return [r.processo_codigo for r in records if r.processo_codigo]


def ensure_final_output_tree(final_output_dir: Path) -> None:
    final_output_dir.mkdir(parents=True, exist_ok=True)
    for name in FINAL_SUBDIRS:
        (final_output_dir / name).mkdir(parents=True, exist_ok=True)


def _copy_file(src: Path, dest: Path) -> Path:
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dest)
    logger.info("Final output ← %s", dest)
    return dest


def _copy_matching_prefix(
    source_dir: Path,
    dest_dir: Path,
    prefixes: list[str],
    *,
    patterns: tuple[str, ...] = ("*",),
) -> list[Path]:
    if not source_dir.is_dir():
        return []
    dest_dir.mkdir(parents=True, exist_ok=True)
    copied: list[Path] = []
    for pattern in patterns:
        for src in sorted(source_dir.glob(pattern)):
            if not src.is_file():
                continue
            if prefixes and not any(src.name.startswith(prefix) for prefix in prefixes):
                continue
            copied.append(_copy_file(src, dest_dir / src.name))
    return copied


def _copy_requests(
    source_dir: Path,
    dest_dir: Path,
    *,
    codigos: list[str],
    prefixes: list[str],
    since: datetime | None,
) -> list[Path]:
    """Copy request HTML dumps related to this run (by codigo / CNJ; mtime fallback)."""
    if not source_dir.is_dir():
        return []
    dest_dir.mkdir(parents=True, exist_ok=True)

    needles: list[str] = []
    for c in codigos:
        if c:
            needles.append(c.lower())
    for prefix in prefixes:
        needles.append(prefix.lower())
        if "_" in prefix:
            needles.append(prefix.split("_", 1)[0].lower())
    needles = [n for n in needles if n]

    def _match_name(name: str) -> bool:
        name_l = name.lower()
        return any(n in name_l for n in needles)

    files = [p for p in source_dir.iterdir() if p.is_file()]
    primary = [p for p in files if _match_name(p.name)]

    # mtime fallback only when nothing matched by codigo/CNJ
    if not primary and since is not None:
        since_ts = since.timestamp()
        primary = [p for p in files if p.stat().st_mtime >= since_ts]

    copied: list[Path] = []
    for src in sorted(primary):
        copied.append(_copy_file(src, dest_dir / src.name))
    return copied


def _copy_test_persistence(source_dir: Path, dest_dir: Path) -> list[Path]:
    if not source_dir.is_dir():
        return []
    dest_dir.mkdir(parents=True, exist_ok=True)
    files = sorted(source_dir.glob("*.xlsx"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not files:
        return []
    # Newest workbook for this run (REFACTOR creates one per execution).
    return [_copy_file(files[0], dest_dir / files[0].name)]


def publish_final_output(
    settings: Settings,
    records: list[PrecatorioRecord],
    *,
    run_started_at: datetime | None = None,
) -> dict[str, list[Path]]:
    """
    Mirror REFACTOR_TJSP-main/output into FINAL_OUTPUT_DIR for this run's records.
    Directory layout matches the reference /opt/PROJETO_ALEXA/output tree.
    """
    ensure_final_output_tree(settings.final_output_dir)
    prefixes = record_prefixes(records)
    codigos = record_codigos(records)
    refactor_output = settings.refactor_path / "output"

    published: dict[str, list[Path]] = {
        "json": _copy_matching_prefix(
            refactor_output / "json",
            settings.final_output_dir / "json",
            prefixes,
            patterns=("*.json",),
        ),
        "parsing": _copy_matching_prefix(
            refactor_output / "parsing",
            settings.final_output_dir / "parsing",
            prefixes,
        ),
        "depre_prioridade": _copy_matching_prefix(
            refactor_output / "depre_prioridade",
            settings.final_output_dir / "depre_prioridade",
            prefixes,
        ),
        "gemini": _copy_matching_prefix(
            refactor_output / "gemini",
            settings.final_output_dir / "gemini",
            prefixes,
        ),
        "calculo": _copy_matching_prefix(
            refactor_output / "calculo",
            settings.final_output_dir / "calculo",
            prefixes,
        ),
        "n_meses_gemini": _copy_matching_prefix(
            refactor_output / "n_meses_gemini",
            settings.final_output_dir / "n_meses_gemini",
            prefixes,
        ),
        "requests": _copy_requests(
            refactor_output / "requests",
            settings.final_output_dir / "requests",
            codigos=codigos,
            prefixes=prefixes,
            since=run_started_at,
        ),
        "test_persistence": _copy_test_persistence(
            refactor_output / "test_persistence",
            settings.final_output_dir / "test_persistence",
        ),
        "root": [],
    }

    for name in ROOT_FILES:
        src = refactor_output / name
        if src.is_file():
            published["root"].append(_copy_file(src, settings.final_output_dir / name))

    # Guarantee empty sibling dirs exist even when REFACTOR produced nothing there.
    ensure_final_output_tree(settings.final_output_dir)

    total = sum(len(paths) for paths in published.values())
    logger.info(
        "Published %s artifact(s) → %s | layout=REFACTOR mirror | prefixes=%s",
        total,
        settings.final_output_dir,
        prefixes,
    )
    return published


def write_run_index(
    dest_dir: Path,
    *,
    records: list[PrecatorioRecord],
    published: dict[str, list[Path]],
    refactor_exit_code: int,
    final_output_dir: Path,
) -> Path:
    """Write run index under logs/ (not inside canonical output/)."""
    import json

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    dest_dir.mkdir(parents=True, exist_ok=True)
    path = dest_dir / f"run_index_{stamp}.json"
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "final_output_dir": str(final_output_dir),
        "refactor_exit_code": refactor_exit_code,
        "precatorios": [
            {
                "numero_de_processo": r.numero_de_processo,
                "numero_do_incidente": r.numero_do_incidente,
                "processo_codigo": r.processo_codigo,
                "txt_line": r.txt_line,
            }
            for r in records
        ],
        "published": {
            key: [str(p) for p in paths] for key, paths in published.items()
        },
        "layout_subdirs": list(FINAL_SUBDIRS),
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("Run index written: %s", path)
    return path


def assert_output_layout(final_output_dir: Path) -> list[str]:
    """Return list of missing required subdirs (empty = OK)."""
    missing: list[str] = []
    if not final_output_dir.is_dir():
        return [str(final_output_dir)]
    for name in FINAL_SUBDIRS:
        if not (final_output_dir / name).is_dir():
            missing.append(name)
    return missing
