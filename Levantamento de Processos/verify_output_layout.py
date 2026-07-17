#!/usr/bin/env python3
"""Verify FINAL_OUTPUT_DIR mirrors REFACTOR_TJSP-main/output layout."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from tjsp_pipeline.config import Settings
from tjsp_pipeline.integration.final_output import FINAL_SUBDIRS, assert_output_layout


def tree_names(root: Path) -> set[str]:
    if not root.is_dir():
        return set()
    return {p.name for p in root.iterdir()}


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify output folder layout parity")
    parser.add_argument(
        "--dir",
        type=Path,
        default=None,
        help="Pasta a verificar (default: FINAL_OUTPUT_DIR do .env)",
    )
    parser.add_argument(
        "--strict-files",
        action="store_true",
        help="Also require at least one json file in final output",
    )
    args = parser.parse_args()

    settings = Settings.load()
    final = Path(args.dir) if args.dir else settings.final_output_dir
    refactor = settings.refactor_path / "output"

    missing = assert_output_layout(final)
    refactor_dirs = {d for d in FINAL_SUBDIRS if (refactor / d).is_dir()}
    final_dirs = {d for d in FINAL_SUBDIRS if (final / d).is_dir()}

    print(f"FINAL_OUTPUT_DIR: {final}")
    print(f"REFACTOR output:  {refactor}")
    print(f"Required subdirs: {list(FINAL_SUBDIRS)}")
    print(f"Final has:        {sorted(final_dirs)}")
    print(f"Refactor has:     {sorted(refactor_dirs)}")

    ok = True
    if missing:
        print(f"FAIL missing dirs: {missing}")
        ok = False
    else:
        print("PASS directory layout matches REFACTOR output/")

    # Extra root files allowed in reference: non_persisted.csv
    # Also allow criterion folders when verifying the aggregate root.
    allowed_extra = {
        "non_persisted.csv",
        "non_persisted.xlsx",
        "nome",
        "cpf",
        "processo",
        "custom",
    }
    extra = tree_names(final) - set(FINAL_SUBDIRS) - allowed_extra
    # ignore hidden
    extra = {e for e in extra if not e.startswith(".")}
    if extra:
        print(f"WARN unexpected root entries (not in REFACTOR layout): {sorted(extra)}")

    json_files = list((final / "json").glob("*.json")) if (final / "json").is_dir() else []
    print(f"json files: {len(json_files)}")
    for p in sorted(json_files)[:10]:
        print(f"  • {p.name}")

    if args.strict_files and not json_files:
        print("FAIL --strict-files: no json artifacts")
        ok = False

    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
