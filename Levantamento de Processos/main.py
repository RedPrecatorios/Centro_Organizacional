#!/usr/bin/env python3
"""TJSP e-SAJ Precatório scraper + REFACTOR_TJSP JSON pipeline.

Search by:
  --nome / positional   → cbPesquisa=NMPARTE
  --cpf                 → cbPesquisa=DOCPARTE
  --processo            → cbPesquisa=NUMPROC

Outputs land under:
  FINAL_OUTPUT_DIR/<nome|cpf|processo>/<slug>/{json,parsing,...}
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from tjsp_pipeline.config import Settings
from tjsp_pipeline.pipeline import PipelineResult, run_pipeline
from tjsp_pipeline.scraper.search_url import SearchQuery, build_search_query


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Consulta precatórios no e-SAJ por nome, CPF ou número de processo, "
            "envia ao REFACTOR_TJSP-main e publica JSON em pastas por critério."
        )
    )
    parser.add_argument(
        "nome",
        nargs="?",
        default=None,
        help='Nome da parte (ex.: "Heloisa Maria Fernandes Queiroz")',
    )
    parser.add_argument(
        "--nome",
        dest="nomes",
        action="append",
        default=None,
        help="Nome da parte (repetível)",
    )
    parser.add_argument(
        "--cpf",
        dest="cpfs",
        action="append",
        default=None,
        help="CPF da parte — 11 dígitos (repetível)",
    )
    parser.add_argument(
        "--processo",
        dest="processos",
        action="append",
        default=None,
        help="Número CNJ do processo (repetível)",
    )
    parser.add_argument(
        "--search-url",
        help="URL de busca e-SAJ crua (sobrescreve critérios; 1 única query)",
    )
    parser.add_argument(
        "--flat-output",
        action="store_true",
        help="Publica direto em FINAL_OUTPUT_DIR (sem pasta nome/cpf/processo)",
    )
    parser.add_argument(
        "--scrape-only",
        action="store_true",
        help="Apenas coleta precatórios; não executa REFACTOR",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Processa no máximo N precatórios por query (útil para teste)",
    )
    parser.add_argument(
        "--processo-codigo",
        action="append",
        default=None,
        help="Filtra precatório(s) pelo processo.codigo e-SAJ (repetível)",
    )
    parser.add_argument(
        "--reprocessamento",
        action="store_true",
        help=(
            "Força REFACTOR em --reprocessamento (não pula processos já no banco). "
            "Sem esta flag, o pipeline tenta --preenchimento e faz fallback "
            "automático para reprocessamento se nenhum JSON for gerado."
        ),
    )
    parser.add_argument(
        "--no-headless",
        action="store_true",
        help="Executa Chrome com interface gráfica",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Log DEBUG",
    )
    return parser


def collect_queries(args: argparse.Namespace) -> list[SearchQuery]:
    """Build ordered SearchQuery list from CLI flags."""
    queries: list[SearchQuery] = []

    nomes: list[str] = list(args.nomes or [])
    if args.nome:
        nomes.insert(0, args.nome)

    for nome in nomes:
        queries.append(build_search_query(nome=nome))
    for cpf in args.cpfs or []:
        queries.append(build_search_query(cpf=cpf))
    for processo in args.processos or []:
        queries.append(build_search_query(processo=processo))

    return queries


def print_result(result: PipelineResult) -> None:
    print("\n=== Resultado ===")
    if result.search_kind and result.search_label:
        print(f"Critério: {result.search_kind} = {result.search_label}")
    print(f"Search URL: {result.search_url}")
    print(f"Links Precatório encontrados: {result.precatorio_links_found}")
    print(f"Registros extraídos: {len(result.records)}")
    for record in result.records:
        print(f"  • {record.txt_line} (codigo={record.processo_codigo})")
    if result.refactor_exit_code is not None:
        print(f"REFACTOR exit code: {result.refactor_exit_code}")
    if result.final_output_dir:
        print(f"Final output dir: {result.final_output_dir}")
    if result.final_json_outputs:
        print("JSON final:")
        for path in result.final_json_outputs:
            print(f"  • {path}")
    if result.json_outputs:
        print("Debug copies em logs/:")
        for path in result.json_outputs:
            print(f"  • {path}")
    if result.run_index_path:
        print(f"Run index: {result.run_index_path}")
    if result.manifest_path:
        print(f"Manifest: {result.manifest_path}")
    if result.errors:
        print("Erros:")
        for err in result.errors:
            print(f"  • {err}")


def result_exit_code(result: PipelineResult) -> int:
    if not result.records:
        return 1
    if result.errors and not result.final_json_outputs and result.refactor_exit_code is not None:
        return result.refactor_exit_code or 1
    return result.refactor_exit_code or 0


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    settings = Settings.load()
    if args.no_headless:
        settings.headless = False

    refactor_mode = "reprocessamento" if args.reprocessamento else None
    exit_codes: list[int] = []

    # Raw URL mode (single query, optional flat output)
    if args.search_url:
        out = None if args.flat_output else settings.final_output_dir / "custom" / "search_url"
        try:
            result = run_pipeline(
                settings,
                search_url=args.search_url,
                skip_refactor=args.scrape_only,
                limit=args.limit,
                processo_codigos=args.processo_codigo,
                refactor_mode=refactor_mode,
                output_dir=out,
                search_kind="custom",
                search_label="search_url",
            )
        except Exception as exc:
            logging.exception("Pipeline failed: %s", exc)
            return 1
        print_result(result)
        return result_exit_code(result)

    try:
        queries = collect_queries(args)
    except ValueError as exc:
        logging.error("%s", exc)
        return 2

    if not queries:
        # Backward-compatible default: ESAJ_SEARCH_URL from .env / NADIR
        logging.info("Nenhum critério informado — usando ESAJ_SEARCH_URL do settings")
        out = None if args.flat_output else settings.final_output_dir / "nome" / "default"
        try:
            result = run_pipeline(
                settings,
                search_url=settings.esaj_search_url,
                skip_refactor=args.scrape_only,
                limit=args.limit,
                processo_codigos=args.processo_codigo,
                refactor_mode=refactor_mode,
                output_dir=out,
                search_kind="nome",
                search_label="default",
            )
        except Exception as exc:
            logging.exception("Pipeline failed: %s", exc)
            return 1
        print_result(result)
        return result_exit_code(result)

    logging.info("Fila de consultas: %s", len(queries))
    for idx, query in enumerate(queries, start=1):
        logging.info(
            "[%s/%s] %s=%s → %s",
            idx,
            len(queries),
            query.kind.value,
            query.label,
            query.url,
        )
        out: Path | None = None
        if not args.flat_output:
            out = settings.final_output_dir / query.relative_output_dir
        try:
            result = run_pipeline(
                settings,
                search_url=query.url,
                skip_refactor=args.scrape_only,
                limit=args.limit,
                processo_codigos=args.processo_codigo,
                refactor_mode=refactor_mode,
                output_dir=out,
                search_kind=query.kind.value,
                search_label=query.label,
            )
        except Exception as exc:
            logging.exception(
                "Pipeline failed for %s=%s: %s", query.kind.value, query.label, exc
            )
            exit_codes.append(1)
            continue
        print_result(result)
        exit_codes.append(result_exit_code(result))

    # Fail if any query failed
    return 0 if exit_codes and all(c == 0 for c in exit_codes) else (1 if exit_codes else 1)


if __name__ == "__main__":
    sys.exit(main())
