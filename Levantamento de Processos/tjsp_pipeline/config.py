from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent


@dataclass(frozen=True)
class ProxyConfig:
    host: str
    port: int
    username: str
    password: str

    @property
    def server_arg(self) -> str:
        return f"--proxy-server=http://{self.host}:{self.port}"

    @property
    def requests_url(self) -> str:
        return f"http://{self.username}:{self.password}@{self.host}:{self.port}"


@dataclass
class Settings:
    project_root: Path
    log_dir: Path
    debug_html_dir: Path
    final_output_dir: Path
    proxy: ProxyConfig
    headless: bool
    browser_timeout_seconds: int
    page_load_wait_seconds: float
    esaj_search_url: str
    refactor_path: Path
    refactor_mode: str
    refactor_test_mode: bool
    flask_db_host: str
    flask_db_port: str
    flask_db_user: str
    flask_db_password: str
    flask_db_database: str
    preenchimento_sql: str
    gemini_api_key: str
    use_gemini_api: str
    enable_persistence: str
    calculo_api_url: str
    calculo_api_token: str
    use_calculo_api: str

    @classmethod
    def load(cls, env_path: Path | None = None) -> Settings:
        env_file = env_path or (PROJECT_ROOT / ".env")
        load_dotenv(env_file, override=False)

        log_dir = PROJECT_ROOT / os.getenv("LOG_DIR", "logs")
        debug_html_dir = PROJECT_ROOT / os.getenv("DEBUG_HTML_DIR", "logs/debug_html")

        # Prefer local folders next to this project when Linux server paths are absent.
        default_output = PROJECT_ROOT / "output"
        default_refactor = PROJECT_ROOT.parent / "REFACTOR_TJSP-main"
        legacy_refactor = Path("/opt/PROJETO_ALEXA/REFACTOR_TJSP-main")
        legacy_output = Path("/opt/PROJETO_ALEXA/output")

        final_output_raw = os.getenv("FINAL_OUTPUT_DIR", "").strip()
        if final_output_raw:
            final_output_dir = Path(final_output_raw)
        elif legacy_output.is_dir():
            final_output_dir = legacy_output
        else:
            final_output_dir = default_output

        refactor_raw = os.getenv("REFACTOR_TJSP_PATH", "").strip()
        if refactor_raw:
            refactor_path = Path(refactor_raw)
        elif legacy_refactor.is_dir():
            refactor_path = legacy_refactor
        else:
            refactor_path = default_refactor

        return cls(
            project_root=PROJECT_ROOT,
            log_dir=log_dir,
            debug_html_dir=debug_html_dir,
            final_output_dir=final_output_dir,
            proxy=ProxyConfig(
                host=os.getenv("WEBSHARE_PROXY_HOST", "p.webshare.io"),
                port=int(os.getenv("WEBSHARE_PROXY_PORT", "80")),
                username=os.getenv("WEBSHARE_PROXY_USERNAME", "eleazile-rotate"),
                password=os.getenv(
                    "WEBSHARE_PROXY_PASSWORD",
                    "t3y62qg3mn02",
                ),
            ),
            headless=os.getenv("HEADLESS", "true").strip().lower()
            in {"1", "true", "yes", "on"},
            browser_timeout_seconds=int(os.getenv("BROWSER_TIMEOUT_SECONDS", "180")),
            page_load_wait_seconds=float(os.getenv("PAGE_LOAD_WAIT_SECONDS", "4")),
            esaj_search_url=os.getenv(
                "ESAJ_SEARCH_URL",
                "https://esaj.tjsp.jus.br/cpopg/search.do?conversationId="
                "&cbPesquisa=NMPARTE&dadosConsulta.valorConsulta=NADIR+COSTA+DE+OLIVEIRA"
                "&chNmCompleto=true&cdForo=-1",
            ),
            refactor_path=refactor_path,
            refactor_mode=os.getenv("REFACTOR_MODE", "preenchimento"),
            refactor_test_mode=os.getenv("REFACTOR_TEST_MODE", "true").strip().lower()
            in {"1", "true", "yes", "on"},
            flask_db_host=os.getenv("FLASK_DB_HOST", "159.223.109.249"),
            flask_db_port=os.getenv("FLASK_DB_PORT", "3306"),
            flask_db_user=os.getenv("FLASK_DB_USER", "redprecatorios"),
            flask_db_password=os.getenv(
                "FLASK_DB_PASSWORD",
                "w7F32LWkFMzpEd8nRC3XUcK6fpmcnMjBptdBpHL2PtJFJ7ynjyzybEgW5hs7ugjU",
            ),
            flask_db_database=os.getenv("FLASK_DB_DATABASE", "flaskdb"),
            preenchimento_sql=os.getenv(
                "PREENCHIMENTO_SQL",
                "SELECT id, numero_de_processo, numero_do_incidente "
                "FROM controle_coleta_TJSP LIMIT 1",
            ),
            gemini_api_key=os.getenv("GEMINI_API_KEY", ""),
            use_gemini_api=os.getenv("USE_GEMINI_API", ""),
            enable_persistence=os.getenv("ENABLE_PERSISTENCE", "false"),
            calculo_api_url=os.getenv("CALCULO_API_URL", "").strip(),
            calculo_api_token=os.getenv("CALCULO_API_TOKEN", "").strip(),
            use_calculo_api=os.getenv("USE_CALCULO_API", "").strip(),
        )

    @property
    def calculo_api_configured(self) -> bool:
        return bool(self.calculo_api_url and self.calculo_api_token)

    def with_api_runtime(
        self,
        *,
        persist: bool = True,
        test_mode: bool = False,
        force_reprocessamento: bool = False,
        force_calculo: bool = False,
    ) -> Settings:
        """Copy settings tuned for the HTTP API path (real persistence, optional calculo)."""
        use_calculo = self.use_calculo_api
        if force_calculo and self.calculo_api_configured:
            use_calculo = "true"
        elif self.calculo_api_configured and not use_calculo:
            use_calculo = "true"
        elif not self.calculo_api_configured:
            use_calculo = "false"
        mode = "reprocessamento" if force_reprocessamento else self.refactor_mode
        return Settings(
            project_root=self.project_root,
            log_dir=self.log_dir,
            debug_html_dir=self.debug_html_dir,
            final_output_dir=self.final_output_dir,
            proxy=self.proxy,
            headless=self.headless,
            browser_timeout_seconds=self.browser_timeout_seconds,
            page_load_wait_seconds=self.page_load_wait_seconds,
            esaj_search_url=self.esaj_search_url,
            refactor_path=self.refactor_path,
            refactor_mode=mode,
            refactor_test_mode=test_mode,
            flask_db_host=self.flask_db_host,
            flask_db_port=self.flask_db_port,
            flask_db_user=self.flask_db_user,
            flask_db_password=self.flask_db_password,
            flask_db_database=self.flask_db_database,
            preenchimento_sql=self.preenchimento_sql,
            gemini_api_key=self.gemini_api_key,
            use_gemini_api=self.use_gemini_api,
            enable_persistence="true" if persist else "false",
            calculo_api_url=self.calculo_api_url,
            calculo_api_token=self.calculo_api_token,
            use_calculo_api=use_calculo,
        )

    def ensure_dirs(self) -> None:
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.debug_html_dir.mkdir(parents=True, exist_ok=True)
        self.final_output_dir.mkdir(parents=True, exist_ok=True)
        for name in (
            "json",
            "parsing",
            "depre_prioridade",
            "requests",
            "gemini",
            "calculo",
            "n_meses_gemini",
            "test_persistence",
        ):
            (self.final_output_dir / name).mkdir(parents=True, exist_ok=True)
