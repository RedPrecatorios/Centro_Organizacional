# dashboard-backend — TJSP Precatório Pipeline

Coleta **Precatórios** do e-SAJ (TJSP) via **undetected-chromedriver** + **Webshare proxy** e gera JSON final via **REFACTOR_TJSP-main**.

## Stack

| Camada | Tecnologia |
|--------|------------|
| Browser | undetected-chromedriver + Chromium |
| Proxy | Webshare rotating (`p.webshare.io:80`) via extensão MV3 + CDP inject |
| Anti-bot | JS stealth (`navigator.webdriver`), console bridge, DOM monitor |
| Parser | BeautifulSoup + expand AJAX "Incidentes e recursos" |
| Preenchimento | REFACTOR_TJSP-main (`--preenchimento --test --txt`) |

## Setup

```bash
cd /opt/PROJETO_ALEXA/dashboard-backend
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
cp .env.example .env   # ajuste credenciais se necessário
```

**Windows (PowerShell):**

```powershell
cd C:\Users\<user>\Downloads\dashboard-backend
python -m venv venv
.\venv\Scripts\pip install -r requirements.txt
Copy-Item .env.example .env   # ajuste paths e credenciais
```

No `.env` Windows, use paths locais, por exemplo:

```
REFACTOR_TJSP_PATH=C:\Users\<user>\Downloads\REFACTOR_TJSP-main
FINAL_OUTPUT_DIR=C:\Users\<user>\Downloads\dashboard-backend\output
CHROME_BINARY=C:\Program Files\Google\Chrome\Application\chrome.exe
```

REFACTOR (primeira vez):

```bash
cd /opt/PROJETO_ALEXA/REFACTOR_TJSP-main
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
```

Sem o `REFACTOR_TJSP-main` no caminho configurado, rode com `--scrape-only` (só coleta).

## API HTTP (async jobs)

Camada estável em `api/` — o frontend só fala com estes endpoints; updates no scrape/REFACTOR passam pelo adapter.

```powershell
.\venv\Scripts\pip install -r requirements.txt
.\venv\Scripts\uvicorn api.app:app --host 0.0.0.0 --port 8000
```

| Método | Rota | Uso |
|--------|------|-----|
| `GET` | `/api/v1/health` | Healthcheck |
| `POST` | `/api/v1/searches` | Body `{"nome":"..."}` → `{job_id, status:"queued"}` |
| `GET` | `/api/v1/searches/{job_id}` | Poll até `done`/`failed`; lista `processos_aptos` |

Fluxo do job: scrape e-SAJ → já no MySQL (`precainfosnew`) volta direto → novos passam pelo REFACTOR com persistência real (sem `--test`) → blacklist fica em `skipped` → resposta une todos os APTOs (DB + pipeline + merge por Requerente).

Calculo: se `CALCULO_API_URL` e `CALCULO_API_TOKEN` estiverem no `.env`, a API ativa o calculo no enrich; senão `Calculo_Atualizado` segue só no JSON.

Jobs ficam em `logs/api_jobs.sqlite3`.

### Autenticação

Defina `API_TOKEN` no `.env` do servidor. As rotas `/api/v1/searches*` exigem:

```
Authorization: Bearer <API_TOKEN>
```

(ou header `X-API-Token`). `GET /api/v1/health` permanece público.

### Cliente cloud (plataforma)

Script isolado em [`clients/tjsp_api_client.py`](clients/tjsp_api_client.py) — só usa `requests`:

```powershell
$env:TJSP_API_BASE_URL="http://127.0.0.1:8000"
$env:TJSP_API_TOKEN="<mesmo API_TOKEN do servidor>"
.\venv\Scripts\python.exe clients\tjsp_api_client.py health
.\venv\Scripts\python.exe clients\tjsp_api_client.py search "Heloisa Maria Fernandes Queiroz"
.\venv\Scripts\python.exe clients\tjsp_api_client.py search "Heloisa Maria Fernandes Queiroz" --wait
.\venv\Scripts\python.exe clients\tjsp_api_client.py status <job_id>
```

### Testes da API (token + envio/recebimento)

```powershell
.\venv\Scripts\pip install -r requirements.txt
.\venv\Scripts\python.exe -m pytest tests\test_api_auth_client.py -q
```

## Uso

```bash
# Consulta pelo nome da parte → precatórios → REFACTOR → /opt/PROJETO_ALEXA/output
.venv/bin/python main.py "Heloisa Maria Fernandes Queiroz"
.venv/bin/python main.py --nome "Nadir Costa de Oliveira"

# Já existe no banco? força regeneração (ou o fallback automático faz isso)
.venv/bin/python main.py "Heloisa Maria Fernandes Queiroz" --reprocessamento

# Apenas coleta (sem REFACTOR)
.venv/bin/python main.py "Heloisa Maria Fernandes Queiroz" --scrape-only

# Teste com 1 precatório
.venv/bin/python main.py "Heloisa Maria Fernandes Queiroz" --limit 1

# URL de busca customizada
.venv/bin/python main.py --search-url "https://esaj.tjsp.jus.br/cpopg/search.do?..."

# Browser visível (debug). Com proxy autenticado o Chrome abre em modo visível
# automaticamente (a extensão MV3 de auth não funciona em headless).
.venv/bin/python main.py "Heloisa Maria Fernandes Queiroz" --no-headless -v
```

**Windows:**

```powershell
.\venv\Scripts\python.exe main.py "Nadir Costa de Oliveira" --scrape-only --limit 1
.\venv\Scripts\python.exe main.py "Nadir Costa de Oliveira" --limit 1
```

## Saídas

| Path | Conteúdo |
|------|----------|
| **`/opt/PROJETO_ALEXA/output/`** | **Espelho do layout REFACTOR `output/`** |
| `output/json/` | JSON final |
| `output/parsing/` | Texto de parsing |
| `output/depre_prioridade/` | Extração DEPRE/prioridade |
| `output/requests/` | HTML das requisições e-SAJ |
| `output/gemini/`, `calculo/`, `n_meses_gemini/`, `test_persistence/` | Artifacts auxiliares |
| `output/non_persisted.csv` | Quando gerado pelo REFACTOR |
| `dashboard-backend/logs/` | Debug (scrape, manifest, run_index, HTML) |

Verificar layout:

```bash
.venv/bin/python verify_output_layout.py --strict-files
```

Override: `FINAL_OUTPUT_DIR` no `.env` (default `/opt/PROJETO_ALEXA/output`).

## Fluxo

1. Abre URL de busca e-SAJ com proxy Webshare
2. Expande toggles **Incidentes e recursos** (AJAX)
3. Filtra links **Precatório** (`linkProcesso` + `classeProcesso`)
4. Visita cada `show.do` e extrai `numero_de_processo/numero_do_incidente`
5. Escreve TXT no REFACTOR e executa `--preenchimento --test` (modo teste)
6. Se nenhum JSON for gerado (processo já em `precainfosnew`), faz fallback automático para `--reprocessamento`
7. Espelha `REFACTOR_TJSP-main/output/` → **`/opt/PROJETO_ALEXA/output/`** (mesmos subdiretórios)

## Referências

- [undetected-chromedriver + proxy (2026)](https://www.scrapingbee.com/blog/undetected-chromedriver-python-tutorial-avoiding-bot-detection/)
- [CDP Fetch.authRequired pattern](https://github.com/ultrafunkamsterdam/undetected-chromedriver/discussions/1798)
- Projeto legado: `parsing-alexa-tjsp`, `COLETA-TJSP-SELENIUM-main`
