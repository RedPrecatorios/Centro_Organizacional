# Centro Organizacional

Hub interno Flask (Gunicorn) para operação de precatórios: WhatsApp, memória de cálculo, levantamento TJSP, EDA Diário, campanhas, contratos e **painel 2FA (TOTP)**.

## Requisitos

- Python 3.10+ (venv em `.venv/`)
- MySQL (`plataforma_central`, memória, blacklist, contratos, EDA)
- PostgreSQL (Evolution / WhatsApp)
- Arquivo `.env` na raiz (nunca versionar segredos)

```bash
cd /mnt/volume_nyc1_1778499775066/Centro_Organizacional
python3.10 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

## Serviços em execução (systemd)

Nesta máquina os projetos abaixo correm como **systemctl**. Estado:

```bash
systemctl status \
  centro-organizacional \
  selo-totp \
  levantamento-processual-api \
  api-calculo \
  api-calculo-drive-worker \
  atualizacao-calculo-api
```

| Serviço systemd | Projeto / pasta | Bind típico | Função |
|-----------------|-----------------|-------------|--------|
| `centro-organizacional` | `Centro_Organizacional/` | `127.0.0.1:8001` | Painel web (Flask + Gunicorn) |
| `selo-totp` | `../automacao-de-acesso/` (`service.py`) | `127.0.0.1:8766` | Geração headless de códigos TOTP (2FA) |
| `levantamento-processual-api` | `Levantamento de Processos/` (deploy em `/opt/levantamento-processual`) | `127.0.0.1:8003` | API FastAPI scrape/enrich TJSP |
| `api-calculo` | `../API_CALCULO/` | `0.0.0.0:9487` | API HTTP de cálculo / planilha DEPRE |
| `api-calculo-drive-worker` | `../API_CALCULO/` | (worker) | Upload de planilhas para Google Drive |
| `atualizacao-calculo-api` | `ATUALIZACAO_CALCULO-main/.../api_atualizacao_calculo.py` | `127.0.0.1:5099` | API interna de atualização de cálculo |

`contratos_gunicorn` existe no systemd, mas pode estar **inactive** se os contratos forem atendidos por outro caminho (PHP/engine).

### Comandos úteis

```bash
# Subir / reiniciar
sudo systemctl enable --now centro-organizacional selo-totp \
  levantamento-processual-api api-calculo api-calculo-drive-worker \
  atualizacao-calculo-api

sudo systemctl restart centro-organizacional
sudo systemctl restart selo-totp

# Logs
journalctl -u centro-organizacional -f
journalctl -u selo-totp -f
journalctl -u levantamento-processual-api -f
journalctl -u api-calculo -f
journalctl -u atualizacao-calculo-api -f
```

---

## 1) Centro Organizacional (painel)

**Unit:** `/etc/systemd/system/centro-organizacional.service`  
**WorkingDirectory:** este repositório  
**User:** `www-data`  
**Env:** `.env` na raiz  
**Start:**

```bash
.venv/bin/gunicorn --config gunicorn.conf.py "app:app"
# ou
sudo systemctl start centro-organizacional
```

Gunicorn usa `workers=1` (EDA Diário mantém estado em memória). Porta padrão: `GUNICORN_BIND=127.0.0.1:8001`.

Painéis principais: Início, Conversas, Memória de Cálculo, PRÉ Análise, Levantamento, Autos e-SAJ, Proposta, Campanha, Blacklist, Localize, EDA (`/eda/`), **2FA** (`/2fa`).

---

## 2) Selo TOTP / 2FA (`selo-totp`)

Repositório separado: [`RedPrecatorios/automacao-de-acesso`](https://github.com/RedPrecatorios/automacao-de-acesso)  
Neste servidor: `/mnt/volume_nyc1_1778499775066/automacao-de-acesso`

Só a geração de códigos (sem UI do Selo). O painel **2FA** do Centro consome `http://127.0.0.1:8766`.

**Unit:** `/etc/systemd/system/selo-totp.service` (exemplo em `deploy/selo-totp.service`)  
**Env:** `/etc/selo-totp.env` (`SELO_DATA`, `SELO_HOST`, `SELO_PORT`, `SELO_API_TOKEN`)  
**Cofre:** `/var/lib/selo/vault.bin`  
**Start:**

```bash
cd /mnt/volume_nyc1_1778499775066/automacao-de-acesso
# usa o mesmo venv do Centro (pyotp + cryptography + Flask)
SELO_DATA=/var/lib/selo SELO_PORT=8766 \
  /mnt/volume_nyc1_1778499775066/Centro_Organizacional/.venv/bin/python service.py

# ou
sudo systemctl start selo-totp
```

Variáveis no `.env` do Centro:

```env
SELO_TOTP_URL=http://127.0.0.1:8766
SELO_API_TOKEN=<mesmo token de /etc/selo-totp.env>
SELO_TOTP_TIMEOUT=2
```

Enrolar chave (CLI):

```bash
sudo -u www-data SELO_DATA=/var/lib/selo \
  /mnt/volume_nyc1_1778499775066/Centro_Organizacional/.venv/bin/python \
  /mnt/volume_nyc1_1778499775066/automacao-de-acesso/cli.py add \
  --secret 'BASE32_OU_otpauth://...' --name 'Conta' --issuer 'Emissor'
```

Permissão da aba **2FA**: admins veem automaticamente; colaboradores só com a checkbox em Utilizadores.

---

## 3) Levantamento Processual API

Código no repo: `Levantamento de Processos/`  
Deploy operacional: `/opt/levantamento-processual` (systemd aponta para lá).

```bash
cd /opt/levantamento-processual   # ou a pasta do repo
# .env com API_KEY / API_TOKEN, API_HOST, API_PORT=8003
./scripts/start_api.sh
# ou
sudo systemctl start levantamento-processual-api
```

Health: `GET http://127.0.0.1:8003/api/v1/health`

---

## 4) API_CALCULO + Drive worker

Pasta: `/mnt/volume_nyc1_1778499775066/API_CALCULO`

```bash
cd /mnt/volume_nyc1_1778499775066/API_CALCULO
# .env com API_PORT (padrão 9487), credenciais MySQL/Drive, etc.
./venv/bin/python run.py                 # api-calculo
./venv/bin/python run_drive_worker.py    # api-calculo-drive-worker

sudo systemctl start api-calculo api-calculo-drive-worker
```

---

## 5) Atualização de cálculo (API interna)

Pasta: `ATUALIZACAO_CALCULO-main/ATUALIZACAO_CALCULO-main/`

```bash
cd ATUALIZACAO_CALCULO-main/ATUALIZACAO_CALCULO-main
# .env + CALCULO_ATUALIZACAO_API_HOST/PORT (padrão 127.0.0.1:5099)
/mnt/volume_nyc1_1778499775066/Centro_Organizacional/.venv/bin/python api_atualizacao_calculo.py

sudo systemctl start atualizacao-calculo-api
```

O Centro chama esta API via `CALCULO_ATUALIZACAO_API_URL` no `.env`.

---

## 6) EDA Diário

Montado no WSGI do Centro em `/eda/` (`eda_integracao.py`). Não é um systemd separado: sobe junto com `centro-organizacional`.

Pasta: `EDA_Diario/` — processamento diário PRC (TJSP/CMP/IMP), enriquecimento, blacklist, export.

---

## 7) Campanha / Contratos

- **Campanha:** módulo `campanha/` no próprio painel (`/campanha`) + CLI `python -m campanha.cli`
- **Contratos:** `contratos_engine/` (PHP PRECA/RPV) integrado via adapter Python na PRÉ Análise

---

## Desenvolvimento local (sem systemd)

```bash
cd /mnt/volume_nyc1_1778499775066/Centro_Organizacional
set -a; source .env; set +a   # se o .env for bash-safe
.venv/bin/python app.py       # ou gunicorn como acima
```

Para o 2FA local, suba também o `selo-totp` (ou `service.py`) antes de abrir `/2fa`.

## Testes

```bash
.venv/bin/pytest tests/ -q
```
