# Deploy remoto — Levantamento Processual (outra cloud)

A UI da plataforma **não muda**. O browser continua a falar com o Flask desta
cloud; o Flask passa a chamar a API do Levantamento na cloud remota.

```
Browser  →  Centro Organizacional (Flask, cloud A)
                │  HTTPS + API_KEY
                ▼
         Levantamento API (FastAPI, cloud B)
                │
                ▼
         Chrome / e-SAJ / REFACTOR / MySQL
```

## 1. Cloud B (Levantamento)

1. Copiar o projeto `Levantamento de Processos/` (e dependências: REFACTOR,
   Chrome, Xvfb, proxies, acesso MySQL `precainfosnew`, CALCULO API).
2. No `.env` da cloud B:

```bash
API_HOST=127.0.0.1          # uvicorn local; nginx expõe 443
API_PORT=8003
API_KEY=<gerar: openssl rand -hex 32>
API_ALLOWED_IPS=<IP_PUBLICA_DA_CLOUD_A>
API_TRUST_PROXY=true
```

3. Subir: `scripts/start_api.sh` (ou systemd).
4. Nginx (TLS) — ver `deploy/nginx-levantamento.conf.example`.
5. Firewall: só 80/443 públicos; **não** abrir 8003 para a internet.

Teste na cloud B:

```bash
curl -sS https://levantamento.SEU_DOMINIO/api/v1/health
curl -sS -X POST https://levantamento.SEU_DOMINIO/api/v1/searches \
  -H "Authorization: Bearer $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"nome":"Teste"}'
```

## 2. Cloud A (plataforma)

No `.env` do Centro Organizacional:

```bash
TJSP_API_BASE_URL=https://levantamento.SEU_DOMINIO
TJSP_API_KEY=<mesmo valor de API_KEY na cloud B>
# TJSP_API_TOKEN=...   # alias legado, opcional
TJSP_API_TIMEOUT=90
TJSP_API_VERIFY_SSL=true
TJSP_POLL_INTERVAL_MS=5000
```

Reiniciar o serviço da plataforma (`centro-organizacional.service`).

A página Levantamento Processual continua igual; só o destino HTTP muda.

## 3. Segurança (checklist)

| Camada | O quê |
|--------|--------|
| TLS | HTTPS no nginx (Let's Encrypt) |
| API key | `API_KEY` + Bearer / `X-API-Key` (compare_digest) |
| IP allowlist | `API_ALLOWED_IPS` = IP da cloud A |
| Firewall | 8003 só localhost |
| CORS | desligado por defeito (BFF Flask) |

## 4. Rollback

Voltar na cloud A:

```bash
TJSP_API_BASE_URL=http://127.0.0.1:8003
TJSP_API_KEY=<token local>
```

e subir de novo o Levantamento local.
