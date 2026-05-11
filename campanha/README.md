## Sistema de Disparos de Campanhas (Email)

Este módulo (`campanha/`) é um **disparador de campanhas por e-mail**, pensado para:

- **Múltiplos domínios/remetentes** (vocês têm muitos domínios) com **divisão automática** (round-robin).
- Modos de disparo:
  - **1 email específico** (teste/alerta pontual)
  - **Base toda** (CSV) com rate-limit e logs
- **Blacklist integrada ao banco** (tabela `blacklist` já existente no projeto) + blacklist extra em arquivo.
- Operação simples via CLI.

---

## 1) Pré-requisitos (para “liberar disparos” com seus domínios)

Para cada domínio/remetente que vai enviar email, garanta:

- **SMTP funcionando** para o `from_email` configurado.
- **SPF**: autoriza o servidor SMTP a enviar pelo domínio.
- **DKIM**: assinatura do conteúdo (seu provedor SMTP normalmente fornece).
- **DMARC**: política e relatórios (recomendado).

Sem isso, a entrega pode cair em spam ou ser rejeitada.

---

## 2) Criar a configuração

Crie `campanha/config.toml` a partir do exemplo:

```bash
python -m campanha.cli init --config-path campanha/config.toml
```

Edite `campanha/config.toml` e configure:

- **Banco MySQL** em `[db.mysql]` (para ler a blacklist em `eda_diario.blacklist`)
- **Conteúdo** em `[content]` (assunto + templates)
- **Domínios/remetentes** em `[[domains]]` (um bloco por domínio)

Importante:

- **dry-run**: começa como `true`. Quando estiver OK e pronto para enviar de verdade, mude para `false` em `[sending].dry_run`.
- **método de envio**: escolha em `[sending].method`:
  - `smtp` (um SMTP por domínio, mais manutenção)
  - `mailgun` (recomendado para muitos domínios; usa API key)

---

## 3) Como editar “Email, nome e processo” (destinatários)

Para disparo em base, crie um CSV com cabeçalho.

O template atual esperado é **`Email,nome,processo`** (case-insensitive). Você pode colocar **colunas extras** além disso (por exemplo `credor`, etc.). Essas colunas extras viram variáveis disponíveis nos templates como `{{credor}}`, `{{processo}}`, etc.

```csv
Email,nome,processo
maria@cliente.com.br,Maria Silva,0000000-00.0000.0.00.0000
joao@cliente.com.br,João Souza,1111111-11.1111.1.11.1111
```

Você pode usar o exemplo: `campanha/recipients.example.csv`.

---

## 4) Blacklist (integrada ao banco + extra)

### 4.1 Blacklist do banco (já existente)

O sistema lê:

- tabela: `blacklist`
- somente: `ativo = 1`
- somente: `tipo = 'EMAIL'`

Isso é controlado por:

- `[blacklist].use_db = true`

### Histórico de disparo na tabela `emails`

Se você ativar:

```toml
[campaign_emails_log]
enabled = true
```

o sistema, **no primeiro disparo com essa opção ligada**:

- garante na tabela `emails` as colunas:

  `campanha_disparo_status`, `campanha_disparo_erro`, `campanha_disparo_data_entrada`, `campanha_disparo_ultimo`,  
  `campanha_disparo_campaign_id`, `campanha_disparo_dry_run`, `campanha_disparo_dominio`, `campanha_disparo_remetente`

e, **para cada resultado da campanha**, executa um `UPDATE` em **todas** as linhas de `emails` cujo `email` coincide (comparação por `UPPER(TRIM(email))`), preenchendo:

- **`campanha_disparo_status`**: `sent`, `failed`, `skipped_blacklist`, `skipped_duplicate`
- **`campanha_disparo_data_entrada`**: preenchida na primeira atualização dessa linha (não reapaga em reruns)
- **`campanha_disparo_ultimo`**: sempre atualizada no disparo atual

Emails que **só existem no CSV** e **não** estão cadastrados em `emails` **não** recebem linha no banco (não há `INSERT`, só `UPDATE`).

Se preferir criar as colunas manualmente (sem rodar a campanha), use `ALTER TABLE emails ADD COLUMN ...` com os mesmos nomes e tipos descritos em `EDA_Diario/Modulos/modulo_banco.py` na definição de `CREATE TABLE ... emails`.

### 4.2 Blacklist extra em arquivo (opcional)

Se quiser bloquear emails fora do banco (temporário), crie:

- `campanha/blacklist_emails_extra.txt`

Formato: **1 email por linha** (linhas começando com `#` são ignoradas).

---

## 5) Disparar 1 email (teste)

```bash
python -m campanha.cli send-single --config campanha/config.toml --to "seuemail@exemplo.com" --name "Seu Nome"
```

Se quiser **forçar um domínio específico**, use `--domain` (o `name` do `[[domains]]`):

```bash
python -m campanha.cli send-single --config campanha/config.toml --to "seuemail@exemplo.com" --domain "dominio1"
```

---

## 6) Disparar para a base (CSV) dividindo por domínios

```bash
python -m campanha.cli send-bulk --config campanha/config.toml --recipients campanha/recipients.example.csv
```

O envio é balanceado por domínio (round-robin) e respeita:

- `[sending].per_domain_per_minute`
- `[sending].max_retries`

---

## 6.1 Envio via Mailgun (API) — recomendado para muitos domínios

1) No `campanha/config.toml`, defina:

- `[sending].method = "mailgun"`
- `[mailgun].api_key = "key-..."`
- `[mailgun].region = "us"` (ou `"eu"`)

2) Em `[[domains]]`, mantenha pelo menos:

- `from_name`
- `from_email` (o domínio do `from_email` precisa estar **verificado** no Mailgun)

Obs: campos SMTP podem ficar em branco quando você usa Mailgun.

---

## Respostas do cliente (encaminhar para outro email)

**Tracking / open / click** no Mailgun não define para onde vai o “Responder”. Para isso use o cabeçalho **`Reply-To`**:

- Em `campanha/config.toml`, em `[sending]`:

  `reply_to = "caixa-central@suaempresa.com.br"`

- Ou por domínio, em `[[domains]]`:

  `reply_to = "..."` (sobrescreve o global)

O cliente ainda vê o remetente como o `from_email` da campanha; ao responder, o destino padrão é o `Reply-To`.

**Alternativa (avançado):** no Mailgun, **Receiving / Routes** + **MX** do domínio apontando para Mailgun, para receber em `...@seudominio.com` e encaminhar via regra — exige configurar inbound e não é obrigatório se `Reply-To` resolver.

---

## 7) Logs e “não enviar duplicado”

O sistema grava logs em:

- `campanha/logs/<campaign_id>.jsonl`

E grava chaves de idempotência em:

- `campanha/state/sent_keys.txt`

Isso evita que um `send-bulk` repetido reenvie a mesma campanha para o mesmo email (com o mesmo `campaign_id` + assunto).

---

## 8) Templates (HTML e texto)

Arquivos padrão:

- `campanha/templates/default.html`
- `campanha/templates/default.txt`

Variáveis disponíveis (substituição simples `{{var}}`):

- `{{name}}`, `{{email}}`, `{{subject}}`
- Tudo que você colocar em `[content.vars]` no TOML, ex: `company_name`, `support_email`, etc.
- Qualquer **coluna extra** do CSV (ex: `credor`, `processo`) vira `{{credor}}`, `{{processo}}`, etc.

---

## 9) Checklist antes de enviar “valendo”

- Ajuste os `[[domains]]` com SMTP correto e credenciais.
- Configure SPF/DKIM/DMARC do(s) domínio(s).
- Faça `send-single` com `dry_run = true` (simulação).
- Troque para `dry_run = false`.
- Faça `send-single` para confirmar entrega real.
- Só então rode `send-bulk`.

## EXECUÇÃO: python3 -m campanha.cli send-bulk --config campanha/config.toml --recipients campanha/recipients.csv