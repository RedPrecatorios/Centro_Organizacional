-- Altera `emails` para incluir colunas do log do disparador (campanha/core.py).
-- Requer MySQL 8.0.12+ (ADD COLUMN IF NOT EXISTS). Para versões mais antigas,
-- rode uma vez cada ADD manualmente ou use `[campaign_emails_log].enabled`, que cria via Python.
--
ALTER TABLE emails
    ADD COLUMN IF NOT EXISTS campanha_disparo_status VARCHAR(40) DEFAULT NULL
        COMMENT 'sent,failed,skipped_blacklist,skipped_duplicate',
    ADD COLUMN IF NOT EXISTS campanha_disparo_erro TEXT DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS campanha_disparo_data_entrada DATETIME(3) DEFAULT NULL
        COMMENT 'Primeiro disparo registrado pela campanha',
    ADD COLUMN IF NOT EXISTS campanha_disparo_ultimo DATETIME(3) DEFAULT NULL
        COMMENT 'Ultimo disparo registrado pela campanha',
    ADD COLUMN IF NOT EXISTS campanha_disparo_campaign_id VARCHAR(191) DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS campanha_disparo_dry_run TINYINT(1) DEFAULT NULL COMMENT '1=simulacao',
    ADD COLUMN IF NOT EXISTS campanha_disparo_dominio VARCHAR(128) DEFAULT NULL
        COMMENT 'Chave domains do disparo',
    ADD COLUMN IF NOT EXISTS campanha_disparo_remetente VARCHAR(320) DEFAULT NULL
        COMMENT 'Remetente from';
