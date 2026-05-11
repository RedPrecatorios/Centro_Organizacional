-- =============================================================================
-- Memória de cálculo (página /memoria-calculo)
-- Banco: MySQL 8+ (InnoDB)
--
-- `id_precainfosnew` identifica o precatório na aplicação; sem FOREIGN KEY (banco novo).
-- Execute no schema correto: USE `seu_banco`;
-- =============================================================================

CREATE TABLE IF NOT EXISTS `memoria_calculo` (
  `id`                       BIGINT NOT NULL AUTO_INCREMENT
    COMMENT 'Chave primária do registo da memória de cálculo.',

  `id_precainfosnew`         BIGINT NOT NULL
    COMMENT 'Identificador do precatório (relação na aplicação; sem FK no BD).',

  `requerente`                VARCHAR(500) NULL
    COMMENT 'Nome do requerente (pesquisa na interface).',
  `numero_de_processo`        VARCHAR(200) NULL
    COMMENT 'Número do processo (desambiguação quando o nome se repete).',
  `numero_do_incidente`        VARCHAR(200) NULL
    COMMENT 'Número do incidente (desambiguação).',

  `principal_bruto`          DECIMAL(18, 2) NOT NULL DEFAULT 0.00,
  `juros`                    DECIMAL(18, 2) NOT NULL DEFAULT 0.00,
  `desc_saude_prev`          DECIMAL(18, 2) NOT NULL DEFAULT 0.00
    COMMENT 'Magnitude do desconto (armazenar positivo; o front pode exibir com sinal).',
  `desc_ir`                  DECIMAL(18, 2) NOT NULL DEFAULT 0.00,
  `percentual_honorarios`   DECIMAL(5, 2) NOT NULL DEFAULT 30.00
    CHECK (`percentual_honorarios` >= 0 AND `percentual_honorarios` <= 100),

  /* Valores de fecho (gravação opcional; recalculáveis na aplicação) */
  `total_bruto`              DECIMAL(18, 2) NULL,
  `reserva_honorarios`        DECIMAL(18, 2) NULL
    COMMENT 'Valor da reserva de honorários em R$ (tipicamente negativo no extrato).',
  `total_liquido`            DECIMAL(18, 2) NULL,

  `feito_por`                VARCHAR(200) NULL
    COMMENT 'Utilizador da plataforma (username) ou "automação" quando pelo robô/API sem utilizador.',

  `criado_em`                TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  `atualizado_em`            TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),

  PRIMARY KEY (`id`),
  UNIQUE KEY `uq_memoria_calculo_1_por_id_precainfosnew` (`id_precainfosnew`),

  KEY `idx_memoria_requerente` (`requerente`(191))
) ENGINE=InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci
  COMMENT = 'Valores da memória de cálculo por precatório (vinculado via id_precainfosnew; sem FK).';

/* A UNIQUE `uq_memoria_calculo_1_por_id_precainfosnew` já indexa `id_precainfosnew`. */
