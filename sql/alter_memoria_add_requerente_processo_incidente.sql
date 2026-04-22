-- Migração: tabelas `memoria_calculo` criadas antes das colunas de identificação
-- Banco: MySQL 8+ (InnoDB). Ajuste o nome do banco se necessário.

ALTER TABLE `memoria_calculo`
  ADD COLUMN `requerente` VARCHAR(500) NULL
    COMMENT 'Nome do requerente (pesquisa na interface).' AFTER `id_precainfosnew`,
  ADD COLUMN `numero_de_processo` VARCHAR(200) NULL
    COMMENT 'Número do processo (desambiguação quando o nome se repete).' AFTER `requerente`,
  ADD COLUMN `numero_do_incidente` VARCHAR(200) NULL
    COMMENT 'Número do incidente (desambiguação).' AFTER `numero_de_processo`;

ALTER TABLE `memoria_calculo`
  ADD KEY `idx_memoria_requerente` (`requerente`(191));
