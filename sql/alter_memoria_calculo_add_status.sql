-- Estado do caso na memória de cálculo (ex.: Sem Saldo = inapto na interface).
ALTER TABLE `memoria_calculo`
  ADD COLUMN `status` VARCHAR(50) NULL DEFAULT NULL
    COMMENT 'Sem Saldo = caso inapto; NULL ou vazio = memória numérica utilizável.'
    AFTER `feito_por`;
