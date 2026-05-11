-- Quem atualizou a memória: utilizador ou automação (robô / chamada direta à API).

ALTER TABLE `memoria_calculo`
  ADD COLUMN IF NOT EXISTS `feito_por` VARCHAR(200) NULL
    COMMENT 'Username na plataforma ou "automação".'
  AFTER `total_liquido`;
