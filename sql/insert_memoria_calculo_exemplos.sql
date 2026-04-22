-- =============================================================================
-- 20 registos de exemplo — memoria_calculo (MySQL 8+)
-- Cenário: precatórios; nomes repetidos com processo/incidente e valores distintos.
-- Execute: USE `sistema_organizacional`;  (ou o teu banco) e depois este ficheiro.
-- Requer: tabela `memoria_calculo` criada (create_memoria_calculo.sql).
-- Para limpar e reimportar (atenção: apaga estes 20 de teste):
--   DELETE FROM `memoria_calculo` WHERE `id_precainfosnew` BETWEEN 200001 AND 200020;
-- =============================================================================

-- Helper mental (igual à UI): total_bruto = principal + juros;
--   reserva = -(total_bruto * percent/100); total_liquido = total_bruto - desc_saude - desc_ir + reserva

INSERT INTO `memoria_calculo` (
  `id_precainfosnew`,
  `requerente`,
  `numero_de_processo`,
  `numero_do_incidente`,
  `principal_bruto`,
  `juros`,
  `desc_saude_prev`,
  `desc_ir`,
  `percentual_honorarios`,
  `total_bruto`,
  `reserva_honorarios`,
  `total_liquido`
) VALUES
  /* 1 — registo isolado */
  (200001, 'João Alves Pereira', '0001234-56.2018.8.26.0100', 'INC-2021-0042',
   187450.00, 52340.50, 15420.00, 0.00, 30.00,
   239790.50, -71937.15, 152433.35),

  /* 2–4 — MESMO NOME, processos e valores diferentes */
  (200002, 'Maria Aparecida da Silva', '0002001-12.2019.8.19.0001', 'INC-2019-110',
   98000.00, 31000.00, 8000.00, 1200.00, 25.00,
   129000.00, -32250.00, 87550.00),
  (200003, 'Maria Aparecida da Silva', '0002015-88.2017.8.19.0001', 'INC-2020-88',
   456700.00, 189200.00, 42100.00, 0.00, 30.00,
   645900.00, -193770.00, 410030.00),
  (200004, 'Maria Aparecida da Silva', '0003000-00.2020.8.19.0001', 'SEM INCIDENTE',
   12000.00, 4500.00, 500.00, 0.00, 28.00,
   16500.00, -4620.00, 11380.00),

  /* 5–6 — NOME duplicado */
  (200005, 'José Carlos Oliveira', '0004111-22.2016.8.11.0001', 'INC-44',
   210000.00, 72000.00, 10000.00, 3500.00, 30.00,
   282000.00, -84600.00, 183900.00),
  (200006, 'José Carlos Oliveira', '0005000-10.2018.8.11.0001', 'INC-2022-7',
   45000.00, 8000.00, 0.00, 0.00, 25.00,
   53000.00, -13250.00, 39750.00),

  (200007, 'Ana Paula Ribeiro Costa', '0006789-01.2019.8.19.0001', 'IC-2019-001',
   300000.00, 110000.00, 20000.00, 5000.00, 28.00,
   410000.00, -114800.00, 270200.00),

  /* 8–9 — Francisco duplicado */
  (200008, 'Francisco de Assis Rocha', '0007890-12.2015.8.19.0001', 'INC-15',
   150000.00, 40000.00, 0.00, 0.00, 30.00,
   190000.00, -57000.00, 133000.00),
  (200009, 'Francisco de Assis Rocha', '0008901-33.2017.8.19.0001', 'INC-18',
   92000.00, 15000.00, 2000.00, 800.00, 30.00,
   107000.00, -32100.00, 72100.00),

  (200010, 'Heitor Augusto Menezes', '0009999-00.2021.8.19.0001', 'IC-21-500',
   75000.00, 20000.00, 0.00, 0.00, 25.00,
   95000.00, -23750.00, 71250.00),

  /* 11–12 — Pedro duplicado */
  (200011, 'Pedro Henrique Nunes', '0001111-22.2018.8.19.0001', 'INC-2018-A',
   500000.00, 200000.00, 35000.00, 0.00, 30.00,
   700000.00, -210000.00, 455000.00),
  (200012, 'Pedro Henrique Nunes', '0001222-33.2019.8.19.0001', 'INC-2019-B',
   3200.00, 800.00, 0.00, 0.00, 30.00,
   4000.00, -1200.00, 2800.00),

  (200013, 'Lúcia Helena Monteiro', '0001333-44.2017.8.19.0001', 'SEM INCID.',
   410000.00, 95000.00, 0.00, 12000.00, 28.00,
   505000.00, -141400.00, 351600.00),

  (200014, 'Roberto Lima Ferreira', '0001444-55.2016.8.19.0001', 'IC-2016-99',
   60000.00, 0.00, 3000.00, 0.00, 25.00,
   60000.00, -15000.00, 42000.00),

  /* 15–17 — Antônia triplicada */
  (200015, 'Antônia Benedita Souza', '0001555-66.2014.8.19.0001', 'IC-1',
   225000.00, 95000.00, 0.00, 0.00, 30.00,
   320000.00, -96000.00, 224000.00),
  (200016, 'Antônia Benedita Souza', '0001666-77.2018.8.19.0001', 'IC-2',
   10000.00, 2000.00, 500.00, 0.00, 30.00,
   12000.00, -3600.00, 7900.00),
  (200017, 'Antônia Benedita Souza', '0001777-88.2020.8.19.0001', 'IC-3',
   128000.00, 40000.00, 0.00, 15000.00, 28.00,
   168000.00, -47040.00, 105960.00),

  (200018, 'César Augusto Pinto', '0001888-99.2019.8.19.0001', 'IC-2019-77',
   180000.00, 50000.00, 0.00, 0.00, 30.00,
   230000.00, -69000.00, 161000.00),

  /* 19–20 — Luiz duplicado, perfis muito distintos (teste de UI) */
  (200019, 'Luiz Fernando Gomes', '0001999-00.2017.8.19.0001', 'IC-10',
   1000000.00, 400000.00, 0.00, 50000.00, 30.00,
   1400000.00, -420000.00, 930000.00),
  (200020, 'Luiz Fernando Gomes', '0002000-11.2019.8.19.0001', 'IC-20',
   5000.00, 0.00, 0.00, 0.00, 25.00,
   5000.00, -1250.00, 3750.00);
