-- Test completo per ETL popolazione
-- Eseguire prima: istat_popolazione_landing.sql
-- Poi: mini_etl_popolazione.sql
-- Infine: questo test

-- 1. Inserisco dati di test
INSERT INTO istat_landing.lt_popolazione_italia_regioni_province 
(frequenza, territorio, indicatore, sesso, eta, stato_civile, time_period, osservazione)
VALUES 
('A', 'Lombardia', 'Popolazione residente', 'Maschi', '0-14 anni', 'Totale', 2023, 1500000),
('A', 'Lombardia', 'Popolazione residente', 'Femmine', '0-14 anni', 'Totale', 2023, 1450000),
('A', 'Lazio', 'Popolazione residente', 'Maschi', '15-64 anni', 'Totale', 2023, 2100000),
('A', 'Lazio', 'Popolazione residente', 'Femmine', '15-64 anni', 'Totale', 2023, 2150000),
('A', 'Veneto', 'Popolazione residente', 'Totale', '65+ anni', 'Totale', 2023, 900000);

-- 2. Esegui ETL
-- (il file mini_etl_popolazione.sql va eseguito dopo aver inserito i dati)

-- 3. Test finali post-ETL
-- Verifica conteggi
SELECT 
  'Test Conteggi' as test_name,
  CASE 
    WHEN (SELECT COUNT(*) FROM istat_transformation.stg_popolazione) = 5 AND
         (SELECT COUNT(*) FROM istat_dwh.fact_popolazione) = 5 
    THEN 'PASS'
    ELSE 'FAIL'
  END as result,
  (SELECT COUNT(*) FROM istat_transformation.stg_popolazione) as staging_count,
  (SELECT COUNT(*) FROM istat_dwh.fact_popolazione) as fact_count;

-- Verifica dimensioni create correttamente
SELECT 
  'Test Dimensioni' as test_name,
  CASE 
    WHEN (SELECT COUNT(*) FROM istat_transformation.dim_regioni) >= 3 AND
         (SELECT COUNT(*) FROM istat_transformation.dim_sesso) >= 2 AND
         (SELECT COUNT(*) FROM istat_transformation.dim_eta) >= 3
    THEN 'PASS'
    ELSE 'FAIL'
  END as result,
  (SELECT COUNT(*) FROM istat_transformation.dim_regioni) as regioni_count,
  (SELECT COUNT(*) FROM istat_transformation.dim_sesso) as sesso_count,
  (SELECT COUNT(*) FROM istat_transformation.dim_eta) as eta_count;

-- Verifica join integrity
SELECT 
  'Test Join Integrity' as test_name,
  CASE 
    WHEN (SELECT COUNT(*) FROM istat_dwh.fact_popolazione WHERE ids_regione IS NULL) = 0
    THEN 'PASS'
    ELSE 'FAIL'
  END as result,
  (SELECT COUNT(*) FROM istat_dwh.fact_popolazione WHERE ids_regione IS NULL) as null_joins;

-- Cleanup (opzionale)
-- DELETE FROM istat_landing.lt_popolazione_italia_regioni_province;