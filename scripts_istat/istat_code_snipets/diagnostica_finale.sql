-- diagnostics_checks.sql
-- Script di controlli/diagnostica per i landing, le dimensioni e i fact.
-- Esegui nell'ordine. Ogni blocco è commentato; adatta gli schemi/nome tabelle se necessario.

-- ------------------------------------------------------------
-- CONFIG: lista di territori/valori da escludere nelle analisi
-- (usata ripetutamente; ripeteremo la logica nei singoli SELECT)
-- ------------------------------------------------------------
-- valori da escludere: 'italia','estero','sud','nord-est','nord-ovest','centro','isole'
-- escludiamo anche tutte le varianti che iniziano con 'provincia autonoma'

-- ------------------------------------------------------------
-- 1) Counts generali delle landing e dei fact principali
-- ------------------------------------------------------------
SELECT 'cnt_lt_condanne_eta_sesso' AS what, count(*) AS cnt
FROM istat_landing.lt_condanne_eta_sesso;

SELECT 'cnt_lt_condanne_reati_violenti_sesso_reg' AS what, count(*) AS cnt
FROM istat_landing.lt_condanne_reati_violenti_sesso_reg;

SELECT 'cnt_lt_denunce_delitti' AS what, count(*) AS cnt
FROM istat_landing.lt_denunce_delitti;

SELECT 'cnt_lt_chiamate_vittime' AS what, count(*) AS cnt
FROM istat_landing.lt_chiamate_vittime;

SELECT 'cnt_fact_condanne_eta_sesso' AS what, count(*) AS cnt
FROM istat_dwh.fact_condanne_eta_sesso;

SELECT 'cnt_fact_condanne_reati_sesso_tot' AS what, count(*) AS cnt
FROM istat_dwh.fact_condanne_reati_sesso_tot;

SELECT 'cnt_fact_denunce_delitti' AS what, count(*) AS cnt
FROM istat_dwh.fact_denunce_delitti;

SELECT 'cnt_fact_chiamate' AS what, count(*) AS cnt
FROM istat_dwh.fact_chiamate;

-- ------------------------------------------------------------
-- 2) Counts *filtrati* nelle landing (escludendo Totale / Italia / Estero / macro-aree / Provincia Autonoma)
-- ------------------------------------------------------------
SELECT 'lt_condanne_eta_sesso_filtered' AS what, count(*) AS cnt
FROM istat_landing.lt_condanne_eta_sesso l
WHERE lower(trim(coalesce(l.sesso,''))) <> 'totale'
  AND trim(coalesce(l.territorio,'')) <> ''
  AND NOT (
    lower(trim(l.territorio)) IN ('italia','estero','sud','nord-est','nord-ovest','centro','isole')
    OR lower(trim(l.territorio)) LIKE 'provincia autonoma%'
  );

SELECT 'lt_condanne_reati_violenti_sesso_reg_filtered' AS what, count(*) AS cnt
FROM istat_landing.lt_condanne_reati_violenti_sesso_reg l
WHERE lower(trim(coalesce(l.sesso,''))) = 'totale'   -- questa landing spesso ha i totali
  AND trim(coalesce(l.territorio,'')) <> ''
  AND NOT (
    lower(trim(l.territorio)) IN ('italia','estero','sud','nord-est','nord-ovest','centro','isole')
    OR lower(trim(l.territorio)) LIKE 'provincia autonoma%'
  );

SELECT 'lt_denunce_delitti_filtered' AS what, count(*) AS cnt
FROM istat_landing.lt_denunce_delitti d
WHERE trim(coalesce(d.eta,'')) <> ''
  AND trim(coalesce(d.territorio,'')) <> '';

-- ------------------------------------------------------------
-- 3) Confronto counts landing_filtered vs fact (per verificare perdita righe)
-- ------------------------------------------------------------
SELECT 'fact_condanne_eta_sesso' AS source, count(*) AS cnt
FROM istat_dwh.fact_condanne_eta_sesso

UNION ALL

SELECT 'landing_condanne_eta_sesso_filtered' AS source, count(*) AS cnt
FROM istat_landing.lt_condanne_eta_sesso l
WHERE lower(trim(coalesce(l.sesso,''))) <> 'totale'
  AND trim(coalesce(l.territorio,'')) <> ''
  AND NOT (
    lower(trim(l.territorio)) IN ('italia','estero','sud','nord-est','nord-ovest','centro','isole')
    OR lower(trim(l.territorio)) LIKE 'provincia autonoma%'
  );

SELECT 'fact_condanne_reati_sesso_tot' AS source, count(*) AS cnt
FROM istat_dwh.fact_condanne_reati_sesso_tot

UNION ALL

SELECT 'landing_condanne_reati_sesso_reg_filtered' AS source, count(*) AS cnt
FROM istat_landing.lt_condanne_reati_violenti_sesso_reg l
WHERE trim(coalesce(l.territorio,'')) <> ''
  AND NOT (
    lower(trim(l.territorio)) IN ('italia','estero','sud','nord-est','nord-ovest','centro','isole')
    OR lower(trim(l.territorio)) LIKE 'provincia autonoma%'
  );

-- ------------------------------------------------------------
-- 4) Elenco distinct territori presenti nelle landing (per trovare varianti)
-- ------------------------------------------------------------
SELECT 'lt_condanne_eta_sesso_territori' AS src, lower(trim(territorio)) AS territorio
FROM istat_landing.lt_condanne_eta_sesso
GROUP BY lower(trim(territorio))
ORDER BY territorio;

SELECT 'lt_condanne_reati_violenti_sesso_reg_territori' AS src, lower(trim(territorio)) AS territorio
FROM istat_landing.lt_condanne_reati_violenti_sesso_reg
GROUP BY lower(trim(territorio))
ORDER BY territorio;

SELECT 'lt_denunce_delitti_territori' AS src, lower(trim(territorio)) AS territorio
FROM istat_landing.lt_denunce_delitti
GROUP BY lower(trim(territorio))
ORDER BY territorio;

-- ------------------------------------------------------------
-- 5) Territori in landing NON mappati a dim_regioni (crea staging tables per ispezione)
-- ------------------------------------------------------------
DROP TABLE IF EXISTS staging_unmapped_regions_condanne_eta;
CREATE TABLE staging_unmapped_regions_condanne_eta AS
SELECT l.*
FROM istat_landing.lt_condanne_eta_sesso l
LEFT JOIN istat_transformation.dim_regioni dr
  ON lower(trim(l.territorio)) = lower(trim(dr.regione))
WHERE dr.ids_regione IS NULL
  AND trim(coalesce(l.territorio,'')) <> ''
  AND NOT (
    lower(trim(l.territorio)) IN ('italia','estero','sud','nord-est','nord-ovest','centro','isole')
    OR lower(trim(l.territorio)) LIKE 'provincia autonoma%'
  );

-- numero di righe non mappate e primi 200 esempi
SELECT 'unmapped_regions_condanne_eta_count' AS what, count(*) AS cnt FROM staging_unmapped_regions_condanne_eta;
SELECT * FROM staging_unmapped_regions_condanne_eta ORDER BY territorio LIMIT 200;


DROP TABLE IF EXISTS staging_unmapped_regions_condanne_reati;
CREATE TABLE staging_unmapped_regions_condanne_reati AS
SELECT l.*
FROM istat_landing.lt_condanne_reati_violenti_sesso_reg l
LEFT JOIN istat_transformation.dim_regioni dr
  ON lower(trim(l.territorio)) = lower(trim(dr.regione))
WHERE dr.ids_regione IS NULL
  AND trim(coalesce(l.territorio,'')) <> ''
  AND NOT (
    lower(trim(l.territorio)) IN ('italia','estero','sud','nord-est','nord-ovest','centro','isole')
    OR lower(trim(l.territorio)) LIKE 'provincia autonoma%'
  );

SELECT 'unmapped_regions_condanne_reati_count' AS what, count(*) AS cnt FROM staging_unmapped_regions_condanne_reati;
SELECT * FROM staging_unmapped_regions_condanne_reati ORDER BY territorio LIMIT 200;

DROP TABLE IF EXISTS staging_unmapped_regions_denunce;
CREATE TABLE staging_unmapped_regions_denunce AS
SELECT d.*
FROM istat_landing.lt_denunce_delitti d
LEFT JOIN istat_transformation.mapping_regione_provincia m
  ON lower(trim(d.territorio)) = lower(trim(m.regione))
WHERE m.ids_provincia IS NULL
  AND trim(coalesce(d.territorio,'')) <> '';

SELECT 'unmapped_regions_denunce_count' AS what, count(*) AS cnt FROM staging_unmapped_regions_denunce;
SELECT * FROM staging_unmapped_regions_denunce ORDER BY territorio LIMIT 200;

-- ------------------------------------------------------------
-- 6) Tipi di reato non mappati (da entrambe le landing rispetto a dim_tipo_reato)
-- ------------------------------------------------------------
DROP TABLE IF EXISTS staging_unmapped_tipo_reato;
CREATE TABLE staging_unmapped_tipo_reato AS
SELECT DISTINCT l.tipo_di_reato, src
FROM (
  SELECT tipo_di_reato, 'lt_condanne_eta_sesso' AS src FROM istat_landing.lt_condanne_eta_sesso
  UNION ALL
  SELECT tipo_di_reato, 'lt_condanne_reati_violenti_sesso_reg' AS src FROM istat_landing.lt_condanne_reati_violenti_sesso_reg
) l
LEFT JOIN istat_transformation.dim_tipo_reato dtr
  ON lower(trim(l.tipo_di_reato)) = lower(trim(dtr.tipo_di_reato))
WHERE dtr.ids_reato IS NULL
  AND trim(coalesce(l.tipo_di_reato,'')) <> '';

SELECT 'unmapped_tipo_reato_count' AS what, count(*) AS cnt FROM staging_unmapped_tipo_reato;
SELECT * FROM staging_unmapped_tipo_reato ORDER BY tipo_di_reato, src LIMIT 200;

-- ------------------------------------------------------------
-- 7) Età non mappate (lt_condanne_eta_sesso, lt_denunce_delitti vs dim_fascia_eta)
-- ------------------------------------------------------------
DROP TABLE IF EXISTS staging_unmapped_eta;
CREATE TABLE staging_unmapped_eta AS
SELECT DISTINCT l.eta_val, l.src
FROM (
  SELECT eta_del_condannato_al_momento_del_reato AS eta_val, 'lt_condanne_eta_sesso' AS src FROM istat_landing.lt_condanne_eta_sesso
  UNION ALL
  SELECT eta AS eta_val, 'lt_denunce_delitti' AS src FROM istat_landing.lt_denunce_delitti
) l
LEFT JOIN istat_transformation.dim_fascia_eta fe
  ON lower(trim(l.eta_val)) = lower(trim(coalesce(fe.eta,'')))
WHERE fe.ids_eta IS NULL
  AND trim(coalesce(l.eta_val,'')) <> '';

SELECT 'unmapped_eta_count' AS what, count(*) AS cnt FROM staging_unmapped_eta;
SELECT * FROM staging_unmapped_eta ORDER BY eta_val, src LIMIT 200;

-- ------------------------------------------------------------
-- 8) Sessi non mappati (landing vs dim_sesso)
-- ------------------------------------------------------------
DROP TABLE IF EXISTS staging_unmapped_sesso;
CREATE TABLE staging_unmapped_sesso AS
SELECT DISTINCT l.sesso AS sesso_val, l.src
FROM (
  SELECT sesso, 'lt_chiamate_vittime' AS src FROM istat_landing.lt_chiamate_vittime
  UNION ALL
  SELECT sesso, 'lt_denunce_delitti' AS src FROM istat_landing.lt_denunce_delitti
  UNION ALL
  SELECT sesso, 'lt_condanne_eta_sesso' AS src FROM istat_landing.lt_condanne_eta_sesso
  UNION ALL
  SELECT sesso, 'lt_condanne_reati_violenti_sesso_reg' AS src FROM istat_landing.lt_condanne_reati_violenti_sesso_reg
) l
LEFT JOIN istat_transformation.dim_sesso ds
  ON lower(trim(l.sesso)) = lower(trim(coalesce(ds.sesso,'')))
WHERE ds.ids_sesso IS NULL
  AND trim(coalesce(l.sesso,'')) <> '';

SELECT 'unmapped_sesso_count' AS what, count(*) AS cnt FROM staging_unmapped_sesso;
SELECT * FROM staging_unmapped_sesso ORDER BY sesso_val, src LIMIT 200;

-- ------------------------------------------------------------
-- 9) time_period (anni) presenti in landing ma NON in dim_anno
-- ------------------------------------------------------------
DROP TABLE IF EXISTS staging_unmapped_time_period;
CREATE TABLE staging_unmapped_time_period AS
SELECT DISTINCT l.time_period AS time_period, l.src
FROM (
  SELECT time_period, 'lt_chiamate_vittime' AS src FROM istat_landing.lt_chiamate_vittime
  UNION ALL
  SELECT time_period, 'lt_condanne_reati_violenti_sesso_reg' AS src FROM istat_landing.lt_condanne_reati_violenti_sesso_reg
  UNION ALL
  SELECT time_period, 'lt_condanne_eta_sesso' AS src FROM istat_landing.lt_condanne_eta_sesso
  UNION ALL
  SELECT time_period, 'lt_denunce_delitti' AS src FROM istat_landing.lt_denunce_delitti
) l
LEFT JOIN istat_transformation.dim_anno da
  ON l.time_period = da.time_period
WHERE da.ids_anno IS NULL
  AND l.time_period IS NOT NULL;

SELECT 'unmapped_time_period_count' AS what, count(*) AS cnt FROM staging_unmapped_time_period;
SELECT * FROM staging_unmapped_time_period ORDER BY time_period, src LIMIT 200;

-- ------------------------------------------------------------
-- 10) Controllo duplicati nelle dimensioni (valore testuale con più ids)
-- ------------------------------------------------------------
SELECT 'dim_tipo_reato_duplicates' AS what, tipo_di_reato, count(*) AS cnt_ids
FROM istat_transformation.dim_tipo_reato
GROUP BY tipo_di_reato
HAVING count(*) > 1
ORDER BY cnt_ids DESC;

SELECT 'dim_fascia_eta_duplicates' AS what, eta, count(*) AS cnt_ids
FROM istat_transformation.dim_fascia_eta
GROUP BY eta
HAVING count(*) > 1
ORDER BY cnt_ids DESC;

SELECT 'dim_regioni_duplicates' AS what, regione, count(*) AS cnt_ids
FROM istat_transformation.dim_regioni
GROUP BY regione
HAVING count(*) > 1
ORDER BY cnt_ids DESC;

SELECT 'dim_sesso_duplicates' AS what, sesso, count(*) AS cnt_ids
FROM istat_transformation.dim_sesso
GROUP BY sesso
HAVING count(*) > 1
ORDER BY cnt_ids DESC;

-- ------------------------------------------------------------
-- 11) Check valori NULL nelle colonne chiave del fact_condanne_eta_sesso
-- ------------------------------------------------------------
SELECT
  count(*) FILTER (WHERE ids_regione IS NULL) AS null_ids_regione,
  count(*) FILTER (WHERE ids_reato IS NULL)   AS null_ids_reato,
  count(*) FILTER (WHERE ids_sesso IS NULL)   AS null_ids_sesso,
  count(*) FILTER (WHERE ids_eta IS NULL)     AS null_ids_eta,
  count(*) AS total_rows
FROM istat_dwh.fact_condanne_eta_sesso;

-- list a sample of rows with null dimension ids
SELECT *
FROM istat_dwh.fact_condanne_eta_sesso
WHERE ids_regione IS NULL OR ids_reato IS NULL OR ids_sesso IS NULL OR ids_eta IS NULL
LIMIT 200;

-- ------------------------------------------------------------
-- 12) Check somme e segni numerici
-- ------------------------------------------------------------
-- somma numeri nel landing (filtered) vs somma nel fact
SELECT 'sum_landing_condanne_eta_sesso_filtered' AS what,
       sum(coalesce(numero_condanne,0)) AS sum_num
FROM istat_landing.lt_condanne_eta_sesso l
WHERE lower(trim(coalesce(l.sesso,''))) <> 'totale'
  AND trim(coalesce(l.territorio,'')) <> ''
  AND NOT (
    lower(trim(l.territorio)) IN ('italia','estero','sud','nord-est','nord-ovest','centro','isole')
    OR lower(trim(l.territorio)) LIKE 'provincia autonoma%'
  );

SELECT 'sum_fact_condanne_eta_sesso' AS what, sum(coalesce(numero_condanne,0)) AS sum_num
FROM istat_dwh.fact_condanne_eta_sesso;

-- valori negativi o anomali
SELECT 'negative_numero_condanne_in_landing' AS what, count(*) AS cnt
FROM istat_landing.lt_condanne_eta_sesso
WHERE numero_condanne < 0;

-- ------------------------------------------------------------
-- 13) Sampling e controlli di qualità generali
-- ------------------------------------------------------------
-- Mostra prime 200 righe delle landing problematiche (es. con territorio vuoto o sesso vuoto)
SELECT * FROM istat_landing.lt_condanne_eta_sesso
WHERE trim(coalesce(territorio,'')) = '' OR trim(coalesce(sesso,'')) = ''
LIMIT 200;

SELECT * FROM istat_landing.lt_condanne_reati_violenti_sesso_reg
WHERE trim(coalesce(territorio,'')) = '' OR trim(coalesce(tipo_di_reato,'')) = ''
LIMIT 200;

-- ------------------------------------------------------------
-- 14) Suggerimenti automatici: crea tabelle di mapping proposte (vuote) per correggere manualmente
-- ------------------------------------------------------------
DROP TABLE IF EXISTS staging_proposed_territorio_mapping;
CREATE TABLE staging_proposed_territorio_mapping (
  raw_territorio varchar,
  mapped_regione varchar,
  notes varchar
);

-- popula con valori distinti non mappati (da condanne_eta)
INSERT INTO staging_proposed_territorio_mapping (raw_territorio)
SELECT DISTINCT trim(territorio)
FROM staging_unmapped_regions_condanne_eta
ORDER BY 1;

-- ------------------------------------------------------------
-- Fine script diagnostica
-- ------------------------------------------------------------
-- Esecuzione consigliata:
-- 1) lancia lo script; analizza i risultati (count, tabelle staging).
-- 2) ispeziona le tabelle staging_* per capire le varianti testuali.
-- 3) popola staging_proposed_territorio_mapping con la corrispondenza corretta
--    (raw_territorio -> mapped_regione) e poi usala per normalizzare i join nei fact.
-- 4) dopo le correzioni ricrea i fact e riesegui i controlli per verificare che non ci siano più valori NULL/non mappati.