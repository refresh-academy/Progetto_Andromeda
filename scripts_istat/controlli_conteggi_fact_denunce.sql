-- controlli
-- totale righe originali
SELECT COUNT(*) AS total_landing FROM istat_landing.lt_denunce_delitti;

-- quante righe del landing sono state mappate e filtrate (quelle che dovevano entrare nel fact)

WITH mapped_landing AS ( 
SELECT
  row_number() OVER () AS ids,
  coalesce(dpr.ids_regione, dr.ids_regione) AS ids_territorio,
  di.ids_indicatore,
  dtd.ids_tipo_delitto,
  ds.ids_sesso,
  dfe.ids_fascia_eta,
  da.ids_anno,
  dd.osservazione AS numero_denunce,
  now() AS load_timestamp,
  'Landing' AS source_system
FROM istat_landing.lt_denunce_delitti dd
LEFT JOIN dim_provincia_regione dpr
  ON trim(upper(dpr.territorio)) = trim(upper(dd.territorio))
LEFT JOIN dim_regioni dr
  ON trim(upper(dr.regione)) = trim(upper(dd.territorio))
JOIN istat_transformation.dim_indicatore di
  ON di.indicatore = dd.indicatore
JOIN istat_transformation.dim_tipo_delitto dtd
  ON dtd.tipo_di_delitto = dd.tipo_di_delitto
JOIN istat_transformation.dim_sesso ds
  ON trim(upper(ds.sesso)) = trim(upper(dd.sesso))
JOIN istat_transformation.dim_fascia_eta dfe
  ON dfe."età" = dd."età"
JOIN istat_transformation.dim_anno da
  ON da.time_period = dd.time_period
WHERE coalesce(dpr.ids_regione, dr.ids_regione) IS NOT NULL
  AND trim(upper(coalesce(dd.sesso, ''))) NOT IN ('TOTALE', 'ENTRAMBI', 'TOT', 'TUTTI', '')
)
SELECT m.*
FROM mapped_landing m
LEFT JOIN istat_transformation.temp_fact_denunce f
  ON m.ids_territorio      = f.ids_territorio
 AND m.ids_indicatore      = f.ids_indicatore
 AND m.ids_tipo_delitto    = f.ids_tipo_delitto
 AND m.ids_sesso           = f.ids_sesso
 AND m.ids_fascia_eta      = f.ids_fascia_eta
 AND m.ids_anno            = f.ids_anno
 AND (m.numero_denunce IS NOT DISTINCT FROM f.numero_denunce)  -- confronta anche i null qui
WHERE f.ids IS NULL
LIMIT 200;

-- quante righe ha la fact appena creata
SELECT COUNT(*) AS fact_count FROM istat_transformation.temp_fact_denunce;
--controllo se c'e' davvero
SELECT 1 FROM istat_transformation.temp_fact_denunce LIMIT 1;
