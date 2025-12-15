
-- schema transformation
DROP SCHEMA IF EXISTS istat_transformation CASCADE;
CREATE SCHEMA istat_transformation;

SET search_path TO istat_transformation;

-- creazione dimensioni in transformation schema

-- dim_territorio (da lt_chiamate_vittime)
DROP TABLE IF EXISTS istat_transformation.dim_territorio;
CREATE TABLE istat_transformation.dim_territorio AS
SELECT row_number() OVER (ORDER BY territorio) AS ids_territorio,
       territorio
FROM (
  SELECT DISTINCT territorio
  FROM istat_landing.lt_chiamate_vittime
) t
ORDER BY territorio;

-- dim_indicatore

drop table if exists istat_transformation.dim_indicatore;
create table istat_transformation.dim_indicatore as
select
	row_number() over (order by indicatore) as ids_indicatore,
	indicatore,
	NOW()::timestamp as load_timestamp,
	'lt_denunce_delitti' as source_system
from (
select distinct trim(indicatore) as indicatore
from istat_landing.lt_denunce_delitti
where trim(coalesce(indicatore, '')) <> ''
) t
order by indicatore;

-- dim_sesso
DROP TABLE IF EXISTS istat_transformation.dim_sesso;
CREATE TABLE istat_transformation.dim_sesso AS
SELECT
  ROW_NUMBER() OVER (ORDER BY sesso) AS ids_sesso,
  sesso,
  NOW()::timestamp AS load_timestamp,
  'lt_chiamate_vittime' AS source_system
FROM (
  SELECT DISTINCT trim(sesso) AS sesso
  FROM istat_landing.lt_chiamate_vittime
  WHERE trim(coalesce(sesso,'')) <> ''
) t
ORDER BY sesso;


-- dim_motivi_chiamata
DROP TABLE IF EXISTS istat_transformation.dim_motivi_chiamata;
CREATE TABLE istat_transformation.dim_motivi_chiamata AS
SELECT row_number() OVER (ORDER BY motivi_della_chiamata) AS ids_motivi_chiamata,
       motivi_della_chiamata
FROM (
  SELECT DISTINCT motivi_della_chiamata
  FROM istat_landing.lt_chiamate_vittime
  WHERE trim(coalesce(motivi_della_chiamata,'')) <> ''
) t
ORDER BY motivi_della_chiamata;


-- dim_fascia_eta (unificata, proveniente da due landing)
DROP TABLE IF EXISTS istat_transformation.dim_fascia_eta;
CREATE TABLE istat_transformation.dim_fascia_eta AS
SELECT
  row_number() OVER (ORDER BY eta) AS ids_eta,
  eta,
  NOW()::timestamp AS load_timestamp,
  source_system
FROM (
  SELECT
    eta,
    string_agg(DISTINCT source, ',') AS source_system
  FROM (
    SELECT DISTINCT trim(eta_del_condannato_al_momento_del_reato) AS eta, 'lt_condanne_eta_sesso' AS source
    FROM istat_landing.lt_condanne_eta_sesso
    WHERE trim(coalesce(eta_del_condannato_al_momento_del_reato, '')) <> ''

    UNION ALL

    SELECT DISTINCT trim(eta) AS eta, 'lt_denunce_delitti' AS source
    FROM istat_landing.lt_denunce_delitti
    WHERE trim(coalesce(eta, '')) <> ''
  ) x
  GROUP BY eta
) t
ORDER BY eta;


-- dim_anno (unisce anni da più landing)
DROP TABLE IF EXISTS istat_transformation.dim_anno;
CREATE TABLE istat_transformation.dim_anno AS
SELECT
  ROW_NUMBER() OVER (ORDER BY time_period) AS ids_anno,
  time_period,
  NOW()::timestamp AS load_timestamp,
  'landing_combined' AS source_system
FROM (
  SELECT DISTINCT time_period FROM istat_landing.lt_chiamate_vittime
  UNION
  SELECT DISTINCT time_period FROM istat_landing.lt_condanne_reati_violenti_sesso_reg
  UNION
  SELECT DISTINCT time_period FROM istat_landing.lt_condanne_eta_sesso
  UNION
  SELECT DISTINCT time_period FROM istat_landing.lt_denunce_delitti
) t
WHERE time_period IS NOT NULL
ORDER BY time_period;


-- dim_tipo_reato (unificata e traccia sorgente)
DROP TABLE IF EXISTS istat_transformation.dim_tipo_reato;
CREATE TABLE istat_transformation.dim_tipo_reato AS
SELECT
  row_number() OVER (ORDER BY tipo_di_reato) AS ids_reato,
  tipo_di_reato,
  NOW()::timestamp AS load_timestamp,
  source_system
FROM (
  SELECT
    tipo_di_reato,
    string_agg(DISTINCT source, ',') AS source_system
  FROM (
    SELECT DISTINCT trim(tipo_di_reato) AS tipo_di_reato, 'lt_condanne_eta_sesso' AS source
    FROM istat_landing.lt_condanne_eta_sesso
    WHERE trim(coalesce(tipo_di_reato, '')) <> ''

    UNION ALL

    SELECT DISTINCT trim(tipo_di_reato) AS tipo_di_reato, 'lt_condanne_reati_violenti_sesso_reg' AS source
    FROM istat_landing.lt_condanne_reati_violenti_sesso_reg
    WHERE trim(coalesce(tipo_di_reato, '')) <> ''
  ) x
  GROUP BY tipo_di_reato
) t
ORDER BY tipo_di_reato;


-- dim_area (macro-aree)
DROP TABLE IF EXISTS istat_transformation.dim_area;
CREATE TABLE istat_transformation.dim_area AS
SELECT 
  ROW_NUMBER() OVER (ORDER BY territorio) AS ids_area,
  territorio AS nome_area,
  NOW()::timestamp AS load_timestamp,
  'lt_chiamate_vittime' AS source_system
FROM (
  SELECT DISTINCT territorio 
  FROM istat_landing.lt_chiamate_vittime
  WHERE territorio IN (
    'Nord-ovest','Nord-est','Centro','Sud','Isole'
  )
) t
ORDER BY territorio;


-- mapping province -> regione
DROP TABLE IF EXISTS istat_transformation.mapping_regione_provincia;
CREATE TABLE istat_transformation.mapping_regione_provincia AS
SELECT
  row_number() OVER (ORDER BY provincia) AS ids_provincia,
  provincia,
  regione,
  NOW()::timestamp AS load_timestamp,
  'istat_script_creazione' AS source_system
FROM (
  SELECT DISTINCT provincia, regione
  FROM (VALUES
    ('Agrigento','Sicilia'),
    ('Alessandria','Piemonte'),
    ('Ancona','Marche'),
    ('Aosta','Valle d''Aosta / Vallée d''Aoste'),
    ('Arezzo','Toscana'),
    ('Ascoli Piceno','Marche'),
    ('Asti','Piemonte'),
    ('Avellino','Campania'),
    ('Bari','Puglia'),
    ('Barletta-Andria-Trani','Puglia'),
    ('Belluno','Veneto'),
    ('Benevento','Campania'),
    ('Bergamo','Lombardia'),
    ('Biella','Piemonte'),
    ('Bologna','Emilia-Romagna'),
    ('Bolzano','Trentino Alto Adige / Südtirol'),
    ('Brescia','Lombardia'),
    ('Brindisi','Puglia'),
    ('Cagliari','Sardegna'),
    ('Caltanissetta','Sicilia'),
    ('Campobasso','Molise'),
    ('Caserta','Campania'),
    ('Catania','Sicilia'),
    ('Catanzaro','Calabria'),
    ('Chieti','Abruzzo'),
    ('Cosenza','Calabria'),
    ('Como','Lombardia'),
    ('Cremona','Lombardia'),
    ('Crotone','Calabria'),
    ('Cuneo','Piemonte'),
    ('Enna','Sicilia'),
    ('Fermo','Marche'),
    ('Ferrara','Emilia-Romagna'),
    ('Firenze','Toscana'),
    ('Foggia','Puglia'),
    ('Forlì-Cesena','Emilia-Romagna'),
    ('Frosinone','Lazio'),
    ('Genova','Liguria'),
    ('Gorizia','Friuli-Venezia Giulia'),
    ('Grosseto','Toscana'),
    ('Imperia','Liguria'),
    ('Isernia','Molise'),
    ('La Spezia','Liguria'),
    ('L''Aquila','Abruzzo'),
    ('Latina','Lazio'),
    ('Lecce','Puglia'),
    ('Lecco','Lombardia'),
    ('Livorno','Toscana'),
    ('Lodi','Lombardia'),
    ('Lucca','Toscana'),
    ('Macerata','Marche'),
    ('Mantova','Lombardia'),
    ('Massa-Carrara','Toscana'),
    ('Matera','Basilicata'),
    ('Messina','Sicilia'),
    ('Milano','Lombardia'),
    ('Modena','Emilia-Romagna'),
    ('Monza e della Brianza','Lombardia'),
    ('Napoli','Campania'),
    ('Novara','Piemonte'),
    ('Nuoro','Sardegna'),
    ('Oristano','Sardegna'),
    ('Padova','Veneto'),
    ('Palermo','Sicilia'),
    ('Parma','Emilia-Romagna'),
    ('Pavia','Lombardia'),
    ('Perugia','Umbria'),
    ('Pesaro e Urbino','Marche'),
    ('Pescara','Abruzzo'),
    ('Piacenza','Emilia-Romagna'),
    ('Pisa','Toscana'),
    ('Pistoia','Toscana'),
    ('Pordenone','Friuli-Venezia Giulia'),
    ('Potenza','Basilicata'),
    ('Prato','Toscana'),
    ('Reggio nell''Emilia','Emilia-Romagna'),
    ('Reggio di Calabria','Calabria'),
    ('Ragusa','Sicilia'),
    ('Ravenna','Emilia-Romagna'),
    ('Rimini','Emilia-Romagna'),
    ('Roma','Lazio'),
    ('Rieti','Lazio'),
    ('Rovigo','Veneto'),
    ('Salerno','Campania'),
    ('Sassari','Sardegna'),
    ('Savona','Liguria'),
    ('Siena','Toscana'),
    ('Siracusa','Sicilia'),
    ('Sondrio','Lombardia'),
    ('Taranto','Puglia'),
    ('Teramo','Abruzzo'),
    ('Terni','Umbria'),
    ('Trapani','Sicilia'),
    ('Trento','Trentino Alto Adige / Südtirol'),
    ('Treviso','Veneto'),
    ('Trieste','Friuli-Venezia Giulia'),
    ('Torino','Piemonte'),
    ('Udine','Friuli-Venezia Giulia'),
    ('Varese','Lombardia'),
    ('Venezia','Veneto'),
    ('Verbano-Cusio-Ossola','Piemonte'),
    ('Vercelli','Piemonte'),
    ('Verona','Veneto'),
    ('Vibo Valentia','Calabria'),
    ('Vicenza','Veneto'),
    ('Viterbo','Lazio')
  ) AS v(provincia, regione)
) s
ORDER BY provincia;


-- fine transformation schema


-- ora creo schema dwh e i fact (istat_dwh per chiarezza)
DROP SCHEMA IF EXISTS istat_dwh CASCADE;
CREATE SCHEMA istat_dwh;

-- Crea fact_vittime nello schema istat_dwh
DROP TABLE IF EXISTS istat_dwh.fact_vittime;
CREATE TABLE istat_dwh.fact_vittime AS
SELECT
  row_number() OVER (ORDER BY lv.time_period, lv.territorio) AS ids,
  dt.ids_territorio,
  ds.ids_sesso,
  mc.ids_motivi_chiamata,
  da.ids_anno,
  lv.osservazione AS numero_chiamate,
  NOW()::timestamp AS load_timestamp,
  'lt_chiamate_vittime' AS source_system
FROM istat_landing.lt_chiamate_vittime lv
JOIN istat_transformation.dim_territorio dt ON dt.territorio = lv.territorio
JOIN istat_transformation.dim_sesso ds ON ds.sesso = lv.sesso
JOIN istat_transformation.dim_motivi_chiamata mc ON mc.motivi_della_chiamata = lv.motivi_della_chiamata
JOIN istat_transformation.dim_anno da ON da.time_period = lv.time_period
ORDER BY ids;


-- crea dim_eta e altre dim locali in transformation sono già create; qui non duplicare


-- crea dim_regioni (se preferisci da mapping, tieni questa versione)
DROP TABLE IF EXISTS istat_transformation.dim_regioni;
CREATE TABLE istat_transformation.dim_regioni AS
SELECT row_number() OVER (ORDER BY regione) AS ids_regione,
       regione
FROM (
  SELECT DISTINCT regione
  FROM istat_transformation.mapping_regione_provincia
  WHERE trim(coalesce(regione,'')) <> ''
) t
ORDER BY regione;


-- crea fact_denunce_delitti (usa dim_fascia_eta)
DROP TABLE IF EXISTS istat_dwh.fact_denunce_delitti;
CREATE TABLE istat_dwh.fact_denunce_delitti AS
SELECT
  row_number() OVER (ORDER BY dd.time_period, dd.territorio) AS ids,
  mpc.ids_provincia AS ids_provincia,
  di.ids_indicatore,
  dtd.ids_tipo_delitto,
  ds.ids_sesso,
  dfe.ids_eta AS ids_fascia_eta,
  da.ids_anno,
  dd.osservazione AS numero_denunce,
  NOW()::timestamp AS load_timestamp,
  'lt_denunce_delitti' AS source_system
FROM istat_landing.lt_denunce_delitti dd
JOIN istat_transformation.mapping_regione_provincia mpc ON mpc.regione = dd.territorio
JOIN istat_transformation.dim_indicatore di ON di.indicatore = dd.indicatore
JOIN istat_transformation.dim_tipo_delitto dtd ON dtd.tipo_di_delitto = dd.tipo_di_delitto
JOIN istat_transformation.dim_sesso ds ON ds.sesso = dd.sesso
JOIN istat_transformation.dim_fascia_eta dfe ON dfe.eta = dd.eta
JOIN istat_transformation.dim_anno da ON da.time_period = dd.time_period
ORDER BY ids;


-- crea fact_chiamate (usa dim_regioni che abbiamo creato dalla mapping)
DROP TABLE IF EXISTS istat_dwh.fact_chiamate;
CREATE TABLE istat_dwh.fact_chiamate AS
SELECT
  row_number() OVER (ORDER BY lv.time_period, lv.territorio) AS ids,
  dr.ids_regione,
  ds.ids_sesso,
  mc.ids_motivi_chiamata,
  da.ids_anno,
  lv.osservazione AS numero_chiamate,
  NOW()::timestamp AS load_timestamp,
  'lt_chiamate_vittime' AS source_system
FROM istat_landing.lt_chiamate_vittime lv
JOIN istat_transformation.dim_regioni dr ON lv.territorio = dr.regione
JOIN istat_transformation.dim_sesso ds ON ds.sesso = lv.sesso
JOIN istat_transformation.dim_motivi_chiamata mc ON mc.motivi_della_chiamata = lv.motivi_della_chiamata
JOIN istat_transformation.dim_anno da ON da.time_period = lv.time_period
ORDER BY ids;


-- correggi il drop table / create del fact_condanne_reati_sesso_tot
DROP TABLE IF EXISTS istat_dwh.fact_condanne_reati_sesso_tot;
CREATE TABLE istat_dwh.fact_condanne_reati_sesso_tot AS
SELECT
  dr.ids_regione,
  l.territorio,
  l.osservazione AS tot_condanne,
  da.ids_anno,
  l.time_period AS anno,
  dtr.ids_reato,
  ds.ids_sesso,
  l.sesso,
  NOW()::timestamp AS load_timestamp,
  'lt_condanne_reati_violenti_sesso_reg' AS source_system
FROM istat_landing.lt_condanne_reati_violenti_sesso_reg l
LEFT JOIN istat_transformation.dim_regioni dr
  ON trim(lower(l.territorio)) = trim(lower(dr.regione))
LEFT JOIN istat_transformation.dim_anno da
  ON l.time_period = da.time_period
LEFT JOIN istat_transformation.dim_tipo_reato dtr
  ON trim(lower(l.tipo_di_reato)) = trim(lower(dtr.tipo_di_reato))
LEFT JOIN istat_transformation.dim_sesso ds
  ON trim(lower(l.sesso)) = trim(lower(ds.sesso));


-- crea fact_condanne_eta_sesso (usa dim_fascia_eta)
DROP TABLE IF EXISTS istat_dwh.fact_condanne_eta_sesso;
CREATE TABLE istat_dwh.fact_condanne_eta_sesso AS
SELECT
  dr.ids_regione,
  l.territorio,
  da.ids_anno,
  l.time_period AS anno,
  ds.ids_sesso,
  l.sesso,
  dea.ids_eta,
  l.eta_del_condannato_al_momento_del_reato AS eta,
  dtr.ids_reato,
  l.tipo_di_reato,
  COALESCE(l.numero_condanne, 0) AS numero_condanne,
  NOW()::timestamp AS load_timestamp,
  'lt_condanne_eta_sesso' AS source_system
FROM istat_landing.lt_condanne_eta_sesso l
LEFT JOIN istat_transformation.dim_regioni dr
  ON lower(trim(l.territorio)) = lower(trim(dr.regione))
LEFT JOIN istat_transformation.dim_anno da
  ON l.time_period = da.time_period
LEFT JOIN istat_transformation.dim_sesso ds
  ON lower(trim(l.sesso)) = lower(trim(ds.sesso))
LEFT JOIN istat_transformation.dim_fascia_eta dea
  ON lower(trim(l.eta_del_condannato_al_momento_del_reato)) = lower(trim(coalesce(dea.eta, '')))
LEFT JOIN istat_transformation.dim_tipo_reato dtr
  ON lower(trim(l.tipo_di_reato)) = lower(trim(dtr.tipo_di_reato))
WHERE lower(trim(coalesce(l.sesso,''))) <> 'totale'
  AND trim(coalesce(l.territorio,'')) <> ''
  AND NOT (
    lower(trim(l.territorio)) IN ('italia','estero','sud','nord-est','nord-ovest','centro','isole')
    OR lower(trim(l.territorio)) LIKE 'provincia autonoma%'
  );