
-- schema transformation
drop schema if exists istat_transformation cascade;
create schema istat_transformation;

set search_path to istat_transformation;

-- creazione dimensioni in transformation schema

-- dim_territorio (da lt_chiamate_vittime)
drop table if exists istat_transformation.dim_territorio;
create table istat_transformation.dim_territorio as
select row_number() over (order by territorio) as ids_territorio,
       territorio
from (
  select distinct territorio
  from istat_landing.lt_chiamate_vittime
) t
order by territorio;

---------------
-- dim_nazione
---------------
DROP TABLE IF EXISTS istat_transformation.dim_nazione;

CREATE TABLE istat_transformation.dim_nazione (
  ids_nazione integer PRIMARY KEY,
  nazione varchar NOT NULL,
  load_timestamp timestamp without time zone,
  source_system varchar
);

INSERT INTO istat_transformation.dim_nazione (ids_nazione, nazione, load_timestamp, source_system) VALUES
  (1, 'Italia', NOW()::timestamp, 'a mano'),
  (2, 'Estero', NOW()::timestamp, 'a mano');

-----------------
-- dim_indicatore
-----------------

drop table if exists istat_transformation.dim_indicatore;
create table istat_transformation.dim_indicatore as
select
	row_number() over (order by indicatore) as ids_indicatore,
	indicatore,
	now()::timestamp as load_timestamp,
	'lt_denunce_delitti' as source_system
from (
select distinct trim(indicatore) as indicatore
from istat_landing.lt_denunce_delitti
where trim(coalesce(indicatore, '')) <> ''
) t
order by indicatore;

-- crea dim_tipo_delitto
drop table if exists istat_transformation.dim_tipo_delitto;
create table istat_transformation.dim_tipo_delitto as
select
  row_number() over (order by tipo_di_delitto) as ids_tipo_delitto,
  tipo_di_delitto,
  now()::timestamp as load_timestamp,
  'lt_denunce_delitti' as source_system
from (
  select distinct trim(tipo_di_delitto) as tipo_di_delitto
  from istat_landing.lt_denunce_delitti
  where trim(coalesce(tipo_di_delitto, '')) <> ''
) t
order by tipo_di_delitto;

-- dim_sesso
drop table if exists istat_transformation.dim_sesso;
create table istat_transformation.dim_sesso as
select
  row_number() over (order by sesso) as ids_sesso,
  sesso,
  now()::timestamp as load_timestamp,
  'lt_chiamate_vittime' as source_system
from (
  select distinct trim(sesso) as sesso
  from istat_landing.lt_chiamate_vittime
  where trim(coalesce(sesso,'')) <> ''
) t
order by sesso;


-- dim_motivi_chiamata
drop table if exists istat_transformation.dim_motivi_chiamata;
create table istat_transformation.dim_motivi_chiamata as
select row_number() over (order by motivi_della_chiamata) as ids_motivi_chiamata,
       motivi_della_chiamata
from (
  select distinct motivi_della_chiamata
  from istat_landing.lt_chiamate_vittime
  where trim(coalesce(motivi_della_chiamata,'')) <> ''
) t
order by motivi_della_chiamata;

-------------------------------------------------------
-- dim_fascia_eta (unificata, proveniente da due landing)
-------------------------------------------------------

drop table if exists istat_transformation.dim_fascia_eta;
create table istat_transformation.dim_fascia_eta as
select
  row_number() over (order by eta) as ids_eta,
  eta,
  now()::timestamp as load_timestamp,
  source_system
from (
  select
    eta,
    string_agg(distinct source, ',') as source_system
  from (
    select distinct trim(eta_del_condannato_al_momento_del_reato) as eta, 'lt_condanne_eta_sesso' as source
    from istat_landing.lt_condanne_eta_sesso
    where trim(coalesce(eta_del_condannato_al_momento_del_reato, '')) <> ''

    union

    select distinct trim(età) as eta, 'lt_denunce_delitti' as source
    from istat_landing.lt_denunce_delitti
    where trim(coalesce(età, '')) <> ''
  ) t
  group by eta
) t1
order by eta;


-- dim_anno (unisce anni da più landing)
drop table if exists istat_transformation.dim_anno;
create table istat_transformation.dim_anno as
select
  row_number() over (order by time_period) as ids_anno,
  time_period,
  now()::timestamp as load_timestamp,
  'landing_combined' as source_system
from (
  select distinct time_period from istat_landing.lt_chiamate_vittime
  union
  select distinct time_period from istat_landing.lt_condanne_reati_violenti_sesso_reg
  union
  select distinct time_period from istat_landing.lt_condanne_eta_sesso
  union
  select distinct time_period from istat_landing.lt_denunce_delitti
) t
where time_period is not null
order by time_period;


-- dim_tipo_reato (unificata e traccia sorgente)
drop table if exists istat_transformation.dim_tipo_reato;
create table istat_transformation.dim_tipo_reato as
select
  row_number() over (order by tipo_di_reato) as ids_reato,
  tipo_di_reato,
  now()::timestamp as load_timestamp,
  source_system
from (
  select
    tipo_di_reato,
    string_agg(distinct source, ',') as source_system
  from (
    select distinct trim(tipo_di_reato) as tipo_di_reato, 'lt_condanne_eta_sesso' as source
    from istat_landing.lt_condanne_eta_sesso
    where trim(coalesce(tipo_di_reato, '')) <> ''

    union --all

    select distinct trim(tipo_di_reato) as tipo_di_reato, 'lt_condanne_reati_violenti_sesso_reg' as source
    from istat_landing.lt_condanne_reati_violenti_sesso_reg
    where trim(coalesce(tipo_di_reato, '')) <> ''
  ) x
  group by tipo_di_reato
) t
order by tipo_di_reato;

-- Crea dim_area (con lista a mano)
DROP TABLE IF EXISTS istat_transformation.dim_area;
CREATE TABLE istat_transformation.dim_area AS
SELECT
  row_number() OVER (ORDER BY nome_area) AS ids_area,
  nome_area,
  NOW()::timestamp AS load_timestamp,
  'a mano' AS source_system
FROM (
  VALUES
    ('Nord-ovest'),
    ('Nord-est'),
    ('Centro'),
    ('Sud'),
    ('Isole'),
    ('Italia'),
    ('Non indicato')
) v(nome_area);

-- mapping regione -> area 
DROP TABLE IF EXISTS istat_transformation.mapping_regione_area;
CREATE TABLE istat_transformation.mapping_regione_area (
  regione varchar,
  nome_area varchar,
  load_timestamp timestamp,
  source_system varchar
);
INSERT INTO istat_transformation.mapping_regione_area (regione, nome_area, load_timestamp, source_system) VALUES
  -- Nord-ovest
  ('Piemonte','Nord-ovest', NOW(), 'a mano'),
  ('Valle d''Aosta / Vallée d''Aoste','Nord-ovest', NOW(), 'a mano'),
  ('Liguria','Nord-ovest', NOW(), 'a mano'),
  ('Lombardia','Nord-ovest', NOW(), 'a mano'),
  -- Nord-est
  ('Veneto','Nord-est', NOW(), 'a mano'),
  ('Friuli-Venezia Giulia','Nord-est', NOW(), 'a mano'),
  ('Trentino Alto Adige / Südtirol','Nord-est', NOW(), 'a mano'),
  ('Emilia-Romagna','Nord-est', NOW(), 'a mano'),
  -- Centro
  ('Toscana','Centro', NOW(), 'a mano'),
  ('Umbria','Centro', NOW(), 'a mano'),
  ('Marche','Centro', NOW(), 'a mano'),
  ('Lazio','Centro', NOW(), 'a mano'),
  -- Sud
  ('Abruzzo','Sud', NOW(), 'a mano'),
  ('Molise','Sud', NOW(), 'a mano'),
  ('Campania','Sud', NOW(), 'a mano'),
  ('Puglia','Sud', NOW(), 'a mano'),
  ('Basilicata','Sud', NOW(), 'a mano'),
  ('Calabria','Sud', NOW(), 'a mano'),
  -- Isole
  ('Sicilia','Isole', NOW(), 'a mano'),
  ('Sardegna','Isole', NOW(), 'a mano'),
  -- voci speciali
  ('Italia','Italia', NOW(), 'a mano'),
  ('Non indicato','Non indicato', NOW(), 'a mano')
  -- non so se serva metterle qui
  --('valle d''aosta / vallée d''aoste','Nord-ovest')
  ;


-- per usare la mapping quando carico il fact_chiamate
-- JOIN istat_transformation.view_region_to_area vra ON lower(trim(lv.territorio)) = lower(trim(vra.regione))
-- poi usa vra.ids_area come foreign key nella tabella fact (o faccio join su dim_area tramite nome_area)
-- qui non sono certo xche' e' tardi ed ho consultato copilot


-- mapping province -> regione
drop table if exists istat_transformation.mapping_regione_provincia;
create table istat_transformation.mapping_regione_provincia as
select
  row_number() over (order by provincia) as ids_provincia,
  provincia,
  regione,
  now()::timestamp as load_timestamp,
  'istat_script_creazione' as source_system
from (
  select distinct provincia, regione
from (values
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
  ) as v(provincia, regione)
) s
order by provincia;


-- fine transformation schema


-- ora creo schema dwh e i fact (istat_dwh per chiarezza)
drop schema if exists istat_dwh cascade;
create schema istat_dwh;

-- crea fact_vittime nello schema istat_dwh
drop table if exists istat_dwh.fact_vittime;
create table istat_dwh.fact_vittime as
select
  row_number() over (order by lv.time_period, lv.territorio) as ids,
  dt.ids_territorio,
  ds.ids_sesso,
  mc.ids_motivi_chiamata,
  da.ids_anno,
  lv.osservazione as numero_chiamate,
  now()::timestamp as load_timestamp,
  'lt_chiamate_vittime' as source_system
from istat_landing.lt_chiamate_vittime lv
join istat_transformation.dim_territorio dt on dt.territorio = lv.territorio
join istat_transformation.dim_sesso ds on ds.sesso = lv.sesso
join istat_transformation.dim_motivi_chiamata mc on mc.motivi_della_chiamata = lv.motivi_della_chiamata
join istat_transformation.dim_anno da on da.time_period = lv.time_period
order by ids;

/* select count(*) from istat_dwh.fact_vittime lft
union all
select count(*) from istat_landing.lt_chiamate_vittime lcv
*/ 

-- crea dim_eta e altre dim locali in transformation sono già create; qui non duplica


-- crea dim_regioni (se preferisci da mapping, tieni questa versione)
drop table if exists istat_transformation.dim_regioni;
create table istat_transformation.dim_regioni as
select row_number() over (order by regione) as ids_regione,
       regione,
        now()::timestamp as load_timestamp,
        'mapping_regione_provincia' as source_system
from (
  select distinct regione
  from istat_transformation.mapping_regione_provincia
  where trim(coalesce(regione,'')) <> ''
) t
order by regione;


DROP TABLE IF EXISTS istat_dwh.fact_denunce_delitti;

CREATE TABLE istat_dwh.fact_denunce_delitti AS
SELECT
  row_number() OVER (ORDER BY dd.time_period, dd.territorio) AS ids,
  dr.ids_regione AS ids_regione,
  di.ids_indicatore,
  dtd.ids_tipo_delitto,
  ds.ids_sesso,
  dfe.ids_eta AS ids_fascia_eta,
  da.ids_anno,
  dd.osservazione AS numero_denunce,
  NOW()::timestamp AS load_timestamp,
  'lt_denunce_delitti' AS source_system
FROM istat_landing.lt_denunce_delitti dd
left JOIN istat_transformation.dim_regioni dr
  ON lower(trim(dr.regione)) = lower(trim(dd.territorio))
left JOIN istat_transformation.dim_indicatore di
  ON lower(trim(di.indicatore)) = lower(trim(dd.indicatore))
left JOIN istat_transformation.dim_tipo_delitto dtd
  ON lower(trim(dtd.tipo_di_delitto)) = lower(trim(dd.tipo_di_delitto))
JOIN istat_transformation.dim_sesso ds
  ON lower(trim(ds.sesso)) = lower(trim(dd.sesso))
JOIN istat_transformation.dim_fascia_eta dfe
  ON lower(trim(dfe.eta)) = lower(trim(dd.età))
JOIN istat_transformation.dim_anno da
  ON da.time_period = dd.time_period
ORDER BY ids;

-- select count(*) from istat_dwh.fact_denunce_delitti;


--------
--SQUADRA :(
--------

-- crea fact_chiamate (usa dim_regioni che abbiamo creato dalla mapping)
drop table if exists istat_dwh.fact_chiamate;
create table istat_dwh.fact_chiamate as
select
  row_number() over (order by lv.time_period, lv.territorio) as ids,
  dr.ids_regione,
  ds.ids_sesso,
  mc.ids_motivi_chiamata,
  da.ids_anno,
  lv.osservazione as numero_chiamate,
  now()::timestamp as load_timestamp,
  'lt_chiamate_vittime' as source_system
from istat_landing.lt_chiamate_vittime lv
join istat_transformation.dim_regioni dr on lv.territorio = dr.regione
join istat_transformation.dim_sesso ds on ds.sesso = lv.sesso
join istat_transformation.dim_motivi_chiamata mc on mc.motivi_della_chiamata = lv.motivi_della_chiamata
join istat_transformation.dim_anno da on da.time_period = lv.time_period
order by ids;


/* 
select count(*) from istat_dwh.fact_chiamate
union all
select count(*) from istat_landing.lt_chiamate_vittime lcv ;
*/

---------------
-- da rivdere quale usare
---------------


/*
create table if not exists istat_dwh.fact_condanne_reati_sesso_tot as
select row_number() over() as 
	ids, 
	ids_nazione,
	ids_reato, 
	ids_sesso,  
	ids_anno, 
	osservazione as numero_condanne,
  now()::timestamp as load_timestamp,
  'lt_condanne_reati_violenti_sesso_reg' as source_system
from istat_landing.lt_condanne_reati_violenti_sesso_reg crv
join istat_transformation.dim_nazione itdn on itdn.nazione=crv.territorio
join istat_transformation.dim_tipo_reato dtr on dtr.tipo_di_reato=crv.tipo_di_reato
join istat_transformation.dim_sesso ds on ds.sesso=crv.sesso
join istat_transformation.dim_anno da on da.time_period=crv.time_period
order by ids asc;
*/


--  create del fact_condanne_reati_sesso_tot

drop table if exists istat_dwh.fact_condanne_reati_sesso_tot;
create table istat_dwh.fact_condanne_reati_sesso_tot as
select
  dr.ids_regione,
  l.territorio,
  l.osservazione as tot_condanne,
  da.ids_anno,
  l.time_period as anno,
  dtr.ids_reato,
  ds.ids_sesso,
  l.sesso,
  now()::timestamp as load_timestamp,
  'lt_condanne_reati_violenti_sesso_reg' as source_system
from istat_landing.lt_condanne_reati_violenti_sesso_reg l
left join istat_transformation.dim_regioni dr
  on trim(lower(l.territorio)) = trim(lower(dr.regione))
left join istat_transformation.dim_anno da
  on l.time_period = da.time_period
left join istat_transformation.dim_tipo_reato dtr
  on trim(lower(l.tipo_di_reato)) = trim(lower(dtr.tipo_di_reato))
left join istat_transformation.dim_sesso ds
  on trim(lower(l.sesso)) = trim(lower(ds.sesso));


-- crea fact_condanne_eta_sesso (usa dim_fascia_eta)
drop table if exists istat_dwh.fact_condanne_reati_eta_sesso;
create table istat_dwh.fact_condanne_reati_eta_sesso as
select
  dr.ids_regione,
  l.territorio,
  da.ids_anno,
  l.time_period as anno,
  ds.ids_sesso,
  l.sesso,
  dea.ids_eta,
  l.eta_del_condannato_al_momento_del_reato as eta,
  dtr.ids_reato,
  l.tipo_di_reato,
  coalesce(l.numero_condanne, 0) as numero_condanne,
  now()::timestamp as load_timestamp,
  'lt_condanne_eta_sesso' as source_system
from istat_landing.lt_condanne_eta_sesso l
join istat_transformation.dim_regioni dr
  on lower(trim(l.territorio)) = lower(trim(dr.regione))
join istat_transformation.dim_anno da
  on l.time_period = da.time_period
join istat_transformation.dim_sesso ds
  on lower(trim(l.sesso)) = lower(trim(ds.sesso))
join istat_transformation.dim_fascia_eta dea
  on lower(trim(l.eta_del_condannato_al_momento_del_reato)) = lower(trim(coalesce(dea.eta, '')))
join istat_transformation.dim_tipo_reato dtr
  on lower(trim(l.tipo_di_reato)) = lower(trim(dtr.tipo_di_reato))
where lower(trim(coalesce(l.sesso,''))) <> 'totale'
  and trim(coalesce(l.territorio,'')) <> ''
  and not (
    lower(trim(l.territorio)) in ('italia','estero','sud','nord-est','nord-ovest','centro','isole')
    or lower(trim(l.territorio)) like 'provincia autonoma%'
  );

/*
select count(*)	from istat_dwh.fact_condanne_reati_sesso_tot fcrst
union
select count(*) from istat_dwh.fact_condanne_reati_eta_sesso fces
*/

--------------------------------
--COPIO TUTTO IN DWH 
--------------------------------


-- creazione dimensioni in dwh_progettoandromeda schema

-- dim_territorio (da lt_chiamate_vittime)
drop table if exists dwh_progettoandromeda.dim_territorio;
create table dwh_progettoandromeda.dim_territorio as
select row_number() over (order by territorio) as ids_territorio,
       territorio
from (
  select distinct territorio
  from istat_landing.lt_chiamate_vittime
) t
order by territorio;

---------------
-- dim_nazione
---------------
DROP TABLE IF EXISTS dwh_progettoandromeda.dim_nazione;

CREATE TABLE dwh_progettoandromeda.dim_nazione (
  ids_nazione integer PRIMARY KEY,
  nazione varchar NOT NULL,
  load_timestamp timestamp without time zone,
  source_system varchar
);

INSERT INTO dwh_progettoandromeda.dim_nazione (ids_nazione, nazione, load_timestamp, source_system) VALUES
  (1, 'Italia', NOW()::timestamp, 'a mano'),
  (2, 'Estero', NOW()::timestamp, 'a mano');

-----------------
-- dim_indicatore
-----------------

drop table if exists dwh_progettoandromeda.dim_indicatore;
create table dwh_progettoandromeda.dim_indicatore as
select
	row_number() over (order by indicatore) as ids_indicatore,
	indicatore,
	now()::timestamp as load_timestamp,
	'lt_denunce_delitti' as source_system
from (
select distinct trim(indicatore) as indicatore
from istat_landing.lt_denunce_delitti
where trim(coalesce(indicatore, '')) <> ''
) t
order by indicatore;

-------------------------
-- crea dim_tipo_delitto
-------------------------

drop table if exists dwh_progettoandromeda.dim_tipo_delitto;
create table dwh_progettoandromeda.dim_tipo_delitto as
select
  row_number() over (order by tipo_di_delitto) as ids_tipo_delitto,
  tipo_di_delitto,
  now()::timestamp as load_timestamp,
  'lt_denunce_delitti' as source_system
from (
  select distinct trim(tipo_di_delitto) as tipo_di_delitto
  from istat_landing.lt_denunce_delitti
  where trim(coalesce(tipo_di_delitto, '')) <> ''
) t
order by tipo_di_delitto;

--------------
-- dim_sesso
--------------

drop table if exists dwh_progettoandromeda.dim_sesso;
create table dwh_progettoandromeda.dim_sesso as
select
  row_number() over (order by sesso) as ids_sesso,
  sesso,
  now()::timestamp as load_timestamp,
  'lt_chiamate_vittime' as source_system
from (
  select distinct trim(sesso) as sesso
  from istat_landing.lt_chiamate_vittime
  where trim(coalesce(sesso,'')) <> ''
) t
order by sesso;

---------------------
-- dim_motivi_chiamata
---------------------

drop table if exists dwh_progettoandromeda.dim_motivi_chiamata;
create table dwh_progettoandromeda.dim_motivi_chiamata as
select row_number() over (order by motivi_della_chiamata) as ids_motivi_chiamata,
       motivi_della_chiamata
from (
  select distinct motivi_della_chiamata
  from istat_landing.lt_chiamate_vittime
  where trim(coalesce(motivi_della_chiamata,'')) <> ''
) t
order by motivi_della_chiamata;

-------------------------------------------------------
-- dim_fascia_eta_istat (unificata, proveniente da due landing)
-------------------------------------------------------

drop table if exists dwh_progettoandromeda.dim_fascia_eta_istat;
create table dwh_progettoandromeda.dim_fascia_eta_istat as
select
  row_number() over (order by eta) as ids_eta,
  eta,
  now()::timestamp as load_timestamp,
  source_system
from (
  select
    eta,
    string_agg(distinct source, ',') as source_system
  from (
    select distinct trim(eta_del_condannato_al_momento_del_reato) as eta, 'lt_condanne_eta_sesso' as source
    from istat_landing.lt_condanne_eta_sesso
    where trim(coalesce(eta_del_condannato_al_momento_del_reato, '')) <> ''

    union

    select distinct trim(età) as eta, 'lt_denunce_delitti' as source
    from istat_landing.lt_denunce_delitti
    where trim(coalesce(età, '')) <> ''
  ) t
  group by eta
) t1
order by eta;


-- dim_anno (unisce anni da più landing)
drop table if exists dwh_progettoandromeda.dim_anno;
create table dwh_progettoandromeda.dim_anno as
select
  row_number() over (order by time_period) as ids_anno,
  time_period,
  now()::timestamp as load_timestamp,
  'landing_combined' as source_system
from (
  select distinct time_period from istat_landing.lt_chiamate_vittime
  union
  select distinct time_period from istat_landing.lt_condanne_reati_violenti_sesso_reg
  union
  select distinct time_period from istat_landing.lt_condanne_eta_sesso
  union
  select distinct time_period from istat_landing.lt_denunce_delitti
) t
where time_period is not null
order by time_period;


-- dim_tipo_reato (unificata e traccia sorgente)
drop table if exists dwh_progettoandromeda.dim_tipo_reato;
create table dwh_progettoandromeda.dim_tipo_reato as
select
  row_number() over (order by tipo_di_reato) as ids_reato,
  tipo_di_reato,
  now()::timestamp as load_timestamp,
  source_system
from (
  select
    tipo_di_reato,
    string_agg(distinct source, ',') as source_system
  from (
    select distinct trim(tipo_di_reato) as tipo_di_reato, 'lt_condanne_eta_sesso' as source
    from istat_landing.lt_condanne_eta_sesso
    where trim(coalesce(tipo_di_reato, '')) <> ''

    union --all

    select distinct trim(tipo_di_reato) as tipo_di_reato, 'lt_condanne_reati_violenti_sesso_reg' as source
    from istat_landing.lt_condanne_reati_violenti_sesso_reg
    where trim(coalesce(tipo_di_reato, '')) <> ''
  ) x
  group by tipo_di_reato
) t
order by tipo_di_reato;

-- Crea dim_area (con lista a mano)
DROP TABLE IF EXISTS dwh_progettoandromeda.dim_area;
CREATE TABLE dwh_progettoandromeda.dim_area AS
SELECT
  row_number() OVER (ORDER BY nome_area) AS ids_area,
  nome_area,
  NOW()::timestamp AS load_timestamp,
  'a mano' AS source_system
FROM (
  VALUES
    ('Nord-ovest'),
    ('Nord-est'),
    ('Centro'),
    ('Sud'),
    ('Isole'),
    ('Italia'),
    ('Non indicato')
) v(nome_area);

-- mapping regione -> area 
DROP TABLE IF EXISTS dwh_progettoandromeda.mapping_regione_area;
CREATE TABLE dwh_progettoandromeda.mapping_regione_area (
  regione varchar,
  nome_area varchar,
  load_timestamp timestamp,
  source_system varchar
);
INSERT INTO dwh_progettoandromeda.mapping_regione_area (regione, nome_area, load_timestamp, source_system) VALUES
  -- Nord-ovest
  ('Piemonte','Nord-ovest', NOW(), 'a mano'),
  ('Valle d''Aosta / Vallée d''Aoste','Nord-ovest', NOW(), 'a mano'),
  ('Liguria','Nord-ovest', NOW(), 'a mano'),
  ('Lombardia','Nord-ovest', NOW(), 'a mano'),
  -- Nord-est
  ('Veneto','Nord-est', NOW(), 'a mano'),
  ('Friuli-Venezia Giulia','Nord-est', NOW(), 'a mano'),
  ('Trentino Alto Adige / Südtirol','Nord-est', NOW(), 'a mano'),
  ('Emilia-Romagna','Nord-est', NOW(), 'a mano'),
  -- Centro
  ('Toscana','Centro', NOW(), 'a mano'),
  ('Umbria','Centro', NOW(), 'a mano'),
  ('Marche','Centro', NOW(), 'a mano'),
  ('Lazio','Centro', NOW(), 'a mano'),
  -- Sud
  ('Abruzzo','Sud', NOW(), 'a mano'),
  ('Molise','Sud', NOW(), 'a mano'),
  ('Campania','Sud', NOW(), 'a mano'),
  ('Puglia','Sud', NOW(), 'a mano'),
  ('Basilicata','Sud', NOW(), 'a mano'),
  ('Calabria','Sud', NOW(), 'a mano'),
  -- Isole
  ('Sicilia','Isole', NOW(), 'a mano'),
  ('Sardegna','Isole', NOW(), 'a mano'),
  -- voci speciali
  ('Italia','Italia', NOW(), 'a mano'),
  ('Non indicato','Non indicato', NOW(), 'a mano')
  -- non so se serva metterle qui
  --('valle d''aosta / vallée d''aoste','Nord-ovest')
  ;


-- per usare la mapping quando carico il fact_chiamate
-- JOIN dwh_progettoandromeda.view_region_to_area vra ON lower(trim(lv.territorio)) = lower(trim(vra.regione))
-- poi usa vra.ids_area come foreign key nella tabella fact (o faccio join su dim_area tramite nome_area)
-- qui non sono certo xche' e' tardi ed ho consultato copilot


-- mapping province -> regione
drop table if exists dwh_progettoandromeda.mapping_regione_provincia;
create table dwh_progettoandromeda.mapping_regione_provincia as
select
  row_number() over (order by provincia) as ids_provincia,
  provincia,
  regione,
  now()::timestamp as load_timestamp,
  'istat_script_creazione' as source_system
from (
  select distinct provincia, regione
from (values
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
  ) as v(provincia, regione)
) s
order by provincia;


-- fine transformation schema

-- crea fact_vittime nello schema dwh_progettoandromeda
drop table if exists dwh_progettoandromeda.fact_vittime;
create table dwh_progettoandromeda.fact_vittime as
select
  row_number() over (order by lv.time_period, lv.territorio) as ids,
  dt.ids_territorio,
  ds.ids_sesso,
  mc.ids_motivi_chiamata,
  da.ids_anno,
  lv.osservazione as numero_chiamate,
  now()::timestamp as load_timestamp,
  'lt_chiamate_vittime' as source_system
from istat_landing.lt_chiamate_vittime lv
join dwh_progettoandromeda.dim_territorio dt on dt.territorio = lv.territorio
join dwh_progettoandromeda.dim_sesso ds on ds.sesso = lv.sesso
join dwh_progettoandromeda.dim_motivi_chiamata mc on mc.motivi_della_chiamata = lv.motivi_della_chiamata
join dwh_progettoandromeda.dim_anno da on da.time_period = lv.time_period
order by ids;

/* select count(*) from dwh_progettoandromeda.fact_vittime lft
union all
select count(*) from istat_landing.lt_chiamate_vittime lcv
*/ 

-- crea dim_eta e altre dim locali in transformation sono già create; qui non duplica

/*
-- crea dim_regioni (se preferisci da mapping, tieni questa versione)
drop table if exists dwh_progettoandromeda.dim_regioni;
create table dwh_progettoandromeda.dim_regioni as
select row_number() over (order by regione) as ids_regione,
       regione,
        now()::timestamp as load_timestamp,
        'mapping_regione_provincia' as source_system
from (
  select distinct regione
  from dwh_progettoandromeda.mapping_regione_provincia
  where trim(coalesce(regione,'')) <> ''
) t
order by regione;*/


DROP TABLE IF EXISTS dwh_progettoandromeda.fact_denunce_delitti;

CREATE TABLE dwh_progettoandromeda.fact_denunce_delitti AS
SELECT
  row_number() OVER (ORDER BY dd.time_period, dd.territorio) AS ids,
  dpr.ids_regione AS ids_regione,
  di.ids_indicatore,
  dtd.ids_tipo_delitto,
  dtd.tipo_di_delitto,
  ds.ids_sesso,
  dfe.ids_eta AS ids_fascia_eta,
  da.ids_anno,
  dd.time_period as anno,
  dd.osservazione AS numero_denunce,
  NOW()::timestamp AS load_timestamp,
  'lt_denunce_delitti' AS source_system
FROM istat_landing.lt_denunce_delitti dd
left JOIN dwh_progettoandromeda.dim_provincia_regione dpr
  ON lower(trim(dpr.territorio)) = lower(trim(dd.territorio))
left join dim_regioni dr
    on trim(upper(dr.regione)) = trim(upper(dd.territorio))
JOIN dwh_progettoandromeda.dim_indicatore di
  ON lower(trim(di.indicatore)) = lower(trim(dd.indicatore))
JOIN dwh_progettoandromeda.dim_tipo_delitto dtd
  ON lower(trim(dtd.tipo_di_delitto)) = lower(trim(dd.tipo_di_delitto))
JOIN dwh_progettoandromeda.dim_sesso ds
  ON lower(trim(ds.sesso)) = lower(trim(dd.sesso))
JOIN dwh_progettoandromeda.dim_fascia_eta_istat dfe
  ON lower(trim(dfe.eta)) = lower(trim(dd.età))
JOIN dwh_progettoandromeda.dim_anno da
  ON da.time_period = dd.time_period
where coalesce(dpr.ids_regione, dr.ids_regione) is not null
ORDER BY numero_denunce desc;

-- select count(*) from dwh_progettoandromeda.fact_denunce_delitti;
-- select * from dwh_progettoandromeda.fact_denunce_delitti
-- select count(ids_tipo_delitto) from dwh_progettoandromeda.fact_denunce_delitti where "ids_tipo_delitto" = 7

-- select count(tipo_di_delitto) from istat_landing.lt_denunce_delitti where "tipo_di_delitto" = 'Percosse'
-- select count(tipo_di_delitto) from istat_landing.lt_denunce_delitti where "tipo_di_delitto" = 'Stalking'
-- select count(tipo_di_delitto) from istat_landing.lt_denunce_delitti where "tipo_di_delitto" = 'Violenze sessuali'
-- select count(tipo_di_delitto) from istat_landing.lt_denunce_delitti where "tipo_di_delitto" = 'Omicidi volontari consumati'

select
  tipo_di_delitto,
  count(*) as cnt,
  sum(count(*)) over () as totale_tutti_i_delitti
from istat_landing.lt_denunce_delitti
group by tipo_di_delitto
order by cnt desc;


-- lista rowcount accurata per tutte le tabelle in schema istat_dwh
-- esegui in psql o client collegato al DB locale

drop table if exists istat_dwh._tmp_table_counts;
create table istat_dwh._tmp_table_counts (
  table_schema text,
  table_name text,
  row_count bigint
);
-- lista rowcount accurata per tutte le tabelle in schema istat_dwh
-- esegui in psql o client collegato al DB locale

drop table if exists istat_dwh._tmp_table_counts;
create table istat_dwh._tmp_table_counts (
  table_schema text,
  table_name text,
  row_count bigint
);

do $$
declare
  r record;
  sql text;
begin
  for r in
    select table_name
    from information_schema.tables
    where table_schema = 'istat_dwh' and table_type = 'BASE TABLE'
    order by table_name
  loop
    sql := format(
      'insert into istat_dwh._tmp_table_counts(table_schema, table_name, row_count) values (%L, %L, (select count(*) from %I.%I))',
      'istat_dwh', r.table_name, 'istat_dwh', r.table_name
    );
    execute sql;
  end loop;
end
$$;

-- riepilogo con totali e percentuali (window function)
select
  table_name,
  row_count,
  sum(row_count) over () as total_rows_all_tables,
  round(100.0 * row_count / nullif(sum(row_count) over (),0), 2) as pct_of_total
from istat_dwh._tmp_table_counts
order by row_count desc;

-- calcola conteggi "partitioned" per colonne che contengono tipo/reato/delitto
-- crea una tabella di riepilogo con cnt, totale, percentuale, rank e cumulativo
drop table if exists istat_dwh._tmp_reati_partition_counts;
create table istat_dwh._tmp_reati_partition_counts (
  table_name text,
  column_name text,
  value text,
  cnt bigint,
  total_rows bigint,
  pct numeric(5,2),
  rank_by_count integer,
  cumulative bigint
);

do $$
declare
  t record;
  c record;
  sql text;
begin
  for t in
    select table_name
    from information_schema.tables
    where table_schema = 'istat_dwh' and table_type = 'BASE TABLE'
    order by table_name
  loop
    for c in
      select column_name
      from information_schema.columns
      where table_schema = 'istat_dwh'
        and table_name = t.table_name
        and (column_name ~* 'tipo|reato|delitto')
    loop
      sql := format($q$
        insert into istat_dwh._tmp_reati_partition_counts(table_name, column_name, value, cnt, total_rows, pct, rank_by_count, cumulative)
        select
          %L as table_name,
          %L as column_name,
          coalesce(cast(val as text),'(null)') as value,
          cnt,
          sum(cnt) over () as total_rows,
          round(100.0 * cnt / nullif(sum(cnt) over (),0), 2) as pct,
          row_number() over (order by cnt desc) as rank_by_count,
          sum(cnt) over (order by cnt desc rows between unbounded preceding and current row) as cumulative
        from (
          select %I as val, count(*) as cnt
          from istat_dwh.%I
          group by %I
        ) x
        order by cnt desc
      $q$, t.table_name, c.column_name, c.column_name, t.table_name, c.column_name);
      execute sql;
    end loop;
  end loop;
end
$$;

-- visualizza il riepilogo ordinato
select *
from istat_dwh._tmp_reati_partition_counts
order by table_name, column_name, rank_by_count;

-- riepilogo conteggi per "tipo_di_reato" e "tipo_di_delitto" preso dalle tabelle landing
drop table if exists istat_dwh._tmp_reati_partition_counts;
create table istat_dwh._tmp_reati_partition_counts (
  table_name text,
  column_name text,
  value text,
  cnt bigint,
  total_rows bigint,
  pct numeric(5,2),
  rank_by_count integer,
  cumulative bigint
);

do $$
declare
  r record;
  sql text;
begin
  for r in
    select table_name, column_name
    from information_schema.columns
    where table_schema = 'istat_landing'
      and column_name in ('tipo_di_reato','tipo_di_delitto')
    order by table_name, column_name
  loop
    sql := format($q$
      insert into istat_dwh._tmp_reati_partition_counts(table_name, column_name, value, cnt, total_rows, pct, rank_by_count, cumulative)
      select
        %L as table_name,
        %L as column_name,
        coalesce(cast(val as text), '(null)') as value,
        cnt,
        sum(cnt) over () as total_rows,
        round(100.0 * cnt / nullif(sum(cnt) over (),0), 2) as pct,
        row_number() over (order by cnt desc) as rank_by_count,
        sum(cnt) over (order by cnt desc rows between unbounded preceding and current row) as cumulative
      from (
        select %I as val, count(*) as cnt
        from istat_landing.%I
        group by %I
      ) x
      order by cnt desc
    $q$, r.table_name, r.column_name, r.column_name, r.table_name, r.column_name);
    execute sql;
  end loop;
end
$$;

select *
from istat_dwh._tmp_reati_partition_counts
order by table_name, column_name, rank_by_count;

-- se vuoi cancellare la tabella temporanea dopo l'analisi:
-- drop table if exists istat_dwh._tmp_table_counts;

-- se vuoi cancellare la tabella temporanea dopo l'analisi:
-- drop table if exists istat_dwh._tmp_table_counts;


/*drop table if exists prova_istat_dwh.fact_denunce_delitti;
create table if not exists prova_istat_dwh.fact_denunce_delitti as
select
    row_number() over() as ids,
    coalesce(dpr.ids_regione, dr.ids_regione) as ids_territorio,
    di.ids_indicatore,
    dtd.ids_tipo_delitto,
    ds.ids_sesso,
    dfe.ids_eta,
    da.ids_anno,
    da.time_period as anno,
    dd.osservazione as numero_denunce,
    now() as load_timestamp,
    'landing' as source_system
from istat_landing.lt_denunce_delitti dd
left join dim_provincia_regione dpr
    on trim(upper(dpr.territorio)) = trim(upper(dd.territorio))
left join dim_regioni dr
    on trim(upper(dr.regione)) = trim(upper(dd.territorio))
join istat_transformation.dim_indicatore di
    on di.indicatore = dd.indicatore
join istat_transformation.dim_tipo_delitto dtd
    on dtd.tipo_di_delitto = dd.tipo_di_delitto
join istat_transformation.dim_sesso ds
    on ds.sesso = dd.sesso
join istat_transformation.dim_fascia_eta dfe
    on dfe.eta = dd.età
join istat_transformation.dim_anno da
    on da.time_period = dd.time_period
where coalesce(dpr.ids_regione, dr.ids_regione) is not null*/


/*
 select count(*) from prova_istat_dwh.fact_denunce_delitti 
 */

--------
--SQUADRA :(
--------

-- crea fact_chiamate (usa dim_regioni che abbiamo creato dalla mapping)
drop table if exists dwh_progettoandromeda.fact_chiamate;
create table dwh_progettoandromeda.fact_chiamate as
select
  row_number() over (order by lv.time_period, lv.territorio) as ids,
  dr.ids_regione,
  ds.ids_sesso,
  mc.ids_motivi_chiamata,
  mc.motivi_della_chiamata,
  da.ids_anno,
  lv.osservazione as numero_chiamate,
  now()::timestamp as load_timestamp,
  'lt_chiamate_vittime' as source_system
from istat_landing.lt_chiamate_vittime lv
join dwh_progettoandromeda.dim_regioni dr on lv.territorio = dr.regione
join dwh_progettoandromeda.dim_sesso ds on ds.sesso = lv.sesso
join dwh_progettoandromeda.dim_motivi_chiamata mc on mc.motivi_della_chiamata = lv.motivi_della_chiamata
join dwh_progettoandromeda.dim_anno da on da.time_period = lv.time_period
order by ids;


/* 
select count(*) from dwh_progettoandromeda.fact_chiamate
union all
select count(*) from istat_landing.lt_chiamate_vittime lcv ;
*/

---------------
-- da rivdere quale usare
---------------


/*
create table if not exists dwh_progettoandromeda.fact_condanne_reati_sesso_tot as
select row_number() over() as 
	ids, 
	ids_nazione,
	ids_reato, 
	ids_sesso,  
	ids_anno, 
	osservazione as numero_condanne,
  now()::timestamp as load_timestamp,
  'lt_condanne_reati_violenti_sesso_reg' as source_system
from istat_landing.lt_condanne_reati_violenti_sesso_reg crv
join dwh_progettoandromeda.dim_nazione itdn on itdn.nazione=crv.territorio
join dwh_progettoandromeda.dim_tipo_reato dtr on dtr.tipo_di_reato=crv.tipo_di_reato
join dwh_progettoandromeda.dim_sesso ds on ds.sesso=crv.sesso
join dwh_progettoandromeda.dim_anno da on da.time_period=crv.time_period
order by ids asc;
*/


--  create del fact_condanne_reati_sesso_tot

drop table if exists dwh_progettoandromeda.fact_condanne_reati_sesso_tot;
create table dwh_progettoandromeda.fact_condanne_reati_sesso_tot as
select
  dr.ids_regione,
  l.territorio,
  l.osservazione as tot_condanne,
  da.ids_anno,
  l.time_period as anno,
  dtr.ids_reato,
  ds.ids_sesso,
  l.sesso,
  now()::timestamp as load_timestamp,
  'lt_condanne_reati_violenti_sesso_reg' as source_system
from istat_landing.lt_condanne_reati_violenti_sesso_reg l
left join dwh_progettoandromeda.dim_regioni dr
  on trim(lower(l.territorio)) = trim(lower(dr.regione))
left join dwh_progettoandromeda.dim_anno da
  on l.time_period = da.time_period
left join dwh_progettoandromeda.dim_tipo_reato dtr
  on trim(lower(l.tipo_di_reato)) = trim(lower(dtr.tipo_di_reato))
left join dwh_progettoandromeda.dim_sesso ds
  on trim(lower(l.sesso)) = trim(lower(ds.sesso));


-- crea fact_condanne_eta_sesso (usa dim_fascia_eta_istat)
drop table if exists dwh_progettoandromeda.fact_condanne_reati_eta_sesso;
create table dwh_progettoandromeda.fact_condanne_reati_eta_sesso as
select
  dr.ids_regione,
  l.territorio,
  da.ids_anno,
  l.time_period as anno,
  ds.ids_sesso,
  l.sesso,
  dea.ids_eta,
  l.eta_del_condannato_al_momento_del_reato as eta,
  dtr.ids_reato,
  l.tipo_di_reato,
  coalesce(l.numero_condanne, 0) as numero_condanne,
  now()::timestamp as load_timestamp,
  'lt_condanne_eta_sesso' as source_system
from istat_landing.lt_condanne_eta_sesso l
join dwh_progettoandromeda.dim_regioni dr
  on lower(trim(l.territorio)) = lower(trim(dr.regione))
join dwh_progettoandromeda.dim_anno da
  on l.time_period = da.time_period
join dwh_progettoandromeda.dim_sesso ds
  on lower(trim(l.sesso)) = lower(trim(ds.sesso))
join dwh_progettoandromeda.dim_fascia_eta_istat dea
  on lower(trim(l.eta_del_condannato_al_momento_del_reato)) = lower(trim(coalesce(dea.eta, '')))
join dwh_progettoandromeda.dim_tipo_reato dtr
  on lower(trim(l.tipo_di_reato)) = lower(trim(dtr.tipo_di_reato))
where lower(trim(coalesce(l.sesso,''))) <> 'totale'
  and trim(coalesce(l.territorio,'')) <> ''
  and not (
    lower(trim(l.territorio)) in ('italia','estero','sud','nord-est','nord-ovest','centro','isole')
    or lower(trim(l.territorio)) like 'provincia autonoma%'
  );

/*
select count(*)	from dwh_progettoandromeda.fact_condanne_reati_sesso_tot fcrst
union
select count(*) from dwh_progettoandromeda.fact_condanne_reati_eta_sesso fces
*/
