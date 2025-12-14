-- ricordarsi di mettere tutte le cose di alessio con time_period


drop schema if exists istat_transformation cascade;
create schema istat_transformation;

set search_path to istat_transformation;

-- creazione dimensioni in transformation schema

drop table if exists dim_territorio;

create table if not exists dim_territorio as select row_number() over() as ids_territorio, territorio from (select distinct territorio from istat_landing.lt_chiamate_vittime);


-- creo dim_sesso usando mio codice e fonte Aa 

drop table if exists istat_transformation.dim_sesso;

create table istat_transformation.dim_sesso
as
select 
ROW_Number () over (order by sesso) as ids_sesso,
sesso,
NOW() as load_timestamp,
'Landing' as source_system
from  
(select distinct sesso from istat_landing.lt_chiamate_vittime
);

/*
drop table if exists dim_sesso;

create table if not exists dim_sesso as select row_number() over() as ids_sesso, sesso from (select distinct sesso from istat_landing.lt_chiamate_vittime); 
*/

drop table if exists dim_motivi_chiamata;

create table if not exists dim_motivi_chiamata as select row_number() over() as ids_motivi_chiamata, motivi_della_chiamata from
(select distinct motivi_della_chiamata from istat_landing.lt_chiamate_vittime);

-- commento codice Aa
-- create table if not exists dim_anno as select row_number() over() as ids_anno, time_period from (select distinct time_period from istat_landing.lt_chiamate_vittime);

-- inserisco mio codice ndAo

-- per la dim_anno creo con una union

drop table if exists istat_transformation.dim_anno;

create table if not exists istat_transformation.dim_anno
as
select
ROW_Number () over (order by time_period) as ids_anno,
time_period,
now () as load_timestamp,
'Landing' as source_system
from (
select distinct time_period from istat_landing.lt_chiamate_vittime
union
select distinct time_period from istat_landing.lt_condanne_reati_violenti_sesso_reg lcrvsr
order by time_period asc);

-- fine inserimento mio codice per anno

drop table if exists istat_transformation.dim_tipo_reato;

create table istat_transformation.dim_tipo_reato
as
select 
ROW_Number () over (order by tipo_di_reato) as ids_reato,
tipo_di_reato,
NOW() as load_timestamp,
'Landing' as source_system
from  
(select distinct tipo_di_reato 	
from istat_landing.lt_condanne_reati_violenti_sesso_reg ltcrv
);

-- creo dim_area

DROP TABLE if exists istat_transformation.dim_area;

create table istat_transformation.dim_area
as
select 
ROW_Number () over (order by territorio) as ids_area,
territorio as nome_area,
NOW() as load_timestamp,
'Landing' as source_system
from  --istat_landing.lt_chiamate_vittime
(select distinct territorio 
from istat_landing.lt_chiamate_vittime
where territorio in 
('Nord-ovest',
'Nord-est',
'Centro',
'Sud',
'Isole'
)
);


-- INIZIO CODICE PROVINCE
-- creo mapping provincia


DROP TABLE IF EXISTS mapping_regione_provincia;

CREATE TABLE mapping_regione_provincia AS
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
    ('Aosta','Valle d''Aosta / Vallée d''Aoste'),
    ('Arezzo','Toscana'),
    ('Ascoli Piceno','Marche'),
    ('Asti','Piemonte'),
    ('Avellino','Campania'),
    ('Bari','Puglia'),
    ('Barletta-Andria-Trani','Puglia'),
    ('Belluno','Veneto'),
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
    ('Salerno','Campania'),
    ('Sassari','Sardegna'),
    ('Savona','Liguria'),
    ('Siena','Toscana'),
    ('Siracusa','Sicilia'),
    ('Sondrio','Lombardia'),
    ('Taranto','Puglia'),
    ('Teramo','Abruzzo'),
    ('Terni','Umbria'),
    ('Trento','Trentino Alto Adige / Südtirol'),
    ('Treviso','Veneto'),
    ('Trieste','Friuli-Venezia Giulia'),
    ('Udine','Friuli-Venezia Giulia'),
    ('Varese','Lombardia'),
    ('Venezia','Veneto'),
    ('Verbano-Cusio-Ossola','Piemonte'),
    ('Vercelli','Piemonte'),
    ('Verona','Veneto'),
    ('Vibo Valentia','Calabria')
  ) AS v(provincia, regione)
) s
ORDER BY provincia;



-- FINE CODICE PROVINCE


drop schema if exists istat_dwh cascade;
create schema istat_dwh;

set search_path to istat_dwh;

create table if not exists fact_vittime as
select row_number() over() as ids, ids_territorio, ids_sesso, ids_motivi_chiamata, ids_anno, osservazione as numero_chiamate
from istat_landing.lt_chiamate_vittime lv
join istat_transformation.dim_territorio dt on dt.territorio=lv.territorio
join istat_transformation.dim_sesso ds on ds.sesso=lv.sesso
join istat_transformation.dim_motivi_chiamata mc on mc.motivi_della_chiamata=lv.motivi_della_chiamata
join istat_transformation.dim_anno da on da.time_period=lv.time_period
order by ids asc;

set search_path to istat_transformation;

create table if not exists dim_eta as
select row_number() over() as ids_fascia_eta, età from 
(select distinct età from istat_landing.lt_denunce_delitti);

create table if not exists dim_indicatore as 
select row_number() over() as ids_indicatore, indicatore from
(select distinct indicatore from istat_landing.lt_denunce_delitti);

create table if not exists dim_tipo_delitto as 
select row_number() over() as ids_tipo_delitto, tipo_di_delitto from
(select distinct tipo_di_delitto from istat_landing.lt_denunce_delitti);

set search_path to istat_dwh;

create table if not exists fact_denunce_delitti as
select row_number() over() as ids, ids_territorio, ids_indicatore, ids_tipo_delitto, ids_sesso, ids_fascia_eta, ids_anno, osservazione as numero_denunce
from istat_landing.lt_denunce_delitti dd
join istat_transformation.mapping_città_regione mpc on mpc.territorio=dd.territorio
join istat_transformation.dim_indicatore di on di.indicatore=dd.indicatore
join istat_transformation.dim_tipo_delitto dtd on dtd.tipo_di_delitto=dd.tipo_di_delitto
join istat_transformation.dim_sesso ds on ds.sesso=dd.sesso
join istat_transformation.dim_fascia_eta dfe on dfe.età=dd.età
join istat_transformation.dim_anno da on da.time_period=dd.time_period
order by ids asc;

set search_path to istat_transformation;

create table if not exists dim_nazione (ids_nazione integer, nazione varchar(3000));

insert into dim_nazione (ids_nazione, nazione) values (1,'Italia'), (2,'Estero');

drop table if exists dim_regioni;
create table if not exists dim_regioni as
select row_number() over(order by territorio) as ids_regione, territorio as regione from
(select distinct territorio from istat_landing.lt_chiamate_vittime where
territorio in ('Marche', 'Sicilia', 'Valle d''Aosta / Vallée d''Aoste', 'Basilicata', 'Abruzzo', 'Piemonte', 'Toscana',
'Lazio', 'Sardegna', 'Liguria', 'Lombardia', 'Campania', 'Puglia', 'Friuli-Venezia Giulia', 'Molise', 'Umbria',
'Veneto', 'Trentino Alto Adige / Südtirol', 'Calabria', 'Emilia-Romagna'))
order by regione asc;

set search_path to dwh_progettoandromeda;

create table if not exists fact_chiamate as
select row_number() over() as ids, ids_regione, ids_sesso, ids_motivi_chiamata, ids_anno, osservazione as numero_chiamate, 
now() as load_timestamp, 'landing' as source_system
from istat_landing.lt_chiamate_vittime lv
join istat_transformation.dim_regioni dr on lv.territorio=dr.regione
join istat_transformation.dim_sesso ds on ds.sesso=lv.sesso
join istat_transformation.dim_motivi_chiamata mc on mc.motivi_della_chiamata=lv.motivi_della_chiamata
join istat_transformation.dim_anno da on da.time_period=lv.time_period
order by ids asc;