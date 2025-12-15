-- minimal ETL per lt_popolazione_italia_regioni_province

drop table if exists istat_transformation.stg_popolazione;
create table istat_transformation.stg_popolazione as
select
  frequenza,
  territorio,
  indicatore,
  sesso,
  eta,
  stato_civile,
  time_period,
  osservazione,
  now()::timestamp as staging_loaded_at,
  'lt_popolazione_italia_regioni_province' as staging_source
from istat_landing.lt_popolazione_italia_regioni_province;

drop table if exists istat_dwh.fact_popolazione;
create table istat_dwh.fact_popolazione as
select
  row_number() over (order by s.time_period, s.territorio, s.sesso) as ids,
  r.ids_regione,
  r.regione as territorio,
  ds.ids_sesso,
  'Popolazione al 1º gennaio' as indicatore,
  ds.sesso as sesso,
  s.time_period as anno,
  s.osservazione as osservazione,
  now()::timestamp as load_timestamp,
  'lt_popolazione_italia_regioni_province' as source_system
from istat_transformation.stg_popolazione s
join istat_transformation.dim_regioni r on r.regione = s.territorio
left join istat_transformation.dim_sesso ds on ds.sesso = s.sesso
left join istat_transformation.dim_anno da on da.time_period = s.time_period
order by ids;

create index if not exists idx_stg_popolazione_territorio on istat_transformation.stg_popolazione (territorio);
create index if not exists idx_dim_regioni_regione on istat_transformation.dim_regioni (regione);
create index if not exists idx_dim_indicatore_indicatore on istat_transformation.dim_indicatore (indicatore);

select 'landing_rows' as what, count(*) as cnt from istat_transformation.stg_popolazione;
select 'fact_rows' as what, count(*) as cnt from istat_dwh.fact_popolazione;

select
  count(*) filter (where ids_regione is null) as null_regione,
  count(*) filter (where ids_indicatore is null) as null_indicatore,
  count(*) filter (where ids_sesso is null) as null_sesso,
  count(*) filter (where ids_eta is null) as null_eta,
  count(*) filter (where ids_anno is null) as null_anno,
  count(*) as total_rows
from istat_dwh.fact_popolazione;