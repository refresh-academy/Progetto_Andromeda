
drop table if exists istat_transformation.stg_popolazione;
create table istat_transformation.stg_popolazione as
select
  trim(frequenza) as frequenza,
  trim(territorio) as territorio,
  trim(indicatore) as indicatore,
  trim(sesso) as sesso,
  trim(eta) as eta,
  trim(stato_civile) as stato_civile,
  case when time_period is null then null else time_period end as time_period,
  case
    when osservazione is null then null
    when trim(observazione) = '' then null
    else nullif(regexp_replace(trim(observazione)::text, '[^0-9\-]', '', 'g'), '')::int
  end as osservazione,
  now()::timestamp as staging_loaded_at,
  'lt_popolazione_italia_regioni_province' as staging_source
from istat_landing.lt_popolazione_italia_regioni_province;

drop table if exists istat_transformation.dim_regioni;
create table istat_transformation.dim_regioni as
select
  row_number() over (order by lower(trim(territorio))) as ids_regione,
  territorio as regione,
  lower(trim(territorio)) as regione_key,
  now()::timestamp as load_timestamp,
  'lt_popolazione' as source_system
from (
  select distinct territorio
  from istat_transformation.stg_popolazione
  where trim(coalesce(territorio,'')) <> ''
) t
order by regione_key;

drop table if exists istat_transformation.dim_sesso;
create table istat_transformation.dim_sesso as
select
  row_number() over (order by lower(trim(sesso))) as ids_sesso,
  sesso,
  lower(trim(sesso)) as sesso_key,
  now()::timestamp as load_timestamp,
  'lt_popolazione' as source_system
from (
  select distinct sesso
  from istat_transformation.stg_popolazione
  where trim(coalesce(sesso,'')) <> ''
) t
order by sesso_key;

drop table if exists istat_transformation.dim_eta;
create table istat_transformation.dim_eta as
select
  row_number() over (order by lower(trim(eta))) as ids_eta,
  eta,
  lower(trim(eta)) as eta_key,
  now()::timestamp as load_timestamp,
  'lt_popolazione' as source_system
from (
  select distinct eta
  from istat_transformation.stg_popolazione
  where trim(coalesce(eta,'')) <> ''
) t
order by eta_key;

drop table if exists istat_transformation.dim_indicatore;
create table istat_transformation.dim_indicatore as
select
  row_number() over (order by lower(trim(indicatore))) as ids_indicatore,
  indicatore,
  lower(trim(indicatore)) as indicatore_key,
  now()::timestamp as load_timestamp,
  'lt_popolazione' as source_system
from (
  select distinct indicatore
  from istat_transformation.stg_popolazione
  where trim(coalesce(indicatore,'')) <> ''
) t
order by indicatore_key;

drop table if exists istat_transformation.dim_anno;
create table istat_transformation.dim_anno as
select
  row_number() over (order by time_period) as ids_anno,
  time_period,
  now()::timestamp as load_timestamp,
  'lt_popolazione' as source_system
from (
  select distinct time_period
  from istat_transformation.stg_popolazione
  where time_period is not null
) t
order by time_period;

drop table if exists istat_dwh.fact_popolazione;
create table istat_dwh.fact_popolazione as
select
  row_number() over (order by s.time_period, s.territorio, s.indicatore, s.sesso, s.eta) as ids,
  r.ids_regione,
  r.regione as territorio,
  i.ids_indicatore,
  i.indicatore as indicatore,
  ds.ids_sesso,
  ds.sesso as sesso,
  de.ids_eta,
  de.eta as eta,
  da.ids_anno,
  s.time_period as anno,
  s.osservazione as numero,
  now()::timestamp as load_timestamp,
  'lt_popolazione_italia_regioni_province' as source_system
from istat_transformation.stg_popolazione s
left join istat_transformation.dim_regioni r
  on r.regione_key = lower(trim(s.territorio))
left join istat_transformation.dim_indicatore i
  on i.indicatore_key = lower(trim(s.indicatore))
left join istat_transformation.dim_sesso ds
  on ds.sesso_key = lower(trim(s.sesso))
left join istat_transformation.dim_eta de
  on de.eta_key = lower(trim(s.eta))
left join istat_transformation.dim_anno da
  on da.time_period = s.time_period
order by ids;

create index if not exists idx_stg_popolazione_territorio on istat_transformation.stg_popolazione (territorio);
create index if not exists idx_dim_regioni_regione_key on istat_transformation.dim_regioni (regione_key);
create index if not exists idx_dim_indicatore_key on istat_transformation.dim_indicatore (indicatore_key);

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

select *
from istat_transformation.stg_popolazione s
left join istat_transformation.dim_regioni r on r.regione_key = lower(trim(s.territorio))
left join istat_transformation.dim_indicatore i on i.indicatore_key = lower(trim(s.indicatore))
left join istat_transformation.dim_sesso ds on ds.sesso_key = lower(trim(s.sesso))
left join istat_transformation.dim_eta de on de.eta_key = lower(trim(s.eta))
left join istat_transformation.dim_anno da on da.time_period = s.time_period
where r.ids_regione is null or i.ids_indicatore is null or ds.ids_sesso is null or de.ids_eta is null or da.ids_anno is null
limit 200;

select distinct trim(territorio) as raw_territorio from istat_transformation.stg_popolazione order by raw_territorio;
select distinct trim(indicatore) as raw_indicatore from istat_transformation.stg_popolazione order by raw_indicatore;
select distinct trim(sesso) as raw_sesso from istat_transformation.stg_popolazione order by raw_sesso;
select distinct trim(eta) as raw_eta from istat_transformation.stg_popolazione order by raw_eta;
select distinct time_period from istat_transformation.stg_popolazione order by time_period;