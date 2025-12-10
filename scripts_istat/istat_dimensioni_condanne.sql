drop table if exists istat_transformation.dim_tipo_reato;

create table istat_transformation.dim_tipo_reato
as
select 
ROW_Number () over (order by tipo_di_reato) as ids_reato,
tipo_di_reato,
NOW() as load_timestamp,
'ETL' as source_system
from  
(select distinct tipo_di_reato 	
from lt_condanne lc
);

drop table if exists istat_transformation.dim_eta_condannato;

create table istat_transformation.dim_eta_condannato
as
select 
ROW_Number () over (order by eta_del_condannato_al_momento_del_reato) as ids_eta_c,
eta_del_condannato_al_momento_del_reato,
NOW() as load_timestamp,
'ETL' as source_system
from  
(select distinct eta_del_condannato_al_momento_del_reato 	
from istat_landing.lt_condanne lc
where eta_del_condannato_al_momento_del_reato != 'Totale'
);

drop table if exists istat_dwh.fact_condanne;

-- fact delle condanne

create table istat_dwh.fact_condanne
as
SELECT tipo_di_reato, eta_del_condannato_al_momento_del_reato, sesso, time_period, numero_condanne, territorio
FROM istat_landing.lt_condanne
partition by tipo_di_reato
group by time_period, tipo_di_reato;

SELECT
  tipo_di_reato,
  time_period,
  SUM(numero_condanne) AS totale_condanne
FROM istat_landing.lt_condanne
GROUP BY tipo_di_reato, time_period
order by totale_condanne DESC;
*/

create table 
as

