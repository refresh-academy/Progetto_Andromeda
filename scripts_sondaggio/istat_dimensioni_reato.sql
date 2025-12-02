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