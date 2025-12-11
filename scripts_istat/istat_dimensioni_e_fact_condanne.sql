set search_path to istat_transformation;

SELECT * FROM istat_landing.lt_condanne_reati_violenti_sesso_reg;

select distinct tipo_di_reato from istat_landing.lt_condanne_reati_violenti_sesso_reg;

drop table if exists dim_tipo_reato;

create table istat_transformation.dim_tipo_reato
as
select 
ROW_Number () over (order by tipo_di_reato) as ids_reato,
tipo_di_reato,
NOW() as load_timestamp,
'ETL' as source_system
from  
(select distinct tipo_di_reato 	
from istat_landing.lt_condanne_reati_violenti_sesso_reg ltcrv
);


-- per la dim_anno usare istat_transformation.dim_anno
-- per la dim_regione usare istat_transformation.dim_regione
-- per dim_sesso usare istat_transformation.dim_sesso

drop table if exists istat_transformation.dim_sesso;

create table istat_transformation.dim_sesso
as
select 
ROW_Number () over (order by sesso) as ids_sesso,
sesso,
NOW() as load_timestamp,
'ETL' as source_system
from  
(select distinct sesso
from istat_landing.lt_condanne_reati_violenti_sesso_reg ltcrv
where sesso != 'Totale'
);

-- procedo con la creazione del fatto


drop table if exists istat_dwh.fact_condanne_reati_violenti_sesso_eta_reg;

create table if not exists istat_dwh.fact_condanne_reati_violenti_sesso_eta_reg as
select row_number() over() as 
	ids, 
	-- ids_regione,  metto in pausa perche' nn abbiamo definito la logica delle regioni
	ids_reato, 
	ids_sesso,  
	ids_anno, 
	osservazione as numero_condanne
from istat_landing.lt_condanne_reati_violenti_sesso_reg crv
-- join istat_transformation.mapping_città_regione mpc on mpc.territorio=dd.territorio
join istat_transformation.dim_tipo_reato dtr on dtr.tipo_di_reato=crv.tipo_di_reato
join istat_transformation.dim_sesso ds on ds.sesso=crv.sesso
join istat_transformation.dim_anno da on da.time_period=crv.time_period
order by ids asc;

select count (*) from istat_dwh.fact_condanne_reati fcr;



