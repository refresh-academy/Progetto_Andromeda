drop schema if exists stereotipi_transformation;
create schema if not exists stereotipi_transformation; 

drop table if exists stereotipi_transformation.dim_territorio;
create table if not exists stereotipi_transformation.dim_territorio as
select row_number() over() as ids_territorio, territorio from
(select distinct territorio 
from stereotipi_landing.accettabilita_stereotipi t );

drop table if exists stereotipi_transformation.dim_frequenza;
create table if not exists stereotipi_transformation.dim_frequenza as
select row_number() over() as ids_frequenza, frequenza from
(select distinct frequenza 
from stereotipi_landing.accettabilita_stereotipi t );

drop table if exists stereotipi_transformation.dim_sesso;
create table if not exists stereotipi_transformation.dim_sesso as
select row_number() over() as ids_sesso, sesso from
(select distinct sesso 
from stereotipi_landing.accettabilita_stereotipi t );

drop table if exists stereotipi_transformation.dim_time;
create table if not exists stereotipi_transformation.dim_time as
select row_number() over() as ids_time, time_period from
(select distinct time_period 
from stereotipi_landing.accettabilita_stereotipi t );
