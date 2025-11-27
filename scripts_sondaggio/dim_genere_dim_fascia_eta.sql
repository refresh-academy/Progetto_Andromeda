select * from progetto_andromeda pa where pa.genere 

select pa.fascia_eta from progetto_andromeda pa 


drop table if exists dim_genere;

create table dim_genere as 
select distinct row_number() over () as ids_genere, pa.genere 
from progetto_andromeda pa;


drop table if exists dim_fascia_eta;

create table dim_fascia_eta as 
select distinct row_number() over () as ids_fascia_eta, pa.fascia_eta
from progetto_andromeda pa;

select * from dim_fascia_eta dfe

select * from dim_genere dg 