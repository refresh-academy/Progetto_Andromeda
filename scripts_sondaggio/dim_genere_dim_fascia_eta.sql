select * from progetto_andromeda pa 

select * from progetto_andromeda pa where pa.genere 

drop table if exists dim_genere as
create table dim_genere as select pa.genere from progetto_andromeda pa --non ha senso

select pa.fascia_eta from progetto_andromeda pa 


drop table if exists dim_genere;

create table dim_genere as 
select distinct pa.genere 
from progetto_andromeda pa;


drop table if exists dim_fascia_eta;

create table dim_fascia_eta as 
select distinct pa.fascia_eta
from progetto_andromeda pa;

select * from dim_fascia_eta dfe

select * from dim_genere dg 


