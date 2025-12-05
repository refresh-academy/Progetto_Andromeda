drop schema if exists stereotipi_dwh CASCADE;
create schema if not exists stereotipi_dwh; 

drop table if exists stereotipi_dwh.dim_territorio CASCADE;
create table if not exists stereotipi_dwh.dim_territorio as
select row_number() over() as ids_territorio, territorio from
(select distinct territorio 
from stereotipi_landing.accettabilita_stereotipi t );

drop table if exists stereotipi_dwh.dim_frequenza CASCADE;
create table if not exists stereotipi_dwh.dim_frequenza as
select row_number() over() as ids_frequenza, frequenza from
(select distinct frequenza 
from stereotipi_landing.accettabilita_stereotipi t );

drop table if exists stereotipi_dwh.dim_sesso CASCADE;
create table if not exists stereotipi_dwh.dim_sesso as
select row_number() over() as ids_sesso, sesso from
(select distinct sesso 
from stereotipi_landing.accettabilita_stereotipi t );

drop table if exists stereotipi_dwh.dim_time CASCADE;
create table if not exists stereotipi_dwh.dim_time as
select row_number() over() as ids_time, time_period from
(select distinct time_period 
from stereotipi_landing.accettabilita_stereotipi t );

drop table if exists fact_accettabilita_stereotipi CASCADE;
create table if not exists fact_accettabilita_stereotipi as
select row_number() over() as ids, ids_frequenza, ids_territorio, indicatore, ids_sesso, 
"stereotipi_sui_ruoli_di_genere,_comportamenti_nella_coppia",
ids_time, osservazione, "stato_dell'osservazione" 
from stereotipi_landing.accettabilita_stereotipi ast
join stereotipi_transformation.dim_frequenza df on df.frequenza=ast.frequenza
join stereotipi_transformation.dim_territorio dt on dt.territorio=ast.territorio 
join stereotipi_transformation.dim_sesso ds on ast.sesso=ds.sesso 
join stereotipi_transformation.dim_time dt2 on ast.time_period=dt2.time_period 
order by ids;

drop table if exists fact_accettabilita_violenza_nella_coppia CASCADE;
create table if not exists fact_accettabilita_violenza_nella_coppia as
select row_number() over() as ids, ids_frequenza, ids_territorio, indicatore, ids_sesso, 
"stereotipi_sui_ruoli_di_genere,_comportamenti_nella_coppia",grado_di_accettabilità,
ids_time, osservazione, "stato_dell_osservazione" 
from stereotipi_landing.accettabilita_violenza_nella_coppia ast
join stereotipi_transformation.dim_frequenza df on df.frequenza=ast.frequenza
join stereotipi_transformation.dim_territorio dt on dt.territorio=ast.territorio 
join stereotipi_transformation.dim_sesso ds on ast.sesso=ds.sesso 
join stereotipi_transformation.dim_time dt2 on ast.time_period=dt2.time_period
order by ids;

drop table if exists fact_cosa_si_consiglierebbe_a_una_donna CASCADE;
create table if not exists fact_cosa_si_consiglierebbe_a_una_donna as
select row_number() over() as ids, ids_frequenza, ids_territorio, indicatore, ids_sesso, 
consigli,
ids_time, osservazione, ast."stato_dell'osservazione" 
from stereotipi_landing.cosa_si_consiglierebbe_a_una_donna ast
join stereotipi_transformation.dim_frequenza df on df.frequenza=ast.frequenza
join stereotipi_transformation.dim_territorio dt on dt.territorio=ast.territorio 
join stereotipi_transformation.dim_sesso ds on ast.sesso=ds.sesso 
join stereotipi_transformation.dim_time dt2 on ast.time_period=dt2.time_period
order by ids;

drop table if exists fact_indicazione_di_alcune_cause_della_violenza_nella_coppia_reg CASCADE;
create table if not exists fact_indicazione_di_alcune_cause_della_violenza_nella_coppia_reg as
select row_number() over() as ids, ids_frequenza, ids_territorio, ast."Indicatore", ids_sesso, 
"Possibili cause",
ids_time, ast."Osservazione", ast."Stato dell'osservazione" 
from stereotipi_landing.indicazione_di_alcune_cause_della_violenza_nella_coppia_reg ast
join dim_frequenza df on df.frequenza=ast."Frequenza"
join dim_territorio dt on dt.territorio=ast."Territorio"
join dim_sesso ds on ds.sesso=ast."Sesso"
join dim_time dt2 on dt2.time_period=ast."TIME_PERIOD"
order by ids;

drop table if exists fact_opinioni_ruoli_tradizionali CASCADE;
create table if not exists fact_opinioni_ruoli_tradizionali as
select row_number() over() as ids, ids_frequenza, ids_territorio, indicatore, ids_sesso, 
"stereotipi_sui_ruoli_di_genere_comportamenti_nella_coppia", grado_di_accordo
ids_time, osservazione, "stato_dell_osservazione" 
from stereotipi_landing.opinioni_ruoli_tradizionali ast
join stereotipi_transformation.dim_frequenza df on df.frequenza=ast.frequenza
join stereotipi_transformation.dim_territorio dt on dt.territorio=ast.territorio 
join stereotipi_transformation.dim_sesso ds on ast.sesso=ds.sesso 
join stereotipi_transformation.dim_time dt2 on ast.time_period=dt2.time_period
order by ids;

drop table if exists fact_opinioni_violenza_sessuali CASCADE;
create table if not exists fact_opinioni_violenza_sessuali as
select row_number() over() as ids, ids_frequenza, ids_territorio, indicatore, ids_sesso, 
"stereotipi_sui_ruoli_di_genere_comportamenti_nella_coppia", grado_di_accordo
ids_time, osservazione, "stato_dell_osservazione" 
from stereotipi_landing.opinioni_violenza_sessuali ast
join stereotipi_transformation.dim_frequenza df on df.frequenza=ast.frequenza
join stereotipi_transformation.dim_territorio dt on dt.territorio=ast.territorio 
join stereotipi_transformation.dim_sesso ds on ast.sesso=ds.sesso 
join stereotipi_transformation.dim_time dt2 on ast.time_period=dt2.time_period
order by ids;

drop table if exists fact_percezione_diffusione_violenza_nella_coppia CASCADE;
create table if not exists fact_percezione_diffusione_violenza_nella_coppia as
select row_number() over() as ids, ids_frequenza, ids_territorio, indicatore, ids_sesso, 
"livello_di_diffusione",
ids_time, osservazione, "stato_dell_osservazione" 
from stereotipi_landing.percezione_diffusione_violenza_nella_coppia ast
join stereotipi_transformation.dim_frequenza df on df.frequenza=ast.frequenza
join stereotipi_transformation.dim_territorio dt on dt.territorio=ast.territorio 
join stereotipi_transformation.dim_sesso ds on ast.sesso=ds.sesso 
join stereotipi_transformation.dim_time dt2 on ast.time_period=dt2.time_period
order by ids;

--creazione chiavi frequenza
ALTER TABLE dim_frequenza
ADD CONSTRAINT dim_frequenza2 UNIQUE (ids_frequenza);

alter table fact_accettabilita_stereotipi
add constraint fact_accettabilita_stereotipi FOREIGN KEY (ids_frequenza) references dim_frequenza (ids_frequenza);

alter table fact_accettabilita_violenza_nella_coppia
add constraint fact_accettabilita_violenza_nella_coppia FOREIGN KEY (ids_frequenza) references dim_frequenza (ids_frequenza);

alter table fact_cosa_si_consiglierebbe_a_una_donna
add constraint fact_cosa_si_consiglierebbe_a_una_donna FOREIGN KEY (ids_frequenza) references dim_frequenza (ids_frequenza);

alter table fact_opinioni_ruoli_tradizionali
add constraint fact_opinioni_ruoli_tradizionali FOREIGN KEY (ids_frequenza) references dim_frequenza (ids_frequenza);

alter table fact_opinioni_violenza_sessuali
add constraint fact_opinioni_violenza_sessuali FOREIGN KEY (ids_frequenza) references dim_frequenza (ids_frequenza);

alter table fact_percezione_diffusione_violenza_nella_coppia
add constraint fact_percezione_diffusione_violenza_nella_coppia FOREIGN KEY (ids_frequenza) references dim_frequenza (ids_frequenza);

alter table fact_percezione_diffusione_violenza_nella_coppia
add constraint fact_percezione_diffusione_violenza_nella_coppia FOREIGN KEY (ids_frequenza) references dim_frequenza (ids_frequenza);

alter table fact_indicazione_di_alcune_cause_della_violenza_nella_coppia_re
add constraint fact_indicazione_di_alcune_cause_della_violenza_nella_coppia_re2 FOREIGN KEY (ids_frequenza) references dim_frequenza (ids_frequenza);


--creazione chiavi sesso
ALTER TABLE dim_sesso
ADD CONSTRAINT dim_sesso2 UNIQUE (ids_sesso);

alter table fact_accettabilita_stereotipi
add constraint fact_sesso FOREIGN KEY (ids_sesso) references dim_sesso (ids_sesso);

alter table fact_accettabilita_violenza_nella_coppia
add constraint fact_accettabilita_violenza_nella_coppia2 FOREIGN KEY (ids_sesso) references dim_sesso (ids_sesso);

alter table fact_cosa_si_consiglierebbe_a_una_donna
add constraint fact_cosa_si_consiglierebbe_a_una_donna2 FOREIGN KEY (ids_sesso) references dim_sesso (ids_sesso);

alter table fact_opinioni_ruoli_tradizionali
add constraint fact_opinioni_ruoli_tradizionali2 FOREIGN KEY (ids_sesso) references dim_sesso (ids_sesso);

alter table fact_opinioni_violenza_sessuali
add constraint fact_opinioni_violenza_sessuali2 FOREIGN KEY (ids_sesso) references dim_sesso (ids_sesso);

alter table fact_percezione_diffusione_violenza_nella_coppia
add constraint fact_percezione_diffusione_violenza_nella_coppia2 FOREIGN KEY (ids_sesso) references dim_sesso (ids_sesso);

alter table fact_indicazione_di_alcune_cause_della_violenza_nella_coppia_re
add constraint fact_indicazione_di_alcune_cause_della_violenza_nella_coppia_re7 FOREIGN KEY (ids_sesso) references dim_sesso (ids_sesso);

--creazione chiavi dim territorio
ALTER TABLE dim_territorio
ADD CONSTRAINT dim_territorio2 UNIQUE (ids_territorio);

alter table fact_accettabilita_stereotipi
add constraint fact_2 FOREIGN KEY (ids_territorio) references dim_territorio (ids_territorio);

alter table fact_accettabilita_violenza_nella_coppia
add constraint fact_accettabilita_violenza_nella_coppia3 FOREIGN KEY (ids_territorio) references dim_territorio (ids_territorio);

alter table fact_cosa_si_consiglierebbe_a_una_donna
add constraint fact_cosa_si_consiglierebbe_a_una_donna3 FOREIGN KEY (ids_territorio) references dim_territorio (ids_territorio);

alter table fact_opinioni_ruoli_tradizionali
add constraint fact_opinioni_ruoli_tradizionali3 FOREIGN KEY (ids_territorio) references dim_territorio (ids_territorio);

alter table fact_opinioni_violenza_sessuali
add constraint fact_opinioni_violenza_sessuali3 FOREIGN KEY (ids_territorio) references dim_territorio (ids_territorio);

alter table fact_percezione_diffusione_violenza_nella_coppia
add constraint fact_percezione_diffusione_violenza_nella_coppia3 FOREIGN KEY (ids_territorio) references dim_territorio (ids_territorio);

alter table fact_indicazione_di_alcune_cause_della_violenza_nella_coppia_re
add constraint fact_indicazione_di_alcune_cause_della_violenza_nella_coppia_re11 FOREIGN KEY (ids_territorio) references dim_territorio (ids_territorio);

--creazione chiavi dim_time
ALTER TABLE dim_time
ADD CONSTRAINT dim_time4 UNIQUE (ids_time);

alter table fact_accettabilita_stereotipi
add constraint fact_5 FOREIGN KEY (ids_time) references dim_time (ids_time);

alter table fact_accettabilita_violenza_nella_coppia
add constraint fact_accettabilita_violenza_nella_coppia5 FOREIGN KEY (ids_time) references dim_time (ids_time);

alter table fact_cosa_si_consiglierebbe_a_una_donna
add constraint fact_cosa_si_consiglierebbe_a_una_donna5 FOREIGN KEY (ids_time) references dim_time (ids_time);

alter table fact_opinioni_ruoli_tradizionali
add constraint fact_opinioni_ruoli_tradizionali5 FOREIGN KEY (ids_time) references dim_time (ids_time);

alter table fact_opinioni_violenza_sessuali
add constraint fact_opinioni_violenza_sessuali5 FOREIGN KEY (ids_time) references dim_time (ids_time);

alter table fact_percezione_diffusione_violenza_nella_coppia
add constraint fact_percezione_diffusione_violenza_nella_coppia5 FOREIGN KEY (ids_time) references dim_time (ids_time);

alter table fact_indicazione_di_alcune_cause_della_violenza_nella_coppia_re
add constraint fact_indicazione_di_alcune_cause_della_violenza_nella_coppia_re11 FOREIGN KEY (ids_time) references dim_time (ids_time);

