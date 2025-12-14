-- mapping_regione_provincia.sql
-- Crea una tabella persistente con tutte le province (nomi ISTAT) e la relativa regione ISTAT.
-- Sono escluse le voci che corrispondono a regioni o a macro-aree.
-- L'id è generato in modo deterministico con row_number() ordinando per regione e provincia.
-- Eseguire in PostgreSQL.

BEGIN;

CREATE TABLE IF NOT EXISTS mapping_regione_provincia (
  id INTEGER PRIMARY KEY,
  provincia TEXT NOT NULL,
  regione TEXT NOT NULL
);

TRUNCATE TABLE mapping_regione_provincia;

WITH data(raw_name, mapped_prov, mapped_reg) AS (
  VALUES
  ('Verona','Verona','Veneto'),
  ('Latina','Latina','Lazio'),
  ('Palermo','Palermo','Sicilia'),
  ('Crotone','Crotone','Calabria'),
  ('Taranto','Taranto','Puglia'),
  ('Ravenna','Ravenna','Emilia-Romagna'),
  ('Imperia','Imperia','Liguria'),
  ('Isernia','Isernia','Molise'),
  ('Venezia','Venezia','Veneto'),
  ('Barletta-Andria-Trani','Barletta-Andria-Trani','Puglia'),
  ('Vicenza','Vicenza','Veneto'),
  ('Fermo','Fermo','Marche'),
  ('Mantova','Mantova','Lombardia'),
  ('Firenze','Firenze','Toscana'),
  ('Trapani','Trapani','Sicilia'),
  ('Rieti','Rieti','Lazio'),
  ('Ancona','Ancona','Marche'),
  ('Alessandria','Alessandria','Piemonte'),
  ('Messina','Messina','Sicilia'),
  ('Treviso','Treviso','Veneto'),
  ('Cosenza','Cosenza','Calabria'),
  ('Caltanissetta','Caltanissetta','Sicilia'),
  ('Rovigo','Rovigo','Veneto'),
  ('Avellino','Avellino','Campania'),
  ('Asti','Asti','Piemonte'),
  ('Campobasso','Campobasso','Molise'),
  ('Roma','Roma','Lazio'),
  ('Prato','Prato','Toscana'),
  ('Monza e della Brianza','Monza e della Brianza','Lombardia'),
  ('Trento','Trento','Trentino-Alto Adige'),
  ('Bergamo','Bergamo','Lombardia'),
  ('Pescara','Pescara','Abruzzo'),
  ('Caserta','Caserta','Campania'),
  ('Bolzano','Bolzano','Trentino-Alto Adige'),
  ('Bologna','Bologna','Emilia-Romagna'),
  ('Perugia','Perugia','Umbria'),
  ('Salerno','Salerno','Campania'),
  ('Pavia','Pavia','Lombardia'),
  ('Teramo','Teramo','Abruzzo'),
  ('Genova','Genova','Liguria'),
  ('Como','Como','Lombardia'),
  ('Ragusa','Ragusa','Sicilia'),
  ('Torino','Torino','Piemonte'),
  ('Cremona','Cremona','Lombardia'),
  ('Rimini','Rimini','Emilia-Romagna'),
  ('Pesaro e Urbino','Pesaro e Urbino','Marche'),
  ('Viterbo','Viterbo','Lazio'),
  ('Ascoli Piceno','Ascoli Piceno','Marche'),
  ('Sondrio','Sondrio','Lombardia'),
  ('Siena','Siena','Toscana'),
  ('Pistoia','Pistoia','Toscana'),
  ('Oristano','Oristano','Sardegna'),
  ('Agrigento','Agrigento','Sicilia'),
  ('Bari','Bari','Puglia'),
  ('Massa-Carrara','Massa-Carrara','Toscana'),
  ('Cuneo','Cuneo','Piemonte'),
  ('Benevento','Benevento','Campania'),
  ('Ferrara','Ferrara','Emilia-Romagna'),
  ('Milano','Milano','Lombardia'),
  ('Pordenone','Pordenone','Friuli-Venezia Giulia'),
  ('Piacenza','Piacenza','Emilia-Romagna'),
  ('Reggio di Calabria','Reggio di Calabria','Calabria'),
  ('Livorno','Livorno','Toscana'),
  ('Macerata','Macerata','Marche'),
  ('Terni','Terni','Umbria'),
  ('Savona','Savona','Liguria'),
  ('Frosinone','Frosinone','Lazio'),
  ('Siracusa','Siracusa','Sicilia'),
  ('Arezzo','Arezzo','Toscana'),
  ('Enna','Enna','Sicilia'),
  ('Padova','Padova','Veneto'),
  ('Udine','Udine','Friuli-Venezia Giulia'),
  ('Lodi','Lodi','Lombardia'),
  ('Catanzaro','Catanzaro','Calabria'),
  ('Catania','Catania','Sicilia'),
  ('Lecco','Lecco','Lombardia'),
  ('Lecce','Lecce','Puglia'),
  ('Belluno','Belluno','Veneto'),
  ('Napoli','Napoli','Campania'),
  ('Novara','Novara','Piemonte'),
  ('Pisa','Pisa','Toscana'),
  ('Brescia','Brescia','Lombardia'),
  ('Gorizia','Gorizia','Friuli-Venezia Giulia'),
  ('Nuoro','Nuoro','Sardegna'),
  ('Matera','Matera','Basilicata'),
  ('Trieste','Trieste','Friuli-Venezia Giulia'),
  ('La Spezia','La Spezia','Liguria'),
  ('L''Aquila','L''Aquila','Abruzzo'),
  ('Varese','Varese','Lombardia'),
  ('Biella','Biella','Piemonte'),
  ('Parma','Parma','Emilia-Romagna'),
  ('Modena','Modena','Emilia-Romagna'),
  ('Sassari','Sassari','Sardegna'),
  ('Vercelli','Vercelli','Piemonte'),
  ('Verbano-Cusio-Ossola','Verbano-Cusio-Ossola','Piemonte'),
  ('Vibo Valentia','Vibo Valentia','Calabria'),
  ('Cagliari','Cagliari','Sardegna'),
  ('Brindisi','Brindisi','Puglia'),
  ('Forlì-Cesena','Forlì-Cesena','Emilia-Romagna'),
  ('Foggia','Foggia','Puglia'),
  ('Lucca','Lucca','Toscana'),
  ('Grosseto','Grosseto','Toscana'),
  ('Potenza','Potenza','Basilicata'),
  ('Reggio nell''Emilia','Reggio nell''Emilia','Emilia-Romagna'),
  ('Chieti','Chieti','Abruzzo')
)
-- Inserisco una lista unica di province (DISTINCT) ordinata per regione e provincia,
-- generando un id deterministico con row_number().
INSERT INTO mapping_regione_provincia (id, provincia, regione)
SELECT
  row_number() OVER (ORDER BY mapped_reg, mapped_prov) AS ids_provincia,
  mapped_prov AS provincia,
  mapped_reg AS regione
FROM (
  SELECT DISTINCT mapped_prov, mapped_reg
  FROM data
  WHERE mapped_prov IS NOT NULL
) t
ORDER BY id;