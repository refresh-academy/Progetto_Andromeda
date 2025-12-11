select 
	count(*)  as vittime_eta, 
	(select count (*) from prova.vittime_genere_fascia_eta_regione vgfer2) as vittime_tipo,
	(select count (*) FROM prova.vittime_sesso_violenza_assistita_subita_figli) as vittime_figli_ass,
	(SELECT count (*)
FROM prova.vittime_prova) as vittime_prova
from vittime_genere_fascia_eta_regione vgfer;


