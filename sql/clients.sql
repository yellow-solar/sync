
-- UPDATE THE CLIENT NEXT OF KIN TO EXCLUDE TEXT
update core.clients a
set next_of_kin_contact_number = left(nullif(coalesce(regexp_replace(next_of_kin_contact_number, '\D', '', 'g'),'0'),''),12)
;

-- Update the lat long
update core.clients a
set client_latitude = ST_X(client_location_lnglat::geometry)
 , client_longitude = ST_Y(client_location_lnglat::geometry) 
 where client_latitude is null or client_longitude is null
;

-- Responsible user
update core.clients a
set responsible_user_id = b.user_id
from core.users b
where a.responsible_user_external_id = b.user_angaza_id
	or a.responsible_user_external_id = b.user_upya_id
;
-----------------------  FOREIGN KEYS -------------------------- 

-- Country
update core.clients a
set country_id = b.country_id
from core.country b
where a.country = b.country_name
;