
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
-----------------------  FOREIGN KEYS -------------------------- 


update core.clients a
set country_id = b.country_id
from core.country b
where a.country = b.country_name
;