
-- MAP THE PRODUCT
update core.stock a
set product = prod.target
from core.stock b
	join upya.columnmaps prod
		on prod.col = 'PRODUCT'
		and prod.val = b.product
where a.asset_number = b.asset_number
	and a.external_sys = b.external_sys
;

-- MAP THE STOCK STATUSES 
-- transit_state
update core.stock a
set transit_state = transit.target
from core.stock b
	join upya.columnmaps transit
		on transit.col = 'TRANSIT_STATE'
		and transit.val = b.transit_state
where a.asset_number = b.asset_number
	and a.external_sys = b.external_sys
;


---- STOCK -----
-- Account ID
-- input if there is a deployed account
update core.stock a
set account_id = b.account_id
from core.accounts b
where a.asset_number = b.asset_number
	and b.asset_number is not null
	and a.external_sys = b.external_sys
;
-- remove if there isn't one
update core.stock a
set account_id = null
from core.accounts b
where a.account_id = b.account_id
	and b.asset_number is null
;

-- HOLDER USER ID
update core.stock a
set holder_id = b.user_id
from core.users b
where a.holder_external_id = user_angaza_id 
	or a.holder_external_id = user_upya_id
;


update core.stock a
set country_id = b.country_id
from core.country b
where a.country = b.country_name
;
