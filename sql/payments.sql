

---- PAYMENTS -----
-- ACCOUNT ID 
update core.payments a
set account_id = b.account_id
from core.accounts b
where a.account_external_id = b.account_external_id
and a.account_id is null
;

-- unit number
update core.payments a
set unit_number = b.unit_number
from core.accounts b
where a.account_external_id = b.account_external_id
	and (a.unit_number is null and b.unit_number is not null)
;

-- pricing group
update core.payments a
set pricing_group = b.pricing_group
from core.accounts b
where a.account_external_id = b.account_external_id
	and a.pricing_group is not null
	and b.account_external_id is not null
;

-- responsible  - 2 parts update the responsible user id first, then add the usernames etc where empty
update core.payments a
set responsible_user_id = b.responsible_user_id
	, responsible_user_ext_id = b.responsible_user_external_id
from core.accounts b
where a.account_external_id = b.account_external_id
	and a.responsible_user_id is null
	and a.effective_utc >= cast('2020-02-22' as timestamp) 
;

update core.payments a
set responsible_user = b.username
	, responsible_user_ext_id = case when a.external_sys = 'angaza' then b.user_angaza_id when a.external_sys = 'upya' then b.user_upya_id end
from core.users b
where a.responsible_user_id = b.user_id
	and a.responsible_user is null
;

update core.payments a
set country_id = b.country_id
from core.country b
where a.country = b.country_name
and a.country_id is null
;