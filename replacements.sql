
---- REPLACEMENTS -----
---------------------------------------

-- Replacements table made from upya data
insert into core.replacements (replacement_external_id, external_sys, product,account_id,old_stock_id,old_unit_number,old_asset_number,new_stock_id,new_unit_number,new_asset_number,
							  replacement_date_utc,source_of_replacement,acc_responsible_user,acc_responsible_user_id,acc_responsible_user_ext_id)
select 'R'||a.previous_unit_number as replacement_external_id
	, a.external_sys
	, a.product
	, a.account_ids
	, old_s.stock_id as old_stock_id
	, old_s.unit_number as old_unit_number
	, old_s.asset_number as old_asset_number
	, a.stock_id as new_stock_id
	, a.unit_number as new_unit_number
	, a.asset_number as new_asset_number
	, a.date_of_replacement_utc as replacement_date_utc
	, 'WEB' source_of_replacement
	, a.responsible_user acc_responsible_user 
	, a.responsible_user_id acc_responsible_user_id
	, a.responsible_user_external_id acc_responsible_user_ext_id

from core.accounts a
-- join core.stock new_s
-- 	on new_s.unit_number = a.unit_number
join core.stock old_s
	on old_s.unit_number = a.previous_unit_number
where a.date_of_replacement_utc is not null
	and 'R'||a.previous_unit_number not in (select replacement_external_id from core.replacements)
	and a.previous_unit_number is not null
	and old_s.archived = false
limit 100
;

-- account id
update core.replacements r
set account_id = a.account_id
from core.accounts a
where r.new_asset_number = a.asset_number
	and a.unit_number is not null
	and a.account_id is null
;

-- stock ids
-- new 
update core.replacements r
set new_stock_id = s.stock_id
	, new_asset_number = s.asset_number
from core.stock s
where r.new_unit_number = s.unit_number
	and s.unit_number is not null
	and s.external_sys != 'upya'
;

-- old
update core.replacements r
set old_stock_id = s.stock_id
	, old_asset_number = s.asset_number
from core.stock s
where r.old_unit_number = s.unit_number
	and s.unit_number is not null
	and s.external_sys != 'upya'
;

-- responsible user - who did the replacement on the system
update core.replacements r
set responsible_user_id = u.user_id
from core.users u
where (r.responsible_user_ext_id = u.user_upya_id or r.responsible_user_ext_id = u.user_angaza_id)
	and u.user_id is not null
;

-- account responsible user - who was the account under at the time
update core.replacements r
set acc_responsible_user_id = u.user_id
from core.users u
where (r.acc_responsible_user_ext_id = u.user_upya_id or r.acc_responsible_user_ext_id = u.user_angaza_id)
	and u.user_id is not null
;
