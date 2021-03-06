-- Accounts


-- MAP THE ACCOUNT STATUS
update core.accounts a
set account_status = status.target
from core.accounts b
	join upya.columnmaps status 
	on status.col = 'ACCOUNT_STATUS'
	and status.val = b.account_status
where a.account_external_id = b.account_external_id
;

-- MAP THE PRODUCT - stock and accounts
update core.accounts a
set product = prod.target
from core.accounts b
	join upya.columnmaps prod
		on prod.col = 'PRODUCT'
		and prod.val = b.product
where a.account_external_id = b.account_external_id
;



----------------------- RECALCULATE ACCOUNT COLUMNS  -------------------------- 

-- UPDATE THE HOUR PRICE
update core.accounts a
set hour_price = a.recurring_amount/a.days_per_recurring_amount/ 24
where a.days_per_recurring_amount is not null
	and a.days_per_recurring_amount!= 0
	and a.hour_price is null
;

-- UPDATE THE DAYS SINCE REGISTRATION
update core.accounts a
set days_since_registration =  extract(epoch from (now() at time zone 'utc') - registration_date_utc)/(3600*24)
;

-- UPDATE THE EXPECTED PAID
update core.accounts a
set expected_paid = least(upfront_price + 24*hour_price*greatest((days_since_registration - upfront_days_included),0), unlock_price)
where hour_price > 0
;

-- UPDATE THE AMOUNT IN ARREARS
update core.accounts a
set amount_in_arrears = greatest(0,expected_paid - total_paid)
;

-- UPDATE THE CUMULATIVE_DAYS_DISABLED
update core.accounts a
set cumulative_days_disabled = case 
			when linked_account_external_id is null then
				greatest(days_since_registration - cumulative_activation_days,0)
			else 0
		end
where external_sys = 'upya'
;

-- UPDATE DAYS_TO_CUTOFF
update core.accounts a
set days_to_cutoff = extract(epoch from date_of_disablement_utc - (now() at time zone 'utc'))/(3600*24)
;

-- UPDATE OUTSTANDING AMOUNT
update core.accounts a
set outstanding_amount = unlock_price - total_paid;
;

-- UPDATE THE NUMBER OF PAYMENTS
update core.accounts a
set number_of_payments = 0
where a.external_sys != 'angaza'
;
update core.accounts a
set number_of_payments = b.number_of_payments
from (select account_external_id, count(*) number_of_payments from core.payments group by 1 ) b
where a.account_external_id is not null
	and a.account_external_id = b.account_external_id
	and a.external_sys != 'angaza'
;


---- ACCOUNTS -----

-- update client ID to get client details
update core.accounts a
set client_id = b.client_id
from core.clients b
where a.client_external_id = b.client_external_id
	and a.client_id is null
;

-- Stock ID
-- input stock id if null
update core.accounts a
set stock_id = b.stock_id
from core.stock b
where a.asset_number = b.asset_number
	and a.external_sys = b.external_sys
	and a.stock_id is null
;

-- Unit Number
update core.accounts a
set unit_number = asset_number
from core.users b
where unit_number is null
	and asset_number is not null
	and a.account_status not in ('DETACHED', 'WRITTEN_OFF')
;

-- Remove unit number if detached, write-off
update core.accounts a
set stock_id = null
	, unit_number = null 
where a.account_status in ('DETACHED', 'WRITTEN_OFF')
;

-- Application IDs
update core.accounts a
set application_id = b.application_id
	, application_external_id = b.application_external_id
from core.applications b
where a.account_external_id = b.account_external_id
	and b.account_external_id is not null
	and a.application_id is null
;

-- Registering user ID
update core.accounts a
set registering_user_id = b.user_id
from core.users b
where a.registering_user_external_id = b.user_angaza_id
	or a.registering_user_external_id = b.user_upya_id
;

-- Registering user name
update core.accounts a
set registering_user = b.full_name
from core.users b
where (a.registering_user_external_id = b.user_angaza_id
	or a.registering_user_external_id = b.user_upya_id)
;

-- Responsible user
update core.accounts a
set responsible_user_id = b.user_id
from core.users b
where a.responsible_user_external_id = b.user_angaza_id
	or a.responsible_user_external_id = b.user_upya_id
;

-- Responsible user name
update core.accounts a
set responsible_user = b.full_name
from core.users b
where (a.responsible_user_external_id = b.user_angaza_id
	or a.responsible_user_external_id = b.user_upya_id)
;


-- COUNTRY
update core.accounts a
set country_id = b.country_id
from core.country b
where a.country = b.country_name
	and a.country_id is null
;


---- REPLACEMENTS -----
---------------------------------------

-- Replacements table made from upya data
/* insert into core.replacements (replacement_external_id, external_sys, product,account_id,old_stock_id,old_unit_number,old_asset_number,new_stock_id,new_unit_number,new_asset_number,
							  replacement_date_utc,source_of_replacement,acc_responsible_user,acc_responsible_user_id,acc_responsible_user_ext_id)
select 'R'||a.previous_unit_number as replacement_external_id
	, a.external_sys
	, a.product
	, a.account_id
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
; */

/* -- account id
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
; */
