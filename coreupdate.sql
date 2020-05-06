------- MAPPING COLUMNS ---------

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

-- MAP THE APPLICATION STATUS
update core.applications a
set status = app_status.target
from core.applications b
	join upya.columnmaps app_status
		on app_status.col = 'APPLICATION_STATUS'
		and app_status.val = b.status
where a.application_external_id = b.application_external_id
;
-- account status in applications
update core.applications a
set account_status = null
where external_sys = 'upya_uganda'
	and account_external_id is null
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

-- UPDATE THE EXTERNAL_SYS for the USERS
update core.users a 
set external_sys = 'angaza and upya'
where a.user_upya_id is not null and a.user_angaza_id is not null
;

-- UPDATE THE CLIENT NEXT OF KIN TO EXCLUDE TEXT
update core.clients a
set next_of_kin_contact_number = left(nullif(coalesce(regexp_replace(next_of_kin_contact_number, '\D', '', 'g'),'0'),''),12)
;

-----------------------  FOREIGN KEYS -------------------------- 

-- product ID
-- pricing group ID
-- manufacturer ID


---- APPLICATIONS -----
-- Account external id
update core.applications a
set account_external_id = b.account_external_id
from core.accounts b
where a.application_external_id = b.account_external_id
		and b.account_external_id is not null
;

-- Account ID
update core.applications a
set account_id = b.account_id
from core.accounts b
where a.account_external_id = b.account_external_id
	and b.account_external_id is not null
;

-- Unit number
update core.applications a
set unit_number = b.unit_number
from core.accounts b
where a.account_external_id = b.account_external_id
	and a.external_sys = 'upya'
	and a.status = 'DEPLOYED'
;

--  Recorder user ID
update core.applications a
set recorder_id = b.user_id
	, recorder = b.full_name
from core.users b
where a.recorder_ext_id = b.user_angaza_id
	or a.recorder_ext_id = b.user_upya_id
;

--  Recorder user name
update core.applications a
set recorder = b.full_name
from core.users b
where a.recorder is null and
	(
		a.recorder_ext_id = b.user_angaza_id
		or a.recorder_ext_id = b.user_upya_id
	)
;

-- Note/credit decision user
update core.applications a
set note_recorder = b.webuser_fullname
	, note_recorder_id = b.webuser_id
from core.webusers b
where (a.note_recorder_external_id = b.webuser_angaza_id or a.note_recorder_external_id = b.webuser_upya_id)
	and b.webuser_id is not null
	and a.note_recorder_id is null
;

-- Responsible user ID
update core.applications a
set responsible_user_id = b.user_id
from core.users b
where (a.responsible_user_ext_id = b.user_angaza_id or a.responsible_user_ext_id = b.user_upya_id)
	and b.user_id is not null
;


---- CLIENTS -----
-- update client ID to get client details
update core.accounts a
set client_id = b.client_id
from core.clients b
where a.client_external_id = b.client_external_id
;

---- ACCOUNTS -----
-- Stock ID
-- input stock id if null
update core.accounts a
set stock_id = b.stock_id
from core.stock b
where a.asset_number = b.asset_number
	and a.external_sys = b.external_sys
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
where a.registering_user is null and
	(a.registering_user_external_id = b.user_angaza_id
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
where a.responsible_user is null and
	(a.responsible_user_external_id = b.user_angaza_id
	or a.responsible_user_external_id = b.user_upya_id)
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

---- PAYMENTS -----
-- ACCOUNT ID 
update core.payments a
set account_id = b.account_id
from core.accounts b
where a.account_external_id = b.account_external_id
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

-- COUNTRY
update core.accounts a
set country_id = b.country_id
from core.country b
where a.country = b.country_name
;
update core.payments a
set country_id = b.country_id
from core.country b
where a.country = b.country_name
;
update core.stock a
set country_id = b.country_id
from core.country b
where a.country = b.country_name
;
update core.applications a
set country_id = b.country_id
from core.country b
where a.country = b.country_name
;
update core.clients a
set country_id = b.country_id
from core.country b
where a.country = b.country_name
;

-- PRODUCT AND PRICING GROUP TABLES 
INSERT INTO core.products (product, product_external_name)
	select distinct product, product from upya.stock
	where product is not null
		and product not in (select product from core.products)
		;
		
INSERT INTO core.pricing_groups(
	product, pricing_group)
	select distinct product, pricing_group from angaza.accounts
	where pricing_group is not null
		and pricing_group not in (select pricing_group from core.pricing_groups)
;


