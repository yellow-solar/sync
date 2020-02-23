
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

-- MAP THE PRODUCT
update core.accounts a
set product = prod.target
from core.accounts b
	join upya.columnmaps prod
		on prod.col = 'PRODUCT'
		and prod.val = b.product
where a.account_external_id = b.account_external_id
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

----------------------- CREATE COLUMNS  -------------------------- 

-- UPDATE THE HOUR PRICE
update core.accounts a
set hour_price = a.recurring_amount/a.days_per_recurring_amount/ 24
where a.days_per_recurring_amount is not null
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

-----------------------  FOREIGN KEYS -------------------------- 
-- updat client ID to get client details
update core.accounts a
set client_id = b.client_id
from core.clients b
where a.client_external_id = b.client_external_id
;

-- STOCK ID in accounts
update core.accounts a
set stock_id = b.stock_id
from core.stock b
where a.unit_number = b.unit_number
;
update core.accounts a
set stock_id = null
	, unit_number = null 
where a.account_status = 'DETACHED'
;

-- APPLICATION IDs IN ACCOUNTS
update core.accounts a
set application_id = b.application_id
	, application_external_id = b.application_external_id
from core.applications b
where a.account_external_id = b.account_external_id
	and b.account_external_id is not null
;

-- ACCOUNT ID in APPLICATIONS
update core.applications a
set account_id = b.account_id
from core.accounts b
where a.account_external_id = b.account_external_id
	and b.account_external_id is not null
;
-- UNIT NUMBER, ACCOUNT ID in Applications
update core.applications a
set unit_number = b.unit_number
	, account_external_id = b.account_external_id
from core.accounts b
where a.account_external_id = b.account_external_id
	and a.external_sys = 'upya'
	and a.status = 'DEPLOYED'
;


-- AGENT AND USERS ID IN ACCOUNTS, APPLICATIONS
-- registering, 

-- responsible user
update core.accounts a
set responsible_user_id = b.user_id
from core.users b
where a.responsible_user_external_id = b.user_angaza_id
	or a.responsible_user_external_id = b.user_upya_id
;

-- recorder id,  responsible user id in applications
update core.applications a
set responsible_user_id = b.user_id
from core.users b
where a.responsible_user_ext_id = b.user_angaza_id
	or a.responsible_user_ext_id = b.user_upya_id
;

update core.applications a
set recorder_id = b.user_id
from core.users b
where a.recorder_ext_id = b.user_angaza_id
	or a.recorder_ext_id = b.user_upya_id
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


