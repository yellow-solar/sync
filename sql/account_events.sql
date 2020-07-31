
--- ACCOUNT EVENTS ---
update core.account_events 
set replaced_unit_number = substring(event_info from '[0-9]{8}')
where event_type = 'REPLACED'
	and replaced_unit_number is null
	and zoho_id is null
;