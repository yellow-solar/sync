
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

-- application status is approved if already approved
update core.applications 
set status = 'APPROVED'
where account_external_id is not null
and status = 'APPROVAL_PENDING'
;



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



update core.applications a
set country_id = b.country_id
from core.country b
where a.country = b.country_name
;
