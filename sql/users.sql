
-- UPDATE THE EXTERNAL_SYS for the USERS
update core.users a 
set external_sys = 'angaza and upya'
where a.user_upya_id is not null and a.user_angaza_id is not null
;
