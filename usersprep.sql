-- USER FIXES
-- Upya - Malawi
update core.users a
set user_email = b.user_email
	, username = b.username
from upya.users b
where b.user_upya_id = a.user_upya_id
 and b.user_email != a.user_email
;

--Upya Uganda
update core.users a
set user_email = b.user_email
	, username = b.username
from upya_uganda.users b
where b.user_upya_id = a.user_upya_id
 and b.user_email != a.user_email
;

-- Web USER FIXES
-- Upya - Malawi
update core.webusers a
set webuser_email = b.webuser_email
	, webuser_username = b.webuser_username
from upya.webusers b
where b.webuser_upya_id = a.webuser_upya_id
 and b.webuser_email != a.webuser_email
;

--Upya Uganda
update core.webusers a
set webuser_email = b.webuser_email
	, webuser_username = b.webuser_username
from upya_uganda.webusers b
where b.webuser_upya_id = a.webuser_upya_id
 and b.webuser_email != a.webuser_email
;
