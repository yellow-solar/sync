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
