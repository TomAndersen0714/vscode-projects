select msg_time,
	if (
		real_buyer_nick is not null
		and real_buyer_nick != '',
		real_buyer_nick,
		cnick
	) as cnick,
	snick,
	msg,
	answer_id,
	act,
	send_msg_from,
	is_withdraw
from dwd.withdraw_context
where query_id = '64183f4afd1ac200015aa8a0'
order by msg_time


