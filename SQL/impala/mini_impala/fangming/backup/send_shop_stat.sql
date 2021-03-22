--  add partition
alter table app_mp.reception_shop_send_stat add if not exists range partition ${var:ds_nodash} <= VALUES < ${var:tomorrow_ds_nodash};
--
upsert into app_mp.reception_shop_send_stat(ds_nodash,shop_id,total_recp_buyers_amount,trans_amount,recv_question_amount,idfy_question_amount,robot_reply_amount)
select ${var:ds_nodash}
,shop_id
,count(distinct cnick) as total_recp_buyers_amount
,count(distinct if(send_msg_from='4', cnick, null)) as trans_amount
,sum(if(act='recv_msg', 1,0)) as recv_question_amount
,sum(if(is_identified=1 and act = 'recv_msg',1,0)) as idfy_question_amount
,count(distinct if(send_msg_from='0' and act='send_msg',msg_id,null)) as robot_reply_amount
FROM dwd.mini_xdrs_log
where day=${var:ds_nodash} and mode = 'SEND'
group by 1,2;

upsert into app_mp.reception_shop_send_stat(ds_nodash,shop_id, send_recp_buyers_amount)
select ${var:ds_nodash}
,shop_id
,total_recp_buyers_amount - trans_amount
from app_mp.reception_shop_send_stat
where ds_nodash = ${var:ds_nodash};
