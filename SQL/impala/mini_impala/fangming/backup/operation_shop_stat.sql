upsert into app_mp.operation_shop_stat(ds_nodash, shop_name, total_recv_amount, total_identified_amount, total_resp_amount)
select ${var:ds_nodash}, shop_name, sum(if(act='recv_msg', 1, 0)), sum(is_identified), count(distinct if(act='send_msg' and send_msg_from='0', msg_id, null)) from dwd.mini_xdrs_log where day = ${var:ds_nodash} group by 1,2;

with dim_shop as (
        select shop_id, shop_name from dwd.mini_xdrs_log where day=${var:ds_nodash} group by 1,2
)
upsert into app_mp.operation_shop_stat(ds_nodash, shop_name, send_recv_msg_amount, send_resp_amount, trans_amount, total_recp_buyers_amount, send_recp_buyers_amount)
select ds_nodash, shop_name, recv_question_amount, robot_reply_amount, trans_amount, total_recp_buyers_amount, send_recp_buyers_amount 
from app_mp.reception_shop_send_stat as r join dim_shop as d using(shop_id);

upsert into app_mp.operation_shop_stat(ds_nodash, shop_name, hybd_subnick_amount, hybd_recv_msg_amount, hybd_resp_amount)
select ds_nodash, split_part(subnick, ':', 1), cast(count(distinct subnick) as int), sum(recv_question_amount), sum(auto_reply_amount)
from app_mp.reception_subnick_hybd_stat where ds_nodash = ${var:ds_nodash} group by 1,2;

upsert into app_mp.operation_shop_stat(ds_nodash, shop_name, identified_rate, resp_rate, send_resp_rate, hybd_resp_rate)
select ds_nodash, shop_name
        ,cast(if(total_recv_amount=0, 0, total_identified_amount/total_recv_amount) as float)
        ,cast(if(total_recv_amount=0, 0, total_resp_amount/total_recv_amount) as float)
        ,cast(if(send_recv_msg_amount=0, 0, send_resp_amount / send_recv_msg_amount) as float)
        ,cast(if(hybd_recv_msg_amount=0, 0, hybd_resp_amount / hybd_recv_msg_amount) as float)
from app_mp.operation_shop_stat where ds_nodash = ${var:ds_nodash};