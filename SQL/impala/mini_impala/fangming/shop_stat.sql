upsert into app_mp.shop_stat(ds_nodash, shop_id, send_recv_question_amount, 
send_identified_question_amount, send_auto_reply_amount, send_recp_buyers_amount, trans_amount)
SELECT ds_nodash
        ,shop_id
        ,cast(recv_question_amount as int)
        ,cast(idfy_question_amount as int)
        ,cast(robot_reply_amount as int)
        ,cast(send_recp_buyers_amount as int)
        ,cast(trans_amount as int)
FROM app_mp.reception_shop_send_stat where ds_nodash = {{ ds_nodash }};

upsert into app_mp.shop_stat (ds_nodash, shop_id, 
hybd_recv_question_amount, hybd_identified_question_amount, 
hybd_auto_reply_amount, hybd_recp_buyers_amount, 
subnick_amount, human_avg_resp_interval, avg_resp_interval)
select 
        ds_nodash
        ,shop_id
        ,cast(sum(recv_question_amount) as int)
        ,cast(sum(identified_question_amount) as int)
        ,cast(sum(auto_reply_amount) as int)
        ,cast(sum(recp_buyers_amount) as int)
        ,cast(count(distinct subnick) as int)
        ,cast(if(sum(human_resp_pair_amount) = 0, 0, sum(human_resp_interval_amount) / sum(human_resp_pair_amount)) as float)
        ,cast(if(sum(hybd_resp_pair_amount) = 0, 0, sum(hybd_resp_interval_amount) / sum(hybd_resp_pair_amount)) as float)
from app_mp.reception_subnick_hybd_stat where ds_nodash = {{ ds_nodash }} group by 1,2;

upsert into app_mp.shop_stat (ds_nodash, shop_id, shop_name, robot_send_amount, human_send_amount)
select day
,shop_id
,max(shop_name)
,cast(sum(if(act='send_msg' and send_msg_from in ('0', '3'), 1, 0)) as int)
,cast(sum(if(act='send_msg' and send_msg_from = '2', 1, 0)) as int)
from dwd.mini_xdrs_log where day = {{ ds_nodash }} group by 1,2;