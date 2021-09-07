-- 1 app_mp.sub_nick_receive
upsert into  app_mp.sub_nick_receive (
shop_id, sub_nick, day, platform,mode, received_session, buyer_session, seller_session, robot_session,update_time)
SELECT shop_id,
       snick,
       '{{ ds }}' as day,
       'tb' as platform,
       mode,
       count(cnick) as received_session,
       sum(if(session_start_role ='买家',1,0)) AS buyer_session,
       sum(if(session_start_role ='客服',1,0)) AS seller_session,
       0 as robot_session,
       from_unixtime(unix_timestamp(now()), 'yyyy-MM-dd HH:mm:ss') AS update_time
FROM ods.chat_session
WHERE DAY = {{ ds_nodash }}
group by 1,2,3,4,5;

-- 2
upsert into app_mp.sub_nick_receive (
    shop_id,
    sub_nick,
    day,
    platform,
    mode,
    received_cuv,
    received_pv,
    identified_pv,
        reply_pair,
        manual_reply_pair,
        manual_reply_rt,
        reply_rt,
    auto_reply_pv,
    robot_reply_rate
    )
select
shop_id,
subnick as sub_nick,
'{{ ds }}' as day,
'tb' as platform,
'HYBRID' as mode,
recp_buyers_amount as received_cuv, -- 服务买家人数
recv_question_amount as received_pv, --接收问题数
identified_question_amount as identified_pv,
hybd_resp_pair_amount as reply_pair,
human_resp_pair_amount as manual_reply_pair,
cast(human_avg_resp_interval * human_resp_pair_amount * 1000  as bigint)  as  manual_reply_rt,
cast(avg_resp_interval * hybd_resp_pair_amount * 1000   as bigint) as  reply_rt,
auto_reply_amount as auto_reply_pv,
auto_reply_rate as  robot_reply_rate
 from app_mp.reception_subnick_hybd_stat where ds_nodash = {{ ds_nodash  }} ;



--3
upsert into app_mp.sub_nick_receive (
    shop_id,
    sub_nick,
    day,
    platform,
    mode,
    manual_reply_mv,
    click_reply_pv,
    question_answer_volume,
    update_time)
with t1 as (
select
     shop_id,
    snick,
    split_part(snick,':',1) as nick,
    platform,
    cnick,
    act,
    send_msg_from,
    is_robot_answer,
    'HYBRID' as mode,
    shop_question_id
from dwd.mini_xdrs_log
where day = {{ ds_nodash }}
and strleft(cnick, 10) != 'comxiaoduo'
and regexp_replace(split_part(snick, ':', 1), 'cntaobao', '') != regexp_replace(regexp_replace(split_part(cnick, ':', 1), 'cntaobao', ''), 'cnalichn', '')
and snick not like '%服务助手%'),
t2 as (select shop_id,plat_shop_name from  dim.shop_nick),
t3 as (
select
    t1.shop_id,
    split_part(snick,'cntaobao',2) as sub_nick,
    '{{ ds }}' as day,
    t1.platform,
    mode,
    sum(if(act = 'send_msg' and send_msg_from = '3',1,0)) as click_reply_pv,
        sum(if(act = 'send_msg' and send_msg_from = '2',1,0)) as manual_reply_mv,
    from_unixtime(unix_timestamp(now()), 'yyyy-MM-dd HH:mm:ss') AS update_time,
    sum(if(act = 'send_msg',1,0)) as total_send,
    sum(if(subString(act,1,5) = 'send_' or subString(act,1,5) = 'copy_',1,0)) as not_manul
from t1
join [shuffle] t2
on t1.shop_id = t2.shop_id
group by 1,2,3,4,5)
select shop_id,sub_nick,day,
    platform,mode,
    manual_reply_mv,
    click_reply_pv,
    total_send as question_answer_volume,
    update_time
from t3   where shop_id not in (select shop_id from dim.shop_upgrade);

-- 4
upsert into  app_mp.sub_nick_receive (
    shop_id,
    sub_nick,
    day,
    platform,
    mode,
    received_cuv,
    manual_reply_mv,
    click_reply_pv,
    received_pv,
    identified_pv,
    auto_reply_pv,
    robot_reply_rate,
    question_answer_volume,
    update_time)
with t1 as (
select
     shop_id,
    snick,
    split_part(snick,':',1) as nick,
    platform,
    cnick,
    act,
    send_msg_from,
    is_robot_answer,
    'SEND' as mode,
    is_identified,
    msg_id,
    shop_question_id
from dwd.mini_xdrs_log
where day = {{ ds_nodash }}
and strleft(cnick, 10) != 'comxiaoduo'
and regexp_replace(split_part(snick, ':', 1), 'cntaobao', '') != regexp_replace(regexp_replace(split_part(cnick, ':', 1), 'cntaobao', ''), 'cnalichn', '')
and snick like '%服务助手%'),
t2 as (select shop_id,plat_shop_name from  dim.shop_nick),
t3 as (
select
    t1.shop_id,
    split_part(snick,'cntaobao',2) as sub_nick,
    '{{ ds }}' as day,
    t1.platform,
    mode,
    count(distinct if(msg_id='',NULL,msg_id)) as auto_reply_pv,
    count(distinct cnick) as received_cuv,
    sum(if(act =  'recv_msg',1,0)) as received_pv,
    sum(if( (is_identified = 1 or shop_question_id != '') and act in ('recv_msg'),1,0)) as identified_pv,
    sum(if(act = 'send_msg' and send_msg_from = '3',1,0)) as click_reply_pv,
        0 as manual_reply_mv,
    from_unixtime(unix_timestamp(now()), 'yyyy-MM-dd HH:mm:ss') AS update_time,
    sum(if(act = 'send_msg',1,0)) as total_send
from t1
join [shuffle] t2
on t1.shop_id = t2.shop_id
group by 1,2,3,4,5),
t4 as (select shop_id,robot_reply_amount  from app_mp.reception_shop_send_stat where  ds_nodash ={{ds_nodash}})
select t3.shop_id, 
sub_nick,
    day,
    platform,
    mode,
    received_cuv,
    manual_reply_mv,
    click_reply_pv,
    received_pv,
    identified_pv,
    robot_reply_amount as auto_reply_pv,
    robot_reply_amount/received_pv as robot_reply_rate,
    total_send as question_answer_volume,
    update_time
from t3 left join t4 
on t3.shop_id = t4.shop_id
where  t3.shop_id not in (select shop_id from dim.shop_upgrade) 
;


-- 5 
upsert into app_mp.sub_nick_receive (
    shop_id,
    sub_nick,
    day,
    platform,
    mode,
    click_reply_pv
)
select shop_id,
    split_part(snick,'cntaobao',2) as sub_nick,
    '{{ ds }}' as day,
    platform,
    mode,
    count( distinct if(act in ('send_robot_msg','copy_robot_msg'),msg_id,null)) as click_reply_pv  
  from  dwd.mini_xdrs_log 
where day = {{ ds_nodash }}   and mode != ''  and act in ('send_robot_msg','copy_robot_msg') and shop_id in   (select shop_id from dim.shop_upgrade) 
group by 1,2,3,4,5;

