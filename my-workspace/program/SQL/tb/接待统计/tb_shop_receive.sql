-- step 1 
upsert into app_mp.shop_receive (
    shop_id,
    day, 
    platform,
    received_cuv, -- "服务买家人数"
    buyer_cuv, -- "买家主动咨询人数"
    seller_cuv, -- "客服发起会话人数"
    robot_cuv, 
    update_time
)
with t1 as (
    select 
        shop_id,
        split_part(snick,':',1) as nick,
        platform,
        cnick,
        concat_ws('',cast(msg_time as String),
        subString(if(msg_id = '' or msg_id is null,'9999999999999999',msg_id),10,3)) as time,
        act,
        send_msg_from 
    from dwd.mini_xdrs_log
    where day = {{ds_nodash}} 
    and strleft(cnick, 10) != 'comxiaoduo' 
    and regexp_replace(split_part(snick, ':', 1), 'cntaobao', '') != regexp_replace(regexp_replace(split_part(cnick, ':', 1), 'cntaobao', ''), 'cnalichn', '') 
    and (act = 'recv_msg' or act = 'send_msg') 
    order by time
),
t2 as (
    select
        shop_id,
        nick,
        platform,
        cnick,
        row_number() over (partition by nick,platform,cnick order by time) as rn,
        act,
        send_msg_from 
    from t1
),
t3 as (
    select * 
    from t2 
    where rn <= 1
),
t4 as (
    select
        shop_id,
        nick,
        platform,
        count(distinct cnick) as received_cuv,
        sum(if(act = 'recv_msg' or (act = 'send_msg' and send_msg_from = '1'),1,0)) as buyer_cuv,
        sum(if(act = 'send_msg' and (send_msg_from != '0' and send_msg_from != '1'),1,0)) as seller_cuv,
        sum(if(act = 'send_msg' and send_msg_from = '0',1,0)) as robot_cuv 
    from t3 
    group by 1,2,3
),
t5 as (
    select 
        shop_id,
        plat_shop_name
    from dim.shop_nick
)
select 
     t5.shop_id,
    '{{ ds }}' as day,
    t4.platform,
    received_cuv,
    buyer_cuv,
    seller_cuv,
    robot_cuv,
    from_unixtime(unix_timestamp(now()), 'yyyy-MM-dd HH:mm:ss') AS update_time 
from t4 
join [shuffle] t5 
on t4.shop_id = t5.shop_id  ;

-- step 2 
upsert into app_mp.shop_receive (
    shop_id,
    day,
    platform,
    received_session, -- "服务买家人次"
    buyer_session, -- "买家发起会话"
    seller_session, -- "客服发起会话"
    robot_session,
    received_pv, -- "接收问题数"
    identified_pv, -- "识别问题数"
    identified_rate, -- "识别率(e.g. 0.65)"
    auto_reply_pv, -- "机器人自动回复数"
    click_reply_pv, -- "人工点击采纳"
    robot_reply_rate, -- "机器人应答率"
    update_time
)
with t1 as (
    select 
        shop_id,
        '{{ ds }}' as day,
        platform,
        sum(received_session) as received_session,
        sum(buyer_session) as buyer_session,
        sum(seller_session) as seller_session,
        sum(robot_session) as robot_session,
        sum(received_pv) as received_pv,
        sum(identified_pv) as identified_pv,
        sum(auto_reply_pv) as auto_reply_pv,
        sum(click_reply_pv) as click_reply_pv 
    from app_mp.sub_nick_receive 
    where day = '{{ ds }}' 
    group by shop_id,platform
)
select 
    shop_id,
    day,
    platform,
    received_session,
    buyer_session,
    seller_session,
    robot_session,
    received_pv,
    identified_pv,
    if(received_pv = 0,0,identified_pv / received_pv) as identified_rate,
    auto_reply_pv,click_reply_pv,
    if(received_pv = 0,0,auto_reply_pv / received_pv) as robot_reply_rate,
    from_unixtime(unix_timestamp(now()), 'yyyy-MM-dd HH:mm:ss') as update_time 
from t1;

-- step 3
upsert into app_mp.shop_receive (
    shop_id,
    day,
    platform,
    shop_question_rate, -- "自定义问题占比(e.g. 0.97)"
    question_b_rate, -- "行业问题占比(e.g. 0.0013)"
    update_time)
with t1 as (
select
    shop_id,
    snick,
    split_part(snick,':',1) as nick,
    platform,
    act,
    is_identified,
    shop_question_id 
from dwd.mini_xdrs_log
where day = {{ ds_nodash }}
and strleft(cnick, 10) != 'comxiaoduo' 
and regexp_replace(split_part(snick, ':', 1), 'cntaobao', '') != regexp_replace(regexp_replace(split_part(cnick, ':', 1), 'cntaobao', ''), 'cnalichn', '') 
),
t2 as (
    select 
        shop_id,
    plat_shop_name
    from   dim.shop_nick
),
t3 as (
    select 
        t1.shop_id,
        '{{ ds }}' as day,
        t1.platform,
        sum(if((is_identified = 1 or shop_question_id != '') and act='recv_msg', 1,0)) as total,
        sum(if((shop_question_id != '' and act='recv_msg'),1,0)) as total_shop_question 
    from t1 
    join [shuffle] t2 
    on t1.shop_id = t2.shop_id 
    group by 1,2,3
)
select 
    shop_id,
    day,
    platform,
    if(total = 0,0,total_shop_question / total) as shop_question_rate,
    if(total = 0,0,(total - total_shop_question) / total) as question_b_rate,
    from_unixtime(unix_timestamp(now()), 'yyyy-MM-dd HH:mm:ss') as update_time 
from t3 ;

