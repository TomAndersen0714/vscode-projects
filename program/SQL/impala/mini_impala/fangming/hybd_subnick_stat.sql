-- add partition
alter table app_mp.reception_subnick_hybd_stat add if not exists range partition {{ ds_nodash }} <= VALUES < {{ tomorrow_ds_nodash }};
--
upsert INTO app_mp.reception_subnick_hybd_stat(ds_nodash, shop_id, subnick, recp_buyers_amount, recv_question_amount, identified_question_amount, auto_reply_amount, robot_recp_buyer_amount)
SELECT {{ ds_nodash }}
        ,shop_id
        ,replace(snick, 'cntaobao', '')
        ,count(DISTINCT if(act='recv_msg', cnick, null))
        ,sum(CASE 
                        WHEN act = 'recv_msg'
                                THEN 1
                        ELSE 0
                        END)
        ,sum(CASE act
                        WHEN 'recv_msg'
                                THEN is_identified
                        ELSE 0
                        END)
        ,count(distinct (CASE 
                        WHEN send_msg_from = '0'
                                AND act = 'send_msg'
                                THEN msg_id
                        ELSE null
                        END))
        ,count(distinct (CASE
                        WHEN send_msg_from in ('0', '3')
                                AND act = 'send_msg'
                                THEN cnick
                        ELSE null
                        END))
FROM dwd.mini_xdrs_log
WHERE day = {{ ds_nodash }} and mode = 'HYBRID'
GROUP BY 1
        ,2
        ,3;

-- fix recp_buyers_amount by tb reminder
upsert INTO app_mp.reception_subnick_hybd_stat(ds_nodash, shop_id, subnick, recp_buyers_amount)
with send_table as (
        select shop_id, snick, cnick
        from dwd.mini_xdrs_log where day = {{ ds_nodash }} and mode = 'HYBRID' and act = 'send_msg' group by 1,2,3
), recv_table as (
        select snick, cnick
        from dwd.mini_xdrs_log where day = {{ ds_nodash }} and mode = 'HYBRID' and act = 'recv_msg' group by 1,2
), only_send_pair as (
        select shop_id, s.snick, s.cnick
        from send_table as s left join[broadcast] recv_table as r using(snick, cnick)
        where r.snick is null
), trans_pair as (
        select shop_id, snick, cnick
        from dwd.mini_xdrs_log where day = {{ ds_nodash }} and send_msg_from = '4'
), extra_recp_cnicks as (
select t.shop_id, replace(t.snick, 'cntaobao', '') as subnick, count(distinct t.cnick) as cnick_amount
from trans_pair as t join only_send_pair as o using(snick, cnick) group by 1,2)
select {{ ds_nodash }}, r.shop_id, r.subnick, r.recp_buyers_amount + cnick_amount
from app_mp.reception_subnick_hybd_stat as r join extra_recp_cnicks as e using(subnick)
where r.ds_nodash = {{ ds_nodash }};

------
------
upsert
INTO app_mp.reception_subnick_hybd_stat(ds_nodash, shop_id, subnick, identified_rate, auto_reply_rate)
SELECT ds_nodash
        ,shop_id
        ,subnick
        ,cast(CASE recv_question_amount
                WHEN 0
                        THEN 0
                ELSE identified_question_amount / recv_question_amount
                END as float)
        ,cast(CASE recv_question_amount
                WHEN 0
                        THEN 0
                ELSE auto_reply_amount / recv_question_amount
                END as float)
FROM app_mp.reception_subnick_hybd_stat
WHERE ds_nodash = {{ ds_nodash }};
-----------------
-----------------
upsert
INTO app_mp.reception_subnick_hybd_stat(ds_nodash, shop_id, subnick, avg_resp_interval, hybd_resp_interval_amount, hybd_resp_pair_amount)
WITH t
AS (
        SELECT shop_id
                ,snick
                ,cnick
                ,act
                ,create_time
                ,row_number() OVER (
                        PARTITION BY shop_id, snick
                        ,cnick ORDER BY create_time
                        ) AS num
        FROM dwd.mini_xdrs_log
        WHERE day = {{ ds_nodash }}
        AND mode = 'HYBRID'
        ), validate_QA_pair as (
SELECT t1.shop_id
        ,t1.snick
        ,t1.cnick
        ,t1.act
        ,t1.create_time
        ,CASE 
                WHEN t2.act IS NULL
                        THEN true
                ELSE t1.act != t2.act
                END AS is_first
FROM t AS t1
LEFT JOIN t AS t2 ON t1.snick = t2.snick
        AND t1.cnick = t2.cnick
        AND t1.num - 1 = t2.num
WHERE t2.act IS NULL or t1.act != t2.act)
, ranged_QA_pair as (
SELECT shop_id
        ,snick
        ,cnick
        ,act
        ,create_time
        ,row_number() OVER (
                PARTITION BY shop_id
                ,snick
                ,cnick ORDER BY create_time
                ) AS num
FROM validate_QA_pair
), pair_with_itvl as (
SELECT p1.shop_id
        ,p1.snick
        ,p1.cnick
        ,(cast(to_timestamp(p2.create_time, 'yyyy-MM-dd HH:mm:ss.SSS') AS double) - cast(to_timestamp(p1.create_time, 'yyyy-MM-dd HH:mm:ss.SSS') AS double)) AS itvl
FROM ranged_QA_pair AS p1
LEFT JOIN ranged_QA_pair AS p2 ON p1.snick = p2.snick
        AND p1.cnick = p2.cnick
        AND p1.num + 1 = p2.num
        AND p1.shop_id = p2.shop_id
WHERE p1.act = 'recv_msg'
        AND p2.create_time IS NOT NULL
        AND (cast(to_timestamp(p2.create_time, 'yyyy-MM-dd HH:mm:ss.SSS') AS double) - cast(to_timestamp(p1.create_time, 'yyyy-MM-dd HH:mm:ss.SSS') AS double)) < 10 * 60)
SELECT {{ ds_nodash }}
        ,shop_id
        ,replace(snick, 'cntaobao', '')
        ,cast(avg(itvl) AS FLOAT)
        ,cast(sum(itvl) as bigint)
        ,count(*)
FROM pair_with_itvl
GROUP BY 1
        ,2
        ,3;
-----
-----
upsert
INTO app_mp.reception_subnick_hybd_stat(ds_nodash, shop_id, subnick, human_avg_resp_interval, human_resp_interval_amount, human_resp_pair_amount)
WITH t
AS (
        SELECT shop_id
                ,snick
                ,cnick
                ,act
                ,create_time
                ,send_msg_from
                ,row_number() OVER (
                        PARTITION BY shop_id, snick
                        ,cnick ORDER BY create_time
                        ) AS num
        FROM dwd.mini_xdrs_log
        WHERE day = {{ ds_nodash }}
        AND mode = 'HYBRID'
        ), validate_QA_pair as (
SELECT t1.shop_id
        ,t1.snick
        ,t1.cnick
        ,t1.act
        ,t1.create_time
        ,t1.send_msg_from
        ,CASE 
                WHEN t2.act IS NULL
                        THEN true
                ELSE t1.act != t2.act
                END AS is_first
FROM t AS t1
LEFT JOIN t AS t2 ON t1.snick = t2.snick
        AND t1.cnick = t2.cnick
        AND t1.num - 1 = t2.num
WHERE (t2.act IS NULL or t1.act != t2.act))
, ranged_QA_pair as (
SELECT shop_id
        ,snick
        ,cnick
        ,act
        ,create_time
        ,send_msg_from
        ,row_number() OVER (
                PARTITION BY shop_id
                ,snick
                ,cnick ORDER BY create_time
                ) AS num
FROM validate_QA_pair
), pair_with_itvl as (
SELECT p1.shop_id
        ,p1.snick
        ,p1.cnick
        ,(cast(to_timestamp(p2.create_time, 'yyyy-MM-dd HH:mm:ss.SSS') AS double) - cast(to_timestamp(p1.create_time, 'yyyy-MM-dd HH:mm:ss.SSS') AS double)) AS itvl
FROM ranged_QA_pair AS p1
LEFT JOIN ranged_QA_pair AS p2 ON p1.snick = p2.snick
        AND p1.cnick = p2.cnick
        AND p1.num + 1 = p2.num
        AND p1.shop_id = p2.shop_id
WHERE p1.act = 'recv_msg'
        AND p2.send_msg_from = '2'
        AND p2.create_time IS NOT NULL
        AND (cast(to_timestamp(p2.create_time, 'yyyy-MM-dd HH:mm:ss.SSS') AS double) - cast(to_timestamp(p1.create_time, 'yyyy-MM-dd HH:mm:ss.SSS') AS double)) < 10 * 60)
SELECT {{ ds_nodash }}
        ,shop_id
        ,replace(snick, 'cntaobao', '')
        ,cast(avg(itvl) AS FLOAT)
        ,cast(sum(itvl) as bigint)
        ,count(*)
FROM pair_with_itvl
GROUP BY 1
        ,2
        ,3;
