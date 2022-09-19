-- 方太淘宝会话切分
INSERT INTO ft_dwd.session_msg_detail_all
SELECT
    day, platform, shop_id, shop_name,
    lower(hex(MD5(concat(toString(day), shop_id, snick, cnick, real_buyer_nick, toString(session_num))))) AS session_id,
    snick,
    cnick,
    real_buyer_nick,
    act,
    msg_id,
    msg_time,
    msg,
    plat_goods_id,
    is_first_msg_within_session
FROM (
    SELECT
        day,
        platform,
        shop_id,
        shop_name,
        snick,
        cnick,
        real_buyer_nick,
        plat_goods_ids,
        acts,
        msg_times,
        pre_msg_times,
        msg_time_delta_mins,
        is_first_msg_within_sessions,
        session_nums,
        msgs,
        msg_ids
    FROM (
        SELECT
            day,
            platform,
            shop_id,
            shop_name,
            snick,
            cnick,
            real_buyer_nick,
            groupArray(plat_goods_id) AS plat_goods_ids,
            groupArray(act) AS acts,
            groupArray(msg_time) AS msg_times,
            arrayPushFront(arrayPopBack(msg_times), toDateTime(1)) AS pre_msg_times,
            arrayMap((x,y)->( (toDateTime(x)-toDateTime(y))/60 ), msg_times, pre_msg_times) AS msg_time_delta_mins,
            arrayMap((x) -> (IF(x>10, 1, 0)), msg_time_delta_mins) AS is_first_msg_within_sessions,
            arrayMap((x,y)->(arraySum(arraySlice(is_first_msg_within_sessions, 1, y))), is_first_msg_within_sessions, arrayEnumerate(is_first_msg_within_sessions)) AS session_nums,
            groupArray(msg) AS msgs,
            groupArray(msg_id) AS msg_ids
        FROM (
            SELECT
                day,
                platform,
                shop_id,
                '{{shop_name}}' AS shop_name,
                replaceOne(snick,'cntaobao','') AS snick,
                replaceOne(cnick,'cntaobao','') AS cnick,
                real_buyer_nick,
                plat_goods_id,
                act,
                msg_time,
                msg,
                msg_id
            FROM ft_ods.xdrs_logs_all
            WHERE day = {{ds_nodash}}
            AND platform = 'tb'
            AND shop_id = '{{shop_id}}'
            AND act IN ['send_msg', 'recv_msg']
            ORDER BY day, platform, shop_id, snick, cnick, real_buyer_nick, msg_time
        ) AS message_info
        GROUP BY day, platform, shop_id, shop_name, snick, cnick, real_buyer_nick
    ) AS session_info
) AS session_detail_info
ARRAY JOIN
    plat_goods_ids AS plat_goods_id,
    acts AS act,
    msg_times AS msg_time,
    pre_msg_times AS pre_msg_time,
    msg_time_delta_mins AS msg_time_delta_min,
    is_first_msg_within_sessions AS is_first_msg_within_session,
    session_nums AS session_num,
    msgs AS msg,
    msg_ids AS msg_id
;

-- 等待数据写入
SELECT sleep(3);