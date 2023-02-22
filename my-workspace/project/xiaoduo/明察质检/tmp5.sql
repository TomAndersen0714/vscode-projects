SELECT t.`day`,
    t.shop_id,
    t.platform,
    transfer_cnick_count,
    recv_uv
FROM (
        SELECT `day`,
            shop_id,
            platform,
            shop_name,
            count(DISTINCT t2.cnick) AS transfer_cnick_count
        FROM (
                SELECT msgid,
                    transfer_state
                FROM ods.assistant_transfer_all
                WHERE day = 20230206
            ) t1
            JOIN (
                SELECT `day`,
                    shop_id,
                    platform,
                    replace(splitByString(':', snick) [1], 'cntaobao', '') AS shop_name,
                    msg,
                    cnick,
                    create_time,
                    question_b_standard_q,
                    msg_id
                FROM ods.xdrs_logs_all
                WHERE day = 20230206
                    AND act = 'recv_msg'
            ) t2 ON t1.msgid = t2.msg_id
        GROUP BY `day`,
            shop_id,
            platform,
            shop_name
    ) t
    FULL OUTER JOIN (
        SELECT `day`,
            shop_id,
            platform,
            replace(splitByString(':', snick) [1], 'cntaobao', '') AS shop_name,
            count(DISTINCT cnick) AS recv_uv
        FROM ods.xdrs_logs_all
        WHERE day = 20230206
            AND platform = 'tb'
            AND snick LIKE '%服务助手%'
            AND act = 'recv_msg'
        GROUP BY `day`,
            shop_id,
            platform,
            shop_name
    ) x ON t.`day` = x.`day`
    AND t.shop_id = x.shop_id
    AND t.platform = x.platform
    AND t.shop_name = x.shop_name