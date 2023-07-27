SELECT
    u_day,
    platform,
    shop_id,
    snick,
    cnick,
    real_buyer_nick,
    -- 切分会话生成QA切分标记, PS: 可能存在单个Q, 单个A, 单个QA, 多个QA四种情况, 此切分方法只能切分多QA的情况
    arrayMap(
        (x, y)->(if(x = 'send_msg' AND sorted_msg_acts[y-1] = 'recv_msg', 1, 0)),
        sorted_msg_acts,
        arrayEnumerate(sorted_msg_acts)
    ) AS _qa_split_tags,
    arraySum(_qa_split_tags)
FROM (
    SELECT
        u_day,
        platform,
        shop_id,
        snick,
        cnick,
        real_buyer_nick,
        arraySort(groupArray(msg_milli_timestamp)) AS msg_milli_timestamps,
        arraySort((x, y)->y, groupArray(act), groupArray(msg_milli_timestamp)) AS sorted_msg_acts
    FROM (
        SELECT
            toUInt32(day) AS u_day,
            platform,
            shop_id,
            replaceOne(snick,'cnjd','') AS snick,
            replaceOne(cnick,'cnjd','') AS cnick,
            '' AS real_buyer_nick,
            toUInt64(toFloat64(toDateTime64(create_time, 3))*1000) AS msg_milli_timestamp,
            act
        FROM ods.xdrs_logs_all
        WHERE day = 20230606
        AND act IN ['send_msg', 'recv_msg']
    )
    GROUP BY u_day,
        platform,
        shop_id,
        snick,
        cnick,
        real_buyer_nick
) AS xdrs_logs
FORMAT Null;