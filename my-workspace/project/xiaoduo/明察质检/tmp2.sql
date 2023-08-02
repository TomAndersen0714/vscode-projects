SELECT
    u_day,
    platform,
    shop_id,
    snick,
    cnick,
    real_buyer_nick,
    COUNT(1) AS cnt
FROM (
    SELECT
        toUInt32(day) AS u_day,
        platform,
        shop_id,
        replaceOne(snick,'cntaobao','') AS snick,
        replaceOne(cnick,'cntaobao','') AS cnick,
        real_buyer_nick,
        toUInt64(toFloat64(toDateTime64(create_time, 3))*1000) AS msg_milli_timestamp,
        act
    FROM ods.xdrs_logs_all
    PREWHERE day = 20230801
    AND shop_id GLOBAL IN (
        SELECT shop_id
        FROM xqc_dim.shop_latest_all
        WHERE company_id GLOBAL IN (
            SELECT _id
            FROM xqc_dim.company_latest_all
            WHERE has(white_list, 'VOC')
        )
        AND platform IN ['tb']
    )
    AND act IN ['send_msg', 'recv_msg']
) AS xdrs_logs
GROUP BY u_day,
    platform,
    shop_id,
    snick,
    cnick,
    real_buyer_nick
HAVING cnt > 2000