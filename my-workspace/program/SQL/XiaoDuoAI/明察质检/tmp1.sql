SELECT
    msg_time_delta_min_within_session,
    COUNT(1) AS cnt
FROM (
    SELECT
        day, platform, shop_id, snick, cnick,
        groupArray(msg_time) AS msg_times,
        arrayPushFront(arrayPopBack(msg_times), msg_times[1]) AS pre_msg_times,
        arrayMap((x,y)->( toInt32((toDateTime(x)-toDateTime(y))/60) ), msg_times, pre_msg_times) AS msg_time_delta_mins_within_session
    FROM (
        SELECT 
            day, platform, shop_id, snick, cnick, msg_time
        FROM ods.xdrs_logs_all
        WHERE day = {{ds_nodash}}
        AND shop_id GLOBAL IN (
            SELECT DISTINCT
                shop_id
            FROM xqc_dim.xqc_shop_all
            WHERE day = toYYYYMMDD(yesterday())
            AND company_id = '{{ company_id }}'
            AND platform = 'tb'
        )
        ORDER BY day, platform, shop_id, snick, cnick, msg_time ASC
    ) AS msg_info
    GROUP BY day, platform, shop_id, snick, cnick
) AS msg_time_info
ARRAY JOIN
    msg_time_delta_mins_within_session AS msg_time_delta_min_within_session
GROUP BY msg_time_delta_min_within_session
ORDER BY msg_time_delta_min_within_session