SELECT
    day,
    platform,
    shop_id,
    buyer_nick,
    real_buyer_nick,
    order_id,
    arraySort(groupArray(timestamp)) AS order_status_timestamps,
    arraySort((x, y)->y, groupArray(status), groupArray(timestamp)) AS order_statuses
FROM (
    SELECT DISTINCT
        day,
        '{{platform}}' AS platform,
        shop_id,
        buyer_nick,
        '' AS real_buyer_nick,
        order_id,
        toUInt64(time) AS timestamp,
        status
    FROM ods.order_event_all
    WHERE day = {{ds_nodash}}
    AND shop_id IN {{shop_id}}
    -- 筛选当天每个买家的最新创建的订单
    AND order_id IN (
        SELECT latest_order_id
        FROM (
            SELECT
                buyer_nick,
                arrayReverseSort(
                    (x,y)->y, order_ids, times
                )[1] AS latest_order_id,
                groupArray(order_id) AS order_ids,
                groupArray(time) AS times
            FROM ods.order_event_all
            WHERE day = {{ds_nodash}}
            AND shop_id IN {{shop_id}}
            AND status = 'created'
            GROUP BY buyer_nick
        )
    )
)
GROUP BY day,
    platform,
    shop_id,
    buyer_nick,
    real_buyer_nick,
    order_id
