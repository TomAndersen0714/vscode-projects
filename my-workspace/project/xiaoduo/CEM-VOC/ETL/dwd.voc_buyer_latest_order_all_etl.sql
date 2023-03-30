-- stage_1, 写入当天创建的订单记录
INSERT INTO dwd.voc_buyer_latest_order_all
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
        '{platform}' AS platform,
        shop_id,
        buyer_nick,
        '' AS real_buyer_nick,
        order_id,
        toUInt64(time) AS timestamp,
        status
    FROM ods.order_event_all
    WHERE day = {ds_nodash}
    AND shop_id IN {shop_id}
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
            WHERE day = {ds_nodash}
            AND shop_id IN {shop_id}
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



-- stage_2, 更新过去创建的订单记录, 并剔除当天有下单的买家
INSERT INTO dwd.voc_buyer_latest_order_all
SELECT
    day,
    platform,
    shop_id,
    buyer_nick,
    real_buyer_nick,
    order_id,
    arrayConcat(past_order_info.order_status_timestamps, past_order_update_info.order_status_timestamps) AS order_status_timestamps,
    arrayConcat(past_order_info.order_statuses, past_order_update_info.order_statuses) AS order_statuses
FROM (
    -- 过去创建的订单记录, 剔除当天有下单的买家
    SELECT
        *
    FROM dwd.voc_buyer_latest_order_all
    WHERE day = {yesterday_ds_nodash}
    AND shop_id IN {shop_id}
    -- 剔除当天下过单的买家
    AND buyer_nick NOT IN (
        SELECT
            buyer_nick
        FROM ods.order_event_all
        WHERE day = {ds_nodash}
        AND shop_id IN {shop_id}
        AND status = 'created'
    )
) AS past_order_info
LEFT JOIN (
    -- 当天产生但在过去创建的订单记录, 用于更新过去创建的订单记录状态字段, 剔除当天有下单的买家
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
            '{platform}' AS platform,
            shop_id,
            buyer_nick,
            '' AS real_buyer_nick,
            order_id,
            toUInt64(time) AS timestamp,
            status
        FROM ods.order_event_all
        WHERE day = {ds_nodash}
        AND shop_id IN {shop_id}
        -- 剔除当天下过单的买家
        AND buyer_nick NOT IN (
            SELECT 
                buyer_nick
            FROM ods.order_event_all
            WHERE day = {ds_nodash}
            AND shop_id IN {shop_id}
            AND status = 'created'
        )
    )
    GROUP BY day,
        platform,
        shop_id,
        buyer_nick,
        real_buyer_nick,
        order_id
) AS past_order_update_info
USING(
    day,
    platform,
    shop_id,
    buyer_nick,
    real_buyer_nick,
    order_id
)