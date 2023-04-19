                SELECT
                    day, platform, shop_id, buyer_nick, real_buyer_nick,
                    order_id, timestamp, status
                FROM (
                    SELECT DISTINCT
                        toUInt32(day) AS day,
                        shop_id,
                        buyer_nick,
                        '' AS real_buyer_nick,
                        order_id,
                        toUInt64(time) AS timestamp,
                        status
                    FROM ods.order_event_all
                    WHERE day = {ds_nodash}
                    AND shop_id IN {VOC_SHOP_IDS}
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
                            AND shop_id IN {VOC_SHOP_IDS}
                            AND status = 'created'
                            GROUP BY buyer_nick
                        )
                    )
                ) AS order_create_info
                GLOBAL LEFT JOIN (
                    SELECT
                        company_id,
                        shop_id,
                        platform
                    FROM numbers(1)
                    ARRAY JOIN
                        {VOC_COMPANY_IDS} AS company_id,
                        {VOC_SHOP_IDS} AS shop_id,
                        {VOC_PLATFORMS} AS platform
                ) AS voc_shop_info
                USING(shop_id)