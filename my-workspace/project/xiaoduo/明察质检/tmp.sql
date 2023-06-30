            -- 查询每个当天没有下单的买家, 其对应的历史订单数据, 并更新其历史订单的状态字段, 以及版本日期
            SELECT
                20230628 AS day,
                platform,
                shop_id,
                buyer_nick,
                real_buyer_nick,
                order_id,
                arrayConcat(past_order_info.order_status_timestamps, past_order_update_info.order_status_timestamps) AS order_status_timestamps,
                arrayConcat(past_order_info.order_statuses, past_order_update_info.order_statuses) AS order_statuses
            FROM (
                -- 查询历史创建的订单对应的订单状态数据, 剔除当天有下单的买家
                SELECT
                    *
                FROM dwd.voc_buyer_latest_order_all
                WHERE day = 20230627
                AND shop_id GLOBAL IN (
                    SELECT shop_id
                    FROM xqc_dim.shop_latest_all
                    WHERE company_id GLOBAL IN (
                        SELECT _id
                        FROM xqc_dim.company_latest_all
                        WHERE has(white_list, 'VOC')
                    )
                    AND platform = 'dy'
                )
                -- 剔除当天有下单的买家
                AND buyer_nick GLOBAL NOT IN (
                    SELECT DISTINCT
                        buyer_nick
                    FROM remote('10.20.133.174:9000', 'ods.dy_order_event_all')
                    WHERE day = 20230628
                    AND shop_id GLOBAL IN (
                        SELECT shop_id
                        FROM xqc_dim.shop_latest_all
                        WHERE company_id GLOBAL IN (
                            SELECT _id
                            FROM xqc_dim.company_latest_all
                            WHERE has(white_list, 'VOC')
                        )
                        AND platform = 'dy'
                    )
                    AND status = 'created'
                )
            ) AS past_order_info
            LEFT JOIN (
                -- 查询当天的订单状态数据中, 属于历史订单的状态数据
                SELECT
                    platform,
                    shop_id,
                    buyer_nick,
                    real_buyer_nick,
                    order_id,
                    arraySort(groupArray(timestamp)) AS order_status_timestamps,
                    arraySort((x, y)->y, groupArray(status), groupArray(timestamp)) AS order_statuses
                FROM (
                    SELECT
                        platform, shop_id, buyer_nick, real_buyer_nick,
                        order_id, timestamp, status
                    FROM (
                        SELECT DISTINCT
                            shop_id,
                            buyer_nick,
                            '' AS real_buyer_nick,
                            order_id,
                            toUInt64(time) AS timestamp,
                            status
                        FROM remote('10.20.133.174:9000', 'ods.dy_order_event_all')
                        WHERE day = 20230628
                        AND shop_id GLOBAL IN (
                            SELECT shop_id
                            FROM xqc_dim.shop_latest_all
                            WHERE company_id GLOBAL IN (
                                SELECT _id
                                FROM xqc_dim.company_latest_all
                                WHERE has(white_list, 'VOC')
                            )
                            AND platform = 'dy'
                        )
                        -- 剔除当天下过单的买家
                        AND buyer_nick GLOBAL NOT IN (
                            SELECT 
                                buyer_nick
                            FROM remote('10.20.133.174:9000', 'ods.dy_order_event_all')
                            WHERE day = 20230628
                            AND shop_id GLOBAL IN (
                                SELECT shop_id
                                FROM xqc_dim.shop_latest_all
                                WHERE company_id GLOBAL IN (
                                    SELECT _id
                                    FROM xqc_dim.company_latest_all
                                    WHERE has(white_list, 'VOC')
                                )
                                AND platform = 'dy'
                            )
                            AND status = 'created'
                        )
                    ) AS order_update_info
                    GLOBAL LEFT JOIN (
                        SELECT
                            company_id,
                            shop_id,
                            platform
                        FROM xqc_dim.shop_latest_all
                        WHERE company_id GLOBAL IN (
                            SELECT _id
                            FROM xqc_dim.company_latest_all
                            WHERE has(white_list, 'VOC')
                        )
                        AND platform = 'dy'
                    ) AS voc_shop_info
                    USING(shop_id)
                ) AS order_update_info
                GROUP BY
                    platform,
                    shop_id,
                    buyer_nick,
                    real_buyer_nick,
                    order_id
            ) AS past_order_update_info
            USING(
                platform,
                shop_id,
                buyer_nick,
                real_buyer_nick,
                order_id
            )