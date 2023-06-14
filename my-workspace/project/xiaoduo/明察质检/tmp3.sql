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
                WHERE day = 20230605
                AND shop_id IN ['60b72d421edc070017428380', '603c98af400eb6001029be86', '58ddbef5369f9931fd6a1cb5', '614aa89338af59001621003d', '6151272333743000150e1869', '5e8be6d0e4f3320016ea3faa', '600553e5d8891f00111e4a71', '62416239773256001c03f83b', '626bd05db89a800019266951', '60e4192bf7d2f001ca988e52', '5cac112e98ef4100118a9c9f', '5bbde9d25a9f7250fd5c3234']
                -- 剔除当天下过单的买家
                AND buyer_nick NOT IN (
                    SELECT
                        buyer_nick
                    FROM ods.order_event_all
                    WHERE day = 20230606
                    AND shop_id IN ['60b72d421edc070017428380', '603c98af400eb6001029be86', '58ddbef5369f9931fd6a1cb5', '614aa89338af59001621003d', '6151272333743000150e1869', '5e8be6d0e4f3320016ea3faa', '600553e5d8891f00111e4a71', '62416239773256001c03f83b', '626bd05db89a800019266951', '60e4192bf7d2f001ca988e52', '5cac112e98ef4100118a9c9f', '5bbde9d25a9f7250fd5c3234']
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
                    SELECT
                        u_day AS day, platform, shop_id, buyer_nick, real_buyer_nick,
                        order_id, timestamp, status
                    FROM (
                        SELECT DISTINCT
                            toUInt32(day) AS u_day,
                            shop_id,
                            buyer_nick,
                            '' AS real_buyer_nick,
                            order_id,
                            toUInt64(time) AS timestamp,
                            status
                        FROM ods.order_event_all
                        WHERE day = 20230606
                        AND shop_id IN ['60b72d421edc070017428380', '603c98af400eb6001029be86', '58ddbef5369f9931fd6a1cb5', '614aa89338af59001621003d', '6151272333743000150e1869', '5e8be6d0e4f3320016ea3faa', '600553e5d8891f00111e4a71', '62416239773256001c03f83b', '626bd05db89a800019266951', '60e4192bf7d2f001ca988e52', '5cac112e98ef4100118a9c9f', '5bbde9d25a9f7250fd5c3234']
                        -- 剔除当天下过单的买家
                        AND buyer_nick NOT IN (
                            SELECT 
                                buyer_nick
                            FROM ods.order_event_all
                            WHERE day = 20230606
                            AND shop_id IN ['60b72d421edc070017428380', '603c98af400eb6001029be86', '58ddbef5369f9931fd6a1cb5', '614aa89338af59001621003d', '6151272333743000150e1869', '5e8be6d0e4f3320016ea3faa', '600553e5d8891f00111e4a71', '62416239773256001c03f83b', '626bd05db89a800019266951', '60e4192bf7d2f001ca988e52', '5cac112e98ef4100118a9c9f', '5bbde9d25a9f7250fd5c3234']
                            AND status = 'created'
                        )
                    ) AS order_update_info
                    GLOBAL LEFT JOIN (
                        SELECT
                            company_id,
                            shop_id,
                            platform
                        FROM numbers(1)
                        ARRAY JOIN
                            ['63fc50f0a06a5ecd9a249ac9', '6242bf16b9caa9fa6158fe3c', '6242bf16b9caa9fa6158fe3c', '6242bf16b9caa9fa6158fe3c', '6242bf16b9caa9fa6158fe3c', '6242bf16b9caa9fa6158fe3c', '6239c9270707ce52b8130b88', '6239c9270707ce52b8130b88', '6239c9270707ce52b8130b88', '5f747ba42c90fd0001254404', '5f747ba42c90fd0001254404', '622f015949926b789b1c5a1f'] AS company_id,
                            ['60b72d421edc070017428380', '603c98af400eb6001029be86', '58ddbef5369f9931fd6a1cb5', '614aa89338af59001621003d', '6151272333743000150e1869', '5e8be6d0e4f3320016ea3faa', '600553e5d8891f00111e4a71', '62416239773256001c03f83b', '626bd05db89a800019266951', '60e4192bf7d2f001ca988e52', '5cac112e98ef4100118a9c9f', '5bbde9d25a9f7250fd5c3234'] AS shop_id,
                            ['tb', 'tb', 'tb', 'tb', 'tb', 'tb', 'tb', 'tb', 'tb', 'tb', 'tb', 'tb'] AS platform
                    ) AS voc_shop_info
                    USING(shop_id)
                ) AS order_update_info
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