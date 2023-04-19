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
                        -- 剔除当天下过单的买家
                        AND buyer_nick NOT IN (
                            SELECT 
                                buyer_nick
                            FROM ods.order_event_all
                            WHERE day = {ds_nodash}
                            AND shop_id IN {VOC_SHOP_IDS}
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
                            {VOC_COMPANY_IDS} AS company_id,
                            {VOC_SHOP_IDS} AS shop_id,
                            {VOC_PLATFORMS} AS platform
                    ) AS voc_shop_info
                    USING(shop_id)