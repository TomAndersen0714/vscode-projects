insert into {sink_table}
SELECT order_id,
    shop_id,
    buyer_nick,
    real_buyer_nick,
    payment,
    order_seller_price,
    post_fee,
    CASE
        WHEN original_status IN (
            'TRADE_CREATED',
            'DengDaiFuKuan',
            'NOT_PAY',
            'UNPAID'
        ) THEN 'created'
        WHEN original_status IN (
            'TRADE_PAYMENT',
            'DengDaiDaYin',
            'DengDaiDaBao',
            'DengDaiFaHuo',
            'DengDaiChuKu',
            'WAIT_SELLER_STOCK_OUT',
            'PAUSE',
            'UN_KNOWN'
        ) THEN 'paid'
        WHEN original_status IN (
            'WAIT_GOODS_RECEIVE_CONFIRM',
            'POP_ORDER_OUT_STORAGE',
            'ZanTing',
            'DengDaiQueRenShouHuo'
        ) THEN 'shipped'
        WHEN original_status IN (
            'FINISHED_L',
            'FINISH',
            'WanCheng'
        ) THEN 'succeeded'
        WHEN original_status IN (
            'TRADE_CANCELED',
            'SuoDing',
            'PeiSongTuiHuo',
            'PARENT_TRADE_CANCELED',
            'DELIVERY_RETURN',
            'LOCKED',
            'CANCEL'
        ) THEN 'closed'
        ELSE 'not_match'
    END AS status,
    original_status,
    modified,
    plat_goods_ids,
    plat_goods_price_arr,
    plat_goods_num_arr,
    plat_goods_title_arr,
    step_trade_status,
    step_paid_fee,
    order_type,
    `day`
FROM (
        SELECT tmc.order_id,
            tmc.shop_id,
            trade.buyer_nick,
            '' AS real_buyer_nick,
            trade.payment,
            '' AS post_fee,
            tmc.original_status,
            tmc.order_update_time AS modified,
            trade.plat_goods_ids,
            trade.plat_goods_price_arr,
            trade.plat_goods_num_arr,
            trade.plat_goods_title_arr,
            '' AS step_trade_status,
            '' AS step_paid_fee,
            tmc.order_type,
            trade.order_seller_price,
            tmc.`day`
        FROM (
                SELECT order_id,
                    '{shop_id}' AS shop_id,
                    order_status AS original_status,
                    order_update_time,
                    order_type,
                    `day`
                FROM (
                        SELECT *
                        FROM ods.jd_order_trade_server_work_all
                        WHERE `day` = {ds_nodash}
                            AND vender_id = '{vender_id}'
                            AND order_create_time != order_update_time
                        UNION ALL
                        SELECT *
                        FROM ods.jd_order_trade_server_work_all
                        WHERE `day` = {ds_nodash}
                            AND vender_id = '{vender_id}'
                            AND order_status = 'TRADE_CREATED'
                    )
            ) AS tmc
            LEFT JOIN (
                SELECT order_id,
                    shop_id,
                    buyer_nick,
                    argMax(payment, `time`) / 100 AS payment,
                    argMax(plat_goods_ids, `time`) AS plat_goods_ids,
                    argMax(plat_goods_names, `time`) AS plat_goods_title_arr,
                    argMax(plat_goods_price, `time`) AS plat_goods_price_arr,
                    argMax(plat_goods_count, `time`) AS plat_goods_num_arr,
                    argMax(balance_used, `time`) AS balance_used,
                    argMax(order_seller_price, `time`) AS order_seller_price,
                    `day`
                FROM ods.order_event_all
                WHERE `day` = {ds_nodash}
                    AND shop_id = '{shop_id}'
                GROUP BY `day`,
                    order_id,
                    shop_id,
                    buyer_nick
            ) AS trade USING(
                `day`,
                shop_id,
                order_id
            )
        UNION ALL
        SELECT order_id,
            shop_id,
            buyer_nick,
            '' AS real_buyer_nick,
            payment / 100 AS payment,
            '' AS post_fee,
            original_status,
            toString(`time`) AS modified,
            plat_goods_ids,
            plat_goods_price AS plat_goods_price_arr,
            plat_goods_count AS plat_goods_num_arr,
            plat_goods_names AS plat_goods_title_arr,
            '' AS step_trade_status,
            '' AS step_paid_fee,
            '' AS order_type,
            order_seller_price,
            `day`
        FROM ods.order_event_all
        WHERE `day` = {ds_nodash}
            AND shop_id = '{shop_id}'
    );