INSERT INTO {sink_table}
SELECT `day`,
    '{platform}' AS platform,
    shop_id,
    order_id,
    toFloat64(if(post_fee = '', '0', post_fee)),
    buyer_nick,
    real_buyer_nick,
    new_status,
    original_status,
    payment AS order_payment,
    new_order_seller_price,
    goods_id,
    goods_title,
    toFloat64(if(goods_price = '', '0', goods_price)) AS f_goods_price,
    toInt32(if(goods_num = '', '0', goods_num)) AS i_goods_num,
    if(
        goods_payment_sum = 0,
        if(
            length(new_plat_goods_price_arr) = 0,
            order_payment,
            order_payment / goods_id_cnt
        ),
        order_payment * (
            (f_goods_price * i_goods_num) / goods_payment_sum
        )
    ) AS goods_payment,
    if(
        goods_payment_sum = 0,
        if(
            length(new_plat_goods_price_arr) = 0,
            new_order_seller_price,
            new_order_seller_price *(1 / length(new_plat_goods_price_arr))
        ),
        new_order_seller_price * (
            (f_goods_price * i_goods_num) / goods_payment_sum
        )
    ) AS goods_seller_price,
    step_trade_status,
    if(
        order_type = 'step'
        and toInt64(goods_payment) = 0,
        '0',
        step_paid_fee
    ) as step_paid_fee,
    order_type,
    modified
FROM (
        SELECT *,
            'FRONT_PAID_FINAL_NOPAID' AS step_trade_status,
            goods_step_fee AS step_paid_fee,
            'step' AS order_type,
            `day`,
            if(
                g_info.goods_id != ''
                AND step_deposit_start <= toDateTime(splitByString('.', modified) [1])
                AND step_deposit_end >= toDateTime(splitByString('.', modified) [1]),
                1,
                0
            ) AS step_flag,
            goods_id
        FROM (
                SELECT *,
                    goods_id,
                    goods_price,
                    goods_title,
                    goods_num
                FROM (
                        --付定金订单
                        SELECT DISTINCT
                            day,
                            order_id,
                            shop_id,
                            buyer_nick,
                            real_buyer_nick,
                            payment,
                            if(
                                toFloat64OrZero(order_seller_price) = 0,
                                toString(payment),
                                order_seller_price
                            ) AS order_seller_price,
                            post_fee,
                            'deposited' AS new_status,
                            original_status,
                            modified,
                            plat_goods_ids,
                            plat_goods_price_arr,
                            plat_goods_num_arr,
                            plat_goods_title_arr,
                            toFloat64(
                                if(order_seller_price = '', '0', order_seller_price)
                            ) AS new_order_seller_price,
                            arrayMap(
                                (x, y)->toFloat64(if(x = '', '0', x)) * toFloat64(y),
                                plat_goods_price_arr,
                                plat_goods_num_arr
                            ) AS new_plat_goods_price_arr,
                            arraySum(new_plat_goods_price_arr) AS goods_payment_sum,
                            length(plat_goods_ids) AS goods_id_cnt
                        FROM ft_ods.order_event_all
                        WHERE `day` = {ds_nodash}
                            AND shop_id = '{shop_id}'
                            AND status = 'created'
                            AND length(plat_goods_ids) != 0
                    )
                    ARRAY JOIN
                        plat_goods_ids AS goods_id,
                        plat_goods_price_arr AS goods_price,
                        plat_goods_title_arr AS goods_title,
                        plat_goods_num_arr AS goods_num
            )
            LEFT JOIN (
                SELECT goods_id,
                    toDateTime(step_deposit_start_time) AS step_deposit_start,
                    toDateTime(step_deposit_end_time) AS step_deposit_end,
                    goods_step_fee
                FROM ft_ods.presell_goods_config_all
                WHERE shop_id = '{shop_id}'
                    AND toDate(step_deposit_start_time) <= toDate('{ds}')
                    AND toDate(step_deposit_end_time) >= toDate('{ds}')
            ) AS g_info USING(goods_id)
        HAVING g_info.goods_id != ''
            AND step_deposit_start <= toDateTime(splitByString('.', modified) [1])
            AND step_deposit_end >= toDateTime(splitByString('.', modified) [1])
    )