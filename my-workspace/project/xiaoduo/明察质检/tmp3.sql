SELECT `day`,
    '{platform}' AS platform,
    shop_id,
    order_id,
    toFloat64(if(post_fee = '', '0', post_fee)),
    buyer_nick,
    real_buyer_nick,
    status,
    original_status,
    payment AS order_payment,
    new_order_seller_price,
    goods_id,
    if(goods_title = goods_id, '', goods_title),
    toFloat64(if(goods_price = goods_id, '0', goods_price)) AS f_goods_price,
    toInt32(if(goods_num = goods_id, '0', goods_num)) AS i_goods_num,
    if(
        goods_payment_sum = 0,
        if(
            length(new_plat_goods_price_arr) = 0,
            payment,
            payment / l_goods
        ),
        payment * (
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
        FROM (
                --付定金订单
                SELECT DISTINCT
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
                    'deposited' AS status,
                    original_status,
                    modified,
                    plat_goods_ids,
                    plat_goods_price_arr,
                    plat_goods_num_arr,
                    plat_goods_title_arr,
                    'FRONT_PAID_FINAL_NOPAID' AS step_trade_status,
                    goods_step_fee AS step_paid_fee,
                    'step' AS order_type,
                    `day`,
                    if(
                        g_info.goods_id != ''
                        AND step_deposit_start <= toDateTime(splitByString('.', modified)[1])
                        AND step_deposit_end >= toDateTime(splitByString('.', modified)[1]),
                        1,
                        0
                    ) AS step_flag,
                    goods_id
                FROM (
                        SELECT *,
                            length(plat_goods_ids) AS l_goods,
                            if(
                                length(plat_goods_price_arr) != l_goods,
                                plat_goods_ids,
                                arraySlice(plat_goods_price_arr, 1, length(plat_goods_ids))
                            ) AS join_plat_goods_price_arr,
                            if(
                                length(plat_goods_title_arr) != l_goods,
                                plat_goods_ids,
                                arraySlice(plat_goods_title_arr, 1, length(plat_goods_ids))
                            ) AS join_plat_goods_title_arr,
                            if(
                                length(plat_goods_num_arr) != l_goods,
                                plat_goods_ids,
                                arraySlice(plat_goods_num_arr, 1, length(plat_goods_ids))
                            ) AS join_plat_goods_num_arr,
                            toFloat64(
                                if(order_seller_price = '', '0', order_seller_price)
                            ) AS new_order_seller_price,
                            if(
                                length(plat_goods_price_arr) = 0,
                                [0],
                                arrayMap(
                                    (x, y)->toFloat64(if(x = '', '0', x)) * toFloat64(y),
                                    plat_goods_price_arr,
                                    plat_goods_num_arr
                                )
                            ) AS new_plat_goods_price_arr,
                            arraySum(new_plat_goods_price_arr) AS goods_payment_sum
                        FROM ft_ods.order_event_all
                        WHERE `day` = {ds_nodash}
                            AND shop_id = '{shop_id}'
                            AND status = 'created'
                        ARRAY JOIN
                            plat_goods_ids AS goods_id,
                            join_plat_goods_price_arr AS goods_price,
                            join_plat_goods_title_arr AS goods_title,
                            join_plat_goods_num_arr AS goods_num
                    ) AS o_info
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
                    AND step_deposit_start <= toDateTime(splitByString('.', modified)[1])
                    AND step_deposit_end >= toDateTime(splitByString('.', modified)[1])
                UNION ALL

                -- created订单
                SELECT DISTINCT
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
                    status,
                    original_status,
                    modified,
                    plat_goods_ids,
                    plat_goods_price_arr,
                    plat_goods_num_arr,
                    plat_goods_title_arr,
                    if(step_flag = 1, 'FRONT_NOPAID_FINAL_NOPAID', '') AS step_trade_status,
                    if(step_flag = 1, goods_step_fee, '') AS step_paid_fee,
                    if(step_flag = 1, 'step', order_type) AS order_type,
                    `day`,
                    if(
                        g_info.goods_id != ''
                        AND step_deposit_start <= toDateTime(splitByString('.', modified)[1])
                        AND step_deposit_end >= toDateTime(splitByString('.', modified)[1]),
                        1,
                        0
                    ) AS step_flag,
                    goods_id
                FROM (
                        SELECT *,
                            length(plat_goods_ids) AS l_goods,
                            if(
                                length(plat_goods_price_arr) != l_goods,
                                plat_goods_ids,
                                arraySlice(plat_goods_price_arr, 1, length(plat_goods_ids))
                            ) AS join_plat_goods_price_arr,
                            if(
                                length(plat_goods_title_arr) != l_goods,
                                plat_goods_ids,
                                arraySlice(plat_goods_title_arr, 1, length(plat_goods_ids))
                            ) AS join_plat_goods_title_arr,
                            if(
                                length(plat_goods_num_arr) != l_goods,
                                plat_goods_ids,
                                arraySlice(plat_goods_num_arr, 1, length(plat_goods_ids))
                            ) AS join_plat_goods_num_arr,
                            toFloat64(
                                if(order_seller_price = '', '0', order_seller_price)
                            ) AS new_order_seller_price,
                            if(
                                length(plat_goods_price_arr) = 0,
                                [0],
                                arrayMap(
                                    (x, y)->toFloat64(if(x = '', '0', x)) * toFloat64(y),
                                    plat_goods_price_arr,
                                    plat_goods_num_arr
                                )
                            ) AS new_plat_goods_price_arr,
                            arraySum(new_plat_goods_price_arr) AS goods_payment_sum
                        FROM ft_ods.order_event_all
                        WHERE `day` = {ds_nodash}
                            AND shop_id = '{shop_id}'
                            AND status = 'created'
                        ARRAY JOIN
                            plat_goods_ids AS goods_id,
                            join_plat_goods_price_arr AS goods_price,
                            join_plat_goods_title_arr AS goods_title,
                            join_plat_goods_num_arr AS goods_num
                    ) AS o_info
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
                UNION ALL

                -- 付尾款 订单
                SELECT DISTINCT
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
                    status,
                    original_status,
                    modified,
                    plat_goods_ids,
                    plat_goods_price_arr,
                    plat_goods_num_arr,
                    plat_goods_title_arr,
                    if(step_flag = 1, 'FRONT_PAID_FINAL_PAID', '') AS step_trade_status,
                    if(step_flag = 1, goods_step_fee, '') AS step_paid_fee,
                    if(step_flag = 1, 'step', order_type) AS order_type,
                    `day`,
                    if(
                        g_info.goods_id != ''
                        AND step_final_start <= toDateTime(splitByString('.', modified)[1])
                        AND step_final_end >= toDateTime(splitByString('.', modified)[1]),
                        1,
                        0
                    ) AS step_flag,
                    goods_id
                FROM (
                        SELECT *,
                            length(plat_goods_ids) AS l_goods,
                            if(
                                length(plat_goods_price_arr) != l_goods,
                                plat_goods_ids,
                                arraySlice(plat_goods_price_arr, 1, length(plat_goods_ids))
                            ) AS join_plat_goods_price_arr,
                            if(
                                length(plat_goods_title_arr) != l_goods,
                                plat_goods_ids,
                                arraySlice(plat_goods_title_arr, 1, length(plat_goods_ids))
                            ) AS join_plat_goods_title_arr,
                            if(
                                length(plat_goods_num_arr) != l_goods,
                                plat_goods_ids,
                                arraySlice(plat_goods_num_arr, 1, length(plat_goods_ids))
                            ) AS join_plat_goods_num_arr,
                            toFloat64(
                                if(order_seller_price = '', '0', order_seller_price)
                            ) AS new_order_seller_price,
                            if(
                                length(plat_goods_price_arr) = 0,
                                [0],
                                arrayMap(
                                    (x, y)->toFloat64(if(x = '', '0', x)) * toFloat64(y),
                                    plat_goods_price_arr,
                                    plat_goods_num_arr
                                )
                            ) AS new_plat_goods_price_arr,
                            arraySum(new_plat_goods_price_arr) AS goods_payment_sum
                        FROM ft_ods.order_event_all
                        WHERE `day` = {ds_nodash}
                            AND shop_id = '{shop_id}'
                            AND status != 'created'
                        ARRAY JOIN
                            plat_goods_ids AS goods_id,
                            join_plat_goods_price_arr AS goods_price,
                            join_plat_goods_title_arr AS goods_title,
                            join_plat_goods_num_arr AS goods_num
                    ) AS o_info
                    LEFT JOIN (
                        SELECT goods_id,
                            toDateTime(step_final_start_time) AS step_final_start,
                            toDateTime(step_final_end_time) AS step_final_end,
                            goods_step_fee
                        FROM ft_ods.presell_goods_config_all
                        WHERE shop_id = '{shop_id}'
                            AND toDate(step_final_start_time) <= toDate('{ds}')
                            AND toDate(step_final_end_time) >= toDate('{ds}')
                    ) AS g_info USING(goods_id)
            )
        WHERE `day` = {ds_nodash}
            AND shop_id = '{shop_id}'
            AND length(plat_goods_ids) != 0
    )