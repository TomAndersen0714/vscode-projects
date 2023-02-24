INSERT INTO {sink_table}
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
        payment * ((f_goods_price * i_goods_num) / goods_payment_sum)
    ) AS goods_payment,
    if(
        goods_payment_sum = 0,
        if(
            length(new_plat_goods_price_arr) = 0,
            new_order_seller_price,
            new_order_seller_price *(1 / length(new_plat_goods_price_arr))
        ),
        new_order_seller_price * ((f_goods_price * i_goods_num) / goods_payment_sum)
    ) AS goods_seller_price,
    step_trade_status,
    step_paid_fee,
    order_type,
    modified
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
            toFloat64(if(order_seller_price = '', '0', order_seller_price)) AS new_order_seller_price,
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
        FROM (
                -- 非预售订单 汇总
                SELECT DISTINCT order_id,
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
                    splitByString('.', modified) [1] AS modified,
                    plat_goods_ids,
                    plat_goods_price_arr,
                    plat_goods_num_arr,
                    plat_goods_title_arr,
                    step_trade_status,
                    step_paid_fee,
                    order_type,
                    `day`
                FROM ft_ods.order_event_all
                WHERE `day` = {ds_nodash}
                    AND shop_id = '{shop_id}'
                    AND order_id NOT IN (
                        SELECT DISTINCT order_id
                        FROM ft_ods.presell_order_detail_all
                        WHERE `day` = {ds_nodash}
                            AND shop_id = '{shop_id}'
                    )
                UNION ALL

                -- 预售订单状态回填
                SELECT DISTINCT order_id,
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
                    splitByString('.', modified) [1] AS modified,
                    plat_goods_ids,
                    plat_goods_price_arr,
                    plat_goods_num_arr,
                    plat_goods_title_arr,
                    if(
                        order_status = '1',
                        'FRONT_NOPAID_FINAL_NOPAID',
                        'FRONT_PAID_FINAL_PAID'
                    ) AS step_trade_status,
                    if(
                        status NOT IN ('created', 'paid'),
                        pay_balance_real,
                        pay_bargain_real
                    ) AS step_paid_fee,
                    'step' AS order_type,
                    `day`
                FROM (
                        SELECT *
                        FROM ft_ods.order_event_all
                        WHERE `day` = {ds_nodash}
                            AND shop_id = '{shop_id}'
                    ) AS ods
                    JOIN (
                        SELECT *
                        FROM ft_ods.presell_order_detail_all
                        WHERE `day` = {ds_nodash}
                            AND shop_id = '{shop_id}'
                    ) AS pre USING(shop_id, `day`, order_id)
                UNION ALL

                -- 待付定金订单状态回填
                SELECT DISTINCT order_id,
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
                    'deposited' AS original_status,
                    toString(toDateTime(toInt64(update_time) / 1000)) AS modified,
                    plat_goods_ids,
                    plat_goods_price_arr,
                    plat_goods_num_arr,
                    plat_goods_title_arr,
                    'FRONT_PAID_FINAL_NOPAID' AS step_trade_status,
                    pay_bargain_plan AS step_paid_fee,
                    'step' AS order_type,
                    pre.`day` AS `day`
                FROM (
                        SELECT *
                        FROM ft_ods.order_event_all
                        WHERE `day` IN (
                                {yesterday_ds_nodash},
                                {ds_nodash}
                            )
                            AND shop_id = '{shop_id}'
                    ) AS ods
                    JOIN (
                        SELECT *
                        FROM ft_ods.presell_order_detail_all
                        WHERE `day` = {ds_nodash}
                            AND shop_id = '{shop_id}'
                            AND order_status = '1'
                    ) AS pre USING(
                        shop_id,
                        order_id
                    )
            )
        WHERE `day` = {ds_nodash}
            AND shop_id = '{shop_id}'
            AND length(plat_goods_ids) != 0
    )
    ARRAY JOIN
        plat_goods_ids AS goods_id,
        join_plat_goods_price_arr AS goods_price,
        join_plat_goods_title_arr AS goods_title,
        join_plat_goods_num_arr AS goods_num