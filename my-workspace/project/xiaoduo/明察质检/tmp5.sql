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
                            arrayJoin(plat_goods_ids) AS goods_id
                        FROM ft_ods.order_event_all
                        WHERE `day` = {ds_nodash}
                            AND shop_id = '{shop_id}'
                            AND status = 'created'
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