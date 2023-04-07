SELECT `day`,
    shop_id,
    '{{platform}}' AS platform,
    snick,
    'all' as goods_id,
    stat_label,
    stat_value,
    toString(now64(3, 'Asia/Shanghai')) as update_at
FROM (
        select shop_id,
            `day`,
            snick,
            sum(payment) as collocation_amt,
            --2+x搭配额
            count(distinct cnick) as collocation_cnt --2+x搭配量
        from (
                SELECT shop_id,
                    `day`,
                    snick,
                    cnick,
                    groupArray(order_id) as order_ids,
                    groupArray(goods_id) as goods_ids,
                    arraySum(groupArray(goods_payment)) as payment,
                    length(arrayDistinct(groupArray(tag))) as tags_cnt
                FROM (
                        SELECT *
                        FROM (
                                SELECT *,
                                    if(
                                        sku_info.sku_id != '',
                                        0,
                                        1
                                    ) AS tag
                                FROM (
                                        SELECT shop_id,
                                            buyer_nick AS cnick,
                                            order_id,
                                            goods_id,
                                            goods_payment
                                        FROM ft_dwd.order_detail_all
                                        WHERE `day` <= toYYYYMMDD(addDays(toDate('{{ds}}'), { { cycle } } - 1))
                                            AND `day` >= toYYYYMMDD(toDate('{{ds}}'))
                                            AND shop_id = '{{shop_id}}'
                                            AND status IN ('shipped')
                                            AND goods_id GLOBAL NOT IN (
                                                SELECT goods_id
                                                FROM ft_dim.goods_info_all
                                                WHERE `type` IN (
                                                        '1',
                                                        '2',
                                                        '3',
                                                        '4'
                                                    )
                                            )
                                    ) AS order_info
                                    LEFT JOIN (
                                        SELECT DISTINCT sku_id
                                        FROM ft_dim.main_goods_info_all
                                        WHERE product_class IN ('油烟机', '灶具')
                                    ) AS sku_info ON order_info.goods_id = sku_info.sku_id
                            )
                            JOIN (
                                SELECT DISTINCT `day`,
                                    order_id,
                                    snick
                                FROM ft_dwd.ask_order_cov_detail_all
                                WHERE `day` = toYYYYMMDD(toDate('{{ds}}'))
                                    AND cycle = { { cycle } }
                                    AND paid_time != ''
                                    AND shop_id = '{{shop_id}}'
                            ) USING order_id
                    )
                GROUP BY shop_id,
                    `day`,
                    snick,
                    cnick
                having tags_cnt > 1
            )
        group by shop_id,
            `day`,
            snick
    ) ARRAY
    JOIN ['{{cycle}}_out_of_stock_collocation_amt', '{{cycle}}_out_of_stock_paid_collocation_cnt'] AS stat_label,
    [toString(collocation_amt), toString(collocation_cnt)] AS stat_value;