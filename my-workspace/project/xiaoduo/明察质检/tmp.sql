--客服维度_出库状态
SELECT `day`,
    shop_id,
    '{{platform}}' AS platform,
    snick,
    'all' AS goods_id,
    '{{cycle}}_out_of_stock_buyer_distribution' AS stat_label,
    toString(
        array(groupArray(toInt64(sales_count)), groupArray(toInt64(buyer_distribution)))
    ) AS stat_value,
    toString(now64(3, 'Asia/Shanghai')) AS update_at
FROM (
    SELECT `day`,
        shop_id,
        snick,
        sales_count,
        count(distinct real_buyer_nick) AS buyer_distribution
    FROM (
            SELECT `day`,
                shop_id,
                snick,
                real_buyer_nick,
                sum(goods_num) AS sales_count
            FROM (
                    SELECT DISTINCT order_id,
                        snick,
                        real_buyer_nick,
                        goods_payment,
                        `day`,
                        goods_id,
                        goods_num,
                        shop_id,
                        `day`,
                        platform
                    FROM ft_dwd.ask_order_cov_detail_all
                    WHERE shop_id = '{{shop_id}}'
                        AND `day` = toYYYYMMDD(subtractDays(toDate('{{ds}}'), {{cycle}} - 1))
                        AND cycle = {{cycle}}
                        AND goods_id GLOBAL NOT IN (
                            SELECT DISTINCT goods_id
                            FROM ft_dim.goods_info_all
                            WHERE shop_id = '{{shop_id}}'
                                AND `type` IN (
                                    '1',
                                    '2',
                                    '3',
                                    '4'
                                )
                        )
                        and order_id in (
                            SELECT distinct order_id
                            FROM ft_dwd.order_detail_all
                            where shop_id = '{{shop_id}}'
                                and `day` = toYYYYMMDD(subtractDays(toDate('{{ds}}'), {{cycle}} - 1))
                                and status = 'shipped'
                        )
                )
            GROUP BY `day`,
                shop_id,
                snick,
                real_buyer_nick
        )
    GROUP BY `day`,
        shop_id,
        snick,
        sales_count
    ORDER BY sales_count ASC
)
GROUP BY `day`,
    shop_id,
    snick;