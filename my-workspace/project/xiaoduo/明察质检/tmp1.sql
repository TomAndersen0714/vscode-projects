SELECT `day`,
    shop_id,
    '{{platform}}' AS platform,
    snick,
    'all' AS goods_id,
    'out_of_stock_amt' AS stat_label,
    toFloat64(sum(goods_payment)) AS stat_value,
    toString(now64(3, 'Asia/Shanghai')) as update_at
FROM (
    SELECT *
    FROM (
        SELECT shop_id,
            toYYYYMMDD(toDate('{{ds}}')) AS `day`,
            buyer_nick as cnick,
            order_id,
            goods_id,
            goods_payment,
            goods_num
        FROM ft_dwd.order_detail_all
        WHERE `day` <= toYYYYMMDD(addDays(toDate('{{ds}}'), {{cycle}} - 1))
            AND `day` >= toYYYYMMDD(toDate('{{ds}}'))
            AND shop_id = '{{shop_id}}'
            AND status IN ('shipped')
    )
    JOIN (
        SELECT DISTINCT order_id,
            snick
        FROM ft_dwd.ask_order_cov_detail_all
        WHERE `day` = toYYYYMMDD(toDate('{{ds}}'))
            AND cycle = {{cycle}}
            AND paid_time != ''
            AND shop_id = '{{shop_id}}'
    )
    USING order_id
)
GROUP BY shop_id,
    `day`,
    snick