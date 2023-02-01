-- 顾客产出价值衡量
-- 店铺*客服粒度-N件销售人数分布-销售
SELECT `day`,
    shop_id,
    '{{platform}}' AS platform,
    sales_count,
    'buyer_distribution' AS stat_label,
    count(DISTINCT real_buyer_nick) AS stat_value,
    toString(now64(3, 'Asia/Shanghai')) AS update_at
FROM (
        SELECT `day`,
            shop_id,
            real_buyer_nick,
            sum(goods_num) AS sales_count
        FROM ft_dwd.order_detail_all
        WHERE shop_id = '{{shop_id}}'
            AND `day` = { { ds_nodash } }
            AND status IN ('paid', 'deposited') --付定金的商品也算在内
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
        GROUP BY `day`,
            shop_id,
            real_buyer_nick
    )
GROUP BY `day`,
    shop_id,
    platform,
    sales_count