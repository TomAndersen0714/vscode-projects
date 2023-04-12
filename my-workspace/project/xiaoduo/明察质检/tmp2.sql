-- 店铺粒度-2+x搭配额-销售
-- 店铺粒度-2+x搭配量-销售
SELECT
    `day`,
    shop_id,
    'tb' AS platform,
    'all' as goods_id,
    stat_label,
    stat_value,
    toString(now64(3, 'Asia/Shanghai')) as update_at
FROM (
    SELECT `day`,
        shop_id,
        sum(payment) AS collocation_amt, --2+x搭配额
        count(distinct buyer_nick) AS collocation_cnt --2+x搭配量
    FROM (
            SELECT shop_id,
                `day`,
                buyer_nick,
                groupArray(order_id) AS order_ids,
                groupArray(goods_id) AS goods_ids,
                arraySum(groupArray(goods_payment)) AS payment,
                length(arrayDistinct(groupArray(tag))) AS tags_cnt
            FROM (
                    SELECT
                        *,
                        if(
                            sku_info.sku_id != '',
                            0,
                            1
                        ) AS tag
                    FROM (
                        SELECT shop_id,
                            `day`,
                            buyer_nick,
                            order_id,
                            goods_id,
                            goods_payment
                        FROM ft_dwd.order_detail_all
                        WHERE `day` = toYYYYMMDD(toDate('2023-04-06'))
                            AND shop_id = '5cac112e98ef4100118a9c9f'
                            AND status IN ('paid', 'deposited')
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
                        SELECT DISTINCT
                            JSONExtractString(other_fields, 'num_iid') AS sku_id
                        FROM ft_dim.main_goods_info_all
                        WHERE platform = 'tb'
                        AND product_class IN ('油烟机', '灶具')
                    ) AS sku_info
                    ON order_info.goods_id = sku_info.sku_id
                )
            GROUP BY shop_id,
                `day`,
                buyer_nick
            having tags_cnt > 1
        )
    group by shop_id,
        `day`
)
ARRAY JOIN
    ['paid_collocation_amt', 'paid_collocation_cnt'] AS stat_label,
    [toString(collocation_amt), toString(collocation_cnt)] AS stat_value;