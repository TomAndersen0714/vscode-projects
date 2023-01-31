-- 店铺*客服*产品粒度-出库量（不含静默）
-- 店铺*客服*产品粒度-付款量（不含静默）
-- 店铺*客服*产品粒度-出库率（出库量 / 付款量）（不含静默）
-- 店铺*客服*产品粒度-出库人数（不含静默）
-- 店铺*客服*产品粒度-付款人数（不含静默）
-- 店铺*客服*产品粒度-出库率（出库人数 / 付款人数）（不含静默）
SELECT
    `day`,
    shop_id,
    '{{platform}}' as platform,
    snick,
    goods_id,
    stat_label,
    stat_value,
    toString(now64(3, 'Asia/Shanghai')) as update_at
FROM (
    select `day`,
        shop_id,
        snick,
        goods_id,
        ask_order_paid_cnt,
        ask_order_paid_uv,
        out_of_stock_cnt,
        out_of_stock_uv,
        round(
            (
                if(
                    isNull(ask_order_paid_cnt)
                    or ask_order_paid_cnt = 0,
                    0,
                    out_of_stock_cnt / ask_order_paid_cnt
                )
            ) * 100,
            2
        ) as out_of_stock_cnt_rat,
        round(
            (
                if(
                    isNull(ask_order_paid_uv)
                    or ask_order_paid_uv = 0,
                    0,
                    out_of_stock_uv / ask_order_paid_uv
                )
            ) * 100,
            2
        ) as out_of_stock_uv_rat
    from (
        select shop_id,
            `day`,
            snick,
            goods_id,
            ask_order_paid_cnt,
            out_of_stock_cnt
        from (
                select shop_id,
                    `day`,
                    snick,
                    goods_id,
                    sum(goods_count) as ask_order_paid_cnt
                from (
                        SELECT shop_id,
                            `day`,
                            snick,
                            goods_id,
                            goods_num,
                            count(1) * goods_num as goods_count
                        FROM (
                            SELECT DISTINCT shop_id,
                                `day`,
                                order_id,
                                goods_id,
                                goods_num
                            FROM ft_dwd.order_detail_all
                            WHERE shop_id = '{{shop_id}}'
                                AND `day` = toYYYYMMDD(subtractDays(toDate('{{ds}}'),{{cycle}} - 1))
                                AND status IN ('paid', 'deposited')
                                AND goods_id GLOBAL NOT IN (
                                    SELECT goods_id
                                    FROM ft_dim.goods_info_all
                                )
                        )
                        LEFT JOIN (
                                    SELECT DISTINCT shop_id,
                                        `day`,
                                        order_id,
                                        snick
                                    FROM ft_dwd.ask_order_cov_detail_all
                                    WHERE shop_id = '{{shop_id}}'
                                        AND `day` = toYYYYMMDD(subtractDays(toDate('{{ds}}'),{{cycle}} - 1))
                                        AND cycle = {{cycle}}
                                        AND paid_time != ''
                        )
                        USING shop_id,
                            `day`,
                            order_id
                        where (
                                    snick != ''
                                    OR snick IS NULL
                                )
                        group by shop_id,
                            `day`,
                            snick,
                            goods_id,
                            goods_num
                )
                group by shop_id,
                    `day`,
                    snick,
                    goods_id
        )
        full outer join (
                select shop_id,
                    `day`,
                    snick,
                    goods_id,
                    sum(goods_count) as out_of_stock_cnt
                from (
                    SELECT shop_id,
                        `day`,
                        snick,
                        goods_id,
                        goods_num,
                        count(1) * goods_num as goods_count
                    FROM (
                            SELECT DISTINCT shop_id,
                                `day`,
                                order_id,
                                goods_id,
                                goods_num
                            FROM ft_dwd.order_detail_all
                            WHERE shop_id = '{{shop_id}}'
                                AND `day` = toYYYYMMDD(subtractDays(toDate('{{ds}}'),{{cycle}} - 1))
                                AND status IN ('shipped')
                                AND goods_id GLOBAL NOT IN (
                                    SELECT goods_id
                                    FROM ft_dim.goods_info_all
                                )
                    )
                    LEFT JOIN (
                            SELECT DISTINCT shop_id,
                                `day`,
                                order_id,
                                snick
                            FROM ft_dwd.ask_order_cov_detail_all
                            WHERE shop_id = '{{shop_id}}'
                                AND `day` = toYYYYMMDD(subtractDays(toDate('{{ds}}'),{{cycle}} - 1))
                                AND cycle = {{cycle}}
                                AND paid_time != ''
                    )
                    USING shop_id,
                        `day`,
                        order_id
                    where (
                            snick != ''
                            OR snick IS NULL
                        )
                    group by shop_id,
                        `day`,
                        snick,
                        goods_id,
                        goods_num
                )
                group by shop_id,
                    `day`,
                    snick,
                    goods_id
        )
        using shop_id,
            `day`,
            snick,
            goods_id
    )
    full outer join (
        select shop_id,
            `day`,
            snick,
            goods_id,
            ask_order_paid_uv,
            out_of_stock_uv
        from (
            select shop_id,
                `day`,
                snick,
                goods_id,
                count(distinct buyer_nick) as ask_order_paid_uv
            from (
                    select shop_id,
                        `day`,
                        buyer_nick,
                        snick,
                        goods_id,
                        goods_num,
                        status_arr
                    from (
                            select shop_id,
                                `day`,
                                order_id,
                                goods_id,
                                goods_num,
                                buyer_nick,
                                groupArray(status) as status_arr
                            from (
                                SELECT distinct shop_id,
                                    `day`,
                                    order_id,
                                    buyer_nick,
                                    goods_id,
                                    goods_num,
                                    status
                                FROM ft_dwd.order_detail_all
                                WHERE shop_id = '{{shop_id}}'
                                    AND `day` = toYYYYMMDD(subtractDays(toDate('{{ds}}'),{{cycle}} - 1))
                            )
                            group by shop_id,
                                `day`,
                                order_id,
                                goods_id,
                                goods_num,
                                buyer_nick
                    )
                    left join (
                            select distinct shop_id,
                                `day`,
                                order_id,
                                snick,
                                goods_id
                            from ft_dwd.ask_order_cov_detail_all
                            WHERE shop_id = '{{shop_id}}'
                                and `day` = toYYYYMMDD(subtractDays(toDate('{{ds}}'),{{cycle}} - 1))
                                and cycle = {{cycle}}
                                and paid_time != ''
                    ) using (shop_id, `day`, order_id)
                    where snick != ''
            )
            where has(status_arr, 'paid')
            group by shop_id,
                `day`,
                snick,
                goods_id
        )
        full outer join (
            select shop_id,
                `day`,
                snick,
                goods_id,
                count(distinct buyer_nick) as out_of_stock_uv
            from (
                select shop_id,
                    `day`,
                    buyer_nick,
                    snick,
                    goods_id,
                    goods_num,
                    status_arr
                from (
                        select shop_id,
                            `day`,
                            order_id,
                            goods_id,
                            goods_num,
                            buyer_nick,
                            groupArray(status) as status_arr
                        from (
                            SELECT distinct shop_id,
                                `day`,
                                order_id,
                                buyer_nick,
                                goods_id,
                                goods_num,
                                status
                            FROM ft_dwd.order_detail_all
                            WHERE shop_id = '{{shop_id}}'
                                AND `day` = toYYYYMMDD(subtractDays(toDate('{{ds}}'),{{cycle}} - 1))
                        )
                        group by shop_id,
                            `day`,
                            order_id,
                            goods_id,
                            goods_num,
                            buyer_nick
                    )
                    left join (
                        select distinct shop_id,
                            `day`,
                            order_id,
                            snick,
                            goods_id
                        from ft_dwd.ask_order_cov_detail_all
                        WHERE shop_id = '{{shop_id}}'
                            and `day` = toYYYYMMDD(subtractDays(toDate('{{ds}}'),{{cycle}} - 1))
                            and cycle = {{cycle}}
                            and paid_time != ''
                    ) using (shop_id, `day`, order_id)
                where snick != ''
            )
            where has(status_arr, 'shipped')
            group by shop_id,
                `day`,
                snick,
                goods_id
        )
        using (shop_id, `day`, snick, goods_id)
    )
    using (shop_id, `day`, snick, goods_id)
)
ARRAY JOIN
    ['{{cycle}}_ask_order_paid_cnt','{{cycle}}_out_of_stock_cnt','{{cycle}}_out_of_stock_cnt_rat',
     '{{cycle}}_ask_order_paid_order_uv','{{cycle}}_out_of_stock_uv','{{cycle}}_out_of_stock_uv_rat'] AS stat_label,
     [ask_order_paid_cnt, out_of_stock_cnt, out_of_stock_cnt_rat,
      ask_order_paid_uv, out_of_stock_uv, out_of_stock_uv_rat] AS stat_value;