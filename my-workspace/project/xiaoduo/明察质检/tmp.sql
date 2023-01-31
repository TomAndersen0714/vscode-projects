select shop_id,
    `day`,
    snick,
    goods_id,
    "付款量",
    "出库量",
    "付款人数",
    "出库人数",
    round(
        (
            if(
                isNull("付款量")
                or "付款量" = 0,
                0,
                "出库量" / "付款量"
            )
        ) * 100,
        2
    ) as "出库率（量）",
    round(
        (
            if(
                isNull("付款人数")
                or "付款人数" = 0,
                0,
                "出库人数" / "付款人数"
            )
        ) * 100,
        2
    ) as "出库率（人数）"
from (
        --出库量，付款量
        select shop_id,
            `day`,
            snick,
            goods_id,
            "付款量",
            "出库量"
        from (
                select shop_id,
                    `day`,
                    snick,
                    goods_id,
                    sum(goods_count) as "付款量"
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
                                WHERE shop_id = '5cac112e98ef4100118a9c9f'
                                    AND `day` = 20220909
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
                                WHERE shop_id = '5cac112e98ef4100118a9c9f'
                                    AND `day` >= 2022098
                                    AND `day` <= 20220909
                                    AND cycle = '2'
                                    AND paid_time != ''
                            ) USING shop_id,
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
                    sum(goods_count) as "出库量"
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
                                WHERE shop_id = '5cac112e98ef4100118a9c9f'
                                    AND `day` = 20220909
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
                                WHERE shop_id = '5cac112e98ef4100118a9c9f'
                                    AND `day` >= 2022098
                                    AND `day` <= 20220909
                                    AND cycle = '2'
                                    AND paid_time != ''
                            ) USING shop_id,
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
            ) using shop_id,
            `day`,
            snick,
            goods_id
    )
    full outer join (
        --出库人数/付款人数
        select shop_id,
            `day`,
            snick,
            goods_id,
            "付款人数",
            "出库人数"
        from (
                --付款人数
                select shop_id,
                    `day`,
                    snick,
                    goods_id,
                    count(distinct buyer_nick) as "付款人数"
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
                                        WHERE shop_id = '5cac112e98ef4100118a9c9f'
                                            AND `day` = 20220909
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
                                WHERE shop_id = '5cac112e98ef4100118a9c9f'
                                    and `day` >= 20220908
                                    AND `day` <= 20220909
                                    and cycle = '2'
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
                --出库人数
                select shop_id,
                    `day`,
                    snick,
                    goods_id,
                    count(distinct buyer_nick) as "出库人数"
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
                                        WHERE shop_id = '5cac112e98ef4100118a9c9f'
                                            AND `day` = 20220909
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
                                WHERE shop_id = '5cac112e98ef4100118a9c9f'
                                    and `day` >= 20220908
                                    AND `day` <= 20220909
                                    and cycle = '2'
                                    and paid_time != ''
                            ) using (shop_id, `day`, order_id)
                        where snick != ''
                    )
                where has(status_arr, 'shipped')
                group by shop_id,
                    `day`,
                    snick,
                    goods_id
            ) using (shop_id, `day`, snick, goods_id)
    ) using (shop_id, `day`, snick, goods_id)