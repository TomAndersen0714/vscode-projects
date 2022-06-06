SELECT g3.plat_goods_id AS plat_goods_id,
    g3.plat_goods_name,
    if(g4.name = '', '空类目', g4.name) AS name
FROM (
        SELECT g1.plat_goods_id AS plat_goods_id,
            g1.plat_goods_name AS plat_goods_name,
            g2.category_id AS category_id
        FROM (
                SELECT _id,
                    plat_goods_id,
                    plat_goods_name
                FROM ods.goods_t_all
                WHERE `day` = toYYYYMMDD(addDays(today(), -1))
                    AND shop_id = '5b7e402c89bc464c71271638'
                    and `status` = 1
                    and plat_goods_id not in (
                        select plat_goods_id
                        from zhl_dim.goods_fliter_all
                        where shop_id = '5b7e402c89bc464c71271638'
                    )
            ) AS g1
            LEFT JOIN (
                SELECT _id,
                    goods_id,
                    category_id
                FROM dim.goods_relationship_all
                WHERE shop_id = '5b7e402c89bc464c71271638'
            ) AS g2 ON g1._id = g2.goods_id
    ) AS g3
    LEFT JOIN (
        SELECT _id,
            name
        FROM dim.goods_classification_category_all
        WHERE shop_id = '5b7e402c89bc464c71271638'
    ) AS g4 ON g3.category_id = g4._id
WHERE name != '空类目'