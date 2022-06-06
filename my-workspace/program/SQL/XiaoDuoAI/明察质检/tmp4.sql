SELECT *
FROM dim.goods_all
WHERE abs(cityHash64(shop_id)) % 48 = 4
AND plat_goods_id IN (
    SELECT plat_goods_id
    FROM (
        SELECT
            plat_goods_id,
            COUNT(1) AS cnt
        FROM dim.goods_all
        WHERE abs(cityHash64(shop_id)) % 48 = 4
        GROUP BY plat_goods_id
        HAVING cnt > 1
    )
)