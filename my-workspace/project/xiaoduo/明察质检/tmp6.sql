SELECT plat_goods_id AS product_id,
    sum(pv) AS hot,
    cast(sum(paid_uv) * 100 / sum(reception_uv) AS int) AS order_cr
FROM app_mp.presale_day_platform_snick_goods
WHERE DAY BETWEEN 20230320 AND 20230320
    AND snick_oid = '634270d1bda541001722969f'
    AND plat_goods_id IN ('673779868645', '693696488215', '702229182055')
GROUP BY plat_goods_id;
-- trace:810e04669677222824a23e2412a7844d