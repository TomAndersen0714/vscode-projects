-- 账单明细-流量账单明细
SELECT
    platform,
    shop_id,
    shop_name,
    seller_nick,
    SUM(dialog_cnt) AS dialog_sum,
    SUM(cnick_uv) AS cnick_uv_sum
FROM xqc_dws.xplat_shop_stat_all
WHERE day BETWEEN toYYYYMMDD(toDate('{{ day.start=week_ago }}')) 
    AND toYYYYMMDD(toDate('{{ day.end=today }}'))
AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
GROUP BY platform, shop_id, shop_name, seller_nick


