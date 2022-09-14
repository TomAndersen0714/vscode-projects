-- 账单明细-流量开销趋势图(测试)
SELECT
    day,
    company_id,
    SUM(dialog_cnt) AS dialog_sum,
    SUM(cnick_uv) AS cnick_uv_sum
FROM remote('10.22.134.220:29000', xqc_dws.xplat_shop_stat_all)
WHERE day BETWEEN toYYYYMMDD(toDate('{{ day.start=week_ago }}')) 
    AND toYYYYMMDD(toDate('{{ day.end=today }}'))
AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
GROUP BY day, company_id