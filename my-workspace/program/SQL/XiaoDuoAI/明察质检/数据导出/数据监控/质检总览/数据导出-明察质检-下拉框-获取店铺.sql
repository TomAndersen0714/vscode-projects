-- 数据导出-明察质检-下拉框-获取店铺
SELECT DISTINCT seller_nick
FROM xqc_dim.xqc_shop_all
WHERE day=toYYYYMMDD(yesterday())
AND platform = '{{ platform }}'
AND company_id = '{{ company_id=61602afd297bb79b69c06118 }}'