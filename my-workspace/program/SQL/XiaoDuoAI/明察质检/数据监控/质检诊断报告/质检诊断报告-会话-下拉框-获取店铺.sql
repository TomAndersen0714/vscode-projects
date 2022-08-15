-- 质检诊断报告-会话-下拉框-获取店铺
SELECT DISTINCT
    seller_nick
FROM xqc_dim.xqc_shop_all
WHERE day=toYYYYMMDD(yesterday())
-- 筛选指定平台
AND platform = 'tb'
-- 筛选指定企业的店铺
AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
ORDER BY seller_nick COLLATE 'zh'