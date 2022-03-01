-- 接待量-店铺数+子账号数+接待顾客数
SELECT
    COUNT(DISTINCT seller_nick) AS `店铺数`,
    COUNT(DISTINCT snick) AS `子账号数`,
    COUNT(DISTINCT cnick) AS `接待顾客数`
FROM xqc_ods.dialog_all
WHERE day = toYYYYMMDD(today())
-- 已订阅店铺
AND shop_id GLOBAL IN (
    SELECT tenant_id AS shop_id
    FROM xqc_dim.company_tenant
    WHERE company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
    AND platform = '{{ platform=tb }}'
)