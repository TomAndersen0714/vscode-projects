-- 接待量-店铺数+子账号数+接待顾客数
SELECT
    COUNT(DISTINCT seller_nick) AS `店铺数`,
    COUNT(DISTINCT snick) AS `子账号数`,
    COUNT(DISTINCT cnick) AS `接待顾客数`
FROM xqc_ods.dialog_all
WHERE day = toYYYYMMDD(today())
-- 组织架构包含店铺
AND shop_id GLOBAL IN (
    SELECT department_id AS shop_id
    FROM xqc_dim.group_all
    WHERE company_id = '{{ company_id=61602afd297bb79b69c06118 }}'
    AND is_shop = 'True'
    AND platform = '{{ platform=tb }}'
)

/* -- 已订阅店铺
-- PS: 和组织架构所包含店铺二选一
AND shop_id GLOBAL IN (
    SELECT tenant_id AS shop_id
    FROM xqc_dim.company_tenant
    WHERE company_id = '{{ company_id=61602afd297bb79b69c06118 }}'
    AND platform = '{{ platform=tb }}'
) */

-- 权限隔离
AND (
        shop_id IN splitByChar(',','{{ shop_id_list=615faf72b0c5f1001957c249 }}')
        OR
        snick IN splitByChar(',','{{ snick_list=NULL }}')
    )