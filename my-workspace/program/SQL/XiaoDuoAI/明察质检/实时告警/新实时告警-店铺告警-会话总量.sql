-- 新实时告警-店铺告警-会话总量
SELECT
    COUNT(1) AS `会话总量`
FROM
    xqc_ods.dialog_all
WHERE day BETWEEN toYYYYMMDD(toDate('{{ day.start=today }}')) 
    AND toYYYYMMDD(toDate('{{ day.end=today }}'))
    AND shop_id GLOBAL IN (
    -- 已订阅店铺
    SELECT tenant_id AS shop_id
    FROM xqc_dim.company_tenant
    WHERE company_id = '{{ company_id=5f73e9c1684bf70001413636 }}'
    AND platform = '{{ platform=tb }}'
)
AND (
    -- 权限隔离
        shop_id IN splitByChar(',','{{ shop_id_list=5bfe7a6a89bc4612f16586a5,5e7dbfa6e4f3320016e9b7d1 }}')
        OR
        snick IN splitByChar(',','{{ snick_list=null }}')
    )
    -- 下拉框筛选
AND platform = '{{ platform=tb }}'
AND seller_nick='{{ shop_name=杜可风按 }}'