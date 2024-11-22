-- 新实时告警-店铺告警-获取告警项
SELECT DISTINCT
    warning_type
FROM xqc_ods.alert_all FINAL
WHERE day BETWEEN toYYYYMMDD(toDate('{{ day.start=today }}')) 
    AND toYYYYMMDD(toDate('{{ day.end=today }}'))
    -- 已订阅店铺
    AND shop_id GLOBAL IN (
        SELECT tenant_id AS shop_id
        FROM xqc_dim.company_tenant
        WHERE company_id = '{{ company_id=5f73e9c1684bf70001413636 }}'
        -- 下拉框-平台
        AND platform = '{{ platform=tb }}'
    )
    -- 权限隔离
    AND (
        shop_id IN splitByChar(',','{{ shop_id_list=5bfe7a6a89bc4612f16586a5,5e7dbfa6e4f3320016e9b7d1 }}')
        OR
        snick IN splitByChar(',','{{ snick_list=null }}')
    )
    -- 下拉框-平台
    AND platform = '{{ platform=tb }}'
    -- 下拉框-店铺
    AND (
        '{{ shop_ids }}' = ''
        OR
        shop_id IN splitByChar(',','{{ shop_ids }}')
    )
    -- 下拉框-告警等级
    AND (
        '{{ levels }}' = ''
        OR
        toString(level) IN splitByChar(',','{{ levels }}')
    )
ORDER BY level ASC