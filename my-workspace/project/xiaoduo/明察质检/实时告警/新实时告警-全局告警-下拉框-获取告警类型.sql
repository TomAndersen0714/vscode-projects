-- 新实时告警-全局告警-下拉框-获取告警类型
SELECT DISTINCT 
    warning_type as `告警类型`
FROM xqc_ods.alert_all
WHERE day BETWEEN toYYYYMMDD(toDate('{{ day.start=week_ago }}'))
    AND toYYYYMMDD(toDate('{{ day.end=today }}'))
AND shop_id GLOBAL IN (
    -- 已订阅店铺
    SELECT tenant_id AS shop_id
    FROM xqc_dim.company_tenant
    WHERE company_id = '{{ company_id=5f73e9c1684bf70001413636 }}'
)
-- 权限隔离
AND (
        shop_id IN splitByChar(',','{{ shop_id_list=5bfe7a6a89bc4612f16586a5 }}') 
        OR
        snick IN splitByChar(',','{{ snick_list=null }}')
    )
-- 下拉框筛选
AND if({{ level=-1 }}!=-1,level={{ level=-1 }},level IN [1,2,3]) -- 告警等级
ORDER BY level ASC
UNION ALL
SELECT '全部' as `告警类型`