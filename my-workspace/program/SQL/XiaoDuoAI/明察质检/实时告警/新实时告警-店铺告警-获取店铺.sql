-- 新实时告警-店铺告警-获取店铺
SELECT DISTINCT 
    tenant_label AS shop_name
FROM xqc_dim.company_tenant
    -- 已订阅店铺
WHERE company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
    -- 权限隔离
AND tenant_id IN splitByChar(',','{{ shop_id_list=5cac112e98ef4100118a9c9f,60e4192bf7d2f001ca988e52 }}')
    -- 下拉框筛选
AND platform = '{{ platform=tb }}'
ORDER BY tenant_label ASC