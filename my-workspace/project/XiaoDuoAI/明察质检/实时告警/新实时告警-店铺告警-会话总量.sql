-- 新实时告警-店铺告警-会话总量
SELECT
    COUNT(1) AS `会话总量`
FROM
    xqc_ods.dialog_all
WHERE day BETWEEN toYYYYMMDD(toDate('{{ day.start=today }}')) 
    AND toYYYYMMDD(toDate('{{ day.end=today }}'))
    -- 已订阅店铺
    AND shop_id GLOBAL IN (
        SELECT tenant_id AS shop_id
        FROM xqc_dim.company_tenant
        WHERE company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
        AND platform = '{{ platform=tb }}'
    )
    -- 权限隔离
    AND (
        shop_id IN splitByChar(',','{{ shop_id_list=5cac112e98ef4100118a9c9f,60e4192bf7d2f001ca988e52 }}')
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