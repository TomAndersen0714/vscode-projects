-- 新实时告警-店铺告警-等级分布
SELECT
    CASE
        WHEN level=1 THEN '初级告警'
        WHEN level=2 THEN '中级告警'
        WHEN level=3 THEN '高级告警'
        ELSE '其他'
    END AS alert,
    count(1) AS alert_cnt
FROM (
    SELECT
        id,
        if(level>3, 4, level) AS level
    FROM xqc_ods.alert_all FINAL
    WHERE day BETWEEN toYYYYMMDD(toDate('{{ day.start=today }}')) 
        AND toYYYYMMDD(toDate('{{ day.end=today }}'))
        -- 过滤旧版标准
        AND level IN [1,2,3]
        -- 已订阅店铺
        AND shop_id GLOBAL IN (
            SELECT tenant_id AS shop_id
            FROM xqc_dim.company_tenant
            WHERE company_id = '{{ company_id=5f73e9c1684bf70001413636 }}'
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
) AS alert_info
group by level
order by level DESC, alert_cnt desc