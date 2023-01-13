WITH 
( SELECT toYYYYMMDD(today()) ) AS today,
( SELECT toYYYYMMDD(yesterday()) ) AS yesterday
SELECT
    CASE
        WHEN level=1 THEN '初级告警'
        WHEN level=2 THEN '中级告警'
        WHEN level=3 THEN '高级告警'
        ELSE '其他'
    END AS `告警等级`,
    count(1) AS `告警总量`
FROM (
    SELECT
        id,
        if(level>3,4,level) AS level
    FROM xqc_ods.alert_all FINAL
    WHERE day = today
    -- 筛选新版标准
    AND level IN [1,2,3]
    -- 已订阅店铺
    AND shop_id GLOBAL IN (
        SELECT tenant_id AS shop_id
        FROM xqc_dim.company_tenant
        WHERE company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
        AND platform = '{{ platform=tb }}'
    )
    AND platform = '{{ platform=tb }}'
)
group by level
order by `告警等级` DESC, `告警总量` desc