WITH 
( SELECT toYYYYMMDD(today()) ) AS today,
( SELECT toYYYYMMDD(yesterday()) ) AS yesterday
SELECT
    sum(level=1) AS `初级告警总量`,
    sum(level=1 AND is_finished = 'True') AS `初级告警完结量`,
    if(
        `初级告警总量`!=0,
        round(`初级告警完结量`/`初级告警总量`,4),
        0.00
    ) AS `初级告警完结率`,
    concat(toString(`初级告警完结率`*100),'%') AS `初级告警完结率%`,
    sum(level=2) AS `中级告警总量`,
    sum(level=2 AND is_finished = 'True') AS `中级告警完结量`,
    if(
        `中级告警总量`!=0,
        round(`中级告警完结量`/`中级告警总量`,4),
        0.00
    ) AS `中级告警完结率`,
    concat(toString(`中级告警完结率`*100),'%') AS `中级告警完结率%`,
    sum(level>=3) AS `高级告警总量`,
    sum(level>=3 AND is_finished = 'True') AS `高级告警完结量`,
    if(
        `高级告警总量`!=0,
        round(`高级告警完结量`/`高级告警总量`,4),
        0.00
    ) AS `高级告警完结率`,
    concat(toString(`高级告警完结率`*100),'%') AS `高级告警完结率%`
FROM xqc_ods.alert_all FINAL
WHERE day = today
-- 过滤旧版标准
AND level IN [1,2,3]
-- 已订阅店铺
AND shop_id GLOBAL IN (
    SELECT tenant_id AS shop_id
    FROM xqc_dim.company_tenant
    WHERE company_id = '{{ company_id=61602afd297bb79b69c06118 }}'
    AND platform = '{{ platform=tb }}'
)
AND platform = '{{ platform=tb }}'