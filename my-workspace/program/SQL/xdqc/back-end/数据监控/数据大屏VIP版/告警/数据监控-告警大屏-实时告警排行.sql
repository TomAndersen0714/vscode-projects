WITH ( SELECT toYYYYMMDD(today()) ) AS today
SELECT
    CASE
        WHEN `level`=1 THEN '初级'
        WHEN `level`=2 THEN '中级'
        WHEN `level`=3 THEN '高级'
    END AS `告警等级`,
    warning_type AS `告警项`,
    count(1) AS `告警数量`,
    sum(is_finished = 'False') AS `未完结数量`,
    concat(
        toString(if(`告警数量`!=0, round((`告警数量`-`未完结数量`)/`告警数量`*100,1), 0.0)),
        '%'
    ) AS `完结率`
FROM xqc_ods.alert_all FINAL
WHERE day=today
-- 已订阅店铺
AND shop_id GLOBAL IN (
    SELECT tenant_id AS shop_id
    FROM xqc_dim.company_tenant
    WHERE company_id = '{{ company_id=61602afd297bb79b69c06118 }}'
    AND platform = '{{ platform=tb }}'
)
AND platform = '{{ platform=tb }}'
-- 筛选新版本告警
AND `level` IN [1,2,3]
GROUP BY `level`, warning_type
ORDER BY `level` DESC, `未完结数量` DESC, warning_type DESC