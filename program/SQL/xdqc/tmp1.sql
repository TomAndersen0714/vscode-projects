WITH 
( SELECT toYYYYMMDD(today()) ) AS today
SELECT
    CONCAT(seller_nick,'  ',toString(level_3_alert_dialog_cnt)) AS `店铺`,
    level_3_alert_cnt AS `告警量`,
    dialog_cnt AS `会话总量`,
    if(
        dialog_cnt!=0,
        round(level_3_alert_dialog_cnt/dialog_cnt,4),
        0.00
    ) AS `高级告警率`,
    concat(toString(round(`高级告警率`*100,2)),'%') AS `高级告警率%`,
    if(
        dialog_cnt!=0,
        round(level_3_alert_finished_cnt/level_3_alert_cnt,4),
        0.00
    ) AS `高级告警完结率`,
    concat(toString(round(`高级告警完结率`*100,2)),'%') AS `高级告警完结率%`
FROM (
    SELECT
        seller_nick,
        COUNT(1) AS dialog_cnt
    FROM xqc_ods.dialog_all
    WHERE day = today
    -- 已订阅店铺
    AND shop_id GLOBAL IN (
        SELECT tenant_id AS shop_id
        FROM xqc_dim.company_tenant
        WHERE company_id = '{{ company_id=61602afd297bb79b69c06118 }}'
        AND platform = '{{ platform=tb }}'
    )
    AND platform = '{{ platform=tb }}'
    GROUP BY seller_nick
)
GLOBAL LEFT JOIN (
    SELECT
        seller_nick,
        COUNT(1) AS level_3_alert_cnt,
        SUM(is_finished = 'True') AS level_3_alert_finished_cnt,
        COUNT(DISTINCT dialog_id) AS level_3_alert_dialog_cnt
    FROM xqc_ods.alert_all FINAL
    WHERE day = today
    -- 高级告警
    AND level = 3
    -- 已订阅店铺
    AND shop_id GLOBAL IN (
        SELECT tenant_id AS shop_id
        FROM xqc_dim.company_tenant
        WHERE company_id = '{{ company_id=61602afd297bb79b69c06118 }}'
        AND platform = '{{ platform=tb }}'
    )
    AND platform = '{{ platform=tb }}'
    GROUP BY seller_nick
)
USING seller_nick
ORDER BY `高级告警率` DESC, `高级告警完结率` DESC
LIMIT 3