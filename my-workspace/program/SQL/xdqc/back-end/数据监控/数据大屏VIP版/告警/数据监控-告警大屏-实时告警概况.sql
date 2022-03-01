WITH 
( SELECT toYYYYMMDD(today()) ) AS today,
( SELECT toYYYYMMDD(yesterday()) ) AS yesterday,
(
    SELECT COUNT(1)
    FROM xqc_ods.dialog_all
    WHERE day = today

    -- 已订阅店铺
    AND shop_id GLOBAL IN (
        SELECT tenant_id AS shop_id
        FROM xqc_dim.company_tenant
        WHERE company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
        AND platform = '{{ platform=tb }}'
    )

) AS today_dialog_cnt, -- 当天目前已有会话总量
(
    SELECT
        COUNT(1) AS `告警总量`
    FROM xqc_ods.alert_all FINAL
    WHERE day = yesterday
    -- 已订阅店铺
    AND shop_id GLOBAL IN (
        SELECT tenant_id AS shop_id
        FROM xqc_dim.company_tenant
        WHERE company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
        AND platform = '{{ platform=tb }}'
    )
    -- 过滤旧版标准
    AND level IN [1,2,3]
    AND platform = '{{ platform=tb }}'
) AS yesterday_alert_cnt -- 昨日告警总量
SELECT
    today_dialog_cnt AS `会话总量`,
    COUNT(1) AS `告警总量`,
    CONCAT(
        toString(if(yesterday_alert_cnt!=0, round(`告警会话量`/yesterday_alert_cnt*100,2), 0.00)),
        '%'
    ) AS `日环比`,
    COUNT(DISTINCT dialog_id) AS `告警会话量`,
    CONCAT(
        toString(if(`会话总量`!=0, round(`告警会话量`/`会话总量`*100,2), 0.00)),
        '%'
    ) AS `告警比例`
FROM xqc_ods.alert_all FINAL
WHERE day = today
-- 已订阅店铺
AND shop_id GLOBAL IN (
    SELECT tenant_id AS shop_id
    FROM xqc_dim.company_tenant
    WHERE company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
    AND platform = '{{ platform=tb }}'
)
-- 过滤旧版标准
AND level IN [1,2,3]

