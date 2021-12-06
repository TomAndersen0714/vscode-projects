-- 店铺会话占比
WITH 
( SELECT toYYYYMMDD(today()) ) AS today,
(
    SELECT COUNT(1)
    FROM xqc_ods.dialog_all
    WHERE day = today

    -- 已订阅店铺
    AND shop_id GLOBAL IN (
        SELECT tenant_id AS shop_id
        FROM xqc_dim.company_tenant
        WHERE company_id = '{{ company_id=61602afd297bb79b69c06118 }}'
        AND platform = '{{ platform=tb }}'
    )
) AS today_dialog_cnt -- 当天目前已有会话总量
SELECT
    seller_nick, -- 店铺名
    COUNT(1) AS shop_today_dialog_cnt, -- 店铺当天会话量
    if(
        today_dialog_cnt != 0, round(shop_today_dialog_cnt/today_dialog_cnt*100,2), 0.00
    ) AS shop_today_dialog_cnt_percent -- 店铺当天会话量占比
FROM xqc_ods.dialog_all
WHERE day = today
-- 已订阅店铺
AND shop_id GLOBAL IN (
    SELECT tenant_id AS shop_id
    FROM xqc_dim.company_tenant
    WHERE company_id = '{{ company_id=61602afd297bb79b69c06118 }}'
    AND platform = '{{ platform=tb }}'
)
GROUP BY seller_nick
ORDER BY shop_today_dialog_cnt DESC