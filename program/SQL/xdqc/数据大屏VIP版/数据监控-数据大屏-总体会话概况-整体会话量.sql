-- 总体会话概况-今日会话总量+昨日会话总量+近30天会话总量+日环比+月环比

-- 欧普照明 company_id = 61602afd297bb79b69c06118
-- 欧普照明官方旗舰店 platform = 'tb' AND shop_id = '615faf72b0c5f1001957c249'
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
        WHERE company_id = '{{ company_id=61602afd297bb79b69c06118 }}'
        AND platform = '{{ platform=tb }}'
    )

) AS today_dialog_cnt, -- 当天目前已有会话总量
(
    SELECT COUNT(1)
    FROM xqc_ods.dialog_all
    WHERE day = yesterday

    -- 已订阅店铺
    AND shop_id GLOBAL IN (
        SELECT tenant_id AS shop_id
        FROM xqc_dim.company_tenant
        WHERE company_id = '{{ company_id=61602afd297bb79b69c06118 }}'
        AND platform = '{{ platform=tb }}'
    )
    -- 前一天同时刻(小时)
    AND hour < toHour(now())
) AS yesterday_dialog_cnt -- 昨天同时刻会话总量
SELECT
    today_dialog_cnt AS `今日`,
    yesterday_dialog_cnt AS `昨日`,
    CONCAT(
        toString(
            if(
                yesterday_dialog_cnt != 0, 
                round((today_dialog_cnt - yesterday_dialog_cnt)/yesterday_dialog_cnt*100,2), 
                0.00)
        ),'%'
    )
     AS `日环比`