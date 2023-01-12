-- 集团实时概况(会话总量+日环比量)
-- 日环比量:分钟级别
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
        -- AND platform = '{{ platform=jd }}'
    )
    -- 权限隔离
    AND (
            shop_id IN splitByChar(',','{{ shop_id_list=5cac112e98ef4100118a9c9f }}')
            OR
            snick IN splitByChar(',','{{ snick_list=方太官方旗舰店:柚子 }}')
        )
) AS today_dialog_cnt, -- 当天目前会话总量
(
    SELECT COUNT(1)
    FROM xqc_ods.dialog_all
    WHERE day = yesterday
    -- 已订阅店铺
    AND shop_id GLOBAL IN (
        SELECT tenant_id AS shop_id
        FROM xqc_dim.company_tenant
        WHERE company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
        -- AND platform = '{{ platform=jd }}'
    )
    -- 权限隔离
    AND (
            shop_id IN splitByChar(',','{{ shop_id_list=5cac112e98ef4100118a9c9f }}')
            OR
            snick IN splitByChar(',','{{ snick_list=方太官方旗舰店:柚子 }}')
        )
    AND `time`<=toString(now())
) AS yesterday_dialog_cnt -- 昨天同时刻会话总量
SELECT
    today_dialog_cnt, -- 当天目前会话总量
    if(
        yesterday_dialog_cnt != 0, round((today_dialog_cnt - yesterday_dialog_cnt)/yesterday_dialog_cnt*100,1), 0.0
    ) AS today_relative_ratio -- 当天会话总量日环比(秒级)