-- 总体会话概况-今日会话总量+昨日会话总量+近30天会话总量+日环比+月环比

-- 欧普照明 company_id = 61602afd297bb79b69c06118
-- 欧普照明官方旗舰店 platform = 'tb' AND shop_id = '615faf72b0c5f1001957c249'
WITH 
( SELECT toYYYYMMDD(today()) ) AS today,
( SELECT toYYYYMMDD(yesterday()) ) AS yesterday,
( SELECT toYYYYMMDD(yesterday()-30) ) AS 30_days_ago,
( SELECT toYYYYMMDD(yesterday()-60) ) AS 60_days_ago,
(
    SELECT COUNT(1)
    FROM xqc_ods.dialog_all
    WHERE day = today

    -- 组织架构包含店铺
    AND shop_id GLOBAL IN (
        SELECT department_id AS shop_id
        FROM xqc_dim.group_all
        WHERE company_id = '{{ company_id=61602afd297bb79b69c06118 }}'
        AND is_shop = 'True'
        AND platform = '{{ platform=tb }}'
    )
    
    /* -- 已订阅店铺
    -- PS: 和组织架构所包含店铺二选一
    AND shop_id GLOBAL IN (
        SELECT tenant_id AS shop_id
        FROM xqc_dim.company_tenant
        WHERE company_id = '{{ company_id=61602afd297bb79b69c06118 }}'
        AND platform = '{{ platform=tb }}'
    ) */

    -- 权限隔离
    AND (
            shop_id IN splitByChar(',','{{ shop_id_list=615faf72b0c5f1001957c249 }}')
            OR
            snick IN splitByChar(',','{{ snick_list=NULL }}')
        )
) AS today_dialog_cnt, -- 当天目前已有会话总量
(
    SELECT COUNT(1)
    FROM xqc_ods.dialog_all
    WHERE day = yesterday
    -- 组织架构包含店铺
    AND shop_id GLOBAL IN (
        SELECT department_id AS shop_id
        FROM xqc_dim.group_all
        WHERE company_id = '{{ company_id=61602afd297bb79b69c06118 }}'
        AND is_shop = 'True'
        AND platform = '{{ platform=tb }}'
    )
    
    /* -- 已订阅店铺
    -- PS: 和组织架构所包含店铺二选一
    AND shop_id GLOBAL IN (
        SELECT tenant_id AS shop_id
        FROM xqc_dim.company_tenant
        WHERE company_id = '{{ company_id=61602afd297bb79b69c06118 }}'
        AND platform = '{{ platform=tb }}'
    ) */

    -- 权限隔离
    AND (
            shop_id IN splitByChar(',','{{ shop_id_list=615faf72b0c5f1001957c249 }}')
            OR
            snick IN splitByChar(',','{{ snick_list=NULL }}')
        )
    -- 前一天同时刻(小时)
    AND hour < toHour(now())
) AS yesterday_dialog_cnt, -- 昨天同时刻会话总量
(
    SELECT COUNT(1)
    FROM xqc_ods.dialog_all
    WHERE day > 30_days_ago AND day <= yesterday
    -- 组织架构包含店铺
    AND shop_id GLOBAL IN (
        SELECT department_id AS shop_id
        FROM xqc_dim.group_all
        WHERE company_id = '{{ company_id=61602afd297bb79b69c06118 }}'
        AND is_shop = 'True'
        AND platform = '{{ platform=tb }}'
    )
    
    /* -- 已订阅店铺
    -- PS: 和组织架构所包含店铺二选一
    AND shop_id GLOBAL IN (
        SELECT tenant_id AS shop_id
        FROM xqc_dim.company_tenant
        WHERE company_id = '{{ company_id=61602afd297bb79b69c06118 }}'
        AND platform = '{{ platform=tb }}'
    ) */

    -- 权限隔离
    AND (
            shop_id IN splitByChar(',','{{ shop_id_list=615faf72b0c5f1001957c249 }}')
            OR
            snick IN splitByChar(',','{{ snick_list=NULL }}')
        )
) AS 30_days_dialog_cnt, -- 近30日会话总量
(
    SELECT COUNT(1)
    FROM xqc_ods.dialog_all
    WHERE day > 60_days_ago AND day <= 30_days_ago
    -- 组织架构包含店铺
    AND shop_id GLOBAL IN (
        SELECT department_id AS shop_id
        FROM xqc_dim.group_all
        WHERE company_id = '{{ company_id=61602afd297bb79b69c06118 }}'
        AND is_shop = 'True'
        AND platform = '{{ platform=tb }}'
    )
    
    /* -- 已订阅店铺
    -- PS: 和组织架构所包含店铺二选一
    AND shop_id GLOBAL IN (
        SELECT tenant_id AS shop_id
        FROM xqc_dim.company_tenant
        WHERE company_id = '{{ company_id=61602afd297bb79b69c06118 }}'
        AND platform = '{{ platform=tb }}'
    ) */

    -- 权限隔离
    AND (
            shop_id IN splitByChar(',','{{ shop_id_list=615faf72b0c5f1001957c249 }}')
            OR
            snick IN splitByChar(',','{{ snick_list=NULL }}')
        )
) AS previous_30_days_dialog_cnt -- 上一个30天会话总量
SELECT
    today_dialog_cnt AS `今日`,
    yesterday_dialog_cnt AS `昨日`,
    if(
        yesterday_dialog_cnt != 0, round((today_dialog_cnt - yesterday_dialog_cnt)/yesterday_dialog_cnt*100,2), 0.00
    ) AS day_relative_ratio AS `日环比`,
    30_days_dialog_cnt AS `近30天`,
    if(
        previous_30_days_dialog_cnt != 0, round((30_days_dialog_cnt - previous_30_days_dialog_cnt)/previous_30_days_dialog_cnt*100,2), 0.00
    ) AS month_relative_ratio AS `月环比`