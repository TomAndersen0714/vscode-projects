-- 店铺会话占比
WITH 
( SELECT toYYYYMMDD(today()) ) AS today,
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
) AS today_dialog_cnt -- 当天目前已有会话总量
SELECT
    seller_nick, -- 店铺名
    COUNT(1) AS shop_today_dialog_cnt, -- 店铺当天会话量
    if(
        today_dialog_cnt != 0, round(shop_today_dialog_cnt/today_dialog_cnt*100,2), 0.00
    ) AS shop_today_dialog_cnt_percent -- 店铺当天会话量占比
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
GROUP BY seller_nick
ORDER BY shop_today_dialog_cnt DESC