-- 集团实时概况(会话总量+日环比量+告警分布+告警比例+告警完结率)
WITH 
( SELECT toYYYYMMDD(today()) ) AS today,
( SELECT toYYYYMMDD(yesterday()) ) AS yesterday,
(
    SELECT COUNT(1)
    FROM xqc_ods.dialog_all
    WHERE day = today
    -- 组织架构包含店铺
    AND shop_id GLOBAL IN (
        SELECT department_id AS shop_id
        FROM xqc_dim.group_all
        WHERE company_id = '{{ company_id=6131e6554524490001fc6825 }}'
        AND is_shop = 'True'
        -- AND platform = '{{ platform=jd }}'
    )
    -- 权限隔离
    AND (
            shop_id IN splitByChar(',','{{ shop_id_list=6139c118e16787000fb8a1cf }}')
            OR
            snick IN splitByChar(',','{{ snick_list=NULL }}')
        )
) AS today_dialog_cnt, -- 当天目前会话总量
(
    SELECT COUNT(1)
    FROM xqc_ods.dialog_all
    WHERE day = yesterday
    -- 组织架构包含店铺
    AND shop_id GLOBAL IN (
        SELECT department_id AS shop_id
        FROM xqc_dim.group_all
        WHERE company_id = '{{ company_id=6131e6554524490001fc6825 }}'
        AND is_shop = 'True'
        -- AND platform = '{{ platform=jd }}'
    )
    -- 权限隔离
    AND (
            shop_id IN splitByChar(',','{{ shop_id_list=6139c118e16787000fb8a1cf }}')
            OR
            snick IN splitByChar(',','{{ snick_list=NULL }}')
        )
    -- 前一天同时刻
    AND hour < toHour(now())
) AS yesterday_dialog_cnt -- 昨天同时刻会话总量
SELECT
    today_dialog_cnt, -- 当天目前会话总量
    if(
        yesterday_dialog_cnt != 0, round((today_dialog_cnt - yesterday_dialog_cnt)/yesterday_dialog_cnt*100,1), 0.0
    ) AS today_relative_ratio, -- 当天会话总量日环比(小时级)
    sum(level=1) AS level_1_cnt, -- 初级告警总量
    sum(level=2) AS level_2_cnt, -- 中级告警总量
    sum(level=3) AS level_3_cnt, -- 高级告警总量
    -- 告警分布
    level_1_cnt+level_2_cnt+level_3_cnt AS warning_sum, -- 告警总量
    if(warning_sum!=0, round(level_1_cnt/warning_sum*100,1), 0.0) AS level_1_proportion, -- 初级告警占比
    if(warning_sum!=0, round(level_2_cnt/warning_sum*100,1), 0.0) AS level_2_proportion, -- 中级告警占比
    if(warning_sum!=0, round(level_3_cnt/warning_sum*100,1), 0.0) AS level_3_proportion, -- 高级告警占比
    if(today_dialog_cnt!=0, round(level_2_cnt/today_dialog_cnt*100,1), 0.0) AS level_2_ratio, -- 中级告警比例
    if(today_dialog_cnt!=0, round(level_3_cnt/today_dialog_cnt*100,1), 0.0) AS level_3_ratio, -- 高级告警比例
    if(level_2_cnt!=0, round(sum(level=2 and is_finished='True')/level_2_cnt*100,1), 0.0) AS level_2_finished_ratio, -- 中级告警完结率
    if(level_3_cnt!=0, round(sum(level=3 and is_finished='True')/level_3_cnt*100,1), 0.0) AS level_3_finished_ratio-- 高级告警完结率
FROM
    xqc_ods.alert_all
WHERE
    day = today
-- 组织架构包含店铺
AND shop_id GLOBAL IN (
        SELECT department_id AS shop_id
        FROM xqc_dim.group_all
        WHERE company_id = '{{ company_id=6131e6554524490001fc6825 }}'
        AND is_shop = 'True'
        -- AND platform = '{{ platform=jd }}'
)
-- 权限隔离
AND (
        shop_id IN splitByChar(',','{{ shop_id_list=6139c118e16787000fb8a1cf }}')
        OR
        snick IN splitByChar(',','{{ snick_list=NULL }}')
    )
-- 筛选新版本告警
AND `level` IN [1,2,3]