-- BG实时概况(会话总量+告警分布+日环比+中高级告警比例+告警完结率)
-- PS: 对于BG部门新增/更名的问题,由于所有统计都是下钻到了店铺维度,因此即使BG变更,其环比计算依旧不受影响
WITH 
( SELECT toYYYYMMDD(today()) ) AS today,
( SELECT toYYYYMMDD(yesterday()) ) AS yesterday
SELECT -- BG部门维度聚合统计
    bg_name,
    sum(snick_today_dialog_cnt) AS bg_today_dialog_cnt, -- BG当天当前的会话总量
    sum(snick_yesterday_dialog_cnt) AS bg_yesterday_dialog_cnt, -- BG昨天同时刻的会话总量
    if(
        bg_yesterday_dialog_cnt!=0, round(sum(diff_dialog_cnt)/bg_yesterday_dialog_cnt*100,2), 0.00
    ) AS bg_dialog_relative_ratio, -- BG会话总量日环比
    sum(snick_today_level_1_cnt) AS bg_today_level_1_cnt, -- BG当天当前初级告警总量 -- BG告警分布
    sum(snick_today_level_2_cnt) AS bg_today_level_2_cnt, -- BG当天当前中级告警总量 -- BG告警分布
    sum(snick_today_level_3_cnt) AS bg_today_level_3_cnt, -- BG当天当前高级告警总量 -- BG告警分布
    (bg_today_level_1_cnt+bg_today_level_2_cnt+bg_today_level_3_cnt) AS bg_today_warning_cnt, -- BG当天当前告警总量
    if(
        bg_today_dialog_cnt!=0, round((bg_today_level_2_cnt+bg_today_level_3_cnt)/bg_today_dialog_cnt*100,2), 0.00
    ) AS bg_level_2_3_ratio, -- BG当天当前中高级告警比例
    sum(snick_today_level_2_finished_cnt) AS bg_today_level_2_finished_cnt, -- BG当天当前已完结中级告警总量
    sum(snick_today_level_3_finished_cnt) AS bg_today_level_3_finished_cnt, -- BG当天当前已完结高级告警总量
    if(
        bg_today_level_2_cnt!=0, round(bg_today_level_2_finished_cnt/bg_today_level_2_cnt*100,2), 0.00
    ) AS bg_level_2_finished_ratio, -- BG当天当前中级告警完结率
    if(
        bg_today_level_3_cnt!=0, round(bg_today_level_3_finished_cnt/bg_today_level_3_cnt*100,2), 0.00
    ) AS bg_level_3_finished_ratio -- BG当天当前高级告警完结率
FROM (

    -- bg_name--shop_id
    SELECT bg_name, shop_id
    FROM (
        -- bg_name--bg_id
        SELECT 
            department_name AS bg_name,
            department_id AS bg_id
        FROM xqc_dim.group_all
        WHERE company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
        AND level = 1
        AND is_shop = 'False'
    )
    GLOBAL LEFT JOIN (
        -- bg_id--shop_id
        SELECT DISTINCT
            parent_department_path[1] AS bg_id,
            department_id AS shop_id
        FROM xqc_dim.group_all
        WHERE company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
        AND is_shop = 'True'
    )
    USING bg_id

) AS bg_shop

GLOBAL LEFT JOIN (

    -- 子账号维度聚合统计
    -- shop_id--snick--statistic
    SELECT *
    FROM (
        -- 子账号维度今天和昨天会话数据聚合统计
        -- shop_id--snick--statistic
        SELECT
            shop_id,
            snick,
            sum(day = yesterday AND `time`<=toString(now())) AS snick_yesterday_dialog_cnt, -- 子账号昨天同时刻会话总量
            sum(day = today) AS snick_today_dialog_cnt, -- 子账号当天当前会话总量
            (snick_today_dialog_cnt - snick_yesterday_dialog_cnt) AS diff_dialog_cnt -- 子账号当天和昨天同时刻会话总量差值(后续上卷聚合)
        FROM xqc_ods.dialog_all
        WHERE day BETWEEN yesterday AND today
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
        GROUP BY shop_id, snick
    ) AS snick_dialog_stat -- 各个子账号两天内的会话统计
    GLOBAL LEFT JOIN (
        -- 子账号维度当天告警数据聚合统计
        -- shop_id--snick--statistic
        SELECT
            shop_id,
            snick,
            count(1) AS snick_today_warning_cnt, -- 子账号当天当前的告警总量
            sum(`level` = 1) AS snick_today_level_1_cnt, -- 子账号当天初级告警量
            sum(`level` = 2) AS snick_today_level_2_cnt, -- 子账号当天中级告警量
            sum(`level` = 3) AS snick_today_level_3_cnt, -- 子账号当天高级告警量
            sum(`level` = 2 AND is_finished = 'True') 
                AS snick_today_level_2_finished_cnt, -- 子账号当天中级已处理告警量
            sum(`level` = 3 AND is_finished = 'True') 
                AS snick_today_level_3_finished_cnt -- 子账号当天高级已处理告警量
        FROM xqc_ods.alert_all
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
        -- 筛选新版本告警
        AND `level` IN [1,2,3]
        GROUP BY shop_id, snick
    ) AS snick_warning_stat -- 各个子账号当天当前的告警统计
    USING shop_id, snick

) AS shop_snick_stat
USING shop_id
GROUP BY bg_name
ORDER BY bg_name ASC
LIMIT 8 -- 因为前端UI长度限制,在查询时写死限制最多8条记录