-- 数据大屏一级监控
集团组织架构表(xqc_dim.group_all)
实时告警表(xqc_ods.alert_all)
会话记录表(xqc_ods.dialog_all)

-- 测试时: 将 toYYYYMMDD(today()) 替换为 20210911, toYYYYMMDD(yesterday()) 替换为 20210910
-- toYYYYMMDD(today()-30) 替换为 20210811
-- 上线时: 再进行逆替换

-- 集团实时概况(分时监控量)
WITH 
( SELECT toYYYYMMDD(today()) ) AS today,
( SELECT toYYYYMMDD(yesterday()) ) AS yesterday
SELECT 
    if(day=yesterday,'昨日','今日') AS d,
    hour,
    sum(id!='') AS cnt -- 分时监控量
FROM xqc_ods.dialog_all
GLOBAL RIGHT JOIN (
    SELECT yesterday AS day, arrayJoin(range(0,24,1)) AS hour
    UNION ALL
    SELECT today AS day, arrayJoin(range(0,24,1)) AS hour
)
USING day, hour
WHERE day BETWEEN yesterday AND today
-- 已订阅店铺
AND shop_id GLOBAL IN (
    SELECT tenant_id AS shop_id
    FROM xqc_dim.company_tenant
    WHERE company_id = '{{ company_id=6131e6554524490001fc6825 }}'
    -- AND platform = '{{ platform=jd }}'
)
-- 权限隔离
AND (
        shop_id IN splitByChar(',','{{ shop_id_list=6139c118e16787000fb8a1cf }}')
        OR
        snick IN splitByChar(',','{{ snick_list=NULL }}')
    )
GROUP BY day,hour
ORDER BY day ASC,hour ASC


-- 集团实时概况(分时监控量)(改,昨日的hour固定长度为23,今日的hour根据当前时间范围进行展示)
WITH 
( SELECT toYYYYMMDD(today()) ) AS today,
( SELECT toYYYYMMDD(yesterday()) ) AS yesterday
SELECT
    if(day=yesterday,'昨日','今日') AS d,
    hour,
    cnt
FROM (
    SELECT
        day,
        hour,
        sum(id!='') AS cnt -- 分时监控量
    FROM xqc_ods.dialog_all
    WHERE day BETWEEN yesterday AND today
    -- 已订阅店铺
    AND shop_id GLOBAL IN (
        SELECT tenant_id AS shop_id
        FROM xqc_dim.company_tenant
        WHERE company_id = '{{ company_id=6131e6554524490001fc6825 }}'
        -- AND platform = '{{ platform=jd }}'
    )
    -- 权限隔离
    AND (
            shop_id IN splitByChar(',','{{ shop_id_list=6139c118e16787000fb8a1cf }}')
            OR
            snick IN splitByChar(',','{{ snick_list=NULL }}')
        )
    GROUP BY day,hour
)
GLOBAL RIGHT JOIN (
    SELECT yesterday AS day, arrayJoin(range(0,24,1)) AS hour
    UNION ALL
    SELECT today AS day, arrayJoin(range(0,toHour(now()),1)) AS hour
)
USING day, hour
ORDER BY day ASC,hour ASC


-- 集团实时概况(会话总量+日环比量+告警分布+告警比例+告警完结率)
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
        WHERE company_id = '{{ company_id=6131e6554524490001fc6825 }}'
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
    -- 已订阅店铺
    AND shop_id GLOBAL IN (
        SELECT tenant_id AS shop_id
        FROM xqc_dim.company_tenant
        WHERE company_id = '{{ company_id=6131e6554524490001fc6825 }}'
        -- AND platform = '{{ platform=jd }}'
    )
    -- 权限隔离
    AND (
            shop_id IN splitByChar(',','{{ shop_id_list=6139c118e16787000fb8a1cf }}')
            OR
            snick IN splitByChar(',','{{ snick_list=NULL }}')
        )
    -- 前一天同时刻
    AND `time`<=toString(now())
) AS yesterday_dialog_cnt -- 昨天同时刻会话总量
SELECT
    today_dialog_cnt, -- 当天目前会话总量
    if(
        yesterday_dialog_cnt != 0, round((today_dialog_cnt - yesterday_dialog_cnt)/yesterday_dialog_cnt*100,2), 0.00
    ) AS today_relative_ratio, -- 当天会话总量日环比(秒级)
    sum(level=1) AS level_1_cnt, -- 初级告警总量
    sum(level=2) AS level_2_cnt, -- 中级告警总量
    sum(level=3) AS level_3_cnt, -- 高级告警总量
    -- 告警分布
    level_1_cnt+level_2_cnt+level_3_cnt AS warning_sum, -- 告警总量
    if(warning_sum!=0, round(level_1_cnt/warning_sum*100,2), 0.00) AS level_1_proportion, -- 初级告警占比
    if(warning_sum!=0, round(level_2_cnt/warning_sum*100,2), 0.00) AS level_2_proportion, -- 中级告警占比
    if(warning_sum!=0, round(level_3_cnt/warning_sum*100,2), 0.00) AS level_3_proportion, -- 高级告警占比
    if(today_dialog_cnt!=0, round(level_2_cnt/today_dialog_cnt*100,2), 0.00) AS level_2_ratio, -- 中级告警比例
    if(today_dialog_cnt!=0, round(level_3_cnt/today_dialog_cnt*100,2), 0.00) AS level_3_ratio, -- 高级告警比例
    if(level_2_cnt!=0, round(sum(level=2 and is_finished='True')/level_2_cnt*100,2), 0.00) AS level_2_finished_ratio, -- 中级告警完结率
    if(level_3_cnt!=0, round(sum(level=3 and is_finished='True')/level_3_cnt*100,2), 0.00) AS level_3_finished_ratio-- 高级告警完结率
FROM
    xqc_ods.alert_all
WHERE
    day = today
-- 已订阅店铺
AND shop_id GLOBAL IN (
    SELECT tenant_id AS shop_id
    FROM xqc_dim.company_tenant
    WHERE company_id = '{{ company_id=6131e6554524490001fc6825 }}'
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


-- 集团中高等级实时告警(集团各等级告警实时分类统计)
WITH ( SELECT toYYYYMMDD(today()) ) AS today
SELECT
    `level`, -- 告警等级
    warning_type, -- 告警项描述
    sum(is_finished = 'False') AS not_finished_cnt, -- 各告警项未处理总量
    sum(1) AS warning_cnt, -- 各告警项总量
    if(warning_cnt!=0, round((warning_cnt-not_finished_cnt)/warning_cnt*100,2), 0.00) AS warning_finished_ratio-- 各告警项完结率
FROM xqc_ods.alert_all
WHERE day=today
-- 已订阅店铺
AND shop_id GLOBAL IN (
    SELECT tenant_id AS shop_id
    FROM xqc_dim.company_tenant
    WHERE company_id = '{{ company_id=6131e6554524490001fc6825 }}'
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
GROUP BY `level`, warning_type
ORDER BY `level` DESC, warning_type DESC


-- 集团近30日告警趋势
WITH 
( SELECT toYYYYMMDD(today()) ) AS today,
( SELECT toYYYYMMDD(yesterday()) ) AS yesterday,
( SELECT toYYYYMMDD(today()-30) ) AS month_ago
SELECT
    concat(substr(toString(day),5,2),'/',substr(toString(day),7,2))  as d,
    dialog_cnt, -- 每日会话总量
    level_2_3_sum, -- 每日告警总量(中高级)
    if(dialog_cnt!=0, round(level_2_3_sum/dialog_cnt*100,2), 0.00) AS level_2_3_ratio-- 每日告警比例(中高级)
FROM (
    SELECT day,
        COUNT(1) AS dialog_cnt
    FROM xqc_ods.dialog_all
    WHERE day BETWEEN month_ago AND today
    -- 已订阅店铺
    AND shop_id GLOBAL IN (
        SELECT tenant_id AS shop_id
        FROM xqc_dim.company_tenant
        WHERE company_id = '{{ company_id=6131e6554524490001fc6825 }}'
        -- AND platform = '{{ platform=jd }}'
    )
    -- 权限隔离
    AND (
            shop_id IN splitByChar(',','{{ shop_id_list=6139c118e16787000fb8a1cf }}')
            OR
            snick IN splitByChar(',','{{ snick_list=NULL }}')
        )
    GROUP BY day
) AS dialog_cnt_daily
GLOBAL LEFT JOIN (
    SELECT 
        day,
        sum(level=2) AS level_2_cnt, -- 中级告警总量
        sum(level=3) AS level_3_cnt, -- 高级告警总量
        (level_2_cnt + level_3_cnt) AS level_2_3_sum -- 中高级告警总和
    FROM xqc_ods.alert_all
    WHERE day BETWEEN month_ago AND today
    -- 已订阅店铺
    AND shop_id GLOBAL IN (
        SELECT tenant_id AS shop_id
        FROM xqc_dim.company_tenant
        WHERE company_id = '{{ company_id=6131e6554524490001fc6825 }}'
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
    GROUP BY day
) AS level_2_3_sum_daily
USING day
ORDER BY day ASC


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
        WHERE company_id = '{{ company_id=6131e6554524490001fc6825 }}'
        AND level = 1
        AND is_shop = 'False'
    )
    GLOBAL LEFT JOIN (
        -- bg_id--shop_id
        SELECT DISTINCT
            parent_department_path[1] AS bg_id,
            department_id AS shop_id
        FROM xqc_dim.group_all
        WHERE company_id = '{{ company_id=6131e6554524490001fc6825 }}'
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
            WHERE company_id = '{{ company_id=6131e6554524490001fc6825 }}'
            -- AND platform = '{{ platform=jd }}'
        )
        -- 权限隔离
        AND (
                shop_id IN splitByChar(',','{{ shop_id_list=6139c118e16787000fb8a1cf }}')
                OR
                snick IN splitByChar(',','{{ snick_list=NULL }}')
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
            WHERE company_id = '{{ company_id=6131e6554524490001fc6825 }}'
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
        GROUP BY shop_id, snick
    ) AS snick_warning_stat -- 各个子账号当天当前的告警统计
    USING shop_id, snick

) AS shop_snick_stat
USING shop_id
GROUP BY bg_name
ORDER BY bg_name ASC
LIMIT 8 -- 因为前端UI长度限制,在查询时写死限制8条记录