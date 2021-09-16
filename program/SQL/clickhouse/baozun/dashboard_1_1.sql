-- 数据大屏一级监控
宝尊集团组织架构表(xqc_ods.baozun_shop_snick_all)
实时告警表(xqc_ods.event_alert_all)
会话记录表(xqc_ods.dialog_all)

-- 集团实时概况(分时监控量)
SELECT day,
    hour,
    COUNT(1) -- 分时监控量
FROM xqc_ods.dialog_all
WHERE
    day BETWEEN {{day.start=yesterday}} AND {{day.end=today}}
    AND snick IN ({{snick_list}})
GROUP BY day, hour
ORDER BY day ASC, hour ASC

-- 集团实时概况(会话总量+日环比量)
-- 日环比量:分钟级别
/* WITH (
    SELECT COUNT(1)
    FROM xqc_ods.dialog_all
    WHERE day = {{day.end=today}}
    AND snick IN ({{snick_list}})
) AS today_dialog_cnt, -- 当天目前会话总量
SELECT
    COUNT(1) AS yesterday_dialog_cnt, -- 昨天同时刻会话总量
    if(yesterday_dialog_cnt!=0, round((today_dialog_cnt - yesterday_dialog_cnt)/yesterday_dialog_cnt*100,2), 0.00) -- 会话总量日环比(分钟级)
FROM xqc_ods.dialog_all
WHERE day = {{day.end=yesterday}}
AND snick IN ({{snick_list}})
AND `time`<=toString(now()) */

-- 集团实时概况(会话总量+日环比量+告警分布+告警比例+告警完结率)
WITH (
    SELECT COUNT(1)
    FROM xqc_ods.dialog_all
    WHERE day = {{day.end=today}}
    AND snick IN ({{snick_list}})
) AS today_dialog_cnt, -- 当天目前会话总量
(
    SELECT COUNT(1)
    FROM xqc_ods.dialog_all
    WHERE day = {{day.end=yesterday}}
    AND snick IN ({{snick_list}})
    AND `time`<=toString(now())
) AS yesterday_dialog_cnt -- 昨天同时刻会话总量
SELECT
    today_dialog_cnt, -- 当天目前会话总量
    if(
        yesterday_dialog_cnt != 0, round((today_dialog_cnt - yesterday_dialog_cnt)/yesterday_dialog_cnt*100,2), 0.00
    ), -- 当天会话总量日环比(秒级)
    sum(level=1) AS level_1_cnt, -- 初级告警总量
    sum(level=2) AS level_2_cnt, -- 中级告警总量
    sum(level=3) AS level_3_cnt, -- 高级告警总量
    -- 告警分布
    level_1_cnt+level_2_cnt+level_3_cnt AS warning_sum, -- 告警总量
    if(warning_sum!=0, round(level_1_cnt/warning_sum*100,2), 0.00), -- 初级告警占比
    if(warning_sum!=0, round(level_2_cnt/warning_sum*100,2), 0.00), -- 中级告警占比
    if(warning_sum!=0, round(level_3_cnt/warning_sum*100,2), 0.00), -- 高级告警占比
    if(today_dialog_cnt!=0, round(level_2_cnt/today_dialog_cnt*100,2), 0.00), -- 中级告警比例
    if(today_dialog_cnt!=0, round(level_3_cnt/today_dialog_cnt*100,2), 0.00), -- 高级告警比例
    if(level_2_cnt!=0, round(sum(level=2 and is_finish)/level_2_cnt*100,2), 0.00), -- 中级告警完结率
    if(level_3_cnt!=0, round(sum(level=3 and is_finish)/level_3_cnt*100,2), 0.00) -- 高级告警完结率
FROM
    xqc_ods.event_alert_all FINAL
WHERE
    day = {{day.end=today}}
    AND dialog_id GLOBAL IN (
        SELECT id
        FROM xqc_ods.dialog_all
        WHERE snick IN ({{snick_list}})
    )

-- 集团中高等级实时预警(集团各等级告警实时分类统计)
SELECT
    `level`, -- 告警等级
    warning_type, -- 告警项描述
    sum(is_finish = "False") AS not_finished_cnt, -- 各告警项未处理总量
    sum(1) AS warning_cnt, -- 各告警项总量
    if(warning_cnt!=0, round((warning_cnt-not_finished_cnt)/warning_cnt*100,2), 0.00) -- 各告警项完结率
FROM xqc_ods.event_alert_all FINAL
WHERE day={{day.end=today}}
AND dialog_id GLOBAL IN (
    SELECT id
    FROM xqc_ods.dialog_all
    WHERE snick IN ({{snick_list}})
)
GROUP BY `level`, warning_type
ORDER BY `level` DESC, warning_type DESC

-- 集团近30日告警趋势
SELECT
    day,
    dialog_cnt, -- 每日会话总量
    level_2_3_sum, -- 每日告警总量(中高级)
    if(dialog_cnt!=0, round(level_2_3_sum/dialog_cnt*100,2), 0.00) -- 每日告警比例
FROM (
    SELECT day,
        COUNT(1) AS dialog_cnt
    FROM xqc_ods.dialog_all
    WHERE day BETWEEN {{day.start=month_ago}} AND {{day.end=today}}
    AND snick IN ({{snick_list}})
    GROUP BY day
) AS dialog_cnt_daily
GLOBAL LEFT JOIN (
    SELECT 
        day,
        sum(level=2) AS level_2_cnt, -- 中级告警总量
        sum(level=3) AS level_3_cnt, -- 高级告警总量
        (level_2_cnt + level_3_cnt) AS level_2_3_sum -- 中高级告警总和
    FROM xqc_ods.event_alert_all FINAL
    WHERE day BETWEEN {{day.start=month_ago}} AND {{day.end=yesterday}}
    AND dialog_id GLOBAL IN (
        SELECT id
        FROM xqc_ods.dialog_all
        WHERE snick IN ({{snick_list}})
    )
    GROUP BY day
) AS level_2_3_sum_daily
USING day
ORDER BY day ASC


-- BG实时概况(会话总量+告警分布+日环比+中高级告警比例+告警完结率)
-- PS: 对于BG部门新增/更名的问题,由于所有统计都是下钻到了店铺维度,因此即使BG变更,其环比计算依旧不受影响
SELECT -- BG部门维度聚合统计
    BG,
    sum(shop_today_dialog_cnt) AS bg_today_dialog_cnt, -- BG当天当前的会话总量
    sum(shop_yesterday_dialog_cnt) AS bg_yesterday_dialog_cnt, -- BG昨天同时刻的会话总量
    if(
        bg_yesterday_dialog_cnt!=0, round(sum(diff_dialog_cnt)/bg_yesterday_dialog_cnt*100,2), 0.00
    ), -- BG会话总量日环比
    sum(shop_today_level_1_cnt) AS bg_today_level_1_cnt, -- BG当天当前初级告警总量 -- BG告警分布
    sum(shop_today_level_2_cnt) AS bg_today_level_2_cnt, -- BG当天当前中级告警总量 -- BG告警分布
    sum(shop_today_level_3_cnt) AS bg_today_level_3_cnt, -- BG当天当前高级告警总量 -- BG告警分布
    if(
        bg_today_dialog_cnt!=0, round((bg_today_level_2_cnt+bg_today_level_3_cnt)/bg_today_dialog_cnt*100,2), 0.00
    ), -- BG当天当前中高级告警比例
    sum(shop_today_level_2_finished_cnt) AS bg_today_level_2_finished_cnt, -- BG当天当前已完结中级告警总量
    sum(shop_today_level_3_finished_cnt) AS bg_today_level_3_finished_cnt, -- BG当天当前已完结高级告警总量
    if(
        bg_today_level_2_cnt!=0, round(bg_today_level_2_finished_cnt/bg_today_level_2_cnt*100,2), 0.00
    ), -- BG当天当前中级告警完结率
    if(
        bg_today_level_3_cnt!=0, round(bg_today_level_3_finished_cnt/bg_today_level_3_cnt*100,2), 0.00
    ) -- BG当天当前高级告警完结率
FROM (
    SELECT BG, snick
    FROM xqc_ods.baozun_shop_snick_all
) AS bg_shop_map
GLOBAL LEFT JOIN (
    SELECT -- 子账号维度聚合统计
        *
    FROM (
        SELECT -- 店铺维度今天和昨天会话数据聚合统计
            snick,
            sum(day = {{day.start=yesterday}} AND `time`<=now()) AS shop_yesterday_dialog_cnt, -- 店铺昨天同时刻会话总量
            sum(day = {{day.end=today}}) AS shop_today_dialog_cnt, -- 店铺当天当前会话总量
            (shop_today_dialog_cnt - shop_yesterday_dialog_cnt) AS diff_dialog_cnt -- 店铺当天和昨天同时刻会话总量差值
        FROM xqc_ods.dialog_all
        WHERE day BETWEEN {{day.start=yesterday}} AND {{day.end=today}}
        AND snick GLOBAL IN (
            SELECT snick
            FROM xqc_ods.baozun_shop_snick_all
        )
        GROUP BY snick
    ) AS shop_dialog_stat -- 各个店铺昨天同时刻的会话总量
    GLOBAL LEFT JOIN (
        SELECT  -- 子账号维度当天告警数据聚合统计
            snick,
            count(1) AS shop_today_warning_cnt, -- 店铺当天当前的告警总量
            sum(`level` = 1) AS shop_today_level_1_cnt, -- 店铺当天初级告警量
            sum(`level` = 2) AS shop_today_level_2_cnt, -- 店铺当天中级告警量
            sum(`level` = 3) AS shop_today_level_3_cnt, -- 店铺当天高级告警量
            sum(`level` = 2 AND is_finish = "True") AS shop_today_level_2_finished_cnt, -- 店铺当天中级已处理告警量
            sum(`level` = 3 AND is_finish = "True") AS shop_today_level_3_finished_cnt, -- 店铺当天高级已处理告警量
        FROM xqc_ods.event_alert_all FINAL
        WHERE day = {{day.end=today}}
        AND dialog_id GLOBAL IN (
            SELECT id
            FROM xqc_ods.dialog_all
            WHERE snick IN ({{snick_list}})
        )
        GROUP BY shop_id
    ) AS shop_warning_stat -- 各个店铺当天当前的会话总量
    USING shop_id
) AS shop_stat
USING shop_id
GROUP BY BG
ORDER BY BG ASC
