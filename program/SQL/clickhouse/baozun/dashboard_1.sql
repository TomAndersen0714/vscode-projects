-- 数据大屏一级监控
宝尊集团组织架构表(xqc_ods.baozun_shop_all)
实时告警表(xqc_ods.event_alert_all)
会话记录表(xqc_ods.dialog_all)

-- 集团实时概况(分时监控量)
SELECT day,
    hour,
    COUNT(1) -- 分时监控量
FROM xqc_ods.dialog_all
WHERE
    day BETWEEN {{day.start=yesterday}} AND {{day.end=today}}
    AND shop_id IN (
        SELECT shop_id
        FROM xqc_ods.baozun_shop_all
    )
GROUP BY day, hour

-- 集团实时概况(会话总量+日环比量)
-- 日环比量:分钟级别
/* WITH (
    SELECT COUNT(1)
    FROM xqc_ods.dialog_all
    WHERE day = {{day.end=today}}
    AND shop_id IN (
        SELECT shop_id
        FROM xqc_ods.baozun_shop_all
    )
) AS today_dialog_cnt, -- 当天目前会话总量
SELECT
    COUNT(1) AS yesterday_dialog_cnt, -- 昨天同时刻会话总量
    if(yesterday_dialog_cnt!=0, (today_dialog_cnt - yesterday_dialog_cnt)/yesterday_dialog_cnt, 0.0) -- 会话总量日环比(分钟级)
FROM xqc_ods.dialog_all
WHERE day = {{day.end=yesterday}}
AND shop_id IN (
    SELECT shop_id
    FROM xqc_ods.baozun_shop_all
)
AND `time`<=toString(now()) */

-- 集团实时概况(会话总量+日环比量)
-- 集团告警分布+告警比例+告警完结率(时间范围:当天, 实时更新频率:分钟级别)
WITH (
    SELECT COUNT(1)
    FROM xqc_ods.dialog_all
    WHERE day = {{day.end=today}}
    AND shop_id IN (
        SELECT shop_id
        FROM xqc_ods.baozun_shop_all
    )
) AS today_dialog_cnt, -- 当天目前会话总量
(
    SELECT COUNT(1)
    FROM xqc_ods.dialog_all
    WHERE day = {{day.end=yesterday}}
    AND shop_id IN (
        SELECT shop_id
        FROM xqc_ods.baozun_shop_all
    )
    AND `time`<=toString(now())
) AS yesterday_dialog_cnt -- 昨天同时刻会话总量
SELECT
    today_dialog_cnt, -- 当天目前会话总量
    if(
        yesterday_dialog_cnt != 0, (today_dialog_cnt - yesterday_dialog_cnt)/yesterday_dialog_cnt, 0.0
    ), -- 当天会话总量日环比(秒级)
    sum(level=1) AS level_1_cnt, -- 初级告警总量
    sum(level=2) AS level_2_cnt, -- 中级告警总量
    sum(level=3) AS level_3_cnt, -- 高级告警总量
    level_1_cnt+level_2_cnt+level_3_cnt AS warning_sum, -- 告警总量
    if(warning_sum!=0, level_1_cnt/warning_sum*100, 0.0), -- 初级告警占比
    if(warning_sum!=0, level_2_cnt/warning_sum*100, 0.0), -- 中级告警占比
    if(warning_sum!=0, level_3_cnt/warning_sum*100, 0.0), -- 高级告警占比
    if(today_dialog_cnt!=0, level_2_cnt/today_dialog_cnt*100, 0.0), -- 中级告警比例
    if(today_dialog_cnt!=0, level_3_cnt/today_dialog_cnt*100, 0.0), -- 高级告警比例
    if(level_2_cnt!=0, sum(level=2 and is_finished)/level_2_cnt*100, 0.0), -- 中级告警完结率
    if(level_3_cnt!=0, sum(level=3 and is_finished)/level_3_cnt*100, 0.0) -- 高级告警完结率
FROM
    xqc_ods.event_alert_all FINAL
WHERE
    day = {{day.end=today}}
    AND shop_id IN (
        SELECT shop_id
        FROM xqc_ods.baozun_shop_all
    )

-- BG告警分布(实时更新:分钟级别)
SELECT
    BG,
    sum(level_1_cnt), -- BG初级告警总量
    sum(level_2_cnt), -- BG中级告警总量
    sum(level_3_cnt)  -- BG高级告警总量
FROM (
    SELECT BG, shop_id
    FROM xqc_ods.baozun_shop_all
) AS bg_shop_map
LEFT JOIN (
    SELECT
        shop_id,
        sum(level=1) AS level_1_cnt, -- 店铺初级告警总量
        sum(level=2) AS level_2_cnt, -- 店铺中级告警总量
        sum(level=3) AS level_3_cnt -- 店铺高级告警总量
    FROM
        xqc_ods.event_alert_all FINAL
    WHERE 
        day = {{day.end=today}}
    AND shop_id IN (
        SELECT shop_id
        FROM xqc_ods.baozun_shop_all
    )
    GROUP BY shop_id
) AS shop_stat
USING shop_id
GROUP BY BG

-- BG实时概况(会话总量+日环比+告警比例+告警完结率)
-- PS: 需要考虑到BG部门新增的问题

SELECT -- BG部门维度聚合统计
    BG,
    sum(shop_today_dialog_cnt) AS bg_today_dialog_cnt, -- BG当天当前的会话总量
    sum(shop_yesterday_dialog_cnt) AS bg_yesterday_dialog_cnt, -- BG昨天当前的会话总量
    if(
        bg_yesterday_dialog_cnt!=0, sum(diff_dialog_cnt)/bg_yesterday_dialog_cnt*100, 0.0
    ), -- BG会话总量日环比
    sum(shop_today_level_2_cnt) AS bg_today_level_2_cnt, -- BG当天当前中级告警总量
    sum(shop_today_level_2_finished_cnt) AS bg_today_level_2_finished_cnt, -- BG当天当前已完结中级告警总量
    sum(shop_today_level_3_cnt) AS bg_today_level_3_cnt, -- BG当天当前高级告警总量
    sum(shop_today_level_3_finished_cnt) AS bg_today_level_3_finished_cnt, -- BG当天当前已完结高级告警总量
    if(
        bg_today_dialog_cnt!=0, (bg_today_level_2_cnt + bg_today_level_3_cnt)/bg_today_dialog_cnt*100, 0.0
    )
    
SELECT -- 店铺维度聚合统计
    *,
    (shop_today_dialog_cnt - shop_yesterday_dialog_cnt) AS diff_dialog_cnt -- 店铺当天和昨天同时刻会话总量之差
FROM (
    SELECT
        shop_id,
        sum(`time`<=now()) AS shop_yesterday_dialog_cnt -- 店铺昨天同时刻会话总量
    FROM xqc_ods.dialog_all
    WHERE day = {{day.start=yesterday}}
    AND shop_id IN (
        SELECT shop_id 
        FROM xqc_ods.baozun_shop_all
    )
    GROUP BY shop_id
) AS shop_yesterday_dialog_stat -- 各个店铺昨天同时刻的会话总量
LEFT JOIN (
    SELECT 
        shop_id,
        count(1) AS shop_today_dialog_cnt, -- 店铺当天当前的会话总量
        sum(`level` = 2) AS shop_today_level_2_cnt, -- 店铺当天中级告警量
        sum(`level` = 3) AS shop_today_level_3_cnt, -- 店铺当天高级告警量
        sum(`level` = 2 AND is_finish = "True") AS shop_today_level_2_finished_cnt, -- 店铺当天中级已处理告警量
        sum(`level` = 3 AND is_finish = "True") AS shop_today_level_3_finished_cnt, -- 店铺当天高级已处理告警量
    FROM xqc_ods.dialog_all
    WHERE day = {{day.end=today}}
        AND shop_id IN (
            SELECT shop_id 
            FROM xqc_ods.baozun_shop_all
        )
    GROUP BY shop_id
) AS shop_today_dialog_stat -- 各个店铺当天当前的会话总量
USING shop_id



-- 集团近30日告警趋势(需要包含当天???)
SELECT
    day,
    dialog_cnt, -- 每日会话总量
    level_2_3_sum, -- 每日告警总量(中高级)
    if(dialog_cnt!=0, level_2_3_sum/dialog_cnt*100, 0.0) -- 每日告警比例
FROM (
    SELECT day,
        COUNT(1) AS dialog_cnt
    FROM xqc_ods.dialog_all
    WHERE day BETWEEN {{day.start=month_ago}} AND {{day.end=yesterday}}
    AND shop_id IN (
        SELECT shop_id
        FROM xqc_ods.baozun_shop_all
    )
    GROUP BY day
) AS dialog_cnt_daily
LEFT JOIN (
    SELECT 
        day,
        sum(level=2) AS level_2_cnt, -- 中级告警
        sum(level=3) AS level_3_cnt, -- 高级告警
        (level_2_cnt + level_3_cnt) AS level_2_3_sum -- 中高级告警总和
    FROM xqc_ods.event_alert_all
    WHERE day BETWEEN {{day.start=month_ago}} AND {{day.end=yesterday}}
    AND shop_id IN (
        SELECT shop_id
        FROM xqc_ods.baozun_shop_all
    )
    GROUP BY day
) AS level_2_3_sum_daily
USING day

-- 集团中高等级实时预警
SELECT
    `level`, -- 告警等级
    warning_type, -- 告警项描述
    sum(is_finish = "False") AS not_finished_cnt, -- 各告警项未处理总量
    sum(1) AS warning_cnt, -- 各告警项总量
    if(warning_cnt!=0, (warning_cnt-not_finished_cnt)/warning_cnt*100, 0.0) -- 各告警项完结率
FROM xqc_ods.event_alert_all FINAL
WHERE day={{day.end=today}}
AND shop_id IN (
    SELECT shop_id
    FROM xqc_ods.baozun_shop_all
)
GROUP BY `level`, warning_type

