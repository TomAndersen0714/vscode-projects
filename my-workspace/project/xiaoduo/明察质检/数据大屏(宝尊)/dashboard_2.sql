-- 集团实时概况(会话总量+日环比量)
-- 日环比量:分钟级别
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
    ) -- 当天会话总量日环比(秒级)


-- BG实时概况(监控量+日环比+中高级告警比例+中高级告警完结率)
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
        SELECT -- 子账号维度今天和昨天会话数据聚合统计
            snick,
            sum(day = {{day.start=yesterday}} AND `time`<=now()) AS shop_yesterday_dialog_cnt, -- 子账号昨天同时刻会话总量
            sum(day = {{day.end=today}}) AS shop_today_dialog_cnt, -- 子账号当天当前会话总量
            (shop_today_dialog_cnt - shop_yesterday_dialog_cnt) AS diff_dialog_cnt -- 子账号当天和昨天同时刻会话总量差值
        FROM xqc_ods.dialog_all
        WHERE day BETWEEN {{day.start=yesterday}} AND {{day.start=today}}
        AND snick GLOBAL IN (
            SELECT snick 
            FROM xqc_ods.baozun_shop_snick_all
        )
        GROUP BY snick
    ) AS shop_dialog_stat -- 各个子账号昨天同时刻的会话总量
    GLOBAL LEFT JOIN (
        SELECT  -- 子账号维度当天告警数据聚合统计
            snick,
            count(1) AS shop_today_warning_cnt, -- 子账号当天当前的告警总量
            sum(`level` = 1) AS shop_today_level_1_cnt, -- 子账号当天初级告警量
            sum(`level` = 2) AS shop_today_level_2_cnt, -- 子账号当天中级告警量
            sum(`level` = 3) AS shop_today_level_3_cnt, -- 子账号当天高级告警量
            sum(`level` = 2 AND is_finish = "True") AS shop_today_level_2_finished_cnt, -- 子账号当天中级已处理告警量
            sum(`level` = 3 AND is_finish = "True") AS shop_today_level_3_finished_cnt, -- 子账号当天高级已处理告警量
        FROM xqc_ods.event_alert_all FINAL
        WHERE day = {{day.end=today}}
            AND snick GLOBAL IN (
                SELECT snick 
                FROM xqc_ods.baozun_shop_snick_all
            )
        GROUP BY snick
    ) AS snick_warning_stat -- 各个子账号当天当前的会话总量
    USING snick
) AS snick_stat
USING snick
GROUP BY BG
ORDER BY BG ASC

-- 集团中高等级实时预警(集团各等级告警实时分类统计)
SELECT
    `level`, -- 告警等级
    warning_type, -- 告警项描述
    sum(is_finish = "False") AS not_finished_cnt, -- 各告警项未处理总量
    sum(1) AS warning_cnt, -- 各告警项总量
    if(warning_cnt!=0, round((warning_cnt-not_finished_cnt)/warning_cnt*100,2), 0.00) -- 各告警项完结率
FROM xqc_ods.event_alert_all FINAL
WHERE day={{day.end=today}}
AND snick GLOBAL IN (
    SELECT snick
    FROM xqc_ods.baozun_shop_snick_all
)
GROUP BY `level`, warning_type
ORDER BY `level` DESC, warning_type DESC

-- 预警列表
-- 结果字段: BG+BU+店铺+客服负责人+客服+子账号+顾客+告警等级+告警内容+告警时间+告警时长+处理完成时间+处理状态
-- 过滤字段: day, is_finish, warning_type, BG, BU, 告警等级, 店铺, 负责人, 子账号
-- 需要支持
-- PS: 可以使用where if语法, 如: SELECT * FROM tmp.test_tbl_all WHERE if('{{id}}'!=' ', _id='{{id}}', _id!='')

SELECT
    BG,
    BU,
    shop_id,
    shop_name,
    superior_name, -- 客服负责人
    employee_name, -- 客服
    snick, -- 子账号
    cnick, -- 顾客
    dialog_id,
    level, -- 告警等级
    warning_type, -- 告警等级
    time, -- 告警时间
    if(is_finish='True',toString(finish_time - time), '-'), -- 告警时长
    finish_time, -- 告警结束时间
    is_finish -- 是否完结
FROM
    xqc_ods.event_alert_all
LEFT JOIN (
    SELECT * FROM (
        SELECT BG, BU, shop_id, shop_name, snick
        FROM xqc_ods.baozun_shop_snick_all
        WHERE if({{BG}}!='全部',BG = {{BG}}, BG!=''),
            AND if({{BU}}!='全部',BU = {{BU}}, BU!=''),
    )GLOBAL LEFT JOIN(
        SELECT superior_name, employee_name, snick, cnick, dialog_id
        FROM xqc_ods.dialog_all
        WHERE day BETWEEN {{day.start=today}} AND {{day.end=today}}
    )
    USING snick
) AS dim_stat
USING dialog_id
-- Q: 下拉框是否支持多选??? 如果支持多选,则使用IN Operator
WHERE day BETWEEN {{day.start=today}} AND {{day.end=today}}
    AND if({{is_finish}}!='全部',is_finish = {{is_finish}}, is_finish!=''),
    AND if({{warning_type}}!='全部',warning_type = {{warning_type}}, warning_type!=''),
    AND if({{level}}!='全部',level = {{level}}, level!=''),
    AND if({{shop_name}}!='全部',shop_name = {{shop_name}}, shop_name!=''),
    AND if({{superior_name}}!='全部',superior_name = {{superior_name}}, superior_name!=''),
    AND if({{snick}}!='全部',snick IN ({{snick}}), snick IN ({{snick_list}})) -- {{snick}} 必须是权限隔离snick列表{{snick_list}}的子集
-- 

