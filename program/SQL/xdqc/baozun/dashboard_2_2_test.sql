-- 数据大屏二级监控
集团组织架构表(xqc_dim.group_all)
实时告警表(xqc_ods.event_alert_1_all)
会话记录表(xqc_ods.dialog_all)

-- 集团实时概况(会话总量+日环比量)
-- 日环比量:分钟级别
WITH (
    SELECT COUNT(1)
    FROM xqc_ods.dialog_all
    WHERE day = 20210915
    -- AND snick IN ({{ snick_list }})
) AS today_dialog_cnt, -- 当天目前会话总量
(
    SELECT COUNT(1)
    FROM xqc_ods.dialog_all
    WHERE day = 20210915
    -- AND snick IN ({{ snick_list }})
    AND `time`<=toString(now())
) AS yesterday_dialog_cnt -- 昨天同时刻会话总量
SELECT
    today_dialog_cnt, -- 当天目前会话总量
    if(
        yesterday_dialog_cnt != 0, round((today_dialog_cnt - yesterday_dialog_cnt)/yesterday_dialog_cnt*100,2), 0.00
    ) AS today_relative_ratio -- 当天会话总量日环比(秒级)


-- BG实时概况(会话总量+告警分布+日环比+中高级告警比例+告警完结率)
-- PS: 对于BG部门新增/更名的问题,由于所有统计都是下钻到了店铺维度,因此即使BG变更,其环比计算依旧不受影响
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

    -- 获取BG下需要查询的snick
    SELECT bg_name, snick
    FROM (
        -- BG_name--BG_id
        SELECT 
            department_name AS bg_name,
            department_id AS bg_id
        FROM xqc_dim.group_all
        WHERE company_id = '5f747ba42c90fd0001254404'
        AND level = 1
        AND is_shop = 'False'
    )
    GLOBAL LEFT JOIN (
        -- BG_id--snick
        SELECT DISTINCT
            parent_department_path[1] AS bg_id,
            snick
        FROM xqc_dim.group_all
        GLOBAL JOIN (
            SELECT 
                mp_shop_id AS shop_id,
                snick
            FROM xqc_dim.snick_all
            WHERE company_id = '5f747ba42c90fd0001254404'
            -- WHERE snick IN ('{{snicks=snick_list}}') or mp_shop_id IN ('{{shop_ids=shop_id_list}}')
        )
        ON department_id = shop_id
        WHERE company_id = '5f747ba42c90fd0001254404'
        AND is_shop = 'True'
    )
    USING bg_id

) AS bg_snick
GLOBAL LEFT JOIN (

    -- 子账号维度聚合统计
    SELECT *
    FROM (
        SELECT -- 子账号维度今天和昨天会话数据聚合统计
            snick,
            sum(day = 20210914 AND `time`<=toString(now())) AS snick_yesterday_dialog_cnt, -- 子账号昨天同时刻会话总量
            sum(day = 20210915) AS snick_today_dialog_cnt, -- 子账号当天当前会话总量
            (snick_today_dialog_cnt - snick_yesterday_dialog_cnt) AS diff_dialog_cnt -- 子账号当天和昨天同时刻会话总量差值
        FROM xqc_ods.dialog_all
        WHERE day BETWEEN 20210914 AND 20210915
        -- AND snick IN ({{snick_list}})
        GROUP BY snick
    ) AS snick_dialog_stat -- 各个子账号昨天同时刻的会话总量
    GLOBAL LEFT JOIN (
        -- 子账号维度当天告警数据聚合统计
        SELECT
            snick,
            count(1) AS snick_today_warning_cnt, -- 子账号当天当前的告警总量
            sum(`level` = 1) AS snick_today_level_1_cnt, -- 子账号当天初级告警量
            sum(`level` = 2) AS snick_today_level_2_cnt, -- 子账号当天中级告警量
            sum(`level` = 3) AS snick_today_level_3_cnt, -- 子账号当天高级告警量
            sum(`level` = 2 AND is_finished = 'True') 
                AS snick_today_level_2_finished_cnt, -- 子账号当天中级已处理告警量
            sum(`level` = 3 AND is_finished = 'True') 
                AS snick_today_level_3_finished_cnt -- 子账号当天高级已处理告警量
        FROM xqc_ods.event_alert_1_all FINAL
        GLOBAL RIGHT JOIN(
            SELECT id AS dialog_id, snick
            FROM xqc_ods.dialog_all
            WHERE day = 20210915
            -- AND snick IN ({{snick_list}})
        ) AS dialog_snick
        USING dialog_id
        WHERE day = 20210915
        GROUP BY snick
    ) AS snick_warning_stat -- 各个子账号当天当前的告警统计
    USING snick

) AS bg_snick_stat
USING snick
GROUP BY bg_name
ORDER BY bg_name ASC


-- 集团中高等级实时告警(集团各等级告警实时分类统计)
SELECT
    `level`, -- 告警等级
    warning_type, -- 告警项描述
    sum(is_finished = 'False') AS not_finished_cnt, -- 各告警项未处理总量
    sum(1) AS warning_cnt, -- 各告警项总量
    if(warning_cnt!=0, round((warning_cnt-not_finished_cnt)/warning_cnt*100,2), 0.00) AS warning_finished_ratio-- 各告警项完结率
FROM xqc_ods.event_alert_1_all FINAL
WHERE day=20210915
AND dialog_id GLOBAL IN (
    SELECT id
    FROM xqc_ods.dialog_all
    WHERE day = 20210915
    -- AND snick IN ({{ snick_list }})
)
AND level IN (1,2,3)
GROUP BY `level`, warning_type
ORDER BY `level` DESC, warning_type DESC


-- 告警列表
-- 结果字段: BG+BU+店铺+客服负责人+客服+子账号+顾客+告警等级+告警内容+告警时间+告警时长+处理完成时间+处理状态
-- 过滤字段: day, is_finished, warning_type, BG, BU, 告警等级, 店铺, 负责人, 子账号
-- 需要支持
-- PS: 可以使用where if语法, 如: SELECT * FROM tmp.test_tbl_all WHERE if('{{id}}'!=' ', _id='{{id}}', _id!='')
-- 针对xqc_dim.snick_all和xqc_ods.dialog_all的查询, 后续都统一会落入到xqc_ods.event_alert_all表中
SELECT
    BG,
    BU,
    shop_name,
    superior_name, -- 客服负责人
    employee_name, -- 客服
    snick, -- 子账号
    cnick, -- 顾客
    dialog_id,
    level, -- 告警等级
    warning_type, -- 告警等级
    time, -- 告警时间
    if(
        is_finished='True',
        round((parseDateTimeBestEffort(finish_time) - parseDateTimeBestEffort(time))/60),
        round((now() - parseDateTimeBestEffort(time))/60)
    ) AS warning_duration, -- 告警时长
    finish_time, -- 告警结束时间
    is_finished -- 是否完结
FROM (
    SELECT *
    FROM xqc_ods.event_alert_1_all
    WHERE day BETWEEN 20210914 AND 20210915
        -- AND if({{is_finished}}!='全部',is_finished = {{is_finished}}, is_finished!=''),
        -- AND if({{warning_type}}!='全部',warning_type = {{warning_type}}, warning_type!=''),
        -- AND if({{level}}!='全部',level = {{level}}, level!='')
) AS event_alert
GLOBAL LEFT JOIN (
    -- BG, BU, shop_name, snick, superior_name, employee_name, cnick, dialog_id
    SELECT * 
    FROM ( 
        -- BG, BU, shop_name, snick
        SELECT *
        FROM (
            SELECT
                parent_department_path[1] AS BG,
                parent_department_path[2] AS BU,
                department_id AS shop_id,
                department_name AS shop_name
            FROM xqc_dim.group_all
            WHERE company_id = '5f747ba42c90fd0001254404'
            AND is_shop = 'True'
            -- AND if({{BG}}!='全部',parent_department_path[1] = {{BG}}, parent_department_path[1]!='')
            -- AND if({{BU}}!='全部',parent_department_path[2] = {{BU}}, parent_department_path[2]!='')
            -- AND if({{shop_name}}!='全部',shop_name = {{shop_name}}, shop_name!='')
        )
        GLOBAL LEFT JOIN(
            SELECT 
                mp_shop_id AS shop_id, 
                snick
            FROM xqc_dim.snick_all
            WHERE company_id = '5f747ba42c90fd0001254404'
            -- AND if({{snick}}!='全部',snick = {{snick}}, snick IN ({{snick_list}})) -- {{snick}} 必须是权限隔离snick列表{{snick_list}}的子集
        ) AS shop_snick
        USING shop_id
    )
    GLOBAL LEFT JOIN(
        SELECT superior_name, employee_name, snick, cnick, id AS dialog_id
        FROM xqc_ods.dialog_all
        WHERE day BETWEEN 20210914 AND 20210915
        -- AND if({{snick}}!='全部',snick = {{snick}}, snick IN ({{snick_list}})) -- {{snick}} 必须是权限隔离snick列表{{snick_list}}的子集
        -- AND if({{superior_name}}!='全部',superior_name = {{superior_name}}, superior_name!=''),
    )
    USING snick
) AS dim_stat
USING dialog_id
-- 支持按照告警时间,告警时长,处理完成时间排序
-- ORDER BY {{order_by_clause}}
-- LIMIT {{limit_clause}}
