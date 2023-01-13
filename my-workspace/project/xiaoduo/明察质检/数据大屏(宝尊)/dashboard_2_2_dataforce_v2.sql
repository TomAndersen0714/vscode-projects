-- 数据大屏二级监控
集团组织架构表(xqc_dim.group_all)
实时告警表(xqc_ods.alert_all)
会话记录表(xqc_ods.dialog_all)

-- 测试时: 将 toYYYYMMDD(today()) 替换为 20210911, toYYYYMMDD(yesterday()) 替换为 20210910
-- toYYYYMMDD(today()-30) 替换为 20210811
-- 上线时: 再进行逆替换

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
        yesterday_dialog_cnt != 0, round((today_dialog_cnt - yesterday_dialog_cnt)/yesterday_dialog_cnt*100,2), 0.00
    ) AS today_relative_ratio -- 当天会话总量日环比(秒级)


-- 集团中高等级实时告警(集团各等级告警实时分类统计)
WITH ( SELECT toYYYYMMDD(today()) ) AS today
SELECT
    `level`, -- 告警等级
    warning_type, -- 告警项描述
    sum(is_finished = 'False') AS not_finished_cnt, -- 各告警项未处理总量
    sum(1) AS warning_cnt, -- 各告警项总量
    if(warning_cnt!=0, round((warning_cnt-not_finished_cnt)/warning_cnt*100,2), 0.00) AS warning_finished_ratio-- 各告警项完结率
FROM xqc_ods.alert_all FINAL
WHERE day=today
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
GROUP BY `level`, warning_type
ORDER BY `level` DESC, warning_type DESC


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
        FROM xqc_ods.alert_all FINAL
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


-- 预警列表(实时查询+导出)
-- !!!!未修改
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
    FROM xqc_ods.alert_all FINAL
    WHERE day BETWEEN 20210914 AND 20210915
) AS event_alert
LEFT JOIN (
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
        )
        GLOBAL LEFT JOIN(
            SELECT 
                mp_shop_id AS shop_id, 
                snick
            FROM xqc_dim.snick_all
            WHERE company_id = '5f747ba42c90fd0001254404'
        ) AS shop_snick
        USING shop_id
    )
    GLOBAL LEFT JOIN(
        SELECT superior_name, employee_name, snick, cnick, id AS dialog_id
        FROM xqc_ods.dialog_all
        WHERE day BETWEEN 20210914 AND 20210915
    )
    USING snick
) AS dim_stat
USING dialog_id
ORDER BY snick DESC

-- 预警列表(实时查询+导出)
-- !!!!测试
-- 测试参数
SELECT 
    BG,
    BU,
    shop_name,
    superior_name,
    employee_name,
    snick,
    cnick,
    dialog_id,
    message_id,
    alert_info.id AS alert_id,
    level,
    warning_type,
    time AS warning_time
    toInt64(
       if(
           is_finished='True',
           round((parseDateTimeBestEffort(if(finish_time!='',finish_time,toString(now()))) - parseDateTimeBestEffort(time))/60),
           round((now() - parseDateTimeBestEffort(time))/60)
        )
    ) AS warning_duration,
    finish_time,
    is_finished
FROM
(
    SELECT
        bg_info.department_name AS BG,
        bu_info.department_name AS BU,
        shop_info.department_id AS shop_id,
        shop_info.department_name AS shop_name
    FROM xqc_dim.group_all AS shop_info 
    GLOBAL LEFT JOIN
    (
        SELECT
            department_id,
            department_name
        FROM xqc_dim.group_all
    ) AS bg_info
    ON parent_department_path[1] = bg_info.department_id 
    GLOBAL LEFT JOIN
    (
        SELECT  
            department_id,
            department_name
        FROM xqc_dim.group_all
    ) AS bu_info
    ON parent_department_path[2] = bu_info.department_id
    WHERE company_id = '6131e6554524490001fc6825'
    AND is_shop = 'True'
    -- 下拉框筛选
    AND if('{{ BG=全部 }}'!='全部',parent_department_path[1]='{{BG=全部}}', parent_department_path[1] !='')
    AND if('{{ BU=全部 }}'!='全部',parent_department_path[2]='{{BU=全部}}', parent_department_path[2] !='')
    AND if('{{ shop_name=全部 }}'!='全部',shop_name='{{shop_name=全部}}', shop_name !='')
) 
GLOBAL INNER JOIN (
    SELECT  *
    FROM xqc_ods.alert_all FINAL
    -- 时间筛选框
    WHERE day BETWEEN {{day.start=today}} AND {{day.end=today}}
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
    -- 下拉框筛选
    AND if('{{ level=全部 }}'!='全部',level = {{level}}, level >=1)
    AND if('{{ warning_type=全部 }}'!='全部',warning_type = '{{warning_type}}', warning_type !='')
    AND if('{{ is_finished=全部 }}'!='全部',is_finished = '{{ is_finished }}', is_finished!='')
) AS alert_info 
USING shop_id
ORDER BY time DESC
LIMIT 20 OFFSET 0


SELECT 
    BG,
    BU,
    shop_name,
    superior_name,
    employee_name,
    snick,
    cnick,
    dialog_id,
    message_id,
    alert_info.id AS alert_id,
    level,
    warning_type,
    time,
    time as warning_time,
    toInt64(
        if(
            is_finished = 'True',
            round(
                (
                    parseDateTimeBestEffort(
                        if(finish_time != '', finish_time, toString(now()))
                    ) - parseDateTimeBestEffort(time)
                ) / 60
            ),
            round((now() - parseDateTimeBestEffort(time)) / 60)
        )
    ) AS warning_duration,
    finish_time,
    is_finished
FROM (
        SELECT
            bg_info.department_name AS BG,
            '' AS BU,
            -- bu_info.department_name AS BU,
            shop_info.department_id AS shop_id,
            shop_info.department_name AS shop_name
        FROM xqc_dim.group_all AS shop_info
        GLOBAL LEFT JOIN (
            SELECT
                department_id,
                department_name
            FROM xqc_dim.group_all
        ) AS bg_info 
        ON shop_info.parent_department_path [1] = bg_info.department_id 
        -- GLOBAL LEFT JOIN (
        --     SELECT department_id,
        --         department_name
        --     FROM xqc_dim.group_all
        -- ) AS bu_info
        -- ON shop_info.parent_department_path [2] = bu_info.department_id
        WHERE company_id = '6131e6554524490001fc6825'
        AND is_shop = 'True'
        AND parent_department_path [1] != ''
        AND parent_department_path [2] != ''
    ) 
    GLOBAL INNER JOIN(
        SELECT *
        FROM xqc_ods.alert_all FINAL
        WHERE day BETWEEN 20211102 AND 20211102
            AND (
                shop_id IN splitByChar(',','{{ shop_id_list }}')
            )
            AND is_finished != ''
            AND level = {{level}}
            AND warning_type != ''
    ) AS alert_info 
    USING shop_id
ORDER BY time DESC
LIMIT 20 OFFSET 0