SELECT
    BG, BU,
    shop_name,
    superior_name, -- 客服负责人
    employee_name, -- 客服
    snick, -- 子账号
    cnick, -- 顾客
    dialog_id,
    message_id,
    level, -- 告警等级
    warning_type, -- 告警项
    time, -- 告警时间
    if(
        is_finished='True',
        round((now() - parseDateTimeBestEffort(time))/60),
        --round((parseDateTimeBestEffort(finish_time) - parseDateTimeBestEffort(time))/60),
        round((now() - parseDateTimeBestEffort(time))/60)
    ) AS warning_duration, -- 告警时长(min)
    finish_time, -- 告警结束时间
    is_finished -- 是否完结
FROM (
    -- BG, BU, shop_name, shop_id, snick
    SELECT *
    FROM (
        SELECT
            parent_department_path[1] AS BG,
            parent_department_path[2] AS BU,
            department_id AS shop_id,
            department_name AS shop_name
        FROM xqc_dim.group_all
        WHERE company_id = {{company_id}}
        AND is_shop = 'True'
        AND if({{BG}}!='全部',parent_department_path[1] = {{BG}}, parent_department_path[1]!='')
        AND if({{BU}}!='全部',parent_department_path[2] = {{BU}}, parent_department_path[2]!='')
        AND if({{shop_name}}!='全部',shop_name = {{shop_name}}, shop_name!='')
    )
    GLOBAL INNER JOIN(
        -- shop_id, snick
        SELECT
            mp_shop_id AS shop_id,
            seller_nick AS shop_name,
            snick
        FROM xqc_dim.snick_all
        WHERE company_id = {{company_id}}
        AND if({{snick}}!='全部',snick = {{snick}}, snick !=''))
        -- {{snick}} 是下拉框选项, {{snick_list}} 是权限隔离支持查看的snick数组, 前者理论上为后者的子集
        AND ( -- 权限隔离筛选条件
            shop_id IN {{shop_id_list}} OR snick IN {{snick_list}}
        )
    ) AS shop_snick
    USING shop_id, shop_name
)
GLOBAL LEFT JOIN(
    -- snick, cnick, level, warning_type, dialog_id, message_id, time, day, is_finished, 
    -- finish_time, shop_id, employee_name, superior_name
    SELECT *
    FROM xqc_ods.alert_all
    WHERE day BETWEEN {{start_day}} AND {{end_day}}
        AND if({{is_finished}}!='全部',is_finished = {{is_finished}}, is_finished!=''),
        AND if({{warning_type}}!='全部',warning_type = {{warning_type}}, warning_type!=''),
        AND if({{level}}!='全部',level = {{level}}, level!='')
        AND if({{superior_name}}!='全部',superior_name = {{superior_name}}, superior_name!=''),
        AND if({{snick}}!='全部',snick = {{snick}}, snick !=''))
        -- {{snick}} 是下拉框选项, {{snick_list}} 是权限隔离支持查看的snick数组, 前者理论上为后者的子集
        AND ( -- 权限隔离筛选条件
            shop_id IN {{shop_id_list}} OR snick IN {{snick_list}}
        )
)
USING snick
ORDER BY {{order_by_clause}}
LIMIT {{limit_clause}}


-- 参数留空
SELECT
    BG, BU,
    shop_name,
    superior_name, -- 客服负责人
    employee_name, -- 客服
    snick, -- 子账号
    cnick, -- 顾客
    dialog_id,
    message_id,
    level, -- 告警等级
    warning_type, -- 告警项
    time, -- 告警时间
    if(
        is_finished='True',
        round((now() - time)/60), -- 测试专用
        --round((parseDateTimeBestEffort(finish_time) - parseDateTimeBestEffort(time))/60),
        round((now() - time)/60)
    ) AS warning_duration, -- 告警时长(min)
    finish_time, -- 告警结束时间
    is_finished -- 是否完结
FROM (
    -- BG, BU, shop_name, shop_id, snick
    SELECT *
    FROM (
        SELECT
            parent_department_path[1] AS BG,
            parent_department_path[2] AS BU,
            department_id AS shop_id,
            department_name AS shop_name
        FROM xqc_dim.group_all
        WHERE company_id = {{company_id=60dd5e791597f82cd050da9f}}
        AND is_shop = 'True'
        AND if({{BG}}!='全部',parent_department_path[1] = {{BG}}, parent_department_path[1]!='')
        AND if({{BU}}!='全部',{{BU}}, parent_department_path[2]!='')
        AND if({{shop_name}}!='全部',shop_name = {{shop_name='方太京东自营旗舰店'}}, shop_name!='')
    )
    GLOBAL INNER JOIN(
        -- shop_id, snick
        SELECT
            mp_shop_id AS shop_id,
            seller_nick AS shop_name,
            snick
        FROM xqc_dim.snick_all
        WHERE company_id = {{company_id=60dd5e791597f82cd050da9f}}
        AND if({{snick}}!='全部',snick = {{snick='全部'}}, snick !=''))
        -- {{snick}} 是下拉框选项, {{snick_list}} 是权限隔离支持查看的snick数组, 前者理论上为后者的子集
        AND ( -- 权限隔离筛选条件
            shop_id IN {{shop_id_list}} OR snick IN {{snick_list}}
        )
    ) AS shop_snick
    USING shop_id, shop_name
)
GLOBAL LEFT JOIN(
    -- snick, cnick, level, warning_type, dialog_id, message_id, time, day, is_finished, 
    -- finish_time, shop_id, employee_name, superior_name
    SELECT *
    FROM xqc_ods.alert_all
    WHERE day BETWEEN {{start_day}} AND {{end_day}}
        AND if({{is_finished}}!='全部',is_finished = {{is_finished}}, is_finished!=''),
        AND if({{warning_type}}!='全部',warning_type = {{warning_type}}, warning_type!=''),
        AND if({{level}}!='全部',level = {{level}}, level!=0)
        AND if({{superior_name}}!='全部',superior_name = {{superior_name}}, superior_name!=''),
        AND if({{snick}}!='全部',snick = {{snick=}}, snick !=''))
        -- {{snick}} 是下拉框选项, {{snick_list}} 是权限隔离支持查看的snick数组, 前者理论上为后者的子集
        AND ( -- 权限隔离筛选条件
            shop_id IN {{shop_id_list}} OR snick IN {{snick_list}}
        )
)
USING snick
ORDER BY {{order_by_clause}}
LIMIT {{limit_clause}}


-- 测试参数
SELECT
    BG, 
    BU,
    shop_name,
    superior_name, -- 客服负责人
    employee_name, -- 客服
    snick, -- 子账号
    cnick, -- 顾客
    dialog_id,
    message_id,
    alert_info.id AS alert_id
    level, -- 告警等级
    warning_type, -- 告警项
    toString(time), -- 告警时间
    if(
        is_finished='True',
        round((now() - time)/60), -- 测试专用
        --round((parseDateTimeBestEffort(finish_time) - time)/60),
        round((now() - time)/60)
    ) AS warning_duration, -- 告警时长(min)
    finish_time, -- 告警结束时间
    is_finished -- 是否完结
FROM (
    -- BG, BU, shop_name, shop_id, snick
    SELECT *
    FROM (
        SELECT
            bg_info.department_name AS BG,
            bu_info.department_name AS BU,
            shop_info.department_id AS shop_id,
            shop_info.department_name AS shop_name
        FROM xqc_dim.group_all AS shop_info
        GLOBAL LEFT JOIN (
            SELECT department_id , department_name
            FROM xqc_dim.group_all
        ) AS bg_info
        ON parent_department_path[1] = bg_info.department_id
        GLOBAL LEFT JOIN (
            SELECT department_id , department_name
            FROM xqc_dim.group_all
        ) AS bu_info
        ON parent_department_path[2] = bu_info.department_id
        WHERE company_id = '5f747ba42c90fd0001254404'
        AND is_shop = 'True'
        AND parent_department_path[1]!=''
        AND parent_department_path[2]!=''
        AND shop_name!=''
    )
    GLOBAL INNER JOIN(
        -- shop_id, snick
        SELECT
            mp_shop_id AS shop_id,
            seller_nick AS shop_name,
            snick
        FROM xqc_dim.snick_all
        WHERE company_id = '5f747ba42c90fd0001254404'
        AND snick !=''
        AND ( -- 权限隔离筛选条件
            shop_id IN ['5cac112e98ef4100118a9c9f'] OR snick IN ['方太官方旗舰店:柚子']
        )
    ) AS shop_snick
    USING shop_id, shop_name
)
GLOBAL INNER JOIN(
    -- snick, cnick, level, warning_type, dialog_id, message_id, time, day, is_finished, 
    -- finish_time, shop_id, employee_name, superior_name
    SELECT *
    FROM xqc_ods.alert_all
    WHERE day BETWEEN 20210901 AND 20210915
        AND is_finished!=''
        AND warning_type!=''
        AND level!=0
        AND superior_name!=''
        AND snick !=''
        AND ( -- 权限隔离筛选条件
            shop_id IN ['5cac112e98ef4100118a9c9f'] OR snick IN ['方太官方旗舰店:柚子']
        )
) AS alert_info
USING snick