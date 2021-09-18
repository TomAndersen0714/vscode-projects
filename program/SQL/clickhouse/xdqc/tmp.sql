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
    warning_type, -- 告警项
    time, -- 告警时间
    if(
        is_finished='True',
        round((parseDateTimeBestEffort(finish_time) - parseDateTimeBestEffort(time))/60),
        round((now() - parseDateTimeBestEffort(time))/60)
    ) AS warning_duration, -- 告警时长(min)
    finish_time, -- 告警结束时间
    is_finished -- 是否完结
FROM (
    SELECT *
    FROM xqc_ods.event_alert_1_all
    WHERE day BETWEEN {{start_day}} AND {{end_day}}
        AND if({{is_finished}}!='全部',is_finished = {{is_finished}}, is_finished!=''),
        AND if({{warning_type}}!='全部',warning_type = {{warning_type}}, warning_type!=''),
        AND if({{level}}!='全部',level = {{level}}, level!='')
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
            WHERE company_id = {{company_id}}
            AND is_shop = 'True'
            AND if({{BG}}!='全部',parent_department_path[1] = {{BG}}, parent_department_path[1]!='')
            AND if({{BU}}!='全部',parent_department_path[2] = {{BU}}, parent_department_path[2]!='')
            AND if({{shop_name}}!='全部',shop_name = {{shop_name}}, shop_name!='')
        )
        GLOBAL LEFT JOIN(
            -- shop_id, snick
            SELECT 
                mp_shop_id AS shop_id, 
                snick
            FROM xqc_dim.snick_all
            WHERE company_id = {{company_id}}
            AND if({{snick}}!='全部',snick = {{snick}}, snick IN ({{snick_list}}))
            -- {{snick}} 是下拉框选项, {{snick_list}} 是权限隔离支持查看的snick数组, 前者必须为后者的子集
        ) AS shop_snick
        USING shop_id
    )
    GLOBAL LEFT JOIN(
        SELECT superior_name, employee_name, snick, cnick, id AS dialog_id
        FROM xqc_ods.dialog_all
        WHERE day BETWEEN {{start_day}} AND {{end_day}}
        AND if({{snick}}!='全部',snick = {{snick}}, snick IN ({{snick_list}}))
        -- {{snick}} 是下拉框选项, {{snick_list}} 是权限隔离支持查看的snick数组, 前者必须为后者的子集
        AND if({{superior_name}}!='全部',superior_name = {{superior_name}}, superior_name!=''),
    )
    USING snick
) AS dim_stat
USING dialog_id
-- ORDER BY {{order_by_clause}} -- 支持按照告警时间,处理完成时间排序
-- LIMIT {{limit_clause}}





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
        round((parseDateTimeBestEffort(finish_time) - parseDateTimeBestEffort(time))/60),
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
    FROM xqc_ods.event_alert_1_all
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