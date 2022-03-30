-- 预警列表(实时查询+导出)
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
LIMIT 100