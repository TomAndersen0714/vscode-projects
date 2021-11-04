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
    AND if('{{ BU=全部 }}'!='全部',parent_department_path[2]='{{BU=全部}}', 1=1)
) 
GLOBAL INNER JOIN (
    SELECT  *
    FROM xqc_ods.alert_all FINAL
    -- 时间筛选框
    WHERE day BETWEEN 20211102 AND 20211102
    -- 已订阅店铺
    AND shop_id GLOBAL IN (
        SELECT tenant_id AS shop_id
        FROM xqc_dim.company_tenant
        WHERE company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
        -- AND platform = '{{ platform=jd }}'
    )
) AS alert_info 
USING shop_id
ORDER BY time DESC
LIMIT 20 OFFSET 0