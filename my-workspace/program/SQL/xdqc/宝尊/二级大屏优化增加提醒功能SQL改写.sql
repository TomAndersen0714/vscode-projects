SELECT
    BG, BU,
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
    toInt64(if(
        is_finished='True',
        round((parseDateTimeBestEffort(if(finish_time!='',finish_time,toString(now()))) - parseDateTimeBestEffort(time))/60),
        round((now() - parseDateTimeBestEffort(time))/60)
    )) AS warning_duration,
    finish_time,
    is_finished,
    if(notify_time!='', 'True', 'False') AS is_notified,
    notify_time,
    if(
        notify_time!='',
        if(
            finish_time!='',
            toString(round((parseDateTimeBestEffort(if(notify_time!='',notify_time,toString(now())))-parseDateTimeBestEffort(if(finish_time!='',finish_time,toString(now()))))/60)),
            toString(round((now()-parseDateTimeBestEffort(if(notify_time!='',notify_time,toString(now()))))/60))
        ),
        ''
    ) AS notify_duration
FROM (
        SELECT
            shop_info.company_id AS company_id,
            shop_info.bg_id AS bg_id,
            bg_info.department_name AS BG,
            shop_info.bu_id AS bu_id,
            bu_info.department_name AS BU,
            shop_info.department_id AS shop_id,
            shop_info.department_name AS shop_name
        FROM (
            SELECT
                parent_department_path[1] AS bg_id,
                parent_department_path[2] AS bu_id,
                parent_department_path,
                company_id,
                department_id,
                department_name
            FROM xqc_dim.group_all
            WHERE is_shop = 'True'
        ) AS shop_info
        GLOBAL LEFT JOIN (
            SELECT department_id , department_name
            FROM xqc_dim.group_all
            WHERE is_shop = 'False'
        ) AS bg_info
        ON shop_info.bg_id = bg_info.department_id
        GLOBAL LEFT JOIN (
            SELECT department_id , department_name
            FROM xqc_dim.group_all
            WHERE is_shop = 'False'
        ) AS bu_info
        ON shop_info.bu_id = bu_info.department_id
        WHERE company_id = '6131e6554524490001fc6825'
)
GLOBAL INNER JOIN(
    SELECT *
    FROM (
        SELECT
            alert_id, time AS notify_time
        FROM xqc_ods.alert_remind_all
        WHERE shop_id IN %s -- strings.Join(allowedShopIds, ",")
    ) AS alert_remind
    GLOBAL RIGHT JOIN (
        SELECT id AS alert_id, *
        FROM xqc_ods.alert_all FINAL
        WHERE %s
    ) AS alert_info
    USING(alert_id)

) AS alert_info
USING shop_id
WHERE is_notified = %s