SELECT count(1) AS COUNT
FROM (
        SELECT shop_info.company_id AS company_id,
            shop_info.bg_id AS bg_id,
            bg_info.department_name AS BG,
            shop_info.bu_id AS bu_id,
            bu_info.department_name AS BU,
            shop_info.department_id AS shop_id,
            shop_info.department_name AS shop_name
        FROM (
                SELECT parent_department_path [1] AS bg_id,
                    parent_department_path [2] AS bu_id,
                    parent_department_path,
                    company_id,
                    department_id,
                    department_name
                FROM xqc_dim.group_all
                WHERE is_shop = 'True'
            ) AS shop_info GLOBAL
            LEFT JOIN (
                SELECT department_id,
                    department_name
                FROM xqc_dim.group_all
                WHERE is_shop = 'False'
            ) AS bg_info ON shop_info.bg_id = bg_info.department_id GLOBAL
            LEFT JOIN (
                SELECT department_id,
                    department_name
                FROM xqc_dim.group_all
                WHERE is_shop = 'False'
            ) AS bu_info ON shop_info.bu_id = bu_info.department_id
        WHERE company_id = '612c53cb7250e1e5140faded'
    ) GLOBAL
    INNER JOIN (
        SELECT *
        FROM (
                SELECT alert_id,
                    time AS notify_time
                FROM xqc_ods.alert_remind_all
                WHERE (
                        shop_id IN ['5bfe7a6a89bc4612f16586a5','5f1f97bdfbb9ba0017f73f18','5f74643b6868e200013e6d46','5f8ff0c0a3967d00188dca48','613ef1e1ec7097000e494123','61c16f73e8e6fc3cd46906a4']
                    )
            ) AS alert_remind GLOBAL
            RIGHT JOIN (
                SELECT *
                FROM xqc_ods.alert_all FINAL
                WHERE day BETWEEN 20220120 AND 20220120
                    AND (
                        shop_id IN ['5bfe7a6a89bc4612f16586a5','5f1f97bdfbb9ba0017f73f18','5f74643b6868e200013e6d46','5f8ff0c0a3967d00188dca48','613ef1e1ec7097000e494123','61c16f73e8e6fc3cd46906a4']
                        OR snick IN []
                    )
                    AND platform != ''
                    AND is_finished != ''
                    AND level != 0
                    AND warning_type != ''
            ) AS alert_info USING(alert_id)
    ) AS alert_info 
    USING shop_id