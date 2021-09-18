SELECT count(1) as count
FROM (
        SELECT *
        FROM (
                SELECT parent_department_path [1] AS BG,
                    parent_department_path [2] AS BU,
                    department_id AS shop_id,
                    department_name AS shop_name
                FROM xqc_dim.group_all
                WHERE company_id = 5f73e9c1684bf70001413636
                    AND is_shop = 'True'
                    AND parent_department_path [1] != ''
                    AND parent_department_path [2] != ''
            ) GLOBAL
            INNER JOIN(
                SELECT mp_shop_id AS shop_id,
                    seller_nick AS shop_name,
                    snick
                FROM xqc_dim.snick_all
                WHERE company_id = 5f73e9c1684bf70001413636
                    AND snick != ''
            ) AS shop_snick USING shop_id,
            shop_name
    ) GLOBAL
    LEFT JOIN(
        SELECT *
        FROM xqc_ods.event_alert_1_all
        WHERE day BETWEEN 19700101 AND 19700101
            AND is_finished != ''
            AND level != ''
            AND warning_type != ''
            AND snick != ''
    ) USING snick