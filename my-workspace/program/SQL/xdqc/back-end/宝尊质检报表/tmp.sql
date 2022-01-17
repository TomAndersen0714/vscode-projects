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