-- 宝尊质检报表-店铺-获取BG
SELECT DISTINCT
    department_name AS bg_name
FROM xqc_dim.group_all
WHERE company_id='{{ company_id=6131e6554524490001fc6825 }}'
AND level=1
AND is_shop = 'False'
ORDER BY bg_name


SELECT DISTINCT
    bg_name
FROM (
    SELECT DISTINCT
        department_id AS bg_id,
        department_name AS bg_name
    FROM xqc_dim.group_all
    WHERE company_id='{{ company_id=6131e6554524490001fc6825 }}'
    AND level=1
    AND is_shop = 'False'
) AS bg_info
GLOBAL LEFT JOIN (
    SELECT DISTINCT
        bg_id,
        bu_id,
        bu_name,
        shop_id
        shop_name
    FROM (
        SELECT DISTINCT
            parent_department_path[1] AS bg_id,
            department_id AS bu_id,
            department_name AS bu_name
        FROM xqc_dim.group_all
        WHERE company_id='{{ company_id=6131e6554524490001fc6825 }}'
        AND level=2
        AND is_shop = 'False'
    ) AS bu_info
    GLOBAL LEFT JOIN (
        SELECT DISTINCT
            parent_department_path[1] AS bg_id,
            parent_department_path[2] AS bu_id,
            department_id AS shop_id,
            department_name AS shop_name
        FROM xqc_dim.group_all
        WHERE company_id='{{ company_id=6131e6554524490001fc6825 }}'
        AND is_shop = 'True'
    ) AS shop_info
    USING(bu_id)
) AS bu_shop_info
ON bg_info.bg_id = bu_info.bg_id
GLOBAL LEFT JOIN (

) AS shop_info
USING(bu_id)
-- 下拉框-BU
WHERE ('{{bu_name}}'='' OR bu_name IN splitByChar(',','{{bu_name}}'))
ORDER BY bg_name


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