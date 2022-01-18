-- 宝尊质检报表-店铺-获取BU
SELECT DISTINCT
    CONCAT(department_name,'//',department_id) AS bu_name
FROM xqc_dim.group_all
WHERE company_id='{{ company_id=6131e6554524490001fc6825 }}'
AND level=2
AND is_shop = 'False'
ORDER BY bu_name ASC


SELECT DISTINCT
    CONCAT(bu_name,'//',bu_id) AS bu_name
FROM (
    SELECT DISTINCT *
    FROM (
        SELECT
            department_id AS bu_id,
            department_name AS bu_name,
            parent_department_path[1] AS bg_id
        FROM xqc_dim.group_all
        WHERE company_id='{{ company_id=6131e6554524490001fc6825 }}'
        AND level=2
        AND is_shop = 'False'
    ) AS bu_info
    GLOBAL LEFT JOIN (
        SELECT DISTINCT
            department_id AS bg_id
        FROM xqc_dim.group_all
        WHERE company_id='{{ company_id=6131e6554524490001fc6825 }}'
        AND level=1
        AND is_shop = 'False'
    ) AS bg_info
    USING(bg_id)
) AS bu_bg_info
GLOBAL LEFT JOIN (
    SELECT DISTINCT
        parent_department_path[2] AS bu_id,
        department_id
    FROM xqc_dim.group_all
    WHERE company_id='{{ company_id=6131e6554524490001fc6825 }}'
    AND is_shop = 'True'
)
USING(bu_id)
WHERE 
-- 下拉框-BG
(
    '{{bg_ids}}'='' 
    OR 
    department_id IN splitByChar(',','{{bg_ids}}')
)
-- 下拉框-店铺
AND (
    '{{shop_ids}}'='' 
    OR 
    department_id IN splitByChar(',','{{shop_ids}}')
)