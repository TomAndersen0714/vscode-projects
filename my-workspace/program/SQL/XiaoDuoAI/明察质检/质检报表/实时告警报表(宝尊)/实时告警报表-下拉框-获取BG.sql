-- 宝尊质检报表-下拉框-获取BG
SELECT DISTINCT
    CONCAT(bg_name,'//',bg_id) AS bg_name
FROM (
    SELECT DISTINCT *
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
            parent_department_path[1] AS bg_id,
            department_id AS bu_id
        FROM xqc_dim.group_all
        WHERE company_id='{{ company_id=6131e6554524490001fc6825 }}'
        AND level=2
        AND is_shop = 'False'
    ) AS bu_info
    USING(bg_id)
) AS bg_bu_info
GLOBAL LEFT JOIN (
    SELECT DISTINCT
        parent_department_path[1] AS bg_id,
        department_id AS shop_id
    FROM xqc_dim.group_all
    WHERE company_id='{{ company_id=6131e6554524490001fc6825 }}'
    AND is_shop = 'True'
) AS shop_info
USING(bg_id)
WHERE 
-- 下拉框-BU
(
    '{{ bu_ids }}'='' 
    OR 
    bu_id IN splitByChar(',','{{ bu_ids }}')
)
-- 下拉框-店铺
AND (
    '{{ shop_ids }}'='' 
    OR 
    shop_id IN splitByChar(',','{{ shop_ids }}')
)
ORDER BY bg_name ASC