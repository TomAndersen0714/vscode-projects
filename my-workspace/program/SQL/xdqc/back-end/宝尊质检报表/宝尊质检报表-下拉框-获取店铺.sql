-- 宝尊质检报表-下拉框-获取店铺
SELECT DISTINCT
    department_id AS shop_id,
    department_name AS shop_name
FROM xqc_dim.group_all
WHERE company_id='{{ company_id=6131e6554524490001fc6825 }}'
AND is_shop = 'True'
-- 下拉框-BG
AND (
    '{{bg_ids}}'='' 
    OR 
    hasAny(parent_department_path,splitByChar(',','{{bg_ids}}'))
)
-- 下拉框-BU
AND (
    '{{bu_ids}}'='' 
    OR 
    hasAny(parent_department_path,splitByChar(',','{{bu_ids}}'))
)
ORDER BY bu_name