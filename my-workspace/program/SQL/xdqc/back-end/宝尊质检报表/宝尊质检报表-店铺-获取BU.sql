-- 宝尊质检报表-店铺-获取BU
SELECT DISTINCT
    department_name AS bu_name
FROM xqc_dim.group_all
WHERE company_id='{{ company_id=6131e6554524490001fc6825 }}'
AND level=2
AND is_shop = 'False'
ORDER BY bu_name