-- 新实时告警-店铺告警-下拉框-获取子账号
SELECT DISTINCT
    snick
FROM xqc_dim.snick_full_info_all
WHERE day = toYYYYMMDD(yesterday())
AND company_id = '6131e6554524490001fc6825'
-- 下拉框-平台
AND platform = 'open'
-- 下拉框-子账号分组id
AND (
    '{{ department_ids }}'=''
    OR
    department_id IN splitByChar(',','{{ department_ids }}')
)
-- 下拉框-店铺名
AND (
    '{{ shop_ids }}' = ''
    OR
    shop_id IN splitByChar(',','{{ shop_ids }}')
)
ORDER BY snick COLLATE 'zh'