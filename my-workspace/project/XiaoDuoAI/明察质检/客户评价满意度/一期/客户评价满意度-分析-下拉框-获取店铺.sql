-- 客户评价满意度-分析-下拉框-获取店铺
SELECT DISTINCT
    seller_nick AS `店铺`
FROM xqc_ods.dialog_eval_all
WHERE day BETWEEN toYYYYMMDD(toDate('{{day.start=yesterday}}'))
    AND toYYYYMMDD(toDate('{{day.end=yesterday}}'))
-- 过滤买家已评价记录
AND eval_time != ''
-- 下拉框-评价等级
AND (
    '{{ eval_codes }}'=''
    OR
    toString(eval_code) IN splitByChar(',','{{ eval_codes }}')
)
-- 筛选指定子账号
AND snick GLOBAL IN (
    -- 当前企业对应的子账号
    SELECT DISTINCT
        snick
    FROM xqc_dim.snick_full_info_all
    WHERE day = toYYYYMMDD(yesterday())
    AND platform = 'tb'
    AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
    -- 下拉框-子账号分组id
    AND (
        '{{ department_ids }}'=''
        OR
        department_id IN splitByChar(',','{{ department_ids }}')
    )
    -- 下拉框-客服ID
    AND (
        '{{ employee_ids }}'=''
        OR
        employee_id IN splitByChar(',','{{ employee_ids }}')
    )
)
ORDER BY seller_nick COLLATE 'zh'
