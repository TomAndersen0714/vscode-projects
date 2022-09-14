-- 客户评价满意度-下拉框-获取子账号分组
SELECT DISTINCT
    concat(department_name,'//',department_id) AS department_name_id
FROM xqc_dim.snick_full_info_all
WHERE day = toYYYYMMDD(yesterday())
AND platform = 'tb'
AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
-- 筛选已有评价的子账号
AND snick GLOBAL IN (
    SELECT DISTINCT
        snick
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
    -- 下拉框-店铺名
    AND (
        '{{ seller_nicks }}'=''
        OR
        seller_nick IN splitByChar(',','{{ seller_nicks }}')
    )
    AND snick GLOBAL IN (
        -- 当前企业对应的子账号
        SELECT DISTINCT
            snick
        FROM xqc_dim.snick_full_info_all
        WHERE day = toYYYYMMDD(yesterday())
        AND platform = 'tb'
        AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
        -- 下拉框-客服ID
        AND (
            '{{ employee_ids }}'=''
            OR
            employee_id IN splitByChar(',','{{ employee_ids }}')
        )
        
    )
)
ORDER BY department_name COLLATE 'zh'