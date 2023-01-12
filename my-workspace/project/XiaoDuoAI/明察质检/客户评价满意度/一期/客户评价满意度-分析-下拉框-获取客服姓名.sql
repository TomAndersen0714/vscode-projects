-- 客户评价满意度-分析-下拉框-获取客服姓名ID
SELECT DISTINCT 
    CONCAT(username, '//', _id) AS `客服姓名ID`
FROM ods.xinghuan_employee_all
WHERE day = toYYYYMMDD(yesterday())
AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
AND _id IN (
    SELECT DISTINCT
        employee_id
    FROM ods.xinghuan_employee_snick_all
    WHERE day = toYYYYMMDD(yesterday())
    AND platform = 'tb'
    AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
    -- 下拉框-子账号分组id
    AND (
        '{{ department_ids }}'=''
        OR
        department_id IN splitByChar(',','{{ department_ids }}')
    )
    -- 被评价过的子账号
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
        -- 筛选企业对应主账号
        AND seller_nick GLOBAL IN (
            SELECT DISTINCT
                seller_nick
            FROM xqc_dim.xqc_shop_all
            WHERE day = toYYYYMMDD(yesterday())
            AND platform = 'tb'
            AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
        )
    )
)
ORDER BY username COLLATE 'zh'