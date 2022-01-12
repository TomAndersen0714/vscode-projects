-- 质检报表-下拉框-获取子账号
SELECT DISTINCT
    snick
FROM dwd.xdqc_dialog_all
WHERE toYYYYMMDD(begin_time) BETWEEN toYYYYMMDD(toDate('{{ day.start=week_ago }}')) AND toYYYYMMDD(toDate('{{ day.end=yesterday }}'))
AND platform = '{{ platform=tb }}'
AND snick IN (
    SELECT distinct snick
    FROM ods.xinghuan_employee_snick_all
    WHERE day BETWEEN toYYYYMMDD(toDate('{{ day.start=week_ago }}')) AND toYYYYMMDD(toDate('{{ day.end=yesterday }}'))
    AND platform = '{{ platform=tb }}'
    AND company_id = '{{ company_id=61602afd297bb79b69c06118 }}'
    -- 下拉框-子账号分组id
    AND (
        '{{ depatment_ids }}'=''
        OR
        department_id IN splitByChar(',','{{ depatment_ids }}')
    )
    -- 下拉框-客服名称
    AND employee_id IN (
        SELECT distinct
            _id AS employee_id
        FROM ods.xinghuan_employee_all
        WHERE day = toYYYYMMDD(yesterday())
        AND company_id = '{{ company_id=61602afd297bb79b69c06118 }}'
        AND (
            '{{ usernames }}'=''
            OR
            username IN splitByChar(',','{{ usernames }}')
        )
    )
)
-- 下拉框-店铺名
AND (
    '{{ seller_nicks }}'=''
    OR
    seller_nick IN splitByChar(',','{{ seller_nicks }}')
)
ORDER BY snick