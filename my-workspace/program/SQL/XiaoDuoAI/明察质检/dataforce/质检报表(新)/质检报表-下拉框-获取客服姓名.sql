-- 质检报表-下拉框-获取客服姓名
SELECT DISTINCT username
FROM ods.xinghuan_employee_all
WHERE day = toYYYYMMDD(yesterday())
AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
AND _id IN (
    SELECT DISTINCT employee_id
    FROM ods.xinghuan_employee_snick_all
    WHERE day = toYYYYMMDD(yesterday())
    AND platform = 'tb'
    AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
    -- 下拉框-子账号分组id
    AND (
        '{{ depatment_ids }}'=''
        OR
        department_id IN splitByChar(',','{{ depatment_ids }}')
    )
    -- 下拉框-子账号
    AND (
        '{{ snicks }}'=''
        OR
        snick IN splitByChar(',','{{ snicks }}')
    )
    -- 最近发生聊天的子账号
    AND snick GLOBAL IN (
        SELECT DISTINCT
            snick
        FROM dwd.xdqc_dialog_all
        WHERE toYYYYMMDD(begin_time) BETWEEN toYYYYMMDD(toDate('{{ day.start=week_ago }}')) AND toYYYYMMDD(toDate('{{ day.end=yesterday }}'))
        AND platform = 'tb'
        -- 下拉框-店铺名
        AND (
            '{{ seller_nicks }}'=''
            OR
            seller_nick IN splitByChar(',','{{ seller_nicks }}')
        )
        -- 下拉框-子账号
        AND (
            '{{ snicks }}'=''
            OR
            snick IN splitByChar(',','{{ snicks }}')
        )
        -- 当前企业对应的子账号
        AND snick GLOBAL IN (
            SELECT distinct snick
            FROM ods.xinghuan_employee_snick_all
            WHERE day = toYYYYMMDD(yesterday())
            AND platform = 'tb'
            AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
        )
    )
)
ORDER BY username COLLATE 'zh'