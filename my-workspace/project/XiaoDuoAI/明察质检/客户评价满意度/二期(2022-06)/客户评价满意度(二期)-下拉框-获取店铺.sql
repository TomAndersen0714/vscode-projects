-- 客户评价满意度(二期)-下拉框-获取店铺
SELECT DISTINCT
    seller_nick AS `店铺`
FROM (
    SELECT
        seller_nick
    FROM xqc_ods.dialog_eval_all
    WHERE day BETWEEN toYYYYMMDD(toDate('{{ day.start=week_ago }}'))
        AND toYYYYMMDD(toDate('{{ day.end=yesterday }}'))
    AND platform = 'tb'
    -- 当前企业对应的子账号
    AND snick GLOBAL IN (
        SELECT DISTINCT snick
        FROM (
            SELECT DISTINCT snick, username
            FROM ods.xinghuan_employee_snick_all AS snick_info
            GLOBAL LEFT JOIN (
                SELECT distinct
                    _id AS employee_id, username
                FROM ods.xinghuan_employee_all
                WHERE day = toYYYYMMDD(yesterday())
                AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
            ) AS employee_info
            USING(employee_id)
            WHERE day = toYYYYMMDD(yesterday())
            AND platform = 'tb'
            AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
            -- 下拉框-子账号分组id
            AND (
                '{{ department_ids }}'=''
                OR
                department_id IN splitByChar(',','{{ department_ids }}')
            )
        ) AS snick_employee_info
        -- 下拉框-客服姓名
        WHERE (
            '{{ usernames }}'=''
            OR
            username IN splitByChar(',','{{ usernames }}')
        )
    )
) AS ods_eval_info
ORDER BY `店铺` COLLATE 'zh'
