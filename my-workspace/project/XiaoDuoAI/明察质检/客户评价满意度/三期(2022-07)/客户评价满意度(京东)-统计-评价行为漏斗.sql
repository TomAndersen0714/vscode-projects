-- 客户评价满意度(京东)-统计-评价行为漏斗
SELECT
    SUM(1) AS `买家评价次数`,
    SUM(eval_code IN [0, 1]) AS `满意次数`,
    SUM(eval_code IN [2, 3, 4]) AS `不满意次数`
FROM xqc_ods.dialog_eval_all
WHERE day BETWEEN toYYYYMMDD(toDate('{{ day.start=week_ago }}'))
    AND toYYYYMMDD(toDate('{{ day.end=yesterday }}'))
AND platform = 'jd'
-- 下拉框-店铺
AND (
    '{{ seller_nicks }}'=''
    OR
    seller_nick IN splitByChar(',',replaceAll('{{ seller_nicks }}', '星环#', ''))
)
-- 当前企业对应的店铺
AND seller_nick GLOBAL IN (
    SELECT DISTINCT
        seller_nick
    FROM xqc_dim.xqc_shop_all
    WHERE day = toYYYYMMDD(yesterday())
    AND company_id = '{{ company_id=5f73e9c1684bf70001413636 }}'
    AND platform = 'jd'
)
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
            AND company_id = '{{ company_id=5f73e9c1684bf70001413636 }}'
        ) AS employee_info
        USING(employee_id)
        WHERE day = toYYYYMMDD(yesterday())
        AND platform = 'jd'
        AND company_id = '{{ company_id=5f73e9c1684bf70001413636 }}'
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