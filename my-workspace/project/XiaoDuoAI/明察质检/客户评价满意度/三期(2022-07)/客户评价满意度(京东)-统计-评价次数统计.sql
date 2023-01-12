-- 客户评价满意度(京东)-统计-评价次数统计
SELECT
    -- 各等级评价次数
    SUM(eval_code = 0) AS `非常满意`,
    SUM(eval_code = 1) AS `满意`,
    SUM(eval_code = 2) AS `一般`,
    SUM(eval_code = 3) AS `不满意`,
    SUM(eval_code = 4) AS `非常不满意`,

    -- 满意率统计
    SUM(1) AS eval_cnt_sum,
    SUM(eval_code IN [0, 1]) AS eval_satisfied_cnt,
    eval_cnt_sum - eval_satisfied_cnt AS eval_unsatisfied_cnt,
    if(eval_cnt_sum!=0, round(eval_satisfied_cnt / eval_cnt_sum * 100, 2), 0.00) AS satisfy_pct,
    if(eval_cnt_sum!=0, round(eval_unsatisfied_cnt / eval_cnt_sum * 100, 2), 0.00) AS unsatisfy_pct,
    CONCAT(toString(satisfy_pct),'%') AS `满意率`,
    CONCAT(toString(unsatisfy_pct),'%') AS `不满意率`
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
-- 下拉框-评价等级
AND (
    '{{ latest_eval_codes }}'=''
    OR
    toString(eval_code) IN splitByChar(',','{{ latest_eval_codes }}')
)