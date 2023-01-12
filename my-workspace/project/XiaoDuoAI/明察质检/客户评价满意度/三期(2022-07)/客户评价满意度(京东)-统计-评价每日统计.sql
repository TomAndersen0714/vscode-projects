-- 客户评价满意度(京东)-统计-评价每日统计
SELECT
    day,
    -- 各等级评价次数
    (eval_0_cnt + eval_1_cnt + eval_2_cnt + eval_3_cnt + eval_4_cnt) AS eval_cnt_sum,
    eval_0_cnt AS `非常满意`,
    eval_1_cnt AS `满意`,
    eval_2_cnt AS `一般`,
    eval_3_cnt AS `不满意`,
    eval_4_cnt AS `非常不满意`,

    -- 各等级评价占比
    if(eval_cnt_sum!=0, round(eval_0_cnt / eval_cnt_sum * 100, 2), 0.00) AS `非常满意%`,
    if(eval_cnt_sum!=0, round(eval_1_cnt / eval_cnt_sum * 100, 2), 0.00) AS `满意%`,
    if(eval_cnt_sum!=0, round(eval_2_cnt / eval_cnt_sum * 100, 2), 0.00) AS `一般%`,
    if(eval_cnt_sum!=0, round(eval_3_cnt / eval_cnt_sum * 100, 2), 0.00) AS `不满意%`,
    if(eval_cnt_sum!=0, round(eval_4_cnt / eval_cnt_sum * 100, 2), 0.00) AS `非常不满意%`
FROM (
    SELECT
        day,
        SUM(eval_code = 0) AS eval_0_cnt,
        SUM(eval_code = 1) AS eval_1_cnt,
        SUM(eval_code = 2) AS eval_2_cnt,
        SUM(eval_code = 3) AS eval_3_cnt,
        SUM(eval_code = 4) AS eval_4_cnt
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
    GROUP BY day
) AS dialog_eval_info
GLOBAL RIGHT JOIN (
    SELECT arrayJoin(
        arrayMap(
            x->toInt32(toYYYYMMDD(toDate(x))),
            range(toUInt32(toDate('{{ day.start=week_ago }}')), toUInt32(toDate('{{ day.end=yesterday }}') + 1), 1)
        )
    ) AS day
) AS day_axis
USING(day)
ORDER BY day ASC