-- 客户评价满意度(二期)-统计-评价占比每日统计
SELECT
    day,
    -- 各等级评价次数
    SUM(latest_eval_time != '') AS eval_cnt_sum,
    SUM(latest_eval_time != '' AND latest_eval_code = 0) AS eval_0_cnt,
    SUM(latest_eval_time != '' AND latest_eval_code = 1) AS eval_1_cnt,
    SUM(latest_eval_time != '' AND latest_eval_code = 2) AS eval_2_cnt,
    SUM(latest_eval_time != '' AND latest_eval_code = 3) AS eval_3_cnt,
    SUM(latest_eval_time != '' AND latest_eval_code = 4) AS eval_4_cnt,

    -- 各等级评价占比
    if(eval_cnt_sum!=0, round(eval_0_cnt / eval_cnt_sum * 100, 2), 0.00) AS `非常满意`,
    if(eval_cnt_sum!=0, round(eval_1_cnt / eval_cnt_sum * 100, 2), 0.00) AS `满意`,
    if(eval_cnt_sum!=0, round(eval_2_cnt / eval_cnt_sum * 100, 2), 0.00) AS `一般`,
    if(eval_cnt_sum!=0, round(eval_3_cnt / eval_cnt_sum * 100, 2), 0.00) AS `不满意`,
    if(eval_cnt_sum!=0, round(eval_4_cnt / eval_cnt_sum * 100, 2), 0.00) AS `非常不满意`

FROM (
    SELECT
        day,
        seller_nick,
        snick,
        cnick,
        max(dialog_id) AS dialog_id,
        source,
        send_time,
        is_invited,

        arraySort(groupArrayIf(eval_time, eval_time !='')) AS eval_times,
        arraySort((x,y)->y, groupArrayIf(eval_code, eval_time != ''), groupArrayIf(eval_time, eval_time != '')) AS eval_codes,
        toString(eval_times[-1]) AS latest_eval_time,
        if(latest_eval_time != '', eval_codes[-1], -1) AS latest_eval_code,
        if(latest_eval_time != '', eval_codes[1], -1) AS first_eval_code,

        CASE
            WHEN (first_eval_code IN (0, 1)) AND (latest_eval_code IN (2, 3, 4)) THEN 'statisfy_to_unstatisfy'
            WHEN (first_eval_code IN (2, 3, 4)) AND (latest_eval_code IN (0, 1)) THEN 'unstatisfy_to_statisfy'
            ELSE ''
        END AS eval_change_model
    FROM (
        SELECT
            seller_nick,
            snick,
            cnick,
            dialog_id,
            eval_code,
            eval_time,
            send_time,
            source,
            if(eval_time != '' AND source = 1, 0, 1) AS is_invited,
            day
        FROM xqc_ods.dialog_eval_all
        WHERE day BETWEEN toYYYYMMDD(toDate('{{ day.start=week_ago }}'))
            AND toYYYYMMDD(toDate('{{ day.end=yesterday }}'))
        AND platform = 'tb'
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
            AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
            AND platform = 'tb'
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
    ) AS ods_snick_eval
    GROUP BY day, seller_nick, snick, cnick, source, send_time, is_invited
    -- 下拉框-最新评价等级
    HAVING (
        '{{ latest_eval_codes }}'=''
        OR
        toString(latest_eval_code) IN splitByChar(',','{{ latest_eval_codes }}')
    )
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
GROUP BY day
ORDER BY day ASC