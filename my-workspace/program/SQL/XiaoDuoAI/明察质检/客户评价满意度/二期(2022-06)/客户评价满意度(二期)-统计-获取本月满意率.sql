-- 客户评价满意度(二期)-统计-获取本月满意率
SELECT
    toString(toStartOfMonth(today())) AS start_of_month,
    toString(subtractDays(addMonths(toStartOfMonth(today()),1),1)) AS end_of_month,
    
    SUM(latest_eval_time != '') AS eval_cnt_sum,
    SUM(latest_eval_time != '' AND latest_eval_code < 2) AS eval_satisfied_cnt,
    eval_cnt_sum - eval_satisfied_cnt AS eval_unsatisfied_cnt,
    if(eval_cnt_sum!=0, round(eval_satisfied_cnt / eval_cnt_sum, 2), 0.00) AS `满意率`,
    if(eval_cnt_sum!=0, round(eval_unsatisfied_cnt / eval_cnt_sum, 2), 0.00) AS `不满意率`

FROM (
    SELECT
        seller_nick,
        snick,
        cnick,
        dialog_id,
        source,
        send_time,
        is_invited,

        arraySort(groupArrayIf(eval_time, eval_time !='')) AS eval_times,
        arraySort((x,y)->y, groupArrayIf(eval_code, eval_time != ''), groupArrayIf(eval_time, eval_time != '')) AS eval_codes,
        toString(eval_times[-1]) AS latest_eval_time,
        if(latest_eval_time != '', eval_codes[-1], -1) AS latest_eval_code,
        if(latest_eval_time != '', eval_codes[1], -1) AS first_eval_code
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
        WHERE day BETWEEN toYYYYMMDD(toStartOfMonth(today()))
            AND toYYYYMMDD(subtractDays(addMonths(toStartOfMonth(today()),1),1))
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
    GROUP BY seller_nick, snick, cnick, dialog_id, source, send_time, is_invited
) AS dialog_eval_info