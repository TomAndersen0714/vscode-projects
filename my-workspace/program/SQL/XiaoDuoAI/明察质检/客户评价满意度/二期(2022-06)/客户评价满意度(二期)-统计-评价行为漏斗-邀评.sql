--客户评价满意度(二期)-统计-评价行为漏斗-邀评
SELECT
    -- 全部评价记录来源统计
    SUM(source != 1) AS `发起邀评次数`,
    SUM(source != 1 AND latest_eval_time != '') AS `买家评价次数`,
    SUM(source != 1 AND latest_eval_time = '') AS `待跟进评价次数`,
    SUM(source != 1 AND latest_eval_time != '' AND latest_eval_code IN [0, 1]) AS `满意次数`,
    SUM(source != 1 AND latest_eval_time != '' AND eval_change_model = 'unstatisfy_to_statisfy') AS `挽回次数`

FROM (
    SELECT
        dialog_id,
        replaceOne(splitByChar(':', user_nick)[1], 'cntaobao','') AS seller_nick,
        replaceOne(user_nick, 'cntaobao', '') AS snick,
        replaceOne(eval_recer, 'cntaobao', '') AS cnick,
        source,
        send_time,

        arraySort(groupArray(eval_time)) AS eval_times,
        arraySort((x,y)->y, groupArray(eval_code), groupArray(eval_time)) AS eval_codes,
        toString(eval_times[-1]) AS latest_eval_time,
        if(latest_eval_time!='', eval_codes[-1], -1) AS latest_eval_code,
        if(latest_eval_time!='', eval_codes[1], -1) AS first_eval_code,

        CASE
            WHEN (first_eval_code IN (0, 1)) AND (latest_eval_code IN (2, 3, 4)) THEN 'statisfy_to_unstatisfy'
            WHEN (first_eval_code IN (2, 3, 4)) AND (latest_eval_code IN (0, 1)) THEN 'unstatisfy_to_statisfy'
            ELSE ''
        END AS eval_change_model

    FROM xqc_ods.snick_eval_all
    WHERE day BETWEEN toYYYYMMDD(toDate('{{ day.start=week_ago }}'))
        AND toYYYYMMDD(toDate('{{ day.end=yesterday }}'))
    -- 下拉框-店铺
    AND (
        '{{ seller_nicks }}'=''
        OR
        seller_nick IN splitByChar(',',replaceAll('{{ seller_nicks }}', '星环#', ''))
    )
    -- 当前企业对应的子账号
    AND user_nick GLOBAL IN (
        SELECT DISTINCT CONCAT('cntaobao', snick) AS plat_snick
        FROM (
            SELECT distinct snick, username
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
        -- 下拉框-客服名称
        WHERE (
            '{{ usernames }}'=''
            OR
            username IN splitByChar(',','{{ usernames }}')
        )
    )
    GROUP BY  dialog_id, user_nick, eval_recer, source, send_time
    -- 下拉框-最新评价等级
    HAVING (
        '{{ latest_eval_codes }}'=''
        OR
        toString(latest_eval_code) IN splitByChar(',','{{ latest_eval_codes }}')
    )
) AS eval_info