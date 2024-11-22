-- 客户评价满意度(二期)-统计-评价次数统计
SELECT
    -- 评价来源统计
    SUM(is_invited = 1) AS `系统邀评次数`,
    SUM(is_invited = 1 AND latest_eval_time != '') AS `买家评价次数`,
    SUM(is_invited = 1 AND latest_eval_time = '') AS `待跟进评价次数`,
    SUM(is_invited = 1 AND latest_eval_time != '' AND latest_eval_code IN [0, 1]) AS `满意次数`,
    SUM(is_invited = 1 AND latest_eval_time != '' AND eval_change_model = 'unstatisfy_to_statisfy') AS `挽回次数`,

    -- 已有评价来源统计
    SUM(source = 0 AND latest_eval_time != '') AS `客服邀评`,
    SUM(source = 1 AND latest_eval_time != '') AS `消费者自主评价`,
    SUM(source = 2 AND latest_eval_time != '') AS `系统邀评`,

    -- 各等级评价次数
    SUM(latest_eval_time != '' AND latest_eval_code = 0) AS `非常满意`,
    SUM(latest_eval_time != '' AND latest_eval_code = 1) AS `满意`,
    SUM(latest_eval_time != '' AND latest_eval_code = 2) AS `一般`,
    SUM(latest_eval_time != '' AND latest_eval_code = 3) AS `不满意`,
    SUM(latest_eval_time != '' AND latest_eval_code = 4) AS `非常不满意`,

    -- 满意率统计
    SUM(latest_eval_time != '') AS eval_cnt_sum,
    SUM(latest_eval_time != '' AND latest_eval_code < 2) AS eval_satisfied_cnt,
    eval_cnt_sum - eval_satisfied_cnt AS eval_unsatisfied_cnt,
    if(eval_cnt_sum!=0, round(eval_satisfied_cnt / eval_cnt_sum * 100, 2), 0.00) AS satisfy_pct,
    if(eval_cnt_sum!=0, round(eval_unsatisfied_cnt / eval_cnt_sum * 100, 2), 0.00) AS unsatisfy_pct,
    CONCAT(toString(satisfy_pct),'%') AS `满意率`,
    CONCAT(toString(unsatisfy_pct),'%') AS `不满意率`


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
            SELECT DISTINCT
                snick
            FROM xqc_dim.snick_full_info_all
            WHERE day = toYYYYMMDD(yesterday())
            AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
            AND platform = 'tb'
            -- 下拉框-子账号分组id
            AND (
                '{{ department_ids }}'=''
                OR
                department_id IN splitByChar(',','{{ department_ids }}')
            )
            -- 下拉框-客服姓名
            AND (
                '{{ usernames }}'=''
                OR
                employee_name IN splitByChar(',','{{ usernames }}')
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