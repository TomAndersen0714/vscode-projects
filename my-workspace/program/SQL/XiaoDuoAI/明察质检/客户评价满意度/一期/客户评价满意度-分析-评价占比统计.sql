-- 客户评价满意度-分析-评价占比统计
SELECT
    SUM(1) AS `评价总数`,
    -- 下拉框-评价等级
    SUM(
        '{{ eval_codes }}'=''
        OR
        toString(eval_code) IN splitByChar(',','{{ eval_codes }}')
    ) AS `当前评价数`,
    CONCAT(toString(if(`评价总数`!=0, round(`当前评价数`/`评价总数`*100,2), 0.00)),'%') AS `评价占比`
FROM (
    SELECT
        replaceOne(splitByChar(':',user_nick)[1],'cntaobao','') AS seller_nick,
        replaceOne(eval_sender,'cntaobao','') AS snick,
        toUInt32(day) as day,
        eval_code
    FROM ods.kefu_eval_detail_all
    WHERE day BETWEEN toYYYYMMDD(toDate('{{day.start=week_ago}}')) AND toYYYYMMDD(toDate('{{day.end=yesterday}}'))
    -- 过滤买家已评价记录
    AND eval_time != ''
    -- 下拉框-店铺
    AND (
        '{{ seller_nicks }}'=''
        OR
        seller_nick IN splitByChar(',',replaceAll('{{ seller_nicks }}', '星环#', ''))
    )
    AND snick IN (
        -- 当前企业对应的子账号
        SELECT DISTINCT snick
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
        -- 下拉框-客服姓名
        WHERE (
            '{{ usernames }}'=''
            OR
            username IN splitByChar(',','{{ usernames }}')
        )
    )
) AS eval_daily_info