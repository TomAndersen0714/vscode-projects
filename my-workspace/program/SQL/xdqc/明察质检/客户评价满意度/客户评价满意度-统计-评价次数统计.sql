-- 客户评价满意度-统计-评价次数统计
SELECT
    COUNT(1) AS `发起邀评次数`,
    SUM(eval_time != '') AS `买家评价次数`,
    SUM(eval_time = '') AS `待跟进评价次数`
FROM (
    SELECT
        replaceOne(splitByChar(':',user_nick)[1],'cntaobao','') AS seller_nick,
        replaceOne(eval_sender,'cntaobao','') AS snick,
        eval_time
    FROM ods.kefu_eval_detail_all
    WHERE day BETWEEN toYYYYMMDD(toDate('{{day.start=week_ago}}')) AND toYYYYMMDD(toDate('{{day.end=yesterday}}'))
    -- 下拉框-店铺
    AND (
        '{{ seller_nicks }}'=''
        OR
        seller_nick IN splitByChar(',',replaceAll('{{ seller_nicks }}', '星环#', ''))
    )
    -- 下拉框-评价等级
    AND (
        '{{ eval_codes }}'=''
        OR
        toString(eval_code) IN splitByChar(',','{{ eval_codes }}')
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
                '{{ depatment_ids }}'=''
                OR
                department_id IN splitByChar(',','{{ depatment_ids }}')
            )
        ) AS snick_employee_info
        -- 下拉框-客服姓名
        WHERE (
            '{{ usernames }}'=''
            OR
            username IN splitByChar(',','{{ usernames }}')
        )
    )
) AS eval_info