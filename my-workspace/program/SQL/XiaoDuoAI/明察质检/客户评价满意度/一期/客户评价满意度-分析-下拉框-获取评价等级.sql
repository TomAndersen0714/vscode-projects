-- 客户评价满意度-分析-下拉框-获取评价等级
SELECT DISTINCT
    CASE
        WHEN eval_code=0 THEN '非常满意//0'
        WHEN eval_code=1 THEN '满意//1'
        WHEN eval_code=2 THEN '一般//2'
        WHEN eval_code=3 THEN '不满意//3'
        WHEN eval_code=4 THEN '非常不满意//4'
        ELSE CONCAT('其他','//',toString(eval_code))
    END AS `评价等级`
FROM (
    SELECT
        replaceOne(splitByChar(':', user_nick)[1], 'cntaobao', '') AS seller_nick,
        replaceOne(user_nick, 'cntaobao', '') AS snick,
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
) AS eval_info
ORDER BY eval_code ASC
