-- 客户满意度评价-分析-评价来源统计
SELECT
    CASE
        WHEN source=0 THEN '客服邀评'
        WHEN source=1 THEN '消费者自主评价'
        WHEN source=2 THEN '系统邀评'
        ELSE '其他'
    END AS `评价来源`,
    COUNT(1) AS `评价次数` 
FROM (
    SELECT
        replaceOne(splitByChar(':',user_nick)[1],'cntaobao','') AS seller_nick,
        replaceOne(eval_sender,'cntaobao','') AS snick,
        source,
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
        -- 下拉框-客服名称
        WHERE (
            '{{ usernames }}'=''
            OR
            username IN splitByChar(',','{{ usernames }}')
        )
    )
) AS eval_info
GROUP BY source
ORDER BY source DESC