-- 客户评价满意度-下拉框-获取店铺
SELECT DISTINCT
    seller_nick
FROM (
    SELECT
        replaceOne(splitByChar(':', user_nick)[1], 'cntaobao', '') AS seller_nick,
        replaceOne(user_nick, 'cntaobao', '') AS snick,
        eval_code
    FROM ods.kefu_eval_detail_all
    WHERE day BETWEEN toYYYYMMDD(toDate('{{day.start=week_ago}}')) AND toYYYYMMDD(toDate('{{day.end=yesterday}}'))
    -- 过滤买家已评价记录
    AND eval_time != ''
    -- 下拉框-评价等级
    AND (
        '{{ eval_codes }}'=''
        OR
        toString(eval_code) IN splitByChar(',','{{ eval_codes }}')
    )
) AS eval_info
WHERE snick IN (
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
ORDER BY seller_nick
