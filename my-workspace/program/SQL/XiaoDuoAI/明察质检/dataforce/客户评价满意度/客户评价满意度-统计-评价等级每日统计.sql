-- 客户评价满意度-统计-评价等级每日统计
SELECT
    CASE
        WHEN eval_code=0 THEN '非常满意'
        WHEN eval_code=1 THEN '满意'
        WHEN eval_code=2 THEN '一般'
        WHEN eval_code=3 THEN '不满意'
        WHEN eval_code=4 THEN '非常不满意'
        ELSE '其他'
    END AS `评价等级`,
    day,
    cnt
FROM (
    SELECT
        day,
        eval_code,
        SUM(1) AS cnt 
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
    ) AS eval_daily_info
    GROUP BY day, eval_code
) AS eval_daily_stat_info
GLOBAL RIGHT JOIN (
    -- 设定X轴锚点
    SELECT day, eval_code
    FROM (
        SELECT arrayJoin(
            arrayMap(
                x->toYYYYMMDD(toDate(x)),
                range(toUInt32(toDate('{{ day.start=week_ago }}')), toUInt32(toDate('{{ day.end=yesterday }}') + 1), 1)
            )
        ) AS day
    ) AS time_axis
    GLOBAL CROSS JOIN (
        SELECT DISTINCT eval_code
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
        ) AS eval_code_info
    ) AS distinct_eval_code
) AS day_eval_code_axis
USING(day, eval_code)
ORDER BY day, eval_code ASC