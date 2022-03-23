-- 客户满意度评价-统计-评价满意度每日统计
SELECT
    eval_code,
    day,
    cnt
FROM (
    SELECT
        eval_code,
        day,
        SUM(1) AS cnt 
    FROM (
        SELECT
            replaceOne(splitByChar(':',user_nick)[1],'cntaobao','') AS seller_nick,
            replaceOne(eval_sender,'cntaobao','') AS snick,
            toUInt32(day) as day,
            eval_code
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
            -- 下拉框-客服名称
            WHERE (
                '{{ usernames }}'=''
                OR
                username IN splitByChar(',','{{ usernames }}')
            )
        )
    ) AS eval_daily_info
    GROUP BY eval_code,day
) AS eval_daily_stat_info
GLOBAL RIGHT JOIN (
    -- 设定X轴锚点
    SELECT day, eval_code
    FROM (
        SELECT arrayJoin(
            arrayMap(
                x->toYYYYMMDD(toDate(x)),
                range(toUInt32(toDate('{{ day.start=week_ago }}')), toUInt32(toDate('{{ day.end=today }}') + 1), 1)
            )
        ) AS day
    ) AS time_axis
    GLOBAL CROSS JOIN (
        SELECT DISTINCT eval_code
        FROM ods.kefu_eval_detail_all
        WHERE day BETWEEN toYYYYMMDD(parseDateTimeBestEffort('{{ day.start=week_ago }}')) AND toYYYYMMDD(parseDateTimeBestEffort('{{ day.end=today }}'))
        AND replaceOne(splitByChar(':',user_nick)[1],'cntaobao','') IN splitByChar(',',replaceAll('{{ seller_nick }}', '星环#', ''))
        AND eval_time != ''
        AND if('{{ snick }}' != '',  replaceOne(eval_sender,'cntaobao','') LIKE '%{{ snick }}%', 1)
    ) AS distinct_eval_code
) AS day_eval_code_axis
USING(day, eval_code)
ORDER BY day, eval_code ASC