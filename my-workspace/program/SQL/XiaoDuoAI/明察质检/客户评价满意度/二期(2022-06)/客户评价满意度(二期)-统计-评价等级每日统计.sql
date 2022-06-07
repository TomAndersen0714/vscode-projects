-- 客户评价满意度(二期)-统计-评价等级每日统计
SELECT
    day,
    level_0_cnt AS `非常满意`,
    level_1_cnt AS `满意`,
    level_2_cnt AS `一般`,
    level_3_cnt AS `不满意`,
    level_4_cnt AS `非常不满意`
FROM (
    SELECT
        day,
        sum(eval_code=0) AS level_0_cnt,
        sum(eval_code=1) AS level_1_cnt,
        sum(eval_code=2) AS level_2_cnt,
        sum(eval_code=3) AS level_3_cnt,
        sum(eval_code=4) AS level_4_cnt
    FROM (
        SELECT
            replaceOne(splitByChar(':',user_nick)[1],'cntaobao','') AS seller_nick,
            replaceOne(eval_sender,'cntaobao','') AS snick,
            toUInt32(day) as day,
            eval_code
        FROM ods.kefu_eval_detail_all
        WHERE day BETWEEN toYYYYMMDD(toDate('{{ day.start=week_ago }}')) AND toYYYYMMDD(toDate('{{ day.end=yesterday }}'))
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
    GROUP BY day
) AS eval_daily_stat_info
GLOBAL RIGHT JOIN (
    -- 设定X轴锚点
    SELECT day
    FROM (
        SELECT arrayJoin(
            arrayMap(
                x->toYYYYMMDD(toDate(x)),
                range(toUInt32(toDate('{{ day.start=week_ago }}')), toUInt32(toDate('{{ day.end=yesterday }}') + 1), 1)
            )
        ) AS day
    ) AS time_axis
) AS day_axis
USING(day)
ORDER BY day ASC