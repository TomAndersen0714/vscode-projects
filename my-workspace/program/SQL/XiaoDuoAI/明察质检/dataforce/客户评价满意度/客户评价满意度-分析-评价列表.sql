-- 客户评价满意度-分析-评价列表
SELECT
    replaceOne(splitByChar(':',user_nick)[1],'cntaobao','') AS seller_nick,
    replaceOne(eval_sender,'cntaobao','') AS snick,
    replaceOne(eval_recer,'cntaobao','') AS cnick,
    seller_nick AS `店铺`,
    CASE
        WHEN eval_code=0 THEN '非常满意'
        WHEN eval_code=1 THEN '满意'
        WHEN eval_code=2 THEN '一般'
        WHEN eval_code=3 THEN '不满意'
        WHEN eval_code=4 THEN '非常不满意'
        ELSE '其他'
    END AS `最后一次评价结果`,
    snick AS `客服子账号`,
    cnick AS `顾客名称`,
    eval_time AS `最后一次评价时间`,
    send_time,
    CASE
        WHEN source=0 THEN '客服邀评'
        WHEN source=1 THEN '消费者自主评价'
        WHEN source=2 THEN '系统邀评'
        ELSE '其他'
    END AS `评价来源`,
    formatDateTime(parseDateTimeBestEffort(toString(day)),'%Y-%m-%d') AS `会话日期` 
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
