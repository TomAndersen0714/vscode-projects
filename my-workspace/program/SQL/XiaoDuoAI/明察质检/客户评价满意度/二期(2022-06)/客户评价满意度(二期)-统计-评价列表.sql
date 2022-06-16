-- 客户评价满意度(二期)-分析-评价列表
SELECT
    dialog_id,
    seller_nick,
    snick,
    cnick,
    source,
    send_time,

    arraySort(groupArrayIf(_eval_time, _eval_time != '')) AS eval_times,
    arraySort((x,y)->y, groupArray(_eval_code, _eval_time != ''), groupArray(_eval_time, _eval_time != '')) AS eval_codes,
    toString(eval_times[-1]) AS latest_eval_time,
    if(latest_eval_time != '', eval_codes[-1], -1) AS latest_eval_code,
    if(latest_eval_time != '', eval_codes[1], -1) AS first_eval_code,

    seller_nick AS `店铺`,
    snick AS `客服子账号`,
    cnick AS `顾客名称`,
    if(source!=1, send_time, '-') AS `邀评时间`,
    if(latest_eval_time != '', latest_eval_time, '-') AS `最新评价时间`,
    CASE
        WHEN latest_eval_code=0 THEN '非常满意'
        WHEN latest_eval_code=1 THEN '满意'
        WHEN latest_eval_code=2 THEN '一般'
        WHEN latest_eval_code=3 THEN '不满意'
        WHEN latest_eval_code=4 THEN '非常不满意'
        ELSE '-'
    END AS `最新评价结果`,
    CASE
        WHEN source=0 THEN '客服邀评'
        WHEN source=1 THEN '消费者自主评价'
        WHEN source=2 THEN '系统邀评'
        ELSE '-'
    END AS `评价来源`,
    CASE
        WHEN (first_eval_code IN (0, 1)) AND (latest_eval_code IN (2, 3, 4)) THEN '满意改不满意'
        WHEN (first_eval_code IN (2, 3, 4)) AND (latest_eval_code IN (0, 1)) THEN '不满意改满意'
        ELSE '-'
    END AS `评价修改记录`,
    if(
        (
            latest_eval_time = ''
            OR
            dateDiff('hour', toDateTime(toDateTime64(latest_eval_time, 0)), now()) <= 24
        ),
        '是',
        '否'
    ) AS `是否可挽回`
FROM (
    SELECT
        dialog_id,
        user_nick,
        if(_eval_time = '', -1, eval_code) AS _eval_code,
        eval_recer,
        eval_sender,
        if(source = 1, send_time, eval_time) AS _eval_time,
        send_time,
        source,

        replaceOne(splitByChar(':', user_nick)[1], 'cntaobao','') AS seller_nick,
        replaceOne(user_nick, 'cntaobao', '') AS snick,
        replaceOne(eval_recer, 'cntaobao', '') AS cnick
    FROM xqc_ods.snick_eval_all
    WHERE day BETWEEN toYYYYMMDD(toDate('{{ day.start=week_ago }}'))
        AND toYYYYMMDD(toDate('{{ day.end=yesterday }}'))
    -- 下拉框-店铺
    AND (
        '{{ seller_nicks }}'=''
        OR
        seller_nick IN splitByChar(',',replaceAll('{{ seller_nicks }}', '星环#', ''))
    )
    -- 当前企业对应的子账号
    AND user_nick GLOBAL IN (
        SELECT DISTINCT plat_snick
        FROM (
            SELECT distinct concat('cntaobao',snick) AS plat_snick, username
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
) AS ods_snick_eval
GROUP BY dialog_id, seller_nick, snick, cnick, source, send_time
-- 单选-评价类型
HAVING (
    ('{{ type }}'='全部')
    OR
    ('{{ type }}'='未评价' AND latest_eval_time = '')
    OR
    ('{{ type }}'='满意' AND latest_eval_time != '' AND latest_eval_code IN (0, 1))
    OR
    ('{{ type }}'='不满意' AND latest_eval_time != '' AND latest_eval_code IN (2, 3, 4))
)
-- -- 下拉框-评价等级
-- AND (
--     '{{ latest_eval_codes }}'=''
--     OR
--     toString(latest_eval_code) IN splitByChar(',','{{ latest_eval_codes }}')
-- )
ORDER BY latest_eval_time DESC