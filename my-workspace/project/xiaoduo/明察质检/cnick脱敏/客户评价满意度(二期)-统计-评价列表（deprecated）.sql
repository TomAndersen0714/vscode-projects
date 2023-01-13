-- 客户评价满意度(二期)-统计-评价列表
SELECT
    eval_info.*,
    dim_snick_department.department_name AS department_name,
    dim_snick_department.employee_name AS employee_name,

    day AS `日期`,
    seller_nick AS `店铺`,
    employee_name AS `客服姓名`,
    snick AS `客服子账号`,
    department_name AS `子账号分组`,

    cnick AS `顾客名称`,
    if(is_invited, send_time, '-') AS `邀评时间`,
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
        day,
        seller_nick,
        snick,
        cnick,
        max(dialog_id) AS dialog_id,
        source,
        send_time,
        is_invited,

        arraySort(groupArrayIf(eval_time, eval_time !='')) AS eval_times,
        arraySort((x,y)->y, groupArrayIf(eval_code, eval_time != ''), groupArrayIf(eval_time, eval_time != '')) AS eval_codes,
        toString(eval_times[-1]) AS latest_eval_time,
        if(latest_eval_time != '', eval_codes[-1], -1) AS latest_eval_code,
        if(latest_eval_time != '', eval_codes[1], -1) AS first_eval_code

    FROM (
        SELECT
            seller_nick,
            snick,
            cnick,
            dialog_id,
            eval_code,
            eval_time,
            send_time,
            source,
            if(eval_time != '' AND source = 1, 0, 1) AS is_invited,
            day
        FROM xqc_ods.dialog_eval_all
        WHERE day BETWEEN toYYYYMMDD(toDate('{{ day.start=week_ago }}'))
            AND toYYYYMMDD(toDate('{{ day.end=yesterday }}'))
        AND platform = 'tb'
        -- 下拉框-店铺
        AND (
            '{{ seller_nicks }}'=''
            OR
            seller_nick IN splitByChar(',',replaceAll('{{ seller_nicks }}', '星环#', ''))
        )
        -- 当前企业对应的店铺
        AND seller_nick GLOBAL IN (
            SELECT DISTINCT
                seller_nick
            FROM xqc_dim.xqc_shop_all
            WHERE day = toYYYYMMDD(yesterday())
            AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
            AND platform = 'tb'
        )
        -- 当前企业对应的子账号
        AND snick GLOBAL IN (
            SELECT DISTINCT snick
            FROM (
                SELECT DISTINCT snick, username
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
    ) AS ods_dialog_eval
    GROUP BY day, seller_nick, snick, cnick, source, send_time, is_invited
    -- 单选-评价类型
    HAVING (
        ('{{ type=全部 }}'='全部')
        OR
        ('{{ type=全部 }}'='未评价' AND latest_eval_time = '')
        OR
        ('{{ type=全部 }}'='满意' AND latest_eval_time != '' AND latest_eval_code IN (0, 1))
        OR
        ('{{ type=全部 }}'='不满意' AND latest_eval_time != '' AND latest_eval_code IN (2, 3, 4))
    )
    ORDER BY latest_eval_time DESC
    LIMIT 15000
) AS eval_info
GLOBAL LEFT JOIN (
    -- 关联子账号分组/子账号员工信息
    SELECT
        snick, employee_name, superior_name, department_id, department_name
    FROM (
        SELECT snick, employee_name, superior_name, department_id
        FROM (
            -- 查询对应企业-平台的所有子账号及其部门ID, 不论其是否绑定员工
            SELECT snick, department_id, employee_id
            FROM ods.xinghuan_employee_snick_all
            WHERE day = toYYYYMMDD(yesterday())
            AND platform = 'tb'
            AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
        ) AS snick_info
        GLOBAL LEFT JOIN (
            SELECT
                _id AS employee_id, username AS employee_name, superior_name
            FROM ods.xinghuan_employee_all
            WHERE day = toYYYYMMDD(yesterday())
            AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
        ) AS employee_info
        USING(employee_id)
    ) AS snick_info
    GLOBAL RIGHT JOIN (
        SELECT
            _id AS department_id, full_name AS department_name
        FROM xqc_dim.snick_department_full_all
        WHERE day = toYYYYMMDD(yesterday())
        AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
    ) AS department_info
    USING (department_id)
) AS dim_snick_department
USING(snick)