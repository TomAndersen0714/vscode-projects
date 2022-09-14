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
        seller_nick,
        snick,
        eval_code
    FROM xqc_ods.dialog_eval_all
    WHERE day BETWEEN toYYYYMMDD(toDate('{{ day.start=yesterday }}'))
        AND toYYYYMMDD(toDate('{{ day.end=yesterday }}'))
    -- 过滤买家已评价记录
    AND eval_time != ''
    -- 下拉框-店铺
    AND (
        '{{ seller_nicks }}'=''
        OR
        seller_nick IN splitByChar(',',replaceAll('{{ seller_nicks }}', '星环#', ''))
    )
    -- 筛选指定子账号
    AND snick GLOBAL IN (
        -- 当前企业对应的子账号
        SELECT DISTINCT
            snick
        FROM xqc_dim.snick_full_info_all
        WHERE day = toYYYYMMDD(yesterday())
        AND platform = 'tb'
        AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
        -- 下拉框-子账号分组id
        AND (
            '{{ department_ids }}'=''
            OR
            department_id IN splitByChar(',','{{ department_ids }}')
        )
        -- 下拉框-客服姓名
        AND (
            '{{ usernames }}'=''
            OR
            employee_name IN splitByChar(',','{{ usernames }}')
        )
    )
) AS eval_info
ORDER BY eval_code ASC
