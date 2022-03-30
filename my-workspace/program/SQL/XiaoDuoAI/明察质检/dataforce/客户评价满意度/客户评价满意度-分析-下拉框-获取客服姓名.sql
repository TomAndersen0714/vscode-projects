-- 客户评价满意度-分析-下拉框-获取客服姓名
SELECT DISTINCT 
    username AS `客服姓名`
FROM ods.xinghuan_employee_all
WHERE day = toYYYYMMDD(yesterday())
AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
AND _id IN (
    SELECT DISTINCT employee_id
    FROM ods.xinghuan_employee_snick_all
    WHERE day = toYYYYMMDD(yesterday())
    AND platform = 'tb'
    AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
    -- 下拉框-子账号分组id
    AND (
        '{{ depatment_ids }}'=''
        OR
        department_id IN splitByChar(',','{{ depatment_ids }}')
    )
    -- 被评价过的子账号
    AND snick IN (
        SELECT DISTINCT
            snick
        FROM (
            SELECT
                replaceOne(splitByChar(':',user_nick)[1],'cntaobao','') AS seller_nick,
                replaceOne(eval_sender,'cntaobao','') AS snick,
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
            -- 下拉框-店铺名
            AND (
                '{{ seller_nicks }}'=''
                OR
                seller_nick IN splitByChar(',','{{ seller_nicks }}')
            )
            -- 当前企业对应的子账号
            AND snick IN (
                SELECT distinct snick
                FROM ods.xinghuan_employee_snick_all
                WHERE day = toYYYYMMDD(yesterday())
                AND platform = 'tb'
                AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
            )
        ) AS eval_info
    )
)
ORDER BY username COLLATE 'zh'