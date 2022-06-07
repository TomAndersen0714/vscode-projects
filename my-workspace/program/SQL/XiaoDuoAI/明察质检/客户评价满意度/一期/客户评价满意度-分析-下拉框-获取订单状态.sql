-- 客户评价满意度-分析-获取订单状态
SELECT DISTINCT
    CASE
        WHEN order_info_status[1]='' THEN '未下单'
        WHEN order_info_status[1]='created' THEN '已下单'
        WHEN order_info_status[1]='deposited' THEN '已付定金'
        WHEN order_info_status[1]='paid' THEN '已付款'
        WHEN order_info_status[1]='shipped' THEN '已发货'
        WHEN order_info_status[1]='succeeded' THEN '已确认收货'
        WHEN order_info_status[1]='closed' THEN '已关闭'
    END AS `订单状态`
FROM dwd.xdqc_dialog_all
WHERE toYYYYMMDD(begin_time) BETWEEN toYYYYMMDD(toDate('{{ day.start=week_ago }}')) AND toYYYYMMDD(toDate('{{ day.end=yesterday }}'))
AND snick GLOBAL IN (
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
-- 下拉框-店铺名
AND (
    '{{ seller_nicks }}'=''
    OR
    seller_nick IN splitByChar(',','{{ seller_nicks }}')
)
-- 过滤已有状态
AND order_info_status[1] IN ['','created','deposited','paid','shipped','succeeded','closed']