-- 明察质检-店铺-客服维度每日加分AI质检项统计
-- PS: 方太 company_id = '5f747ba42c90fd0001254404'
SELECT 
    day AS `日期`,
    seller_nick AS `店铺`,
    employee_id AS `员工ID`,
    employee_name AS `员工姓名`,
    count(1) AS `AI质检项总触发次数`,
    sum(`excellent_type`=1) AS `需求挖掘`,
    sum(`excellent_type`=2) AS `商品细节解答`,
    sum(`excellent_type`=3) AS `卖点传达`,
    sum(`excellent_type`=4) AS `商品推荐`,
    sum(`excellent_type`=5) AS `退换货理由修改`,
    sum(`excellent_type`=6) AS `主动跟进`,
    sum(`excellent_type`=7) AS `无货挽回`,
    sum(`excellent_type`=8) AS `活动传达`,
    sum(`excellent_type`=9) AS `店铺保障`,
    sum(`excellent_type`=10) AS `催拍催付`,
    sum(`excellent_type`=11) AS `核对地址`,
    sum(`excellent_type`=12) AS `好评引导`,
    sum(`excellent_type`=13) AS `优秀结束语`,
    sum(`excellent_type`=14) AS `试听课跟单`
FROM (
    SELECT
        day,
        message_info.seller_nick AS seller_nick,
        snick_employee_info.employee_id AS employee_id,
        snick_employee_info.employee_name AS employee_name,
        arrayJoin(message_info.excellent) AS excellent_type
    FROM (
            SELECT
                day,
                seller_nick,
                snick,
                excellent -- 加分AI质检项类型
            FROM xqc_ods.message_all
            WHERE day BETWEEN toYYYYMMDD(toDate('{{date_range.start}}')) AND toYYYYMMDD(toDate('{{date_range.end}}'))
            AND snick IN (
                SELECT distinct snick
                FROM ods.xinghuan_employee_snick_all
                WHERE day BETWEEN toYYYYMMDD(toDate('{{date_range.start}}')) AND toYYYYMMDD(toDate('{{date_range.end}}'))
                AND company_id = '{{ company_id }}'
            )
            -- 人工发送
            AND auto_send = if('{{机器人/人工发送}}'='True','True','False')
            -- 客服发送
            AND source = if('{{客服/买家发送}}'='True',1,0)
    ) AS message_info
    GLOBAL RIGHT JOIN (
        SELECT
            day,
            employee_id,
            employee_info.employee_name AS employee_name,
            snick_info.snick AS snick
        FROM (
            SELECT *
            FROM ods.xinghuan_employee_snick_all
            WHERE day BETWEEN toYYYYMMDD(toDate('{{date_range.start}}')) AND toYYYYMMDD(toDate('{{date_range.end}}'))
            AND company_id = '{{ company_id }}'
        ) AS snick_info
        GLOBAL RIGHT JOIN (
            SELECT
                day,
                _id AS employee_id, 
                username AS employee_name
            FROM ods.xinghuan_employee_all
            WHERE day BETWEEN toYYYYMMDD(toDate('{{date_range.start}}')) AND toYYYYMMDD(toDate('{{date_range.end}}'))
            AND company_id = '{{ company_id }}'
        ) AS employee_info
        USING(employee_id, day)
    ) AS snick_employee_info
    ON toInt32(message_info.day) = snick_employee_info.day
    AND message_info.snick = snick_employee_info.snick
)
GROUP BY day, seller_nick, employee_id, employee_name
ORDER BY day ASC, `AI质检项客服总触发次数` DESC