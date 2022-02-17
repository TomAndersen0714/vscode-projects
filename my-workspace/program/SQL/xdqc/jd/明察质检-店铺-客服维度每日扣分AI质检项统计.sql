-- 明察质检-店铺-客服维度每日扣分AI质检项统计
-- PS: 方太 company_id = '5f747ba42c90fd0001254404'
SELECT 
    day AS `日期`,
    seller_nick AS `店铺`,
    employee_id AS `员工ID`,
    employee_name AS `员工姓名`,
    count(1) AS `AI质检项客服总触发次数`,
    sum(`abnormal_type`=1) AS `非客服结束会话`,
    sum(`abnormal_type`=2) AS `漏跟进`,
    sum(`abnormal_type`=3) AS `快捷语重复`,
    sum(`abnormal_type`=4) AS `生硬拒绝`,
    sum(`abnormal_type`=5) AS `欠缺安抚`,
    sum(`abnormal_type`=6) AS `答非所问`,
    sum(`abnormal_type`=7) AS `单字回复`,
    sum(`abnormal_type`=8) AS `单句响应慢`,
    sum(`abnormal_type`=9) AS `产品不熟悉`,
    sum(`abnormal_type`=10) AS `活动不熟悉`,
    sum(`abnormal_type`=11) AS `内部回复慢`,
    sum(`abnormal_type`=12) AS `严重超时`,
    sum(`abnormal_type`=13) AS `撤回消息`,
    sum(`abnormal_type`=14) AS `单表情回复`,
    sum(`abnormal_type`=15) AS `异常撤回`,
    sum(`abnormal_type`=16) AS `转接前未有效回复`,
    sum(`abnormal_type`=17) AS `超时未回复`,
    sum(`abnormal_type`=18) AS `顾客撤回`,
    sum(`abnormal_type`=19) AS `前后回复矛盾`,
    sum(`abnormal_type`=20) AS `撤回机器人消息`,
    sum(`abnormal_type`=21) AS `第三方投诉或曝光`,
    sum(`abnormal_type`=22) AS `顾客提及投诉或举报`,
    sum(`abnormal_type`=23) AS `差评或要挟差评`,
    sum(`abnormal_type`=24) AS `反问/质疑顾客`,
    sum(`abnormal_type`=25) AS `违禁词`,
    sum(`abnormal_type`=26) AS `客服冷漠讥讽`,
    sum(`abnormal_type`=27) AS `顾客怀疑假货`,
    sum(`abnormal_type`=28) AS `客服态度消极敷衍`,
    sum(`abnormal_type`=29) AS `售后不满意`
FROM (
    SELECT
        day,
        message_info.seller_nick AS seller_nick,
        snick_employee_info.employee_id AS employee_id,
        snick_employee_info.employee_name AS employee_name,
        arrayJoin(message_info.abnormal) AS abnormal_type
    FROM (
            SELECT
                day,
                seller_nick,
                snick,
                abnormal -- 扣分AI质检项类型
            FROM xqc_ods.message_all
            WHERE day BETWEEN toYYYYMMDD(toDate('{{date_range.start}}')) AND toYYYYMMDD(toDate('{{date_range.end}}'))
            AND snick GLOBAL IN (
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