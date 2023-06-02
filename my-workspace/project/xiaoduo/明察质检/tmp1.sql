SELECT
    dialog_info.begin_time AS `会话时间`,
    snick_depart_info.department_name AS `子账号分组`,
    dialog_info.snick AS `子账号`,
    snick_depart_info.employee_name AS `客服姓名`,
    dialog_info.real_buyer_nick AS `顾客姓名`,
    dialog_info.order_id AS `订单号`,
    multiIf(
       dialog_info.order_status = '', '',
       dialog_info.order_status = 'created', '已创建',
       dialog_info.order_status = 'deposited', '付定金',
       dialog_info.order_status = 'paid', '已付款',
       dialog_info.order_status = 'shipped', '已发货',
       dialog_info.order_status = 'succeeded', '已成功',
       dialog_info.order_status = 'closed', '已关闭',
       ''
    ) AS `订单状态`,
    order_time AS `订单时间`,
    order_payment AS `订单金额`,
    focus_goods_id AS `焦点商品`
FROM (
    SELECT
        _id AS dialog_id,
        begin_time,
        snick,
        real_buyer_nick,
        order_info_status[1] AS order_status,
        order_info_id[1] AS order_id,
        order_info_time[1] AS order_time,
        order_info_payment[1] AS order_payment,
        focus_goods_id
    FROM dwd.xdqc_dialog_all
    WHERE toYYYYMMDD(begin_time) BETWEEN toYYYYMMDD(toDate('{{ day.start }}'))
        AND toYYYYMMDD(toDate('{{ day.end }}'))
    AND platform = 'tb'
    -- 在会话表里，筛选会话轮次
    AND qa_round_sum >= 1
    AND seller_nick IN ['顾家家居旗舰店', 'kuka顾家家居旗舰店']
    AND _id GLOBAL IN (
        SELECT DISTINCT
            dialog_id
        FROM xqc_ods.message_all
        WHERE day BETWEEN toYYYYMMDD(toDate('{{ day.start }}'))
            AND toYYYYMMDD(toDate('{{ day.end }}'))
        AND platform = 'tb'
        AND seller_nick IN ['顾家家居旗舰店', 'kuka顾家家居旗舰店']
        -- 筛选枚举值：客服发送消息1，买家发送消息0
        AND source = 1
        -- 筛选指定焦点商品
        -- AND plat_goods_id = '682312202291'
        -- 包含指定关键字
        AND (
            content LIKE '%企微%' OR
            content LIKE '%企业微信%' OR
            content LIKE '%香薰片' OR
            content LIKE '%VX%' OR
            content LIKE '%企业VX%' OR
            content LIKE '%微信%' OR
            content LIKE '%工作号%' OR
            content LIKE '%工作微信%' OR
            content LIKE '%vx%'
        )
        -- AND content LIKE '%682312202291%'
    )
) AS dialog_info
GLOBAL LEFT JOIN (
    SELECT department_name, snick, employee_name
    FROM xqc_dim.snick_full_info_all
    WHERE day = toYYYYMMDD(yesterday())
    AND company_id = '614d86d84eed94e6fc980b1c'
    AND seller_nick IN ['顾家家居旗舰店', 'kuka顾家家居旗舰店']
) AS snick_depart_info
USING(snick)