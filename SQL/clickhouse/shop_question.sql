SELECT concat(
        '****',
        '',
        substringUTF8(snick, lengthUTF8(snick) / 2)
    ) AS `卖家昵称`,
    concat(
        '****',
        '',
        substringUTF8(cnick, lengthUTF8(cnick) / 2)
    ) AS `买家昵称`,
    act AS `动作`,
    msg AS `消息内容`,
    remind_answer AS `机器人提示内容`,
    toString(
        toDateTime(cast(msg_time AS Int64), 'Asia/Shanghai')
    ) AS `客户端时间`,
    create_time AS `创建时间`,
    plat_goods_id AS `当前商品`,
    question AS `问题分类`,
    question_b_qid AS `QID`,
    question_b_proba AS `问题分类概率`,
    current_sale_stage AS `销售阶段`,
    `mode` AS `模式`,
    msg_id AS `msgid`,
    if(
        reason_zh != '',
        concat(reason_zh, '|', sub_reason_zh),
        ''
    ) AS `未回复原因`
FROM (
        -- 优先级1:通过关键字匹配店铺自定义问题
        SELECT *
        FROM (
                SELECT snick,
                    cnick,
                    act,
                    msg,
                    remind_answer,
                    msg_time,
                    qa_id,
                    plat_goods_id,
                    shop_question_id,
                    question_b_qid,
                    question_b_proba,
                    current_sale_stage,
                    `mode`,
                    create_time,
                    msg_id
                FROM ods.xdrs_log_all
                WHERE shop_question_id != ''
                    AND shop_question_type = 'keyword'
                    AND `day` = { { day } }
                    AND act != ''
                    AND { { snick_sql } } { { cnick_sql } } -- AND snick = ''
                    AND create_time BETWEEN '{{ start_time }}' AND '{{ end_time }}'
            ) AS t1 GLOBAL
            LEFT JOIN (
                SELECT _id AS shop_question_id,
                    question
                FROM dim.shop_question_all
            ) AS t2 USING shop_question_id
        UNION ALL
        -- 优先级2:匹配默认行业场景问题
        SELECT *
        FROM (
                SELECT snick,
                    cnick,
                    act,
                    msg,
                    remind_answer,
                    msg_time,
                    qa_id,
                    plat_goods_id,
                    shop_question_id,
                    question_b_qid,
                    question_b_proba,
                    current_sale_stage,
                    `mode`,
                    create_time,
                    msg_id
                FROM ods.xdrs_log_all
                WHERE shop_question_id = ''
                    AND `day` = { { day } }
                    AND act != ''
                    AND { { snick_sql } } { { cnick_sql } } -- AND snick = ''
                    AND create_time BETWEEN '{{ start_time }}' AND '{{ end_time }}'
            ) t1
            LEFT JOIN (
                SELECT qid AS question_b_qid,
                    question
                FROM ods.question_b
            ) t2 USING question_b_qid
        UNION ALL
        -- 优先级3:通过整句匹配店铺自定义问题
        SELECT *
        FROM (
                SELECT snick,
                    cnick,
                    act,
                    msg,
                    remind_answer,
                    msg_time,
                    qa_id,
                    plat_goods_id,
                    shop_question_id,
                    question_b_qid,
                    question_b_proba,
                    current_sale_stage,
                    `mode`,
                    create_time,
                    msg_id
                FROM ods.xdrs_log_all
                WHERE shop_question_id != ''
                    AND shop_question_type != 'keyword'
                    AND `day` = { { day } }
                    AND act != ''
                    AND { { snick_sql } } { { cnick_sql } } -- AND snick = ''
                    AND create_time BETWEEN '{{ start_time }}' AND '{{ end_time }}'
            ) AS t1 GLOBAL
            LEFT JOIN (
                SELECT _id AS shop_question_id,
                    question
                FROM dim.shop_question_all
            ) AS t2 USING shop_question_id
    )
    LEFT JOIN (
        SELECT qa_id,
            multiIf(
                reason = 1,
                '通用回复未回复',
                reason = 2,
                '关联商品未回复',
                reason = 3,
                '关联商品类型未回复',
                reason = 4,
                '未达到发送阈值未回复',
                reason = 5,
                '因接待设置未回复',
                '未知'
            ) AS reason_zh,
            reason,
            multiIf(
                sub_reason = 1,
                '辅助模式自动发送关闭',
                sub_reason = 2,
                '无人值守自动发送关闭',
                sub_reason = 3,
                '答案缺失（时效）',
                sub_reason = 4,
                '答案缺失',
                sub_reason = 10,
                '机器人前置接待（客户端）',
                sub_reason = 11,
                '针对特定买家不自动回复（客户端）',
                sub_reason = 12,
                '人工抢答未回复（客户端）',
                sub_reason = 13,
                '相同问题不重复回复',
                sub_reason = 14,
                '仅提示模式未自动回复',
                sub_reason = 15,
                '售后问题不自动回复',
                sub_reason = 16,
                '物流答案耗尽',
                sub_reason = 17,
                '账号一样不回复',
                sub_reason = 18,
                '等待焦点商品链接未回复（客户端）',
                sub_reason = 19,
                '转接后转接前问题不回复（客户端）',
                sub_reason = 30,
                '未开启智能辅助开关（小程序）',
                '未知'
            ) AS sub_reason_zh,
            sub_reason
        FROM ods.xdrs_no_reply_logs_all
        WHERE `day` = { { day } }
            AND shop_id = '{{ shop_id }}' -- AND snick = ''
            { { cnick_sql } }
    ) t3 USING qa_id
ORDER BY cnick,
    create_time