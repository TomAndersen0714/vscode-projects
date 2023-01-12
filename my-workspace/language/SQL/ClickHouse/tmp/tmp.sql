SELECT 
    t1.day as "时间",
    received_cuv AS "服务买家人数",
    received_session AS "服务买家人次",
    buyer_session AS "买家发起会话",
    seller_session AS "客服发起会话",
    received_pv AS "接收问题数",
    identified_pv AS "识别问题数",
    round(IF(received_pv = 0 ,0, identified_pv / received_pv )*100,2)AS "识别率",
    auto_reply_pv AS "机器人自动回复",
    click_reply_pv AS "人工点击采纳",
    round(IF(received_pv = 0 ,0, (auto_reply_pv + click_reply_pv) / received_pv )*100,2) AS "应答率",
    round(shop_question_rate*100,2) AS "定义问题占比",
    round(question_b_rate*100,2) AS "行业问题占比",
    buyer_cuv AS "买家主动咨询人数",
    seller_cuv AS "客服发起会话人数"
FROM (
    SELECT
        shop_id,
        day,
        CAST(replace(day,'-','') AS INT) AS _day,
        platform,
        received_cuv,
        buyer_cuv,
        seller_cuv,
        robot_cuv,
        received_session,
        buyer_session,
        seller_session,
        robot_session,
        received_pv,
        identified_pv,
        identified_rate,
        auto_reply_pv,
        click_reply_pv,
        robot_reply_rate,
        shop_question_rate,
        question_b_rate
    FROM app_mp.shop_receive
    WHERE day BETWEEN '{{day_start}}' AND '{{day_end}}'
    AND shop_id ='{{ shop_id }}' 
) AS t1
LEFT JOIN (
    SELECT
        shop_id,
        ds_nodash as day,
        round(human_avg_resp_interval,2)AS `human_avg_resp_interval`,
        round(robot_send_amount * (human_avg_resp_interval - avg_resp_interval) / 3600,2)AS `human_save_hours`
    FROM app_mp.shop_stat
    WHERE ds_nodash BETWEEN CAST(replace('{{day_start}}','-','') AS INT) AND CAST(replace('{{day_end}}','-','') AS INT)
    AND shop_id ='{{ shop_id }}' 
) AS t2
ON t1.shop_id = t2.shop_id AND t1._day = t2.day
ORDER BY `时间` ASC



SELECT 
    day AS "日期",
    received_pv as "接收问题数",
    identified_pv as "识别问题数",
    identified_pv/received_pv as "识别率",
    auto_reply_pv+click_reply_pv as "回复数",
    (auto_reply_pv+click_reply_pv)/received_pv as "应答率"
FROM xd_stat.shop_receive
WHERE shop_id = '{{shop_id}}' 
AND day between '{{start_date}}' and '{{end_date}}'


SELECT 
    day AS "日期",
    received_cuv AS "服务买家人数",
    received_session AS "服务买家人次",
    buyer_session AS "买家发起会话",
    seller_session AS "客服发起会话",
    robot_session AS "机器人发起会话",
    received_pv AS "接收问题数",
    identified_pv AS "识别问题数",
    identified_rate AS "机器人识别率",
    shop_question_rate AS "自定义问题占比",
    question_b_rate AS "行业场景问题占比"
    auto_reply_pv AS "机器人自动回复",
    click_reply_pv AS "人工点击采纳",
    robot_reply_rate AS "机器人应答率"
FROM xd_stat.shop_receive
WHERE day between '{{start_date}}' and '{{end_date}}'
AND shop_id = '{{shop_id}}'
ORDER BY day DESC