-- Impala
with t1 as (
    SELECT shop_id,
        day as `date`,
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
    WHERE day between '{{ day.start=week_ago }}' and '{{ day.end=yesterday }}'
        and shop_id = '{{ shop_id=5cac112e98ef4100118a9c9f }}'
),
t2 as (
    select shop_id,
        concat(
            subString(cast(ds_nodash as String), 1, 4),
            '-',
            subString(cast(ds_nodash as String), 5, 2),
            '-',
            subString(cast(ds_nodash as String), 7, 2)
        ) as `date`,
        round(human_avg_resp_interval, 2) as `human_avg_resp_interval`,
        round(
            robot_send_amount * (human_avg_resp_interval - avg_resp_interval) / 3600,
            2
        ) AS `human_save_hours`
    FROM app_mp.shop_stat
    WHERE concat(
            subString(cast(ds_nodash as String), 1, 4),
            '-',
            subString(cast(ds_nodash as String), 5, 2),
            '-',
            subString(cast(ds_nodash as String), 7, 2)
        ) between '{{ day.start=week_ago }}' and '{{ day.end=yesterday }}'
        and shop_id = '{{ shop_id=5cac112e98ef4100118a9c9f }}'
),
t3 as (
    select t1.shop_id,
        t1.`date`,
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
        (auto_reply_pv + click_reply_pv) / received_pv as robot_reply_rate,
        shop_question_rate,
        question_b_rate,
        human_avg_resp_interval,
        human_save_hours
    from t1
        left join t2 using(shop_id, `date`)
)
select shop_id,
    `date` as "日期",
    platform,
    received_cuv as "服务买家人数",
    buyer_cuv as "买家主动咨询人数",
    seller_cuv as "客服发起会话人数",
    robot_cuv,
    received_session as "服务买家人次",
    buyer_session as "买家发起会话",
    seller_session as "客服发起会话",
    robot_session,
    received_pv as "接收问题数",
    identified_pv as "识别问题数",
    round(identified_rate * 100, 2) as "识别率",
    auto_reply_pv as "机器人自动回复",
    click_reply_pv as "人工点击采纳",
    auto_reply_pv + click_reply_pv as "回复数",
    round(if (is_nan(robot_reply_rate), 0, robot_reply_rate) * 100, 2) as "应答率",
    round(shop_question_rate * 100, 2) as "定义问题占比",
    round(question_b_rate * 100, 2) as "行业问题占比",
    human_avg_resp_interval,
    human_save_hours
from t3
where shop_id = '{{ shop_id=5cac112e98ef4100118a9c9f }}'
order by `date` desc;