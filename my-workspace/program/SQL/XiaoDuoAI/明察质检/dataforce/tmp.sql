WITH t1 AS (
    SELECT split_part(snick, ':', 1) AS seller_nick,
        cnick,
        category,
        act,
        msg,
        remind_answer,
        cast(msg_time AS String) AS msg_time,
        question_b_qid,
        question_b_proba,
        MODE,
        DAY,
        create_time,
        is_robot_answer,
        plat_goods_id,
        current_sale_stage,
        uuid() AS sample_id
    FROM dwd.mini_xdrs_log
    WHERE act = 'recv_msg'
        AND platform = "tb"
        AND DAY >= 20220411
        AND DAY <= 20220413
        AND act not in ('statistics_send_msg', '')
        AND category IN ("nbs")
        AND cast(question_b_qid AS INTEGER) >= 0
        AND question_b_proba > 0.900000
),
t2 AS (
    SELECT *,
        row_number() OVER (
            ORDER BY sample_id
        ) AS rank_id
    FROM t1
),
x2 AS (
    SELECT *
    FROM t2
    WHERE rank_id % 165 = 73
)
SELECT x2.seller_nick,
    x2.cnick,
    x2.category,
    x2.act,
    x2.msg,
    x2.remind_answer,
    x2.msg_time,
    x2.question_b_qid,
    x2.question_b_proba,
    x2.MODE,
    x2.DAY,
    x2.create_time,
    x2.sample_id,
    1 AS flag,
    xd_data.question_b.question,
    x2.is_robot_answer,
    x2.plat_goods_id,
    x2.current_sale_stage
FROM x2
    LEFT JOIN xd_data.question_b ON cast(split_part(x2.question_b_qid, '.', 1) AS integer) = cast(
        split_part(xd_data.question_b.qid, '.', 1) AS integer
    )
WHERE act = "recv_msg"
    AND cast(question_b_qid AS INTEGER) >= 0
ORDER BY seller_nick,
    cnick,
    msg_time;
-- trace:7ecd132a7205544e4c998ea354104d60