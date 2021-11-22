SELECT snick,
    cnick,
    msg,
    act,
    msg_time,
    create_time,
    question_b_qid,
    question_b_standard_q,
    plat_goods_id
FROM dwd.mini_xdrs_log
WHERE DAY >= 20211017 AND DAY <= 20211023
AND send_msg_from NOT IN ('0', '1')
AND shop_id = "60d1a78e7a498200151ff24c"
ORDER BY cnick, msg_time
limit 500000 -- trace:ace5332aa34b262e6e40d2f3319c5f10


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
        AND DAY >= 20211019
        AND DAY <= 20211021
        AND act not in ('statistics_send_msg', '')
        AND category IN ("yqhf")
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
    WHERE rank_id % 65 = 36
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
-- trace:a36f89988235e16bfde6d94ebd9010aa


WITH t1 AS (
    SELECT split_part(snick, ':', 1) AS seller_nick,
        cnick,
        DAY,
        create_time,
        uuid() AS sample_id
    FROM dwd.mini_xdrs_log
    WHERE act = 'recv_msg'
        AND platform = "tb"
        AND DAY >= 20211110
        AND DAY <= 20211116
        AND act not in ('statistics_send_msg', '')
        AND split_part(snick, ':', 1) IN ("cntaobao芳芳之佳旗舰店")
        AND cast(question_b_qid AS INTEGER) >= 0
),
t2 AS (
    SELECT *,
        row_number() OVER (
            ORDER BY sample_id
        ) AS rank_id
    FROM t1
),
t3 AS (
    SELECT *
    FROM t2
    WHERE rank_id % 9 = 3
),
x1 AS (
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
        current_sale_stage
    FROM dwd.mini_xdrs_log
    WHERE platform = "tb"
        AND DAY >= 20211110
        AND DAY <= 20211116
        AND act not in ('statistics_send_msg', '')
        AND split_part(snick, ':', 1) IN ("cntaobao芳芳之佳旗舰店")
),
x2 AS (
    SELECT x1.*,
        t3.sample_id,
        if(
            x1.create_time = t3.create_time
            AND x1.act = 'recv_msg',
            1,
            0
        ) AS flag
    FROM x1
        RIGHT JOIN [shuffle] t3 ON x1.seller_nick = t3.seller_nick
        AND x1.cnick = t3.cnick
)
INSERT overwrite xd_tmp.algorithm_sample_data_all PARTITION (mission_id = 'a838d97bbf79f19b59b1807309579573')
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
    x2.flag,
    xd_data.question_b.question,
    x2.is_robot_answer,
    x2.plat_goods_id,
    x2.current_sale_stage
FROM x2
    LEFT JOIN [shuffle] xd_data.question_b ON cast(split_part(x2.question_b_qid, '.', 1) AS integer) = cast(
        split_part(xd_data.question_b.qid, '.', 1) AS integer
    );
-- trace:01471175414706a4a7ddbe77c0dceac4