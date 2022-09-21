-- 方太京东聊天日志抽取
ALTER TABLE ft_ods.xdrs_logs_local ON CLUSTER cluster_3s_2r DROP PARTITION {{ds_nodash}};

SELECT sleep(3);

INSERT INTO ft_ods.xdrs_logs_all
SELECT
    question_type,
    send_msg_from,
    snick,
    act,
    mode,
    ms_msg_time,
    msg,
    msg_id,
    task_id,
    answer_explain,
    intent,
    mp_category,
    shop_id,
    create_time,
    mp_version,
    qa_id,
    question_b_proba,
    question_b_standard_q,
    is_identified,
    current_sale_stage,
    question_b_qid,
    remind_answer,
    cnick,
    '' AS real_buyer_nick,
    platform,
    msg_time,
    plat_goods_id,
    answer_id,
    robot_answer,
    transfer_type,
    transfer_to,
    transfer_from,
    shop_question_type,
    shop_question_id,
    no_reply_reason,
    no_reply_sub_reason,
    '' AS msg_scenes_source,
    '' AS msg_content_type,
    '' AS trace_id,
    day,
    precise_intent_id,
    precise_intent_standard_q,
    cond_answer_id
FROM ods.xdrs_logs_all
WHERE day = {{ds_nodash}}
AND platform = 'jd'
AND shop_id IN '{{shop_ids}}';


-- 等待数据写入
SELECT sleep(3);