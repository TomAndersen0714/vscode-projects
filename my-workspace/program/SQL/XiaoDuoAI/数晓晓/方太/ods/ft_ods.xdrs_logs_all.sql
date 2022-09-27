CREATE DATABASE IF NOT EXISTS ft_ods ON CLUSTER cluster_3s_2r
ENGINE=Ordinary

-- DROP TABLE ft_ods.xdrs_logs_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE ft_ods.xdrs_logs_local ON CLUSTER cluster_3s_2r
(
    `question_type` Int32,
    `send_msg_from` Int32,
    `snick` String,
    `act` String,
    `mode` String,
    `ms_msg_time` Int64,
    `msg` String,
    `msg_id` String,
    `task_id` String,
    `answer_explain` String,
    `intent` String,
    `mp_category` String,
    `shop_id` String,
    `create_time` DateTime64(6),
    `mp_version` Int32,
    `qa_id` String,
    `question_b_proba` String,
    `question_b_standard_q` String,
    `is_identified` Int8,
    `current_sale_stage` String,
    `question_b_qid` String,
    `remind_answer` String,
    `cnick` String,
    `real_buyer_nick` String,
    `platform` String,
    `msg_time` DateTime,
    `plat_goods_id` String,
    `answer_id` String,
    `robot_answer` String,
    `transfer_type` String,
    `transfer_to` String,
    `transfer_from` String,
    `shop_question_type` String,
    `shop_question_id` String,
    `no_reply_reason` Int32,
    `no_reply_sub_reason` Int32,
    `msg_scenes_source` String,
    `msg_content_type` String,
    `trace_id` String,
    `day` Int32,
    `precise_intent_id` String,
    `precise_intent_standard_q` String,
    `cond_answer_id` String
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/ft_ods/tables/{layer}_{shard}/xdrs_logs_local',
    '{replica}'
)
PARTITION BY day
ORDER BY (shop_id, snick)
SETTINGS index_granularity = 8192, storage_policy = 'rr'


-- DROP TABLE ft_ods.xdrs_logs_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE ft_ods.xdrs_logs_all ON CLUSTER cluster_3s_2r
AS ft_ods.xdrs_logs_local
ENGINE = Distributed('cluster_3s_2r', 'ft_ods', 'xdrs_logs_local', rand())

-- DROP TABLE buffer.ft_ods_xdrs_logs_buffer ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE buffer.ft_ods_xdrs_logs_buffer ON CLUSTER cluster_3s_2r
AS ft_ods.xdrs_logs_all
ENGINE = Buffer('ft_ods', 'xdrs_logs_all', 16, 15, 35, 81920, 409600, 16777216, 67108864)

-- INSERT INTO
-- DROP TABLE ft_ods.xdrs_logs_local ON CLUSTER cluster_3s_2r NO DELAY
INSERT INTO ods.xdrs_logs_all
SELECT *
FROM remote('10.22.113.168:19000', ods.xdrs_logs_all)
WHERE day BETWEEN 20220910 AND 20220918
AND platform = 'tb'
AND shop_id = '5cac112e98ef4100118a9c9f'


-- JD
INSERT INTO buffer.ft_ods_xdrs_logs_buffer
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
    toDateTime64(create_time, 6) AS create_time,
    mp_version,
    qa_id,
    question_b_proba,
    question_b_standard_q,
    is_identified,
    current_sale_stage,
    question_b_qid,
    remind_answer,
    cnick,
    real_buyer_nick,
    platform,
    toDateTime(msg_time) AS msg_time,
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
    msg_scenes_source,
    msg_content_type,
    trace_id,
    day,
    precise_intent_id,
    precise_intent_standard_q,
    cond_answer_id
FROM ft_tmp.xdrs_logs_all
WHERE day BETWEEN 20220910 AND 20220918
AND platform = 'jd'
AND shop_id IN [
    '5e9d390d68283c002457b52f',
    '5edfa47c8f591c00163ef7d6',
    '5e9d350bcff5ed002486ded8',
    '5eb8acf16119f0001cbdaa5f'
]