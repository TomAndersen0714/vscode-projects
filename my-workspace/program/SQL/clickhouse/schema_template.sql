CREATE DATABASE IF NOT EXISTS trino ON CLUSTER cluster_3s_2r
ENGINE=Ordinary

-- DROP TABLE trino.xdrs_logs_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE trino.xdrs_logs_local ON CLUSTER cluster_3s_2r
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
    '/clickhouse/trino/tables/{layer}_{shard}/xdrs_logs_local',
    '{replica}'
)
PARTITION BY day
ORDER BY (shop_id, snick)
SETTINGS index_granularity = 8192, storage_policy = 'rr'


-- DROP TABLE trino.xdrs_logs_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE trino.xdrs_logs_all ON CLUSTER cluster_3s_2r
AS trino.xdrs_logs_local
ENGINE = Distributed('cluster_3s_2r', 'trino', 'xdrs_logs_local', rand())

-- DROP TABLE buffer.trino_xdrs_logs_buffer ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE buffer.trino_xdrs_logs_buffer ON CLUSTER cluster_3s_2r
AS trino.xdrs_logs_all
ENGINE = Buffer('trino', 'xdrs_logs_all', 16, 15, 35, 81920, 409600, 16777216, 67108864)