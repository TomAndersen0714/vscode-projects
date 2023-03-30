CREATE DATABASE IF NOT EXISTS dwd ON CLUSTER cluster_3s_2r
ENGINE=Ordinary;

-- DROP TABLE dwd.voc_chat_log_detail_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE IF NOT EXISTS dwd.voc_chat_log_detail_local ON CLUSTER cluster_3s_2r
(
    `day` UInt32,
    `platform` String,
    `shop_id` String,
    `snick` String,
    `cnick` String,
    `cnick_id` UInt64,
    `real_buyer_nick` String,
    `msg_timestamp` UInt64,
    `msg_id` String,
    `msg` String,
    `act` String,
    `send_msg_from` String,
    `question_b_qid` String,
    `plat_goods_id` String,
    `recent_order_id` String,
    `recent_order_status_timestamp` UInt64,
    `recent_order_status` String,
    `dialog_qa_sum` UInt64
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
PARTITION BY day
ORDER BY (platform, shop_id, snick)
SETTINGS index_granularity = 8192, storage_policy = 'rr';


-- DROP TABLE dwd.voc_chat_log_detail_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE IF NOT EXISTS dwd.voc_chat_log_detail_all ON CLUSTER cluster_3s_2r
AS dwd.voc_chat_log_detail_local
ENGINE = Distributed('cluster_3s_2r', 'dwd', 'voc_chat_log_detail_local', rand());