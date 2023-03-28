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
    `real_buyer_nick` String,
    `act` String,
    `question_b_qid` String,
    `create_time` String,
    `recent_order_id` String,
    `recent_order_status` String,
    `recent_order_timestamp` UInt64,
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


CREATE DATABASE IF NOT EXISTS buffer ON CLUSTER cluster_3s_2r
ENGINE=Ordinary;

-- DROP TABLE buffer.dwd_voc_chat_log_detail_buffer ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE IF NOT EXISTS buffer.dwd_voc_chat_log_detail_buffer ON CLUSTER cluster_3s_2r
AS dwd.voc_chat_log_detail_all
ENGINE = Buffer('dwd', 'voc_chat_log_detail_all', 16, 15, 35, 81920, 409600, 16777216, 67108864);