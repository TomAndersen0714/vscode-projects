CREATE DATABASE IF NOT EXISTS ods ON CLUSTER cluster_3s_2r
ENGINE=Ordinary;

-- DROP TABLE ods.jd_real_time_chat_log_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE IF NOT EXISTS ods.jd_real_time_chat_log_local ON CLUSTER cluster_3s_2r
(
    `shop_id` String,
    `plat_user_id` String,
    `main_username` String,
    `mt` Int64,
    `channel` String,
    `waiter` String,
    `time` Int64,
    `type` String,
    `waiter_send` String,
    `content` String,
    `sid` String,
    `customer` String,
    `sku_id` Int64,
    `day` Int32
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
PARTITION BY day
ORDER BY (shop_id, waiter)
SETTINGS index_granularity = 8192, storage_policy = 'rr';

-- DROP TABLE ods.jd_real_time_chat_log_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE IF NOT EXISTS ods.jd_real_time_chat_log_all ON CLUSTER cluster_3s_2r
AS ods.jd_real_time_chat_log_local
ENGINE = Distributed('cluster_3s_2r', 'ods', 'jd_real_time_chat_log_local', rand());

CREATE DATABASE IF NOT EXISTS buffer ON CLUSTER cluster_3s_2r
ENGINE=Ordinary;

-- DROP TABLE buffer.ods_jd_real_time_chat_log_buffer ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE IF NOT EXISTS buffer.ods_jd_real_time_chat_log_buffer ON CLUSTER cluster_3s_2r
AS ods.jd_real_time_chat_log_all
ENGINE = Buffer('ods', 'jd_real_time_chat_log_all', 16, 15, 35, 81920, 409600, 16777216, 67108864);