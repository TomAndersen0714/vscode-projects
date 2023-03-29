CREATE DATABASE IF NOT EXISTS dwd ON CLUSTER cluster_3s_2r
ENGINE=Ordinary;

-- DROP TABLE dwd.voc_cnick_list_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE IF NOT EXISTS dwd.voc_cnick_list_local ON CLUSTER cluster_3s_2r
(
    `day` UInt32,
    `cnick` String,
    `real_buyer_nick` String,
    `cnick_id` UInt64
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
PARTITION BY day
ORDER BY cnick
SETTINGS index_granularity = 8192, storage_policy = 'rr';


-- DROP TABLE dwd.voc_cnick_list_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE IF NOT EXISTS dwd.voc_cnick_list_all ON CLUSTER cluster_3s_2r
AS dwd.voc_cnick_list_local
ENGINE = Distributed('cluster_3s_2r', 'dwd', 'voc_cnick_list_local', rand());


CREATE DATABASE IF NOT EXISTS buffer ON CLUSTER cluster_3s_2r
ENGINE=Ordinary;


-- DROP TABLE buffer.dwd_voc_cnick_list_buffer ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE IF NOT EXISTS buffer.dwd_voc_cnick_list_buffer ON CLUSTER cluster_3s_2r
AS dwd.voc_cnick_list_all
ENGINE = Buffer('dwd', 'voc_cnick_list_all', 16, 15, 35, 81920, 409600, 16777216, 67108864);