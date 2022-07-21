CREATE DATABASE ods ON CLUSTER cluster_3s_2r

-- DROP TABLE ods.kefu_eval_detail_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE ods.kefu_eval_detail_local ON CLUSTER cluster_3s_2r
(
    `platform` String,
    `user_nick` String,
    `eval_code` Int32,
    `eval_recer` String,
    `eval_sender` String,
    `eval_time` String,
    `send_time` String,
    `source` Int32,
    `desc` String,
    `day` Int32
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
PARTITION BY day
ORDER BY (platform, user_nick)
SETTINGS index_granularity = 8192, storage_policy = 'rr'


-- DROP TABLE ods.kefu_eval_detail_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE ods.kefu_eval_detail_all ON CLUSTER cluster_3s_2r
AS ods.kefu_eval_detail_local
ENGINE = Distributed('cluster_3s_2r', 'ods', 'kefu_eval_detail_local', rand())

-- DROP TABLE buffer.ods_kefu_eval_detail_buffer ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE buffer.ods_kefu_eval_detail_buffer ON CLUSTER cluster_3s_2r
AS ods.kefu_eval_detail_all
ENGINE = Buffer('ods', 'kefu_eval_detail_all', 16, 15, 35, 81920, 409600, 16777216, 67108864)