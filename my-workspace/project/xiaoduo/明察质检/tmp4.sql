CREATE DATABASE IF NOT EXISTS mini_ods ON CLUSTER cluster_3s_2r
ENGINE=Ordinary

CREATE TABLE IF NOT EXISTS mini_ods.kefu_eval_detail_local ON CLUSTER cluster_3s_2r
(
    `user_nick` String,
    `eval_code` Int32,
    `eval_recer` String,
    `real_buyer_nick` String,
    `open_uid` String,
    `eval_sender` String,
    `eval_time` String,
    `send_time` String,
    `source` Int32,
    `day` Int32
) ENGINE = ReplicatedMergeTree(
    '/clickhouse/mini_ods/tables/{layer}_{shard}/kefu_eval_detail_local',
    '{replica}'
) PARTITION BY day
ORDER BY user_nick SETTINGS index_granularity = 8192,
    storage_policy = 'rr'


-- DROP TABLE mini_ods.kefu_eval_detail_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE IF NOT EXISTS mini_ods.kefu_eval_detail_all ON CLUSTER cluster_3s_2r
AS mini_ods.kefu_eval_detail_local
ENGINE = Distributed('cluster_3s_2r', 'mini_ods', 'kefu_eval_detail_local', rand())