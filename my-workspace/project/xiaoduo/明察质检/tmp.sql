CREATE DATABASE IF NOT EXISTS ft_ods ON CLUSTER cluster_3s_2r
ENGINE=Ordinary

-- DROP TABLE ft_ods.kefu_eval_detail_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE IF NOT EXISTS ft_ods.kefu_eval_detail_local ON CLUSTER cluster_3s_2r
(
    `platform` String,
    `shop_id` String,
    `shop_name` String,
    `seller_nick` String,
    `snick` String,
    `cnick` String,
    `real_buyer_nick` String,
    `open_uid` String,
    `user_nick` String,
    `eval_code` Int32,
    `eval_recer` String,
    `eval_sender` String,
    `eval_time` String,
    `send_time` String,
    `source` Int32,
    `day` Int32
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
PARTITION BY day
ORDER BY (platform, shop_id, shop_name)
SETTINGS index_granularity = 8192, storage_policy = 'rr'


-- DROP TABLE ft_ods.kefu_eval_detail_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE IF NOT EXISTS ft_ods.kefu_eval_detail_all ON CLUSTER cluster_3s_2r
AS ft_ods.kefu_eval_detail_local
ENGINE = Distributed('cluster_3s_2r', 'ft_ods', 'kefu_eval_detail_local', rand())