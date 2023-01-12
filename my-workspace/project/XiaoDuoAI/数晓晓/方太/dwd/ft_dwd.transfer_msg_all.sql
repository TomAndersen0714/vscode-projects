CREATE DATABASE IF NOT EXISTS ft_dwd ON CLUSTER cluster_3s_2r
ENGINE=Ordinary

-- DROP TABLE ft_dwd.transfer_msg_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE ft_dwd.transfer_msg_local ON CLUSTER cluster_3s_2r
(
    `id` String,
    `day` Int32,
    `platform` String,
    `shop_id` String,
    `shop_name` String,
    `from_snick` String,
    `to_snick` String,
    `cnick` String,
    `real_buyer_nick` String,
    `create_time` String
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
PARTITION BY day
ORDER BY (platform, shop_name)
SETTINGS index_granularity = 8192, storage_policy = 'rr'


-- DROP TABLE ft_dwd.transfer_msg_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE ft_dwd.transfer_msg_all ON CLUSTER cluster_3s_2r
AS ft_dwd.transfer_msg_local
ENGINE = Distributed('cluster_3s_2r', 'ft_dwd', 'transfer_msg_local', rand())

-- DROP TABLE buffer.ft_dwd_transfer_msg_buffer ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE buffer.ft_dwd_transfer_msg_buffer ON CLUSTER cluster_3s_2r
AS ft_dwd.transfer_msg_all
ENGINE = Buffer('ft_dwd', 'transfer_msg_all', 16, 15, 35, 81920, 409600, 16777216, 67108864)