CREATE DATABASE IF NOT EXISTS ft_dwd ON CLUSTER cluster_3s_2r
ENGINE=Ordinary

-- DROP TABLE ft_dwd.ask_order_cov_detail_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE ft_dwd.ask_order_cov_detail_local ON CLUSTER cluster_3s_2r
(
    `day` Int32,
    `platform` String,
    `shop_id` String,
    `buyer_nick` String,
    `snick` String,
    `session_id` String,
    `focus_goods_id` String,
    `order_id` String,
    `goods_id` String,
    `created_time` String,
    `paid_time` String,
    `payment` Float64,
    `is_refund` Int8,
    `is_transf` Int8
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
PARTITION BY day
ORDER BY (platform, shop_id)
SETTINGS index_granularity = 8192, storage_policy = 'rr'


-- DROP TABLE ft_dwd.ask_order_cov_detail_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE ft_dwd.ask_order_cov_detail_all ON CLUSTER cluster_3s_2r
AS ft_dwd.ask_order_cov_detail_local
ENGINE = Distributed('cluster_3s_2r', 'ft_dwd', 'ask_order_cov_detail_local', rand())

-- DROP TABLE buffer.ft_dwd_ask_order_cov_detail_buffer ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE buffer.ft_dwd_ask_order_cov_detail_buffer ON CLUSTER cluster_3s_2r
AS ft_dwd.ask_order_cov_detail_all
ENGINE = Buffer('ft_dwd', 'ask_order_cov_detail_all', 16, 15, 35, 81920, 409600, 16777216, 67108864)