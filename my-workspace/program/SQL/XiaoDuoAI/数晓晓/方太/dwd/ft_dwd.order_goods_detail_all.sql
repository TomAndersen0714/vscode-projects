CREATE DATABASE IF NOT EXISTS ft_dwd ON CLUSTER cluster_3s_2r
ENGINE=Ordinary

-- DROP TABLE ft_dwd.order_goods_detail_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE ft_dwd.order_goods_detail_local ON CLUSTER cluster_3s_2r
(
    `day` Int32,
    `platform` String,
    `shop_id` String,
    `order_id` String,
    `status` String,
    `order_payment` Float64,
    `goods_id` String,
    `goods_price` Float64,
    `goods_payment` Float64,
    `updated_time` String,
    `is_slience` Int8
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
PARTITION BY day
ORDER BY (platform, shop_id)
SETTINGS index_granularity = 8192, storage_policy = 'rr'


-- DROP TABLE ft_dwd.order_goods_detail_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE ft_dwd.order_goods_detail_all ON CLUSTER cluster_3s_2r
AS ft_dwd.order_goods_detail_local
ENGINE = Distributed('cluster_3s_2r', 'ft_dwd', 'order_goods_detail_local', rand())

-- DROP TABLE buffer.ft_dwd_order_goods_detail_buffer ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE buffer.ft_dwd_order_goods_detail_buffer ON CLUSTER cluster_3s_2r
AS ft_dwd.order_goods_detail_all
ENGINE = Buffer('ft_dwd', 'order_goods_detail_all', 16, 15, 35, 81920, 409600, 16777216, 67108864)