CREATE DATABASE sxx_dim ON CLUSTER cluster_3s_2r

-- DROP TABLE sxx_dim.plat_goods_map_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE sxx_dim.plat_goods_map_local ON CLUSTER cluster_3s_2r
(
    `snapshot_day` Int32,
    `day` Int32,
    `platform` String,
    `shop_id` String,
    `shop_name` String,
    `raw_info` String,
    `custom_shop_name` String,
    `product_id` String,
    `product_name` String,
    `goods_name` String
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
PARTITION BY (snapshot_day)
ORDER BY (platform, product_id, product_name)
SETTINGS index_granularity = 8192, storage_policy = 'rr'


-- DROP TABLE sxx_dim.plat_goods_map_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE sxx_dim.plat_goods_map_all ON CLUSTER cluster_3s_2r
AS sxx_dim.plat_goods_map_local
ENGINE = Distributed('cluster_3s_2r', 'sxx_dim', 'plat_goods_map_local', rand())

-- DROP TABLE buffer.sxx_dim_plat_goods_map_buffer ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE buffer.sxx_dim_plat_goods_map_buffer ON CLUSTER cluster_3s_2r
AS sxx_dim.plat_goods_map_all
ENGINE = Buffer('sxx_dim', 'plat_goods_map_all', 16, 15, 35, 81920, 409600, 16777216, 67108864)