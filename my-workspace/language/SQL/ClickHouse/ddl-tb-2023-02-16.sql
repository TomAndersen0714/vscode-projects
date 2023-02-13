CREATE DATABASE IF NOT EXISTS ft_dim ON CLUSTER cluster_3s_2r
ENGINE=Ordinary;

-- DROP TABLE ft_dim.main_goods_info_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE IF NOT EXISTS ft_dim.main_goods_info_local ON CLUSTER cluster_3s_2r
(
    `platform` String,
    `shop_name` String,
    `sku_id` String,
    `pkg_code` String,
    `pkg_name` String,
    `product_code` String,
    `product_name` String,
    `product_class` String,
    `other_fields` String
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
ORDER BY (platform, shop_name)
SETTINGS index_granularity = 8192, storage_policy = 'rr';


-- DROP TABLE ft_dim.main_goods_info_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE IF NOT EXISTS ft_dim.main_goods_info_all ON CLUSTER cluster_3s_2r
AS ft_dim.main_goods_info_local
ENGINE = Distributed('cluster_3s_2r', 'ft_dim', 'main_goods_info_local', rand());


-- DROP TABLE buffer.main_goods_info_buffer ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE IF NOT EXISTS buffer.main_goods_info_buffer ON CLUSTER cluster_3s_2r
AS ft_dim.main_goods_info_all
ENGINE = Buffer('ft_dim', 'main_goods_info_all', 16, 15, 35, 81920, 409600, 16777216, 67108864);


DROP TABLE ft_dim.goods_info_all ON CLUSTER cluster_3s_2r NO DELAY;

CREATE TABLE IF NOT EXISTS ft_dim.goods_info_all ON CLUSTER cluster_3s_2r
AS ft_dim.goods_info_1_local
ENGINE = Distributed('cluster_3s_2r', 'ft_dim', 'goods_info_1_local', rand());