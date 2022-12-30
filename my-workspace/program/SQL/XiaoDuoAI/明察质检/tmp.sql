CREATE TABLE ft_dim.goods_info_local  ON CLUSTER cluster_3s_2r
(
    `platform` String,
    `shop_id` String,
    `goods_id` String,
    `title` String,
    `cate_lv1` String,
    `cate_lv2` String,
    `cate_lv3` String,
    `enable` String,
    `goods_url` String,
    `price` String,
    `type` String
    `attr` String,
    `raw` String
) ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
ORDER BY (type, goods_id)
SETTINGS storage_policy = 'rr', index_granularity = 8192


CREATE TABLE ft_dim.goods_info_all ON CLUSTER cluster_3s_2r
AS ft_dim.goods_info_local
ENGINE = Distributed('cluster_3s_2r', 'ft_dim', 'goods_info_local', rand())