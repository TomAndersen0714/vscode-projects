CREATE TABLE xqc_dim.xqc_shop_local ON CLUSTER cluster_3s_2r
(
    `_id` String,
    `create_time` String,
    `update_time` String,
    `company_id` String,
    `shop_id` String,
    `platform` String,
    `seller_nick` String,
    `plat_shop_name` String,
    `plat_shop_id` String,
    `day` Int32
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
PARTITION BY day
ORDER BY company_id
SETTINGS storage_policy = 'rr', index_granularity = 8192


CREATE TABLE xqc_dim.xqc_shop_all ON CLUSTER cluster_3s_2r
AS xqc_dim.xqc_shop_local
ENGINE = Distributed('cluster_3s_2r', 'xqc_dim', 'xqc_shop_local', rand())