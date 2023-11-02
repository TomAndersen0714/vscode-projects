-- DROP TABLE dim.platform_shop_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE IF NOT EXISTS dim.platform_shop_local ON CLUSTER cluster_3s_2r
(
    `_id` String,
    `platform` String,
    `plat_user_id` String,
    `plat_shop_name` String,
    `plat_user_real_id` String,
    `open_id` String,
    `user_id` String,
    `account_limit` String,
    `account_limit_v` String,
    `create_time` String,
    `update_time` String
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
ORDER BY (platform, _id)
SETTINGS index_granularity = 8192, storage_policy = 'rr';

-- DROP TABLE dim.platform_shop_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE IF NOT EXISTS dim.platform_shop_all ON CLUSTER cluster_3s_2r
AS dim.platform_shop_local
ENGINE = Distributed('cluster_3s_2r', 'dim', 'platform_shop_local', rand());