CREATE TABLE dim.platform_shop_nick_v2_all ON CLUSTER cluster_3s_2r(
    `shop_id` String,
    `plat_user_id` String,
    `mp_version` Int8,
    `create_time` String,
    `update_time` String,
    `expire_time` String,
    `category_name` String,
    `plat_shop_name` String,
    `platform` String
) ENGINE = Distributed(
    'cluster_3s_2r',
    'dim',
    'platform_shop_nick_v2_local',
    rand()
)

CREATE TABLE dim.platform_shop_nick_v2_local ON CLUSTER cluster_3s_2r (
    `shop_id` String,
    `plat_user_id` String,
    `mp_version` Int8,
    `create_time` String,
    `update_time` String,
    `expire_time` String,
    `category_name` String,
    `plat_shop_name` String,
    `platform` String
) ENGINE = ReplicatedMergeTree(
    '/clickhouse/dim/tables/{layer}_{shard}/platform_shop_nick_v2_local',
    '{replica}'
) PARTITION BY platform
ORDER BY shop_id SETTINGS index_granularity = 8192,
    storage_policy = 'rr'