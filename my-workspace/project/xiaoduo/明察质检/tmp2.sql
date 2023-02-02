CREATE TABLE dim.platform_shop_nick_local ON CLUSTER cluster_3s_2r (
    `shop_id` String,
    `plat_user_id` String,
    `platform` String
) ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
ORDER BY shop_id SETTINGS index_granularity = 8192 
CREATE TABLE dim.platform_shop_nick_all ON CLUSTER cluster_3s_2r
(
        `shop_id` String,
        `plat_user_id` String,
        `platform` String
    ) ENGINE = Distributed(
        'cluster_3s_2r',
        'dim',
        'platform_shop_nick_local',
        rand()
    )