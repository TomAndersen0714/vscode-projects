CREATE TABLE tmp.mini_xdre_shop_local ON CLUSTER cluster_3s_2r(
    `_id` String,
    `platform` String,
    `category_id` String,
    `category_ids` String,
    `model_type` String
) ENGINE = ReplicatedMergeTree(
    '/clickhouse/tmp/tables/{layer}_{shard}/mini_xdre_shop_local',
    '{replica}'
)
ORDER BY _id SETTINGS index_granularity = 8192,
    storage_policy = 'rr'