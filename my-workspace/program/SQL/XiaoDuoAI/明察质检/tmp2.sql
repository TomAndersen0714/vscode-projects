CREATE TABLE cdp_ods.ownership_snapshot_local (
    `platform` String,
    `shop_id` String,
    `cnick_id` String,
    `cnick` String,
    `snick` String,
    `level` Int32,
    `day` Int32
) ENGINE = ReplicatedMergeTree(
    '/clickhouse/cdp_ods/tables/{layer}_{shard}/ownership_snapshot_local',
    '{replica}'
)
PARTITION BY day
ORDER BY (platform, shop_id)
SETTINGS index_granularity = 8192