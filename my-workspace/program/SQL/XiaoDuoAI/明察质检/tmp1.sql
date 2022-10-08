CREATE DATABASE IF NOT EXISTS cdp_ods ON CLUSTER cluster_3s_2r ENGINE = Ordinary

-- DROP TABLE cdp_ods.ownership_snapshot_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE cdp_ods.ownership_snapshot_local ON CLUSTER cluster_3s_2r
(
    `platform` String,
    `shop_id` String,
    `cnick_id` String,
    `cnick` String,
    `snick` String,
    `level` Int32,
    `day` Int32
) ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
PARTITION BY day
ORDER BY (platform, shop_id)
SETTINGS index_granularity = 8192, storage_policy='rr'


-- DROP TABLE cdp_ods.ownership_snapshot_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE cdp_ods.ownership_snapshot_all ON CLUSTER cluster_3s_2r
AS cdp_ods.ownership_snapshot_local
ENGINE = Distributed('cluster_3s_2r', 'cdp_ods', 'ownership_snapshot_local', rand())

-- DROP TABLE buffer.cdp_ods_ownership_snapshot_buffer ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE buffer.cdp_ods_ownership_snapshot_buffer ON CLUSTER cluster_3s_2r
AS cdp_ods.ownership_snapshot_all
ENGINE = Buffer('cdp_ods', 'ownership_snapshot_all', 16, 15, 35, 81920, 409600, 16777216, 67108864)