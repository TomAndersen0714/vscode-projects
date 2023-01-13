CREATE DATABASE sxx_dim ON CLUSTER cluster_3s_2r

-- DROP TABLE sxx_dim.platform_map_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE sxx_dim.platform_map_local ON CLUSTER cluster_3s_2r
(
    `snapshot_day` Int32,
    `platform` String,
    `platform_cn` String
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
PARTITION BY (snapshot_day)
ORDER BY (platform, platform_cn)
SETTINGS index_granularity = 8192, storage_policy = 'rr'


-- DROP TABLE sxx_dim.platform_map_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE sxx_dim.platform_map_all ON CLUSTER cluster_3s_2r
AS sxx_dim.platform_map_local
ENGINE = Distributed('cluster_3s_2r', 'sxx_dim', 'platform_map_local', rand())

-- DROP TABLE buffer.sxx_dim_platform_map_buffer ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE buffer.sxx_dim_platform_map_buffer ON CLUSTER cluster_3s_2r
AS sxx_dim.platform_map_all
ENGINE = Buffer('sxx_dim', 'platform_map_all', 16, 15, 35, 81920, 409600, 16777216, 67108864)