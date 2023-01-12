CREATE DATABASE sxx_ods ON CLUSTER cluster_3s_2r

-- DROP TABLE sxx_ods.warehouse_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE sxx_ods.warehouse_local ON CLUSTER cluster_3s_2r
(
    `day` Int32,
    `platform` String,
    `shop_id` String,
    `shop_name` String,
    `raw_info` String,
    `warehouse_name` String
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
PARTITION BY (day, platform)
ORDER BY (warehouse_name)
SETTINGS index_granularity = 8192, storage_policy = 'rr'


-- DROP TABLE sxx_ods.warehouse_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE sxx_ods.warehouse_all ON CLUSTER cluster_3s_2r
AS sxx_ods.warehouse_local
ENGINE = Distributed('cluster_3s_2r', 'sxx_ods', 'warehouse_local', rand())

-- DROP TABLE buffer.sxx_ods_warehouse_buffer ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE buffer.sxx_ods_warehouse_buffer ON CLUSTER cluster_3s_2r
AS sxx_ods.warehouse_all
ENGINE = Buffer('sxx_ods', 'warehouse_all', 16, 15, 35, 81920, 409600, 16777216, 67108864)