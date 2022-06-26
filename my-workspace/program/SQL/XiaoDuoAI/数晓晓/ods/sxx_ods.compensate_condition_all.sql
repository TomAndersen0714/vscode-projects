CREATE DATABASE sxx_ods ON CLUSTER cluster_3s_2r

-- DROP TABLE sxx_ods.compensate_condition_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE sxx_ods.compensate_condition_local ON CLUSTER cluster_3s_2r
(
    `day` Int32,
    `platform` String,
    `shop_id` String,
    `shop_name` String,
    `raw_info` String,
    `field_name` String,
    `filter_value` String
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
PARTITION BY (day, platform)
ORDER BY (field_name, filter_value)
SETTINGS index_granularity = 8192, storage_policy = 'rr'


-- DROP TABLE sxx_ods.compensate_condition_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE sxx_ods.compensate_condition_all ON CLUSTER cluster_3s_2r
AS sxx_ods.compensate_condition_local
ENGINE = Distributed('cluster_3s_2r', 'sxx_ods', 'compensate_condition_local', rand())

-- DROP TABLE buffer.sxx_ods_compensate_condition_buffer ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE buffer.sxx_ods_compensate_condition_buffer ON CLUSTER cluster_3s_2r
AS sxx_ods.compensate_condition_all
ENGINE = Buffer('sxx_ods', 'compensate_condition_all', 16, 15, 35, 81920, 409600, 16777216, 67108864)