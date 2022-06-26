CREATE DATABASE sxx_ods ON CLUSTER cluster_3s_2r

-- DROP TABLE sxx_ods.responsible_party_map_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE sxx_ods.responsible_party_map_local ON CLUSTER cluster_3s_2r
(
    `day` Int32,
    `platform` String,
    `shop_id` String,
    `shop_name` String,
    `raw_info` String,
    `qc_label_group_name` String,
    `qc_label_sub_group_name` String,
    `compensate_reason_3` String,
    `compensate_reason_4` String,
    `responsible_party` String
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
PARTITION BY (day, platform)
ORDER BY (qc_label_group_name, compensate_reason_3)
SETTINGS index_granularity = 8192, storage_policy = 'rr'


-- DROP TABLE sxx_ods.responsible_party_map_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE sxx_ods.responsible_party_map_all ON CLUSTER cluster_3s_2r
AS sxx_ods.responsible_party_map_local
ENGINE = Distributed('cluster_3s_2r', 'sxx_ods', 'responsible_party_map_local', rand())

-- DROP TABLE buffer.sxx_ods_responsible_party_map_buffer ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE buffer.sxx_ods_responsible_party_map_buffer ON CLUSTER cluster_3s_2r
AS sxx_ods.responsible_party_map_all
ENGINE = Buffer('sxx_ods', 'responsible_party_map_all', 16, 15, 35, 81920, 409600, 16777216, 67108864)