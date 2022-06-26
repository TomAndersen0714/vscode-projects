CREATE DATABASE sxx_dim ON CLUSTER cluster_3s_2r

-- DROP TABLE sxx_dim.qc_group_label_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE sxx_dim.qc_group_label_local ON CLUSTER cluster_3s_2r
(
    `snaphost_day` Int32,
    `create_time` String,
    `update_time` String,
    `company_id` String,
    `platform` String,
    `qc_norm_id` String,
    `group_id` String,
    `group_name` String,
    `sub_group_id` String,
    `sub_group_name` String,
    `responsible_party` String,
    `qc_label_id` String,
    `qc_label_name` String
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
PARTITION BY (snaphost_day)
ORDER BY (platform, qc_label_id)
SETTINGS index_granularity = 8192, storage_policy = 'rr'


-- DROP TABLE sxx_dim.qc_group_label_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE sxx_dim.qc_group_label_all ON CLUSTER cluster_3s_2r
AS sxx_dim.qc_group_label_local
ENGINE = Distributed('cluster_3s_2r', 'sxx_dim', 'qc_group_label_local', rand())

-- DROP TABLE buffer.sxx_ods_qc_group_label_buffer ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE buffer.sxx_ods_qc_group_label_buffer ON CLUSTER cluster_3s_2r
AS sxx_dim.qc_group_label_all
ENGINE = Buffer('sxx_dim', 'qc_group_label_all', 16, 15, 35, 81920, 409600, 16777216, 67108864)