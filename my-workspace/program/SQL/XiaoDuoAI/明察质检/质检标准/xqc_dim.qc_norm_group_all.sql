-- tmp.qc_norm_group_local
CREATE TABLE tmp.qc_norm_group_local ON CLUSTER cluster_3s_2r
(
    `_id` String,
    `create_time` String,
    `update_time` String,
    `company_id` String,
    `platform`  String,
    `qc_norm_id` String,
    `name` String,
    `level` Int32,
    `parent_id` String
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
ORDER BY company_id
SETTINGS storage_policy = 'rr', index_granularity = 8192

-- tmp.qc_norm_group_all
CREATE TABLE tmp.qc_norm_group_all ON CLUSTER cluster_3s_2r
AS tmp.qc_norm_group_local
ENGINE = Distributed('cluster_3s_2r', 'tmp', 'qc_norm_group_local', rand())

-- xqc_dim.qc_norm_group_local
CREATE TABLE xqc_dim.qc_norm_group_local ON CLUSTER cluster_3s_2r
(
    `_id` String,
    `create_time` String,
    `update_time` String,
    `company_id` String,
    `platform`  String,
    `qc_norm_id` String,
    `name` String,
    `level` Int32,
    `parent_id` String,
    `day` Int32
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
PARTITION BY day
ORDER BY company_id
SETTINGS storage_policy = 'rr', index_granularity = 8192

-- xqc_dim.qc_norm_group_all
CREATE TABLE xqc_dim.qc_norm_group_all ON CLUSTER cluster_3s_2r
AS xqc_dim.qc_norm_group_local
ENGINE = Distributed('cluster_3s_2r', 'xqc_dim', 'qc_norm_group_local', rand())