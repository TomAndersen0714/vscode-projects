CREATE DATABASE IF NOT EXISTS tmp ON CLUSTER cluster_3s_2r

-- DROP TABLE tmp.qc_norm_operation_log_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE tmp.qc_norm_operation_log_local ON CLUSTER cluster_3s_2r
(
    `_id` String,
    `platform` String,
    `company_id` String,
    `operator_id` String,
    `operator_name` String,
    `action_verb` String,
    `action_noun` String,
    `object_id` String,
    `object_name` String,
    `create_time` Int64
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
ORDER BY (platform, company_id)
SETTINGS index_granularity = 8192, storage_policy = 'rr'


-- DROP TABLE tmp.qc_norm_operation_log_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE tmp.qc_norm_operation_log_all ON CLUSTER cluster_3s_2r
AS tmp.qc_norm_operation_log_local
ENGINE = Distributed('cluster_3s_2r', 'tmp', 'qc_norm_operation_log_local', rand())

-- DROP TABLE buffer.tmp_qc_norm_operation_log_buffer ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE buffer.tmp_qc_norm_operation_log_buffer ON CLUSTER cluster_3s_2r
AS tmp.qc_norm_operation_log_all
ENGINE = Buffer('tmp', 'qc_norm_operation_log_all', 16, 15, 35, 81920, 409600, 16777216, 67108864)


CREATE DATABASE IF NOT EXISTS xqc_ods ON CLUSTER cluster_3s_2r

-- DROP TABLE xqc_ods.qc_norm_operation_log_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE xqc_ods.qc_norm_operation_log_local ON CLUSTER cluster_3s_2r
(
    `day` Int32,
    `_id` String,
    `platform` String,
    `company_id` String,
    `operator_id` String,
    `operator_name` String,
    `action_verb` String,
    `action_noun` String,
    `object_id` String,
    `object_name` String,
    `create_time` Int64
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
PARTITION BY day
ORDER BY (platform, company_id)
SETTINGS index_granularity = 8192, storage_policy = 'rr'


-- DROP TABLE xqc_ods.qc_norm_operation_log_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE xqc_ods.qc_norm_operation_log_all ON CLUSTER cluster_3s_2r
AS xqc_ods.qc_norm_operation_log_local
ENGINE = Distributed('cluster_3s_2r', 'xqc_ods', 'qc_norm_operation_log_local', rand())

-- DROP TABLE buffer.xqc_ods_qc_norm_operation_log_buffer ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE buffer.xqc_ods_qc_norm_operation_log_buffer ON CLUSTER cluster_3s_2r
AS xqc_ods.qc_norm_operation_log_all
ENGINE = Buffer('xqc_ods', 'qc_norm_operation_log_all', 16, 15, 35, 81920, 409600, 16777216, 67108864)