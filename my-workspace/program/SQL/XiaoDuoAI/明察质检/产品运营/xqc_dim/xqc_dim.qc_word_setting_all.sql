CREATE DATABASE IF NOT EXISTS tmp ON CLUSTER cluster_3s_2r

-- DROP TABLE tmp.qc_word_setting_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE tmp.qc_word_setting_local ON CLUSTER cluster_3s_2r
(
    `_id` String,
    `create_time` String,
    `update_time` String,
    `company_id` String,
    `platform` String,
    `qc_norm_id` String,
    `word` String,
    `check_custom` String,
    `check_service` String,
    `group_id` String,
    `check_labor_only` String
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
ORDER BY (platform, company_id)
SETTINGS index_granularity = 8192, storage_policy = 'rr'


-- DROP TABLE tmp.qc_word_setting_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE tmp.qc_word_setting_all ON CLUSTER cluster_3s_2r
AS tmp.qc_word_setting_local
ENGINE = Distributed('cluster_3s_2r', 'tmp', 'qc_word_setting_local', rand())

-- DROP TABLE buffer.tmp_qc_word_setting_buffer ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE buffer.tmp_qc_word_setting_buffer ON CLUSTER cluster_3s_2r
AS tmp.qc_word_setting_all
ENGINE = Buffer('tmp', 'qc_word_setting_all', 16, 15, 35, 81920, 409600, 16777216, 67108864)


CREATE DATABASE IF NOT EXISTS xqc_dim ON CLUSTER cluster_3s_2r

-- DROP TABLE xqc_dim.qc_word_setting_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE xqc_dim.qc_word_setting_local ON CLUSTER cluster_3s_2r
(
    `day` Int32,
    `_id` String,
    `create_time` String,
    `update_time` String,
    `company_id` String,
    `platform` String,
    `qc_norm_id` String,
    `word` String,
    `check_custom` String,
    `check_service` String,
    `group_id` String,
    `check_labor_only` String
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
PARTITION BY day
ORDER BY (platform, company_id)
SETTINGS index_granularity = 8192, storage_policy = 'rr'


-- DROP TABLE xqc_dim.qc_word_setting_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE xqc_dim.qc_word_setting_all ON CLUSTER cluster_3s_2r
AS xqc_dim.qc_word_setting_local
ENGINE = Distributed('cluster_3s_2r', 'xqc_dim', 'qc_word_setting_local', rand())

-- DROP TABLE buffer.xqc_dim_qc_word_setting_buffer ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE buffer.xqc_dim_qc_word_setting_buffer ON CLUSTER cluster_3s_2r
AS xqc_dim.qc_word_setting_all
ENGINE = Buffer('xqc_dim', 'qc_word_setting_all', 16, 15, 35, 81920, 409600, 16777216, 67108864)