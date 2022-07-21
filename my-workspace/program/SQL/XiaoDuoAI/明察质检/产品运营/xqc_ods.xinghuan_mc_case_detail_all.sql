CREATE DATABASE IF NOT EXISTS tmp ON CLUSTER cluster_3s_2r

-- DROP TABLE tmp.xinghuan_mc_case_detail_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE tmp.xinghuan_mc_case_detail_local ON CLUSTER cluster_3s_2r
(
    `_id` String,
    `create_time` String,
    `update_time` String,
    `company_id` String,
    `dialog_id` String,
    `case_label_id` String,
    `employee_id` String,
    `situation` String,
    `task` String,
    `action` String,
    `result` String,
    `platform` String,
    `dialog_time` Int32,
    `cnick` String,
    `snick` String,
    `department_id` String
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
ORDER BY (platform, company_id)
SETTINGS index_granularity = 8192, storage_policy = 'rr'


-- DROP TABLE tmp.xinghuan_mc_case_detail_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE tmp.xinghuan_mc_case_detail_all ON CLUSTER cluster_3s_2r
AS tmp.xinghuan_mc_case_detail_local
ENGINE = Distributed('cluster_3s_2r', 'tmp', 'xinghuan_mc_case_detail_local', rand())

-- DROP TABLE buffer.tmp_xinghuan_mc_case_detail_buffer ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE buffer.tmp_xinghuan_mc_case_detail_buffer ON CLUSTER cluster_3s_2r
AS tmp.xinghuan_mc_case_detail_all
ENGINE = Buffer('tmp', 'xinghuan_mc_case_detail_all', 16, 15, 35, 81920, 409600, 16777216, 67108864)


CREATE DATABASE IF NOT EXISTS xqc_ods ON CLUSTER cluster_3s_2r

-- DROP TABLE xqc_ods.xinghuan_mc_case_detail_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE xqc_ods.xinghuan_mc_case_detail_local ON CLUSTER cluster_3s_2r
(
    `_id` String,
    `create_time` String,
    `update_time` String,
    `company_id` String,
    `dialog_id` String,
    `case_label_id` String,
    `employee_id` String,
    `situation` String,
    `task` String,
    `action` String,
    `result` String,
    `platform` String,
    `dialog_time` Int32,
    `cnick` String,
    `snick` String,
    `department_id` String
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
ORDER BY (platform, company_id)
SETTINGS index_granularity = 8192, storage_policy = 'rr'


-- DROP TABLE xqc_ods.xinghuan_mc_case_detail_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE xqc_ods.xinghuan_mc_case_detail_all ON CLUSTER cluster_3s_2r
AS xqc_ods.xinghuan_mc_case_detail_local
ENGINE = Distributed('cluster_3s_2r', 'xqc_ods', 'xinghuan_mc_case_detail_local', rand())

-- DROP TABLE buffer.xqc_ods_xinghuan_mc_case_detail_buffer ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE buffer.xqc_ods_xinghuan_mc_case_detail_buffer ON CLUSTER cluster_3s_2r
AS xqc_ods.xinghuan_mc_case_detail_all
ENGINE = Buffer('xqc_ods', 'xinghuan_mc_case_detail_all', 16, 15, 35, 81920, 409600, 16777216, 67108864)