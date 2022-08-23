CREATE DATABASE IF NOT EXISTS tmp ON CLUSTER cluster_3s_2r

-- DROP TABLE tmp.wiped_tag_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE tmp.wiped_tag_local ON CLUSTER cluster_3s_2r
(
    `company_id` String,
    `wipe_content` String,
    `wipe_index` String,
    `wipe_type` Int32,
    `wiper_id` String,
    `wiper_name` String,
    `employee_superior_id` String,
    `employee_superior_name` String,
    `employee_department_path` String,
    `dialog_id` String,
    `platform` String,
    `seller_nick` String,
    `snick` String,
    `cnick` String,
    `dialog_begin_timestamp` Int64,
    `dialog_end_timestamp` Int64,
    `messages_source` Array(Int32),
    `messages_content` Array(String),
    `messages_timestamp` Array(Int64),
    `wipe_message_content` String,
    `wipe_message_source` Int32,
    `wipe_message_timestamp` Int64,
    `create_time` String,
    `day` Int32
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
PARTITION BY day
ORDER BY (platform, company_id)
SETTINGS index_granularity = 8192, storage_policy = 'rr'


-- DROP TABLE tmp.wiped_tag_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE tmp.wiped_tag_all ON CLUSTER cluster_3s_2r
AS tmp.wiped_tag_local
ENGINE = Distributed('cluster_3s_2r', 'tmp', 'wiped_tag_local', rand())

-- DROP TABLE buffer.tmp_wiped_tag_buffer ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE buffer.tmp_wiped_tag_buffer ON CLUSTER cluster_3s_2r
AS tmp.wiped_tag_all
ENGINE = Buffer('tmp', 'wiped_tag_all', 16, 15, 35, 81920, 409600, 16777216, 67108864)


CREATE DATABASE IF NOT EXISTS xqc_ods ON CLUSTER cluster_3s_2r

-- DROP TABLE xqc_ods.wiped_tag_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE xqc_ods.wiped_tag_local ON CLUSTER cluster_3s_2r
(
    `company_id` String,
    `wipe_content` String,
    `wipe_index` String,
    `wipe_type` Int32,
    `wiper_id` String,
    `wiper_name` String,
    `employee_superior_id` String,
    `employee_superior_name` String,
    `employee_department_path` String,
    `dialog_id` String,
    `platform` String,
    `seller_nick` String,
    `snick` String,
    `cnick` String,
    `dialog_begin_timestamp` Int64,
    `dialog_end_timestamp` Int64,
    `messages_source` Array(Int32),
    `messages_content` Array(String),
    `messages_timestamp` Array(Int64),
    `wipe_message_content` String,
    `wipe_message_source` Int32,
    `wipe_message_timestamp` Int64,
    `create_time` String,
    `day` Int32
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
PARTITION BY day
ORDER BY (platform, company_id)
SETTINGS index_granularity = 8192, storage_policy = 'rr'


-- DROP TABLE xqc_ods.wiped_tag_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE xqc_ods.wiped_tag_all ON CLUSTER cluster_3s_2r
AS xqc_ods.wiped_tag_local
ENGINE = Distributed('cluster_3s_2r', 'xqc_ods', 'wiped_tag_local', rand())

-- DROP TABLE buffer.xqc_ods_wiped_tag_buffer ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE buffer.xqc_ods_wiped_tag_buffer ON CLUSTER cluster_3s_2r
AS xqc_ods.wiped_tag_all
ENGINE = Buffer('xqc_ods', 'wiped_tag_all', 16, 15, 35, 81920, 409600, 16777216, 67108864)