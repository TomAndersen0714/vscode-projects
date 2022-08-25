CREATE DATABASE IF NOT EXISTS xqc_dwd ON CLUSTER cluster_3s_2r ENGINE=Ordinary

-- DROP TABLE xqc_dwd.wiped_tag_new_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE xqc_dwd.wiped_tag_new_local ON CLUSTER cluster_3s_2r
(
    `day` Int32,
    `company_id` String,
    `company_name` String,
    `platform` String,
    `shop_id` String,
    `shop_name` String,
    `seller_nick` String,
    `snick` String,
    `cnick` String,
    `dialog_id` String,
    `dialog_begin_timestamp` Int64,
    `dialog_end_timestamp` Int64,
    `messages_source` Array(Int32),
    `messages_content` Array(String),
    `messages_timestamp` Array(Int64),

    `wipe_id` String,
    `wipe_content` String,
    `wipe_index` Int32,
    `wipe_type` Int32,
    `wipe_message_content` String,
    `wipe_message_source` Int32,
    `wipe_message_timestamp` Int64,

    `wiper_id` String,
    `wiper_name` String,
    `employee_superior_id` String,
    `employee_superior_name` String,
    `employee_department_path` String,
    `create_time` String
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
PARTITION BY day
ORDER BY (platform, company_id, day)
SETTINGS index_granularity = 8192, storage_policy = 'rr'


-- DROP TABLE xqc_dwd.wiped_tag_new_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE xqc_dwd.wiped_tag_new_all ON CLUSTER cluster_3s_2r
AS xqc_dwd.wiped_tag_new_local
ENGINE = Distributed('cluster_3s_2r', 'xqc_dwd', 'wiped_tag_new_local', rand())

-- DROP TABLE buffer.xqc_dwd_wiped_tag_new_buffer ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE buffer.xqc_dwd_wiped_tag_new_buffer ON CLUSTER cluster_3s_2r
AS xqc_dwd.wiped_tag_new_all
ENGINE = Buffer('xqc_dwd', 'wiped_tag_new_all', 16, 15, 35, 81920, 409600, 16777216, 67108864)