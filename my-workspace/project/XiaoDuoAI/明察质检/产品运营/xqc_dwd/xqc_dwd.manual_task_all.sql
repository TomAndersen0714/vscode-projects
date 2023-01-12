CREATE DATABASE IF NOT EXISTS xqc_dwd ON CLUSTER cluster_3s_2r ENGINE=Ordinary

-- DROP TABLE xqc_dwd.manual_task_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE xqc_dwd.manual_task_local ON CLUSTER cluster_3s_2r
(
    `_id` String,
    `create_time` String,
    `update_time` String,
    `platform` String,
    `company_id` String,
    `company_name` String,
    `creator` String,
    `name` String,
    `type` String,
    `account_id` String,
    `account_name` String,
    `cycle_strategy` Int32,
    `cycle_date_gte` Int64,
    `cycle_date_lte` Int64,
    `dialog_date` Int64,
    `dialog_date_range_gte` Int64,
    `dialog_date_range_lte` Int64,
    `task_grade` Int64,
    `qc_way` Int32,
    `target_num` Int64,
    `employee_ids` Array(String),
    `real_num` Int64,
    `each_num` Int64
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
ORDER BY (platform, company_id)
SETTINGS index_granularity = 8192, storage_policy = 'rr'


-- DROP TABLE xqc_dwd.manual_task_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE xqc_dwd.manual_task_all ON CLUSTER cluster_3s_2r
AS xqc_dwd.manual_task_local
ENGINE = Distributed('cluster_3s_2r', 'xqc_dwd', 'manual_task_local', rand())

-- DROP TABLE buffer.xqc_dwd_manual_task_buffer ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE buffer.xqc_dwd_manual_task_buffer ON CLUSTER cluster_3s_2r
AS xqc_dwd.manual_task_all
ENGINE = Buffer('xqc_dwd', 'manual_task_all', 16, 15, 35, 81920, 409600, 16777216, 67108864)