CREATE DATABASE IF NOT EXISTS xqc_dwd ON CLUSTER cluster_3s_2r ENGINE=Ordinary

-- DROP TABLE xqc_dwd.manual_task_record_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE xqc_dwd.manual_task_record_local ON CLUSTER cluster_3s_2r
(
    `_id` String,
    `create_time` String,
    `update_time` String,
    `company_id` String,
    `company_name` String,
    `platform` String,
    `day` Int32,
    `date` Int32,
    `task_id` String,
    `task_name` String,
    `qc_type` Int32,
    `qc_way` Int32,
    `account_name` String,
    `qc_norm_id` String,
    `target_num` Int32,
    `mark_num` Int32,
    `ontime_mark_num` Int32,
    `overdue_mark_num` Int32,
    `ai_num` Int32,
    `ai_rate` String,
    `human_num` Int32,
    `human_rate` String
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
ORDER BY (platform, company_id)
SETTINGS index_granularity = 8192, storage_policy = 'rr'


-- DROP TABLE xqc_dwd.manual_task_record_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE xqc_dwd.manual_task_record_all ON CLUSTER cluster_3s_2r
AS xqc_dwd.manual_task_record_local
ENGINE = Distributed('cluster_3s_2r', 'xqc_dwd', 'manual_task_record_local', rand())

-- DROP TABLE buffer.xqc_dwd_manual_task_record_buffer ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE buffer.xqc_dwd_manual_task_record_buffer ON CLUSTER cluster_3s_2r
AS xqc_dwd.manual_task_record_all
ENGINE = Buffer('xqc_dwd', 'manual_task_record_all', 16, 15, 35, 81920, 409600, 16777216, 67108864)