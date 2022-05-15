-- xqc_ods.qt_task_local
-- DROP TABLE xqc_ods.qt_task_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE xqc_ods.qt_task_local ON CLUSTER cluster_3s_2r
(
    `_id` String,
    `create_time` String,
    `update_time` String,
    `company_id` String,
    `platform` String,
    `task_id` String,
    `type` Int32,
    `title` String,
    `first_label` String,
    `second_label` String,
    `relate_staff` Array(String),
    `handler` String,
    `cc_staff` Array(String),
    `expected_finish_time` Int64,
    `real_finish_time` Int64,
    `problem` String,
    `dialog_ids` Array(String),
    `status` Int32,
    `creator` String,
    `attachment` Array(String),
    `record` String,
    `refuse_reason` String,
    `refuse_num` Int32,
    `hold_reason` String,
    `close_reason` String,
    `expire_notify_num` Int32,
    `remark_info` Array(String),
    `snicks` Array(String),
    `cnicks` Array(String)
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
ORDER BY (company_id, platform, task_id)
SETTINGS storage_policy = 'rr', index_granularity = 8192

-- xqc_ods.qt_task_all
-- DROP TABLE xqc_ods.qt_task_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE xqc_ods.qt_task_all ON CLUSTER cluster_3s_2r
AS xqc_ods.qt_task_local
ENGINE = Distributed('cluster_3s_2r', 'xqc_ods', 'qt_task_local', rand())
