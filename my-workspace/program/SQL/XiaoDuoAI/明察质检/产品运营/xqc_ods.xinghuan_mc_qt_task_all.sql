CREATE DATABASE IF NOT EXISTS tmp ON CLUSTER cluster_3s_2r

-- DROP TABLE tmp.xinghuan_mc_qt_task_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE tmp.xinghuan_mc_qt_task_local ON CLUSTER cluster_3s_2r
(
    `_id` String,
    `create_time` String,
    `update_time` String,
    `task_id` String,
    `type` Int32,
    `title` String,
    `first_label` String,
    `second_label` String,
    `relate_staff` Array(String),
    `handler` String,
    `cc_staff` Array(String),
    `expected_finish_time` Int32,
    `real_finish_time` Int32,
    `problem` String,
    `dialog_ids` Array(String),
    `status` Int32,
    `creator`  String,
    `record` String,
    `refuse_reason` String,
    `refuse_num` Int32,
    `hold_reason` String,
    `close_reason` String,
    `platform` String,
    `expire_notify_num` Int32,
    `company_id` String
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
ORDER BY (platform, company_id)
SETTINGS index_granularity = 8192, storage_policy = 'rr'


-- DROP TABLE tmp.xinghuan_mc_qt_task_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE tmp.xinghuan_mc_qt_task_all ON CLUSTER cluster_3s_2r
AS tmp.xinghuan_mc_qt_task_local
ENGINE = Distributed('cluster_3s_2r', 'tmp', 'xinghuan_mc_qt_task_local', rand())

-- DROP TABLE buffer.tmp_xinghuan_mc_qt_task_buffer ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE buffer.tmp_xinghuan_mc_qt_task_buffer ON CLUSTER cluster_3s_2r
AS tmp.xinghuan_mc_qt_task_all
ENGINE = Buffer('tmp', 'xinghuan_mc_qt_task_all', 16, 15, 35, 81920, 409600, 16777216, 67108864)


CREATE DATABASE IF NOT EXISTS xqc_ods ON CLUSTER cluster_3s_2r

-- DROP TABLE xqc_ods.xinghuan_mc_qt_task_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE xqc_ods.xinghuan_mc_qt_task_local ON CLUSTER cluster_3s_2r
(
    `_id` String,
    `create_time` String,
    `update_time` String,
    `task_id` String,
    `type` Int32,
    `title` String,
    `first_label` String,
    `second_label` String,
    `relate_staff` Array(String),
    `handler` String,
    `cc_staff` Array(String),
    `expected_finish_time` Int32,
    `real_finish_time` Int32,
    `problem` String,
    `dialog_ids` Array(String),
    `status` Int32,
    `creator`  String,
    `record` String,
    `refuse_reason` String,
    `refuse_num` Int32,
    `hold_reason` String,
    `close_reason` String,
    `platform` String,
    `expire_notify_num` Int32,
    `company_id` String
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
ORDER BY (platform, company_id)
SETTINGS index_granularity = 8192, storage_policy = 'rr'


-- DROP TABLE xqc_ods.xinghuan_mc_qt_task_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE xqc_ods.xinghuan_mc_qt_task_all ON CLUSTER cluster_3s_2r
AS xqc_ods.xinghuan_mc_qt_task_local
ENGINE = Distributed('cluster_3s_2r', 'xqc_ods', 'xinghuan_mc_qt_task_local', rand())

-- DROP TABLE buffer.xqc_ods_xinghuan_mc_qt_task_buffer ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE buffer.xqc_ods_xinghuan_mc_qt_task_buffer ON CLUSTER cluster_3s_2r
AS xqc_ods.xinghuan_mc_qt_task_all
ENGINE = Buffer('xqc_ods', 'xinghuan_mc_qt_task_all', 16, 15, 35, 81920, 409600, 16777216, 67108864)