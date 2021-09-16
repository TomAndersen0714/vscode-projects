-- XQC公司组织架构维度表
CREATE TABLE xqc_dim.group_local ON CLUSTER cluster_3s_2r(
    `company_id` String,
    `company_name` String,
    `create_time` String,
    `update_time` String,
    `department_id` String,
    `department_name` String,
    `is_shop` String,
    `platform` String,
    `parent_department_path` Array(String),
    `level` Int64
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/xqc_dim/tables/{layer}_{shard}/group_local',
    '{replica}'
)
ORDER BY (`company_id`,`department_id`) 
SETTINGS index_granularity=8192, storage_policy='rr'

CREATE TABLE xqc_dim.group_all ON CLUSTER cluster_3s_2r
AS xqc_dim.group_local
ENGINE = Distributed('cluster_3s_2r', 'xqc_dim', 'group_local', rand())

-- XQC店铺维度表
CREATE TABLE xqc_dim.shop_local ON CLUSTER cluster_3s_2r(
    `id` String,
    `create_time` String,
    `update_time` String,
    `company_id` String,
    `shop_id` String,
    `platform` String,
    `seller_nick` String,
    `plat_shop_name` String,
    `plat_shop_id` String
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/xqc_dim/tables/{layer}_{shard}/shop_local',
    '{replica}'
)
PARTITION BY `platform`
ORDER BY (`company_id`,`plat_shop_name`) 
SETTINGS index_granularity=8192, storage_policy='rr'

CREATE TABLE xqc_dim.shop_all ON CLUSTER cluster_3s_2r 
AS xqc_dim.shop_local
ENGINE = Distributed('cluster_3s_2r', 'xqc_dim', 'shop_local', rand())

-- XQC子账号维度表
CREATE TABLE xqc_dim.snick_local ON CLUSTER cluster_3s_2r(
    `id` String,
    `create_time` String,
    `update_time` String,
    `company_id` String,
    `mp_shop_id` String,
    `platform` String,
    `seller_nick` String,
    `department_id` String,
    `employee_id` String,
    `snick` String,
    `status` Int64,
    `new` String
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/xqc_dim/tables/{layer}_{shard}/snick_local',
    '{replica}'
)
PARTITION BY `platform`
ORDER BY (`snick`) 
SETTINGS index_granularity=8192, storage_policy='rr'

CREATE TABLE xqc_dim.snick_all ON CLUSTER cluster_3s_2r 
AS xqc_dim.snick_local
ENGINE = Distributed('cluster_3s_2r', 'xqc_dim', 'snick_local', rand())


-- XQC实时会话表
CREATE TABLE xqc_ods.dialog_local ON CLUSTER cluster_3s_2r(
    `id` String,
    `platform` String,
    `shop_id` String,
    `seller_nick`  String,
    `snick` String,
    `cnick` String,
    `employee_name` String,
    `superior_name` String,
    `time` String,
    `hour` Int64,
    `day` Int64
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/xqc_ods/tables/{layer}_{shard}/dialog_local',
    '{replica}'
)
PARTITION BY (`day`, `platform`)
ORDER BY `snick`
SETTINGS index_granularity=8192, storage_policy='rr'

CREATE TABLE xqc_ods.dialog_all ON CLUSTER cluster_3s_2r 
AS xqc_ods.dialog_local
ENGINE = Distributed('cluster_3s_2r', 'xqc_ods', 'dialog_local', rand())

CREATE TABLE buffer.dialog_buffer ON CLUSTER cluster_3s_2r 
AS xqc_ods.dialog_all
ENGINE = Buffer('xqc_ods', 'dialog_all', 16, 5, 10, 81920, 409600, 16777216, 67108864)

-- 实时告警表
CREATE TABLE xqc_ods.event_alert_1_local ON CLUSTER cluster_3s_2r(
    `id` String,
    `level` Int64,
    `warning_type` String,
    `dialog_id` String,
    `message_id` String,
    `time` String,
    `day` Int64,
    `is_finished` String,
    `finish_time` String,
    `update_time` DateTime
)
ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/xqc_ods/tables/{layer}_{shard}/event_alert_1_local',
    '{replica}',
    update_time
)
PARTITION BY `day`
ORDER BY (`dialog_id`,`id`)
SETTINGS index_granularity=8192, storage_policy='rr'

CREATE TABLE xqc_ods.event_alert_1_all ON CLUSTER cluster_3s_2r
AS xqc_ods.event_alert_1_local
ENGINE = Distributed('cluster_3s_2r', 'xqc_ods', 'event_alert_1_local', rand())

CREATE TABLE buffer.event_alert_1_buffer ON CLUSTER cluster_3s_2r
AS xqc_ods.event_alert_1_all
ENGINE = Buffer('xqc_ods', 'event_alert_1_all', 16, 5, 10, 81920, 409600, 16777216, 67108864)