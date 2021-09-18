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

-- 实时告警表(改)
CREATE TABLE xqc_ods.alert_local ON CLUSTER cluster_3s_2r(
    `id` String,
    `level` Int64,
    `warning_type` String,
    `dialog_id` String,
    `message_id` String,
    `time` DateTime,
    `day` Int64,
    `is_finished` String,
    `finish_time` String,
    `seller_nick` String,
    `shop_id` String,
    `snick` String,
    `cnick` String,
    `employee_name` String,
    `superior_name` String
)
ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/xqc_ods/tables/{layer}_{shard}/alert_local',
    '{replica}',
    time
)
PARTITION BY `day`
ORDER BY (`level`,`warning_type`)
SETTINGS index_granularity=8192, storage_policy='rr'

CREATE TABLE xqc_ods.alert_all ON CLUSTER cluster_3s_2r
AS xqc_ods.alert_local
ENGINE = Distributed('cluster_3s_2r', 'xqc_ods', 'alert_local', rand())

-- 填充测试数据
TRUNCATE TABLE xqc_ods.alert_local ON CLUSTER cluster_3s_2r
INSERT INTO xqc_ods.alert_all
SELECT
    id, 
    type AS level,
    reason AS warning_type,
    dialog_id,
    'test' AS message_id,
    parseDateTimeBestEffort(create_time) AS time,
    day,
    done as is_finished,
    if(done='True',toString(now()),'') as finish_time,
    `seller_nick`,
    `shop_id`,
    `snick`,
    `cnick`,
    'test' AS employee_name,
    'test' AS superior_name
FROM xqc_ods.event_alert_all
WHERE day BETWEEN 20210624 AND 20210916


ALTER TABLE xqc_dim.group_local ON CLUSTER cluster_3s_2r 
DELETE WHERE company_id='5f747ba42c90fd0001254404'

INSERT INTO xqc_dim.group_all
VALUES 
('5f747ba42c90fd0001254404','方太','','','5f747ba42c90fd0001254401','一级部门1','False','',[],1),
('5f747ba42c90fd0001254404','方太','','','5f747ba42c90fd0001254402','一级部门2','False','',[],1),
('5f747ba42c90fd0001254404','方太','','','5f747ba42c90fd0001254403','一级部门3','False','',[],1),
('5f747ba42c90fd0001254404','方太','','','5f747ba42c90fd0001254404','二级部门1','False','',['5f747ba42c90fd0001254401'],2),
('5f747ba42c90fd0001254404','方太','','','5f747ba42c90fd0001254405','二级部门2','False','',['5f747ba42c90fd0001254403'],2),
('5f747ba42c90fd0001254404','方太','','','5edfa47c8f591c00163ef7d6','方太京东旗舰店','True','jd',['5f747ba42c90fd0001254401','5f747ba42c90fd0001254404'],3),
('5f747ba42c90fd0001254404','方太','','','5e9d390d68283c002457b52f','方太京东自营旗舰店','True','jd',['5f747ba42c90fd0001254402','5f747ba42c90fd0001254405'],3)
('5f747ba42c90fd0001254404','方太','','','5cac112e98ef4100118a9c9f','方太官方旗舰店','True','tb',['5f747ba42c90fd0001254403','5f747ba42c90fd0001254405'],3)