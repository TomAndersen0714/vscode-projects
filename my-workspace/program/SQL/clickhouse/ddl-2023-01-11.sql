CREATE DATABASE IF NOT EXISTS feishu_ods ON CLUSTER cluster_3s_2r
ENGINE=Ordinary;

-- 公共字段表
DROP TABLE IF EXISTS feishu_ods.feishu_work_item_pub_local ON CLUSTER cluster_3s_2r NO DELAY;

CREATE TABLE IF NOT EXISTS feishu_ods.feishu_work_item_pub_local ON CLUSTER cluster_3s_2r
(
    `id` Int32 COMMENT '工作项id',
    `name` String COMMENT '工作流名称',
    `work_item_type_key` String COMMENT '工作项类型',
    `work_item_status` String COMMENT '工作项历史状态(除需求外有值)',
    `project_key` String COMMENT '空间id ',
    `simple_name` String COMMENT '空间域名',
    `sub_stage` String COMMENT '当前工作项状态的key',
    `current_nodes_id` Array(String) COMMENT '当前进行中的所有节点id',
    `current_nodes_name` Array(String) COMMENT '当前进行中的所有节点的状态',
    `current_nodes_owner` Array(Array(String)) COMMENT '当前进行中的所有节点的负责人',
    `state_name` Array(String) COMMENT '节点名字',
    `state_key` Array(String) COMMENT '节点状态',
    `state_start_time` Array(String) COMMENT '节点开始时间 ',
    `state_end_time` Array(String) COMMENT '节点结束时间 ',
    `created_at` String COMMENT '创建时间 ',
    `created_by` String COMMENT '创建者userkey',
    `deleted_at` String COMMENT '删除时间 ',
    `deleted_by` String COMMENT '删除者userkey',
    `updated_at` String COMMENT '更新时间 ',
    `updated_by` String COMMENT '更新者userkey'
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
ORDER BY (`project_key`,`id`)
SETTINGS index_granularity = 8192, storage_policy = 'rr';


DROP TABLE IF EXISTS feishu_ods.feishu_work_item_pub_all ON CLUSTER cluster_3s_2r NO DELAY;

CREATE TABLE IF NOT EXISTS feishu_ods.feishu_work_item_pub_all ON CLUSTER cluster_3s_2r
AS feishu_ods.feishu_work_item_pub_local
ENGINE = Distributed('cluster_3s_2r', 'feishu_ods', 'feishu_work_item_pub_local', rand());

DROP TABLE IF EXISTS buffer.feishu_work_item_pub_buffer ON CLUSTER cluster_3s_2r NO DELAY;

CREATE TABLE IF NOT EXISTS buffer.feishu_work_item_pub_buffer ON CLUSTER cluster_3s_2r
AS feishu_ods.feishu_work_item_pub_all
ENGINE = Buffer('feishu_ods', 'feishu_work_item_pub_all', 16, 15, 35, 81920, 409600, 16777216, 67108864);

-- 自定义字段表
DROP TABLE IF EXISTS feishu_ods.feishu_work_item_custom_local ON CLUSTER cluster_3s_2r NO DELAY;

CREATE TABLE IF NOT EXISTS feishu_ods.feishu_work_item_custom_local ON CLUSTER cluster_3s_2r
(
    `project_key` String COMMENT '空间id',
    `id` Int32 COMMENT '工作项id',
    `field_alias` String COMMENT '字段别名',
    `field_key` String COMMENT '字段key',
    `field_type_key` String COMMENT '字段类型',
    `field_value` String COMMENT '字段值'
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
ORDER BY (`project_key`,`id`)
SETTINGS index_granularity = 8192, storage_policy = 'rr';


DROP TABLE IF EXISTS feishu_ods.feishu_work_item_custom_all ON CLUSTER cluster_3s_2r NO DELAY;

CREATE TABLE IF NOT EXISTS feishu_ods.feishu_work_item_custom_all ON CLUSTER cluster_3s_2r
AS feishu_ods.feishu_work_item_custom_local 
ENGINE = Distributed('cluster_3s_2r', 'feishu_ods', 'feishu_work_item_custom_local', rand());


DROP TABLE IF EXISTS buffer.feishu_work_item_custom_buffer ON CLUSTER cluster_3s_2r NO DELAY;

CREATE TABLE IF NOT EXISTS buffer.feishu_work_item_custom_buffer ON CLUSTER cluster_3s_2r
AS feishu_ods.feishu_work_item_custom_all
ENGINE = Buffer('feishu_ods', 'feishu_work_item_custom_all', 16, 15, 35, 81920, 409600, 16777216, 67108864);

-- 用户信息表
DROP TABLE IF EXISTS feishu_ods.user_info_local ON CLUSTER cluster_3s_2r NO DELAY;

CREATE TABLE IF NOT EXISTS feishu_ods.user_info_local ON CLUSTER cluster_3s_2r
(
    `user_key` String,
    `username` String
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
ORDER BY (`user_key` ,`username`)
SETTINGS index_granularity = 8192, storage_policy = 'rr';


DROP TABLE IF EXISTS feishu_ods.user_info_all ON CLUSTER cluster_3s_2r NO DELAY;

CREATE TABLE IF NOT EXISTS feishu_ods.user_info_all ON CLUSTER cluster_3s_2r
AS feishu_ods.user_info_local
ENGINE = Distributed('cluster_3s_2r', 'feishu_ods', 'user_info_local', rand());