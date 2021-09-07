-- Create database
CREATE DATABASE IF NOT EXISTS tmp ON CLUSTER cluster_3s_2r ENGINE = Ordinary
CREATE DATABASE IF NOT EXISTS xqc_ods ON CLUSTER cluster_3s_2r ENGINE = Ordinary

-- DROP TABLE tmp.xdqc_tb_task_record_daily_local ON CLUSTER cluster_3s_2r
-- Create tmp local table
CREATE TABLE tmp.xdqc_tb_task_record_daily_local ON CLUSTER cluster_3s_2r(
    `_id` String,
    `platform` String,
    `channel` String,
    `seller_nick` String,
    `group` String,
    `date` Int64,
    `account_name` String,
    `account_id` String,
    `task_mode` Int64,
    `dialog_count` Int64,
    `abnormal_dialog_count` Int64,
    `mark_dialog_count` Int64,
    `mp_shop_id` String
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/tmp/tables/{layer}_{shard}/xdqc_tb_task_record_daily_local',
    '{replica}'
)
PARTITION BY (`date`)
ORDER BY (`mp_shop_id`,`account_id`) 
SETTINGS index_granularity=8192,storage_policy='rr'

-- DROP TABLE tmp.xdqc_tb_task_record_daily_all ON CLUSTER cluster_3s_2r 
-- Create tmp distributed table
CREATE TABLE tmp.xdqc_tb_task_record_daily_all ON CLUSTER cluster_3s_2r 
AS tmp.xdqc_tb_task_record_daily_local 
ENGINE = Distributed('cluster_3s_2r', 'tmp', 'xdqc_tb_task_record_daily_local', rand())

-- DROP TABLE xqc_ods.xdqc_tb_task_record_local ON CLUSTER cluster_3s_2r 
-- Create xqc_ods local table
CREATE TABLE xqc_ods.xdqc_tb_task_record_local ON CLUSTER cluster_3s_2r 
AS tmp.xdqc_tb_task_record_daily_local
ENGINE = ReplicatedMergeTree(
    '/clickhouse/xqc_ods/tables/{layer}_{shard}/xdqc_tb_task_record_local',
    '{replica}'
)
PARTITION BY (`date`)
ORDER BY (`mp_shop_id`,`account_id`) 
SETTINGS index_granularity = 8192, storage_policy = 'rr'

-- DROP TABLE xqc_ods.xdqc_tb_task_record_all ON CLUSTER cluster_3s_2r
-- Create xqc_ods distributed table
CREATE TABLE xqc_ods.xdqc_tb_task_record_all ON CLUSTER cluster_3s_2r 
AS xqc_ods.xdqc_tb_task_record_local 
ENGINE = Distributed('cluster_3s_2r', 'xqc_ods', 'xdqc_tb_task_record_local', rand())





-- Create tmp local table
CREATE TABLE tmp.xdqc_tb_task_record_daily_local ON CLUSTER cluster_3s_2r(
    `_id` String,
    `platform` String,
    `channel` String,
    `seller_nick` String,
    `group` String,
    `date` Int64,
    `account_name` String,
    `account_id` String,
    `task_mode` Int64,
    `dialog_count` Int64,
    `abnormal_dialog_count` Int64,
    `mark_dialog_count` Int64,
    `mp_shop_id` String
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/tmp/tables/{layer}_{shard}/xdqc_tb_task_record_daily_local',
    '{replica}'
)
PARTITION BY (`platform`,intDiv(`date`,100))
ORDER BY (`mp_shop_id`,`account_id`) 
SETTINGS index_granularity=8192,storage_policy='rr'

-- Create tmp distributed table
CREATE TABLE tmp.xdqc_tb_task_record_daily_all ON CLUSTER cluster_3s_2r 
AS tmp.xdqc_tb_task_record_daily_local 
ENGINE = Distributed('cluster_3s_2r', 'tmp', 'xdqc_tb_task_record_daily_local', rand())

-- Create xqc_ods local table
CREATE TABLE xqc_ods.xdqc_tb_task_record_local ON CLUSTER cluster_3s_2r 
AS tmp.xdqc_tb_task_record_daily_local
ENGINE = ReplicatedMergeTree(
    '/clickhouse/xqc_ods/tables/{layer}_{shard}/xdqc_tb_task_record_local',
    '{replica}'
)
PARTITION BY (`platform`,intDiv(`date`,100))
ORDER BY (`mp_shop_id`,`account_id`) 
SETTINGS index_granularity = 8192, storage_policy = 'rr'

-- Create xqc_ods distributed table
CREATE TABLE xqc_ods.xdqc_tb_task_record_all ON CLUSTER cluster_3s_2r 
AS xqc_ods.xdqc_tb_task_record_local 
ENGINE = Distributed('cluster_3s_2r', 'xqc_ods', 'xdqc_tb_task_record_local', rand())