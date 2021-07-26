-- Create database
CREATE DATABASE IF NOT EXISTS tmp ENGINE = Ordinary
CREATE DATABASE IF NOT EXISTS xqc_ods ENGINE = Ordinary

-- 京东是单节点,但是设置有多盘存储策略'rr'
-- Create tmp local table
CREATE TABLE tmp.xdqc_tb_task_record_local(
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
ENGINE = MergeTree() 
PARTITION BY (`platform`,intDiv(`date`,100))
ORDER BY (`mp_shop_id`,`account_id`) 
SETTINGS index_granularity=8192,storage_policy='rr'

-- Create xqc_ods local table
CREATE TABLE xqc_ods.xdqc_tb_task_record_local
AS tmp.xdqc_tb_task_record_local
ENGINE = MergeTree() 
PARTITION BY (`platform`,intDiv(`date`,100))
ORDER BY (`mp_shop_id`,`account_id`) 
SETTINGS index_granularity = 8192, storage_policy = 'rr'