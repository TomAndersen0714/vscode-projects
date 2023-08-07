-- DROP TABLE xqc_dim.qc_rule_full_info_latest_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE IF NOT EXISTS xqc_dim.qc_rule_full_info_latest_local ON CLUSTER cluster_3s_2r
(
    `_id` String,
    `create_time` String,
    `update_time` String,
    `company_id` String,
    `platform` String,
    `create_account` String,
    `update_account` String,
    `qc_norm_id` String,
    `qc_norm_name` String,
    `qc_norm_group_id` String,
    `qc_norm_group_name` String,
    `qc_norm_group_full_name` String,
    `template_id` String,
    `name` String,
    `rule_category` Int32,
    `rule_type` Int32,
    `settings` String,
    `check` String,
    `check_target` Int32,
    `alert_level` Int32,
    `notify_way` Int32,
    `notify_target` Int32,
    `score` Int32,
    `threshold` Float64,
    `special_settings` String,
    `status` Int32,
    `day` Int32
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
ORDER BY (company_id, platform)
SETTINGS index_granularity = 8192, storage_policy = 'rr';

-- DROP TABLE xqc_dim.qc_rule_full_info_latest_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE IF NOT EXISTS xqc_dim.qc_rule_full_info_latest_all ON CLUSTER cluster_3s_2r
AS xqc_dim.qc_rule_full_info_latest_local
ENGINE = Distributed('cluster_3s_2r', 'xqc_dim', 'qc_rule_full_info_latest_local', rand());


-- ALTER TABLE xqc_dim.snick_full_info_local ON CLUSTER cluster_3s_2r 
-- DROP COLUMN IF EXISTS `company_id`;
ALTER TABLE xqc_dim.snick_full_info_local ON CLUSTER cluster_3s_2r
ADD COLUMN IF NOT EXISTS `qc_norm_id` String AFTER `department_name`,
ADD COLUMN IF NOT EXISTS `qc_norm_name` String AFTER `qc_norm_id`;


-- ALTER TABLE xqc_dim.snick_full_info_all ON CLUSTER cluster_3s_2r 
-- DROP COLUMN IF EXISTS `company_id`;
ALTER TABLE xqc_dim.snick_full_info_all ON CLUSTER cluster_3s_2r
ADD COLUMN IF NOT EXISTS `qc_norm_id` String AFTER `department_name`,
ADD COLUMN IF NOT EXISTS `qc_norm_name` String AFTER `qc_norm_id`;