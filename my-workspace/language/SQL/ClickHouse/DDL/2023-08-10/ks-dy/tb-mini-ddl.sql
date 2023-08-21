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
    `seller_nick` String,
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
    `status` Int32
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
ORDER BY (company_id, platform, qc_norm_id)
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


CREATE DATABASE IF NOT EXISTS xqc_dwd ON CLUSTER cluster_3s_2r
ENGINE = Ordinary

-- ALTER TABLE ods.xinghuan_dialog_tag_score_local ON CLUSTER cluster_3s_2r 
-- DROP COLUMN IF EXISTS `mark_employee_id`,
-- DROP COLUMN IF EXISTS `mark_employee_name`

ALTER TABLE ods.xinghuan_dialog_tag_score_local ON CLUSTER cluster_3s_2r
ADD COLUMN IF NOT EXISTS `company_id` String AFTER `platform`,
ADD COLUMN IF NOT EXISTS `shop_id` String AFTER `company_id`,
ADD COLUMN IF NOT EXISTS `employee_id` String AFTER `snick`,
ADD COLUMN IF NOT EXISTS `employee_name` String AFTER `employee_id`,
ADD COLUMN IF NOT EXISTS `department_id` String AFTER `employee_name`,
ADD COLUMN IF NOT EXISTS `department_name` String AFTER `department_id`,
ADD COLUMN IF NOT EXISTS `mark_account_id` String AFTER `department_name`,
ADD COLUMN IF NOT EXISTS `mark_account_name` String AFTER `mark_account_id`,
ADD COLUMN IF NOT EXISTS `qc_norm_id` String AFTER `mark_account_name`,
ADD COLUMN IF NOT EXISTS `qc_norm_name` String AFTER `qc_norm_id`,
ADD COLUMN IF NOT EXISTS `qc_norm_group_id` String AFTER `qc_norm_name`,
ADD COLUMN IF NOT EXISTS `qc_norm_group_name` String AFTER `qc_norm_group_id`,
ADD COLUMN IF NOT EXISTS `qc_norm_group_full_name` String AFTER `qc_norm_group_name`;


-- ALTER TABLE ods.xinghuan_dialog_tag_score_all ON CLUSTER cluster_3s_2r 
-- DROP COLUMN IF EXISTS `mark_employee_id`,
-- DROP COLUMN IF EXISTS `mark_employee_name`
ALTER TABLE ods.xinghuan_dialog_tag_score_all ON CLUSTER cluster_3s_2r
ADD COLUMN IF NOT EXISTS `company_id` String AFTER `platform`,
ADD COLUMN IF NOT EXISTS `shop_id` String AFTER `company_id`,
ADD COLUMN IF NOT EXISTS `employee_id` String AFTER `snick`,
ADD COLUMN IF NOT EXISTS `employee_name` String AFTER `employee_id`,
ADD COLUMN IF NOT EXISTS `department_id` String AFTER `employee_name`,
ADD COLUMN IF NOT EXISTS `department_name` String AFTER `department_id`,
ADD COLUMN IF NOT EXISTS `mark_account_id` String AFTER `department_name`,
ADD COLUMN IF NOT EXISTS `mark_account_name` String AFTER `mark_account_id`,
ADD COLUMN IF NOT EXISTS `qc_norm_id` String AFTER `mark_account_name`,
ADD COLUMN IF NOT EXISTS `qc_norm_name` String AFTER `qc_norm_id`,
ADD COLUMN IF NOT EXISTS `qc_norm_group_id` String AFTER `qc_norm_name`,
ADD COLUMN IF NOT EXISTS `qc_norm_group_name` String AFTER `qc_norm_group_id`,
ADD COLUMN IF NOT EXISTS `qc_norm_group_full_name` String AFTER `qc_norm_group_name`;


-- DROP TABLE xqc_dwd.xplat_manual_tag_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE IF NOT EXISTS xqc_dwd.xplat_manual_tag_local ON CLUSTER cluster_3s_2r
AS ods.xinghuan_dialog_tag_score_all
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
PARTITION BY (day, platform)
ORDER BY (company_id, shop_id)
SETTINGS index_granularity = 8192, storage_policy = 'rr';


-- DROP TABLE xqc_dwd.xplat_manual_tag_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE IF NOT EXISTS xqc_dwd.xplat_manual_tag_all ON CLUSTER cluster_3s_2r
AS xqc_dwd.xplat_manual_tag_local
ENGINE = Distributed('cluster_3s_2r', 'xqc_dwd', 'xplat_manual_tag_local', rand());


-- DROP TABLE buffer.xqc_dwd_xplat_manual_tag_buffer ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE IF NOT EXISTS buffer.xqc_dwd_xplat_manual_tag_buffer ON CLUSTER cluster_3s_2r
AS xqc_dwd.xplat_manual_tag_all
ENGINE = Buffer('xqc_dwd', 'xplat_manual_tag_all', 16, 15, 35, 81920, 409600, 16777216, 67108864);

-- ALTER TABLE xqc_dws.snick_stat_local ON CLUSTER cluster_3s_2r 
-- DROP COLUMN IF EXISTS `dialog_score_avg`;
ALTER TABLE xqc_dws.snick_stat_local ON CLUSTER cluster_3s_2r
ADD COLUMN IF NOT EXISTS `company_id` String AFTER `day`,
ADD COLUMN IF NOT EXISTS `shop_id` String AFTER `company_id`,
ADD COLUMN IF NOT EXISTS `qc_norm_id` String AFTER `department_name`,
ADD COLUMN IF NOT EXISTS `qc_norm_name` String AFTER `qc_norm_id`,
ADD COLUMN IF NOT EXISTS `qc_norm_tag_cnt` String AFTER `qc_norm_name`,
ADD COLUMN IF NOT EXISTS `qc_norm_ai_tag_cnt` String AFTER `qc_norm_tag_cnt`,
ADD COLUMN IF NOT EXISTS `qc_norm_custom_tag_cnt` String AFTER `qc_norm_ai_tag_cnt`,
ADD COLUMN IF NOT EXISTS `qc_norm_manual_tag_cnt` String AFTER `qc_norm_custom_tag_cnt`,
ADD COLUMN IF NOT EXISTS `qc_norm_alert_tag_cnt` String AFTER `qc_norm_manual_tag_cnt`,
ADD COLUMN IF NOT EXISTS `dialog_tag_cnt` Int64 AFTER `qc_norm_alert_tag_cnt`,
ADD COLUMN IF NOT EXISTS `dialog_ai_tag_cnt` Int64 AFTER `dialog_tag_cnt`,
ADD COLUMN IF NOT EXISTS `dialog_custom_tag_cnt` Int64 AFTER `dialog_ai_tag_cnt`,
ADD COLUMN IF NOT EXISTS `dialog_manual_tag_cnt` Int64 AFTER `dialog_custom_tag_cnt`,
ADD COLUMN IF NOT EXISTS `excellent_dialog_cnt` Int64 AFTER `dialog_cnt`,
ADD COLUMN IF NOT EXISTS `dialog_score_avg` Float64 AFTER  `manual_add_score_sum`,
-- ADD COLUMN IF NOT EXISTS `ai_tagged_subtract_score_dialog_cnt` Int64 AFTER `ai_tagged_dialog_cnt`,
-- ADD COLUMN IF NOT EXISTS `ai_tagged_add_score_dialog_cnt` Int64 AFTER `ai_tagged_subtract_score_dialog_cnt`,
-- ADD COLUMN IF NOT EXISTS `ai_tagged_zero_score_dialog_cnt` Int64 AFTER `ai_tagged_add_score_dialog_cnt`,
ADD COLUMN IF NOT EXISTS `ai_zero_score_tagged_dialog_cnt` Int64 AFTER `ai_add_score_dialog_cnt`,
-- ADD COLUMN IF NOT EXISTS `custom_tagged_subtract_score_dialog_cnt` Int64 AFTER `custom_tagged_dialog_cnt`,
-- ADD COLUMN IF NOT EXISTS `custom_tagged_add_score_dialog_cnt` Int64 AFTER `custom_tagged_subtract_score_dialog_cnt`,
-- ADD COLUMN IF NOT EXISTS `custom_tagged_zero_score_dialog_cnt` Int64 AFTER `custom_tagged_add_score_dialog_cnt`,
ADD COLUMN IF NOT EXISTS `custom_zero_score_tagged_dialog_cnt` Int64 AFTER `custom_add_score_dialog_cnt`,
-- ADD COLUMN IF NOT EXISTS `manual_tagged_subtract_score_dialog_cnt` Int64 AFTER `manual_tagged_dialog_cnt`,
-- ADD COLUMN IF NOT EXISTS `manual_tagged_add_score_dialog_cnt` Int64 AFTER `manual_tagged_subtract_score_dialog_cnt`,
-- ADD COLUMN IF NOT EXISTS `manual_tagged_zero_score_dialog_cnt` Int64 AFTER `manual_tagged_add_score_dialog_cnt`,
ADD COLUMN IF NOT EXISTS `manual_zero_score_tagged_dialog_cnt` Int64 AFTER `manual_add_score_dialog_cnt`;


-- ALTER TABLE xqc_dws.snick_stat_all ON CLUSTER cluster_3s_2r
-- DROP COLUMN IF EXISTS `dialog_score_avg`;
ALTER TABLE xqc_dws.snick_stat_all ON CLUSTER cluster_3s_2r
ADD COLUMN IF NOT EXISTS `company_id` String AFTER `day`,
ADD COLUMN IF NOT EXISTS `shop_id` String AFTER `company_id`,
ADD COLUMN IF NOT EXISTS `qc_norm_id` String AFTER `department_name`,
ADD COLUMN IF NOT EXISTS `qc_norm_name` String AFTER `qc_norm_id`,
ADD COLUMN IF NOT EXISTS `qc_norm_tag_cnt` String AFTER `qc_norm_name`,
ADD COLUMN IF NOT EXISTS `qc_norm_ai_tag_cnt` String AFTER `qc_norm_tag_cnt`,
ADD COLUMN IF NOT EXISTS `qc_norm_custom_tag_cnt` String AFTER `qc_norm_ai_tag_cnt`,
ADD COLUMN IF NOT EXISTS `qc_norm_manual_tag_cnt` String AFTER `qc_norm_custom_tag_cnt`,
ADD COLUMN IF NOT EXISTS `qc_norm_alert_tag_cnt` String AFTER `qc_norm_manual_tag_cnt`,
ADD COLUMN IF NOT EXISTS `dialog_tag_cnt` Int64 AFTER `qc_norm_alert_tag_cnt`,
ADD COLUMN IF NOT EXISTS `dialog_ai_tag_cnt` Int64 AFTER `dialog_tag_cnt`,
ADD COLUMN IF NOT EXISTS `dialog_custom_tag_cnt` Int64 AFTER `dialog_ai_tag_cnt`,
ADD COLUMN IF NOT EXISTS `dialog_manual_tag_cnt` Int64 AFTER `dialog_custom_tag_cnt`,
ADD COLUMN IF NOT EXISTS `excellent_dialog_cnt` Int64 AFTER `dialog_cnt`,
ADD COLUMN IF NOT EXISTS `dialog_score_avg` Float64 AFTER  `manual_add_score_sum`,
-- ADD COLUMN IF NOT EXISTS `ai_tagged_subtract_score_dialog_cnt` Int64 AFTER `ai_tagged_dialog_cnt`,
-- ADD COLUMN IF NOT EXISTS `ai_tagged_add_score_dialog_cnt` Int64 AFTER `ai_tagged_subtract_score_dialog_cnt`,
-- ADD COLUMN IF NOT EXISTS `ai_tagged_zero_score_dialog_cnt` Int64 AFTER `ai_tagged_add_score_dialog_cnt`,
ADD COLUMN IF NOT EXISTS `ai_zero_score_tagged_dialog_cnt` Int64 AFTER `ai_add_score_dialog_cnt`,
-- ADD COLUMN IF NOT EXISTS `custom_tagged_subtract_score_dialog_cnt` Int64 AFTER `custom_tagged_dialog_cnt`,
-- ADD COLUMN IF NOT EXISTS `custom_tagged_add_score_dialog_cnt` Int64 AFTER `custom_tagged_subtract_score_dialog_cnt`,
-- ADD COLUMN IF NOT EXISTS `custom_tagged_zero_score_dialog_cnt` Int64 AFTER `custom_tagged_add_score_dialog_cnt`,
ADD COLUMN IF NOT EXISTS `custom_zero_score_tagged_dialog_cnt` Int64 AFTER `custom_add_score_dialog_cnt`,
-- ADD COLUMN IF NOT EXISTS `manual_tagged_subtract_score_dialog_cnt` Int64 AFTER `manual_tagged_dialog_cnt`,
-- ADD COLUMN IF NOT EXISTS `manual_tagged_add_score_dialog_cnt` Int64 AFTER `manual_tagged_subtract_score_dialog_cnt`,
-- ADD COLUMN IF NOT EXISTS `manual_tagged_zero_score_dialog_cnt` Int64 AFTER `manual_tagged_add_score_dialog_cnt`,
ADD COLUMN IF NOT EXISTS `manual_zero_score_tagged_dialog_cnt` Int64 AFTER `manual_add_score_dialog_cnt`;


-- ALTER TABLE xqc_dws.xplat_snick_stat_local ON CLUSTER cluster_3s_2r 
-- DROP COLUMN IF EXISTS `manual_zero_score_tagged_dialog_cnt`;
ALTER TABLE xqc_dws.xplat_snick_stat_local ON CLUSTER cluster_3s_2r
ADD COLUMN IF NOT EXISTS `company_id` String AFTER `day`,
ADD COLUMN IF NOT EXISTS `shop_id` String AFTER `company_id`,
ADD COLUMN IF NOT EXISTS `qc_norm_id` String AFTER `department_name`,
ADD COLUMN IF NOT EXISTS `qc_norm_name` String AFTER `qc_norm_id`,
ADD COLUMN IF NOT EXISTS `qc_norm_tag_cnt` String AFTER `qc_norm_name`,
ADD COLUMN IF NOT EXISTS `qc_norm_ai_tag_cnt` String AFTER `qc_norm_tag_cnt`,
ADD COLUMN IF NOT EXISTS `qc_norm_custom_tag_cnt` String AFTER `qc_norm_ai_tag_cnt`,
ADD COLUMN IF NOT EXISTS `qc_norm_manual_tag_cnt` String AFTER `qc_norm_custom_tag_cnt`,
ADD COLUMN IF NOT EXISTS `qc_norm_alert_tag_cnt` String AFTER `qc_norm_manual_tag_cnt`,
ADD COLUMN IF NOT EXISTS `dialog_tag_cnt` Int64 AFTER `qc_norm_alert_tag_cnt`,
ADD COLUMN IF NOT EXISTS `dialog_ai_tag_cnt` Int64 AFTER `dialog_tag_cnt`,
ADD COLUMN IF NOT EXISTS `dialog_custom_tag_cnt` Int64 AFTER `dialog_ai_tag_cnt`,
ADD COLUMN IF NOT EXISTS `dialog_manual_tag_cnt` Int64 AFTER `dialog_custom_tag_cnt`,
ADD COLUMN IF NOT EXISTS `excellent_dialog_cnt` Int64 AFTER `dialog_cnt`,
ADD COLUMN IF NOT EXISTS `dialog_score_avg` Float64 AFTER  `manual_add_score_sum`,
ADD COLUMN IF NOT EXISTS `ai_zero_score_tagged_dialog_cnt` Int64 AFTER `ai_add_score_dialog_cnt`,
ADD COLUMN IF NOT EXISTS `custom_zero_score_tagged_dialog_cnt` Int64 AFTER `custom_add_score_dialog_cnt`,
ADD COLUMN IF NOT EXISTS `manual_zero_score_tagged_dialog_cnt` Int64 AFTER `manual_add_score_dialog_cnt`,
ADD COLUMN IF NOT EXISTS `eval_dialog_cnt` Int64 AFTER `manual_zero_score_tagged_dialog_cnt`,
ADD COLUMN IF NOT EXISTS `eval_level_1_dialog_cnt` Int64 AFTER `eval_dialog_cnt`,
ADD COLUMN IF NOT EXISTS `eval_level_2_dialog_cnt` Int64 AFTER `eval_level_1_dialog_cnt`,
ADD COLUMN IF NOT EXISTS `eval_level_3_dialog_cnt` Int64 AFTER `eval_level_2_dialog_cnt`,
ADD COLUMN IF NOT EXISTS `eval_level_4_dialog_cnt` Int64 AFTER `eval_level_3_dialog_cnt`,
ADD COLUMN IF NOT EXISTS `eval_level_5_dialog_cnt` Int64 AFTER `eval_level_4_dialog_cnt`;


-- ALTER TABLE xqc_dws.xplat_snick_stat_all ON CLUSTER cluster_3s_2r
-- DROP COLUMN IF EXISTS `manual_zero_score_tagged_dialog_cnt`;
ALTER TABLE xqc_dws.xplat_snick_stat_all ON CLUSTER cluster_3s_2r
ADD COLUMN IF NOT EXISTS `company_id` String AFTER `day`,
ADD COLUMN IF NOT EXISTS `shop_id` String AFTER `company_id`,
ADD COLUMN IF NOT EXISTS `qc_norm_id` String AFTER `department_name`,
ADD COLUMN IF NOT EXISTS `qc_norm_name` String AFTER `qc_norm_id`,
ADD COLUMN IF NOT EXISTS `qc_norm_tag_cnt` String AFTER `qc_norm_name`,
ADD COLUMN IF NOT EXISTS `qc_norm_ai_tag_cnt` String AFTER `qc_norm_tag_cnt`,
ADD COLUMN IF NOT EXISTS `qc_norm_custom_tag_cnt` String AFTER `qc_norm_ai_tag_cnt`,
ADD COLUMN IF NOT EXISTS `qc_norm_manual_tag_cnt` String AFTER `qc_norm_custom_tag_cnt`,
ADD COLUMN IF NOT EXISTS `qc_norm_alert_tag_cnt` String AFTER `qc_norm_manual_tag_cnt`,
ADD COLUMN IF NOT EXISTS `dialog_tag_cnt` Int64 AFTER `qc_norm_alert_tag_cnt`,
ADD COLUMN IF NOT EXISTS `dialog_ai_tag_cnt` Int64 AFTER `dialog_tag_cnt`,
ADD COLUMN IF NOT EXISTS `dialog_custom_tag_cnt` Int64 AFTER `dialog_ai_tag_cnt`,
ADD COLUMN IF NOT EXISTS `dialog_manual_tag_cnt` Int64 AFTER `dialog_custom_tag_cnt`,
ADD COLUMN IF NOT EXISTS `excellent_dialog_cnt` Int64 AFTER `dialog_cnt`,
ADD COLUMN IF NOT EXISTS `dialog_score_avg` Float64 AFTER  `manual_add_score_sum`,
ADD COLUMN IF NOT EXISTS `ai_zero_score_tagged_dialog_cnt` Int64 AFTER `ai_add_score_dialog_cnt`,
ADD COLUMN IF NOT EXISTS `custom_zero_score_tagged_dialog_cnt` Int64 AFTER `custom_add_score_dialog_cnt`,
ADD COLUMN IF NOT EXISTS `manual_zero_score_tagged_dialog_cnt` Int64 AFTER `manual_add_score_dialog_cnt`,
ADD COLUMN IF NOT EXISTS `eval_dialog_cnt` Int64 AFTER `manual_zero_score_tagged_dialog_cnt`,
ADD COLUMN IF NOT EXISTS `eval_level_1_dialog_cnt` Int64 AFTER `eval_dialog_cnt`,
ADD COLUMN IF NOT EXISTS `eval_level_2_dialog_cnt` Int64 AFTER `eval_level_1_dialog_cnt`,
ADD COLUMN IF NOT EXISTS `eval_level_3_dialog_cnt` Int64 AFTER `eval_level_2_dialog_cnt`,
ADD COLUMN IF NOT EXISTS `eval_level_4_dialog_cnt` Int64 AFTER `eval_level_3_dialog_cnt`,
ADD COLUMN IF NOT EXISTS `eval_level_5_dialog_cnt` Int64 AFTER `eval_level_4_dialog_cnt`;


DROP TABLE IF EXISTS buffer.xqc_dws_xplat_snick_stat_buffer ON CLUSTER cluster_3s_2r NO DELAY;

CREATE TABLE IF NOT EXISTS buffer.xqc_dws_xplat_snick_stat_buffer ON CLUSTER cluster_3s_2r
AS xqc_dws.xplat_snick_stat_all
ENGINE = Buffer('xqc_dws', 'xplat_snick_stat_all', 16, 15, 35, 81920, 409600, 16777216, 67108864);


-- ALTER TABLE xqc_dws.tag_stat_local ON CLUSTER cluster_3s_2r 
-- DROP COLUMN IF EXISTS `tag_group_full_name`;
ALTER TABLE xqc_dws.tag_stat_local ON CLUSTER cluster_3s_2r
ADD COLUMN IF NOT EXISTS `company_id` String AFTER `day`,
ADD COLUMN IF NOT EXISTS `shop_id` String AFTER `company_id`,
ADD COLUMN IF NOT EXISTS `employee_id` String AFTER `snick`,
ADD COLUMN IF NOT EXISTS `employee_name` String AFTER `employee_id`,
ADD COLUMN IF NOT EXISTS `department_id` String AFTER `employee_name`,
ADD COLUMN IF NOT EXISTS `department_name` String AFTER `department_id`,
ADD COLUMN IF NOT EXISTS `qc_norm_id` String AFTER `snick`,
ADD COLUMN IF NOT EXISTS `qc_norm_name` String AFTER `qc_norm_id`,
ADD COLUMN IF NOT EXISTS `tag_group_full_name` String AFTER `tag_group_name`;


-- ALTER TABLE xqc_dws.tag_stat_all ON CLUSTER cluster_3s_2r 
-- DROP COLUMN IF EXISTS `tag_group_full_name`;
ALTER TABLE xqc_dws.tag_stat_all ON CLUSTER cluster_3s_2r
ADD COLUMN IF NOT EXISTS `company_id` String AFTER `day`,
ADD COLUMN IF NOT EXISTS `shop_id` String AFTER `company_id`,
ADD COLUMN IF NOT EXISTS `employee_id` String AFTER `snick`,
ADD COLUMN IF NOT EXISTS `employee_name` String AFTER `employee_id`,
ADD COLUMN IF NOT EXISTS `department_id` String AFTER `employee_name`,
ADD COLUMN IF NOT EXISTS `department_name` String AFTER `department_id`,
ADD COLUMN IF NOT EXISTS `qc_norm_id` String AFTER `snick`,
ADD COLUMN IF NOT EXISTS `qc_norm_name` String AFTER `qc_norm_id`,
ADD COLUMN IF NOT EXISTS `tag_group_full_name` String AFTER `tag_group_name`;


-- DROP TABLE xqc_dws.xplat_tag_stat_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE IF NOT EXISTS xqc_dws.xplat_tag_stat_local ON CLUSTER cluster_3s_2r
AS xqc_dws.tag_stat_all
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
PARTITION BY (day, platform)
ORDER BY (company_id, shop_id)
SETTINGS index_granularity = 8192, storage_policy = 'rr';


-- DROP TABLE xqc_dws.xplat_tag_stat_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE IF NOT EXISTS xqc_dws.xplat_tag_stat_all ON CLUSTER cluster_3s_2r
AS xqc_dws.xplat_tag_stat_local
ENGINE = Distributed('cluster_3s_2r', 'xqc_dws', 'xplat_tag_stat_local', rand());


DROP TABLE IF EXISTS buffer.xqc_dws_xplat_tag_stat_buffer ON CLUSTER cluster_3s_2r NO DELAY

CREATE TABLE IF NOT EXISTS buffer.xqc_dws_xplat_tag_stat_buffer ON CLUSTER cluster_3s_2r
AS xqc_dws.xplat_tag_stat_all
ENGINE = Buffer('xqc_dws', 'xplat_tag_stat_all', 16, 15, 35, 81920, 409600, 16777216, 67108864);