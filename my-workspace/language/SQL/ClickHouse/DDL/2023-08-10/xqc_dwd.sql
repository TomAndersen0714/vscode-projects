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