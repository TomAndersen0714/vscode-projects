-- ALTER TABLE xqc_dws.snick_stat_local ON CLUSTER cluster_3s_2r 
-- DROP COLUMN IF EXISTS `company_id`;
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
ADD COLUMN IF NOT EXISTS `dialog_score_avg` Float64 AFTER  `excellent_dialog_cnt`,
ADD COLUMN IF NOT EXISTS `ai_tagged_subtract_score_dialog_cnt` Int64 AFTER `ai_tagged_dialog_cnt`,
ADD COLUMN IF NOT EXISTS `ai_tagged_add_score_dialog_cnt` Int64 AFTER `ai_tagged_subtract_score_dialog_cnt`,
ADD COLUMN IF NOT EXISTS `ai_tagged_zero_score_dialog_cnt` Int64 AFTER `ai_tagged_add_score_dialog_cnt`,
ADD COLUMN IF NOT EXISTS `custom_tagged_subtract_score_dialog_cnt` Int64 AFTER `custom_tagged_dialog_cnt`,
ADD COLUMN IF NOT EXISTS `custom_tagged_add_score_dialog_cnt` Int64 AFTER `custom_tagged_subtract_score_dialog_cnt`,
ADD COLUMN IF NOT EXISTS `custom_tagged_zero_score_dialog_cnt` Int64 AFTER `custom_tagged_add_score_dialog_cnt`,
ADD COLUMN IF NOT EXISTS `manual_tagged_subtract_score_dialog_cnt` Int64 AFTER `manual_tagged_dialog_cnt`,
ADD COLUMN IF NOT EXISTS `manual_tagged_add_score_dialog_cnt` Int64 AFTER `manual_tagged_subtract_score_dialog_cnt`,
ADD COLUMN IF NOT EXISTS `manual_tagged_zero_score_dialog_cnt` Int64 AFTER `manual_tagged_add_score_dialog_cnt`;


-- ALTER TABLE xqc_dws.snick_stat_all ON CLUSTER cluster_3s_2r
-- DROP COLUMN IF EXISTS `company_id`;
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
ADD COLUMN IF NOT EXISTS `dialog_score_avg` Float64 AFTER  `excellent_dialog_cnt`,
ADD COLUMN IF NOT EXISTS `ai_tagged_subtract_score_dialog_cnt` Int64 AFTER `ai_tagged_dialog_cnt`,
ADD COLUMN IF NOT EXISTS `ai_tagged_add_score_dialog_cnt` Int64 AFTER `ai_tagged_subtract_score_dialog_cnt`,
ADD COLUMN IF NOT EXISTS `ai_tagged_zero_score_dialog_cnt` Int64 AFTER `ai_tagged_add_score_dialog_cnt`,
ADD COLUMN IF NOT EXISTS `custom_tagged_subtract_score_dialog_cnt` Int64 AFTER `custom_tagged_dialog_cnt`,
ADD COLUMN IF NOT EXISTS `custom_tagged_add_score_dialog_cnt` Int64 AFTER `custom_tagged_subtract_score_dialog_cnt`,
ADD COLUMN IF NOT EXISTS `custom_tagged_zero_score_dialog_cnt` Int64 AFTER `custom_tagged_add_score_dialog_cnt`,
ADD COLUMN IF NOT EXISTS `manual_tagged_subtract_score_dialog_cnt` Int64 AFTER `manual_tagged_dialog_cnt`,
ADD COLUMN IF NOT EXISTS `manual_tagged_add_score_dialog_cnt` Int64 AFTER `manual_tagged_subtract_score_dialog_cnt`,
ADD COLUMN IF NOT EXISTS `manual_tagged_zero_score_dialog_cnt` Int64 AFTER `manual_tagged_add_score_dialog_cnt`;


-- ALTER TABLE xqc_dws.tag_stat_local ON CLUSTER cluster_3s_2r 
-- DROP COLUMN IF EXISTS `company_id`;
ALTER TABLE xqc_dws.tag_stat_local ON CLUSTER cluster_3s_2r
ADD COLUMN IF NOT EXISTS `company_id` String AFTER `day`,
ADD COLUMN IF NOT EXISTS `shop_id` String AFTER `company_id`,
ADD COLUMN IF NOT EXISTS `qc_norm_id` String AFTER `snick`,
ADD COLUMN IF NOT EXISTS `qc_norm_name` String AFTER `qc_norm_id`,
ADD COLUMN IF NOT EXISTS `tag_group_full_name` String AFTER `qc_norm_name`;


-- ALTER TABLE xqc_dws.tag_stat_all ON CLUSTER cluster_3s_2r 
-- DROP COLUMN IF EXISTS `company_id`;
ALTER TABLE xqc_dws.tag_stat_all ON CLUSTER cluster_3s_2r
ADD COLUMN IF NOT EXISTS `company_id` String AFTER `day`,
ADD COLUMN IF NOT EXISTS `shop_id` String AFTER `company_id`,
ADD COLUMN IF NOT EXISTS `qc_norm_id` String AFTER `snick`,
ADD COLUMN IF NOT EXISTS `qc_norm_name` String AFTER `qc_norm_id`,
ADD COLUMN IF NOT EXISTS `tag_group_full_name` String AFTER `qc_norm_name`;


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


-- DROP TABLE buffer.xqc_dwd_xplat_tag_stat_buffer ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE IF NOT EXISTS buffer.xqc_dwd_xplat_tag_stat_buffer ON CLUSTER cluster_3s_2r
AS xqc_dws.xplat_tag_stat_all
ENGINE = Buffer('xqc_dws', 'xplat_tag_stat_all', 16, 15, 35, 81920, 409600, 16777216, 67108864);