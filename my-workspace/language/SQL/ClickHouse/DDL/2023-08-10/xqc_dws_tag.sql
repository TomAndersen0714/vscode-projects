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


DROP TABLE buffer.xqc_dws_xplat_tag_stat_buffer ON CLUSTER cluster_3s_2r NO DELAY

CREATE TABLE IF NOT EXISTS buffer.xqc_dws_xplat_tag_stat_buffer ON CLUSTER cluster_3s_2r
AS xqc_dws.xplat_tag_stat_all
ENGINE = Buffer('xqc_dws', 'xplat_tag_stat_all', 16, 15, 35, 81920, 409600, 16777216, 67108864);