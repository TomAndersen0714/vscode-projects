-- DROP TABLE xqc_dim.company_latest_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE IF NOT EXISTS xqc_dim.company_latest_local ON CLUSTER cluster_3s_2r
AS tmp.xinghuan_company_local
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
ORDER BY (`_id`, `name`)
SETTINGS index_granularity = 8192, storage_policy = 'rr';

-- ALTER TABLE xqc_dim.company_latest_local ON CLUSTER cluster_3s_2r
-- DROP COLUMN IF EXISTS `default_platform`
ALTER TABLE xqc_dim.company_latest_local ON CLUSTER cluster_3s_2r
ADD COLUMN IF NOT EXISTS `default_platform` String AFTER `url`,
ADD COLUMN IF NOT EXISTS `platforms` Array(String) AFTER `default_platform`,
ADD COLUMN IF NOT EXISTS `pri_center_id` String AFTER `platforms`,
ADD COLUMN IF NOT EXISTS `expired_time` String AFTER `pri_center_id`,
ADD COLUMN IF NOT EXISTS `downgrade_strategy` Int64 AFTER `expired_time`,
ADD COLUMN IF NOT EXISTS `white_list` Array(String) AFTER `downgrade_strategy`,
ADD COLUMN IF NOT EXISTS `need_init` String AFTER `platforms`;

-- DROP TABLE xqc_dim.company_latest_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE IF NOT EXISTS xqc_dim.company_latest_all ON CLUSTER cluster_3s_2r
AS xqc_dim.company_latest_local
ENGINE = Distributed('cluster_3s_2r', 'xqc_dim', 'company_latest_local', rand());


-- ALTER TABLE ods.xinghuan_company_local ON CLUSTER cluster_3s_2r
-- DROP COLUMN IF EXISTS `default_platform`
ALTER TABLE ods.xinghuan_company_local ON CLUSTER cluster_3s_2r
ADD COLUMN IF NOT EXISTS `default_platform` String AFTER `url`,
ADD COLUMN IF NOT EXISTS `platforms` Array(String) AFTER `default_platform`,
ADD COLUMN IF NOT EXISTS `pri_center_id` String AFTER `platforms`,
ADD COLUMN IF NOT EXISTS `expired_time` String AFTER `pri_center_id`,
ADD COLUMN IF NOT EXISTS `downgrade_strategy` Int64 AFTER `expired_time`,
ADD COLUMN IF NOT EXISTS `white_list` Array(String) AFTER `downgrade_strategy`,
ADD COLUMN IF NOT EXISTS `need_init` String AFTER `platforms`;


-- ALTER TABLE ods.xinghuan_company_all ON CLUSTER cluster_3s_2r
-- DROP COLUMN IF EXISTS `default_platform`
ALTER TABLE ods.xinghuan_company_all ON CLUSTER cluster_3s_2r
ADD COLUMN IF NOT EXISTS `default_platform` String AFTER `url`,
ADD COLUMN IF NOT EXISTS `platforms` Array(String) AFTER `default_platform`,
ADD COLUMN IF NOT EXISTS `pri_center_id` String AFTER `platforms`,
ADD COLUMN IF NOT EXISTS `expired_time` String AFTER `pri_center_id`,
ADD COLUMN IF NOT EXISTS `downgrade_strategy` Int64 AFTER `expired_time`,
ADD COLUMN IF NOT EXISTS `white_list` Array(String) AFTER `downgrade_strategy`,
ADD COLUMN IF NOT EXISTS `need_init` String AFTER `platforms`;


-- DROP TABLE xqc_dim.shop_latest_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE IF NOT EXISTS xqc_dim.shop_latest_local ON CLUSTER cluster_3s_2r
AS tmp.xqc_shop_local
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
ORDER BY (`company_id`, `platform`)
SETTINGS index_granularity = 8192, storage_policy = 'rr';

-- DROP TABLE xqc_dim.shop_latest_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE IF NOT EXISTS xqc_dim.shop_latest_all ON CLUSTER cluster_3s_2r
AS xqc_dim.shop_latest_local
ENGINE = Distributed('cluster_3s_2r', 'xqc_dim', 'shop_latest_local', rand());