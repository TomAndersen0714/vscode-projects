-- ALTER TABLE xqc_dim.snick_full_info_local ON CLUSTER cluster_3s_2r 
-- DROP COLUMN IF EXISTS `company_name`;
ALTER TABLE xqc_dim.snick_full_info_local ON CLUSTER cluster_3s_2r
ADD COLUMN IF NOT EXISTS `company_name` String AFTER `company_id`,
ADD COLUMN IF NOT EXISTS `company_short_name` String AFTER `company_name`,
ADD COLUMN IF NOT EXISTS `shop_name` String AFTER `shop_id`,
ADD COLUMN IF NOT EXISTS `seller_nick` String AFTER `shop_name`;

-- ALTER TABLE xqc_dim.snick_full_info_all ON CLUSTER cluster_3s_2r
-- DROP COLUMN IF EXISTS `company_name`;
ALTER TABLE xqc_dim.snick_full_info_all ON CLUSTER cluster_3s_2r
ADD COLUMN IF NOT EXISTS `company_name` String AFTER `company_id`,
ADD COLUMN IF NOT EXISTS `company_short_name` String AFTER `company_name`,
ADD COLUMN IF NOT EXISTS `shop_name` String AFTER `shop_id`,
ADD COLUMN IF NOT EXISTS `seller_nick` String AFTER `shop_name`;