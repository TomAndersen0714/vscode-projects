-- ALTER TABLE ods.fishpond_conversion_local ON CLUSTER cluster_3s_2r 
-- DROP COLUMN IF EXISTS `platform`;
ALTER TABLE ods.fishpond_conversion_local ON CLUSTER cluster_3s_2r
ADD COLUMN IF NOT EXISTS `platform` String DEFAULT 'jd' AFTER `shop_id`;

-- ALTER TABLE ods.fishpond_conversion_all ON CLUSTER cluster_3s_2r
-- DROP COLUMN IF EXISTS `platform`;
ALTER TABLE ods.fishpond_conversion_all ON CLUSTER cluster_3s_2r
ADD COLUMN IF NOT EXISTS `platform` String AFTER `shop_id`;

-- ALTER TABLE buffer.fishpond_conversion_buffer ON CLUSTER cluster_3s_2r
-- DROP COLUMN IF EXISTS `platform`;
ALTER TABLE buffer.fishpond_conversion_buffer ON CLUSTER cluster_3s_2r
ADD COLUMN IF NOT EXISTS `platform` String AFTER `shop_id`;