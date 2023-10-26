-- ALTER TABLE xqc_ods.message_local ON CLUSTER cluster_3s_2r 
-- DROP COLUMN IF EXISTS `shop_id`;
ALTER TABLE xqc_ods.message_local ON CLUSTER cluster_3s_2r
ADD COLUMN IF NOT EXISTS `shop_id` String DEFAULT 'jd' AFTER `platform`;

-- ALTER TABLE xqc_ods.message_all ON CLUSTER cluster_3s_2r
-- DROP COLUMN IF EXISTS `shop_id`;
ALTER TABLE xqc_ods.message_all ON CLUSTER cluster_3s_2r
ADD COLUMN IF NOT EXISTS `shop_id` String AFTER `platform`;

-- ALTER TABLE buffer.xqc_message_buffer ON CLUSTER cluster_3s_2r
-- DROP COLUMN IF EXISTS `shop_id`;
ALTER TABLE buffer.xqc_message_buffer ON CLUSTER cluster_3s_2r
ADD COLUMN IF NOT EXISTS `shop_id` String AFTER `platform`;