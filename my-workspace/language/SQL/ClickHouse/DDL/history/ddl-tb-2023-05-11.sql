-- ALTER TABLE dim.shop_precise_intent_local ON CLUSTER cluster_3s_2r 
-- DROP COLUMN IF EXISTS `intent_type`;
ALTER TABLE dim.shop_precise_intent_local ON CLUSTER cluster_3s_2r
ADD COLUMN IF NOT EXISTS `intent_type` String AFTER `is_shop_added`;

-- ALTER TABLE dim.shop_precise_intent_all ON CLUSTER cluster_3s_2r 
-- DROP COLUMN IF EXISTS `intent_type`;
ALTER TABLE dim.shop_precise_intent_all ON CLUSTER cluster_3s_2r
ADD COLUMN IF NOT EXISTS `intent_type` String AFTER `is_shop_added`;