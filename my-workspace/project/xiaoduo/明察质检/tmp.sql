-- ALTER TABLE dim.fishpond_task_local ON CLUSTER cluster_3s_2r 
-- DROP COLUMN IF EXISTS `platform`;
ALTER TABLE dim.fishpond_task_local ON CLUSTER cluster_3s_2r
ADD COLUMN IF NOT EXISTS `platform` String DEFAULT 'jd' AFTER `shop_id`;

-- ALTER TABLE dim.fishpond_task_all ON CLUSTER cluster_3s_2r
-- DROP COLUMN IF EXISTS `platform`;
ALTER TABLE dim.fishpond_task_all ON CLUSTER cluster_3s_2r
ADD COLUMN IF NOT EXISTS `platform` String AFTER `shop_id`;