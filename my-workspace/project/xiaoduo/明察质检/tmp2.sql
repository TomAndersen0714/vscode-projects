-- ALTER TABLE app_fishpond.fishpond_task_stat ON CLUSTER cluster_3s_2r
-- DROP COLUMN IF EXISTS `platform`;
ALTER TABLE app_fishpond.fishpond_task_stat ON CLUSTER cluster_3s_2r
ADD COLUMN IF NOT EXISTS `platform` String AFTER `shop_id`;