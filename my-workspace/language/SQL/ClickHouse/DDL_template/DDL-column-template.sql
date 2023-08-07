-- ALTER TABLE xqc_ods.alert_local ON CLUSTER cluster_3s_2r 
-- DROP COLUMN IF EXISTS `status`;
ALTER TABLE xqc_ods.alert_local ON CLUSTER cluster_3s_2r
ADD COLUMN IF NOT EXISTS `status` Int64 AFTER `is_finished`,
ADD COLUMN IF NOT EXISTS `handler` String AFTER `superior_name`,
ADD COLUMN IF NOT EXISTS `note` String AFTER `handler`,
ADD COLUMN IF NOT EXISTS `remind_num` Int64 AFTER `note`,
ADD COLUMN IF NOT EXISTS `remind_time` DateTime AFTER `remind_num`;

-- ALTER TABLE xqc_ods.alert_all ON CLUSTER cluster_3s_2r
-- DROP COLUMN IF EXISTS `status`;
ALTER TABLE xqc_ods.alert_all ON CLUSTER cluster_3s_2r
ADD COLUMN IF NOT EXISTS `status` Int64 AFTER `is_finished`,
ADD COLUMN IF NOT EXISTS `handler` String AFTER `superior_name`,
ADD COLUMN IF NOT EXISTS `note` String AFTER `handler`,
ADD COLUMN IF NOT EXISTS `remind_num` Int64 AFTER `note`,
ADD COLUMN IF NOT EXISTS `remind_time` DateTime AFTER `remind_num`;

-- ALTER TABLE buffer.xqc_ods_alert_buffer ON CLUSTER cluster_3s_2r
-- DROP COLUMN IF EXISTS `status`;
ALTER TABLE buffer.xqc_ods_alert_buffer ON CLUSTER cluster_3s_2r
ADD COLUMN IF NOT EXISTS `status` Int64 AFTER `is_finished`,
ADD COLUMN IF NOT EXISTS `handler` String AFTER `superior_name`,
ADD COLUMN IF NOT EXISTS `note` String AFTER `handler`,
ADD COLUMN IF NOT EXISTS `remind_num` Int64 AFTER `note`,
ADD COLUMN IF NOT EXISTS `remind_time` DateTime AFTER `remind_num`;