-- ALTER TABLE xqc_ods.alert_local ON CLUSTER cluster_3s_2r 
-- DROP COLUMN IF EXISTS `status`;
ALTER TABLE xqc_ods.alert_local ON CLUSTER cluster_3s_2r
ADD COLUMN IF NOT EXISTS `status` Int64 AFTER `day`;

-- ALTER TABLE xqc_ods.alert_all ON CLUSTER cluster_3s_2r
-- DROP COLUMN IF EXISTS `status`;
ALTER TABLE xqc_ods.alert_all ON CLUSTER cluster_3s_2r
ADD COLUMN IF NOT EXISTS `status` Int64 AFTER `day`;

-- ALTER TABLE buffer.xqc_ods_alert_buffer ON CLUSTER cluster_3s_2r
-- DROP COLUMN IF EXISTS `status`;
ALTER TABLE buffer.xqc_ods_alert_buffer ON CLUSTER cluster_3s_2r
ADD COLUMN IF NOT EXISTS `status` Int64 AFTER `day`;