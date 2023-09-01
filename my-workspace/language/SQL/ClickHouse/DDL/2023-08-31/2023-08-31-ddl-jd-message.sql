-- ALTER TABLE xqc_ods.message_local ON CLUSTER cluster_3s_2r 
-- DROP COLUMN IF EXISTS `msg_scenes_source`;
ALTER TABLE xqc_ods.message_local ON CLUSTER cluster_3s_2r
ADD COLUMN IF NOT EXISTS `msg_scenes_source` String AFTER `source`,
ADD COLUMN IF NOT EXISTS `msg_content_type` String AFTER `content_type`;

-- ALTER TABLE xqc_ods.message_all ON CLUSTER cluster_3s_2r
-- DROP COLUMN IF EXISTS `msg_scenes_source`;
ALTER TABLE xqc_ods.message_all ON CLUSTER cluster_3s_2r
ADD COLUMN IF NOT EXISTS `msg_scenes_source` String AFTER `source`,
ADD COLUMN IF NOT EXISTS `msg_content_type` String AFTER `content_type`;


DROP TABLE buffer.xqc_message_buffer ON CLUSTER cluster_3s_2r NO DELAY;

CREATE TABLE IF NOT EXISTS buffer.xqc_message_buffer ON CLUSTER cluster_3s_2r
AS xqc_ods.message_all
ENGINE = Buffer('xqc_ods', 'message_all', 16, 15, 35, 81920, 409600, 16777216, 67108864);