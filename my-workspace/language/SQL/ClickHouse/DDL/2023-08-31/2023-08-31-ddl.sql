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



-- ALTER TABLE ods.xdqc_dialog_local ON CLUSTER cluster_3s_2r 
-- DROP COLUMN IF EXISTS `msg_scenes_source`;
ALTER TABLE ods.xdqc_dialog_local ON CLUSTER cluster_3s_2r
ADD COLUMN IF NOT EXISTS `service_evaluations_eval_code` Array(Int64) AFTER `qid`,
ADD COLUMN IF NOT EXISTS `service_evaluations_open_uid` Array(String) AFTER `service_evaluations_eval_code`,
ADD COLUMN IF NOT EXISTS `service_evaluations_eval_time` Array(String) AFTER `service_evaluations_open_uid`,
ADD COLUMN IF NOT EXISTS `service_evaluations_send_time` Array(String) AFTER `service_evaluations_eval_time`,
ADD COLUMN IF NOT EXISTS `service_evaluations_source` Array(Int64) AFTER `service_evaluations_send_time`,
ADD COLUMN IF NOT EXISTS `service_evaluations_message_id` Array(String) AFTER `service_evaluations_source`,
ADD COLUMN IF NOT EXISTS `service_evaluations_desc` Array(String) AFTER `service_evaluations_message_id`,
ADD COLUMN IF NOT EXISTS `mark_time` DateTime64(3) AFTER `mark`,
ADD COLUMN IF NOT EXISTS `message_marks_id` Array(String) AFTER `mark_time`,
ADD COLUMN IF NOT EXISTS `message_marks_mark` Array(String) AFTER `message_marks_id`,
ADD COLUMN IF NOT EXISTS `order_info_history_status` Array(Array(String)) AFTER `order_info_time`,
ADD COLUMN IF NOT EXISTS `order_info_history_time` Array(Array(UInt64)) AFTER `order_info_history_status`,
ADD COLUMN IF NOT EXISTS `plat_goods_ids` Array(String) AFTER `order_info_history_time`,
ADD COLUMN IF NOT EXISTS `remark` String AFTER `wx_rule_add_stats_count`,
ADD COLUMN IF NOT EXISTS `desc` String AFTER `remark`;


-- ALTER TABLE ods.xdqc_dialog_all ON CLUSTER cluster_3s_2r
-- DROP COLUMN IF EXISTS `msg_scenes_source`;
ALTER TABLE ods.xdqc_dialog_all ON CLUSTER cluster_3s_2r
ADD COLUMN IF NOT EXISTS `service_evaluations_eval_code` Array(Int64) AFTER `qid`,
ADD COLUMN IF NOT EXISTS `service_evaluations_open_uid` Array(String) AFTER `service_evaluations_eval_code`,
ADD COLUMN IF NOT EXISTS `service_evaluations_eval_time` Array(String) AFTER `service_evaluations_open_uid`,
ADD COLUMN IF NOT EXISTS `service_evaluations_send_time` Array(String) AFTER `service_evaluations_eval_time`,
ADD COLUMN IF NOT EXISTS `service_evaluations_source` Array(Int64) AFTER `service_evaluations_send_time`,
ADD COLUMN IF NOT EXISTS `service_evaluations_message_id` Array(String) AFTER `service_evaluations_source`,
ADD COLUMN IF NOT EXISTS `service_evaluations_desc` Array(String) AFTER `service_evaluations_message_id`,
ADD COLUMN IF NOT EXISTS `mark_time` DateTime64(3) AFTER `mark`,
ADD COLUMN IF NOT EXISTS `message_marks_id` Array(String) AFTER `mark_time`,
ADD COLUMN IF NOT EXISTS `message_marks_mark` Array(String) AFTER `message_marks_id`,
ADD COLUMN IF NOT EXISTS `order_info_history_status` Array(Array(String)) AFTER `order_info_time`,
ADD COLUMN IF NOT EXISTS `order_info_history_time` Array(Array(UInt64)) AFTER `order_info_history_status`,
ADD COLUMN IF NOT EXISTS `plat_goods_ids` Array(String) AFTER `order_info_history_time`,
ADD COLUMN IF NOT EXISTS `remark` String AFTER `wx_rule_add_stats_count`,
ADD COLUMN IF NOT EXISTS `desc` String AFTER `remark`;

DROP TABLE buffer.xdqc_dialog_buffer ON CLUSTER cluster_3s_2r NO DELAY;

CREATE TABLE IF NOT EXISTS buffer.xdqc_dialog_buffer ON CLUSTER cluster_3s_2r
AS ods.xdqc_dialog_all
ENGINE = Buffer('ods', 'xdqc_dialog_all', 16, 15, 35, 81920, 409600, 16777216, 67108864);



-- ALTER TABLE ods.xdqc_dialog_update_local ON CLUSTER cluster_3s_2r 
-- DROP COLUMN IF EXISTS `msg_scenes_source`;
ALTER TABLE ods.xdqc_dialog_update_local ON CLUSTER cluster_3s_2r
ADD COLUMN IF NOT EXISTS `service_evaluations_eval_code` Array(Int64) AFTER `qid`,
ADD COLUMN IF NOT EXISTS `service_evaluations_open_uid` Array(String) AFTER `service_evaluations_eval_code`,
ADD COLUMN IF NOT EXISTS `service_evaluations_eval_time` Array(String) AFTER `service_evaluations_open_uid`,
ADD COLUMN IF NOT EXISTS `service_evaluations_send_time` Array(String) AFTER `service_evaluations_eval_time`,
ADD COLUMN IF NOT EXISTS `service_evaluations_source` Array(Int64) AFTER `service_evaluations_send_time`,
ADD COLUMN IF NOT EXISTS `service_evaluations_message_id` Array(String) AFTER `service_evaluations_source`,
ADD COLUMN IF NOT EXISTS `service_evaluations_desc` Array(String) AFTER `service_evaluations_message_id`,
ADD COLUMN IF NOT EXISTS `mark_time` DateTime64(3) AFTER `mark`,
ADD COLUMN IF NOT EXISTS `message_marks_id` Array(String) AFTER `mark_time`,
ADD COLUMN IF NOT EXISTS `message_marks_mark` Array(String) AFTER `message_marks_id`,
ADD COLUMN IF NOT EXISTS `order_info_history_status` Array(Array(String)) AFTER `order_info_time`,
ADD COLUMN IF NOT EXISTS `order_info_history_time` Array(Array(UInt64)) AFTER `order_info_history_status`,
ADD COLUMN IF NOT EXISTS `plat_goods_ids` Array(String) AFTER `order_info_history_time`,
ADD COLUMN IF NOT EXISTS `remark` String AFTER `wx_rule_add_stats_count`,
ADD COLUMN IF NOT EXISTS `desc` String AFTER `remark`;


-- ALTER TABLE ods.xdqc_dialog_update_all ON CLUSTER cluster_3s_2r
-- DROP COLUMN IF EXISTS `msg_scenes_source`;
ALTER TABLE ods.xdqc_dialog_update_all ON CLUSTER cluster_3s_2r
ADD COLUMN IF NOT EXISTS `service_evaluations_eval_code` Array(Int64) AFTER `qid`,
ADD COLUMN IF NOT EXISTS `service_evaluations_open_uid` Array(String) AFTER `service_evaluations_eval_code`,
ADD COLUMN IF NOT EXISTS `service_evaluations_eval_time` Array(String) AFTER `service_evaluations_open_uid`,
ADD COLUMN IF NOT EXISTS `service_evaluations_send_time` Array(String) AFTER `service_evaluations_eval_time`,
ADD COLUMN IF NOT EXISTS `service_evaluations_source` Array(Int64) AFTER `service_evaluations_send_time`,
ADD COLUMN IF NOT EXISTS `service_evaluations_message_id` Array(String) AFTER `service_evaluations_source`,
ADD COLUMN IF NOT EXISTS `service_evaluations_desc` Array(String) AFTER `service_evaluations_message_id`,
ADD COLUMN IF NOT EXISTS `mark_time` DateTime64(3) AFTER `mark`,
ADD COLUMN IF NOT EXISTS `message_marks_id` Array(String) AFTER `mark_time`,
ADD COLUMN IF NOT EXISTS `message_marks_mark` Array(String) AFTER `message_marks_id`,
ADD COLUMN IF NOT EXISTS `order_info_history_status` Array(Array(String)) AFTER `order_info_time`,
ADD COLUMN IF NOT EXISTS `order_info_history_time` Array(Array(UInt64)) AFTER `order_info_history_status`,
ADD COLUMN IF NOT EXISTS `plat_goods_ids` Array(String) AFTER `order_info_history_time`,
ADD COLUMN IF NOT EXISTS `remark` String AFTER `wx_rule_add_stats_count`,
ADD COLUMN IF NOT EXISTS `desc` String AFTER `remark`;

DROP TABLE buffer.xdqc_dialog_update_buffer ON CLUSTER cluster_3s_2r NO DELAY;

CREATE TABLE IF NOT EXISTS buffer.xdqc_dialog_update_buffer ON CLUSTER cluster_3s_2r
AS ods.xdqc_dialog_update_all
ENGINE = Buffer('ods', 'xdqc_dialog_update_all', 16, 15, 35, 81920, 409600, 16777216, 67108864);


-- ALTER TABLE dwd.xdqc_dialog_local ON CLUSTER cluster_3s_2r 
-- DROP COLUMN IF EXISTS `msg_scenes_source`;
ALTER TABLE dwd.xdqc_dialog_local ON CLUSTER cluster_3s_2r
ADD COLUMN IF NOT EXISTS `service_evaluations_eval_code` Array(Int64) AFTER `qid`,
ADD COLUMN IF NOT EXISTS `service_evaluations_open_uid` Array(String) AFTER `service_evaluations_eval_code`,
ADD COLUMN IF NOT EXISTS `service_evaluations_eval_time` Array(String) AFTER `service_evaluations_open_uid`,
ADD COLUMN IF NOT EXISTS `service_evaluations_send_time` Array(String) AFTER `service_evaluations_eval_time`,
ADD COLUMN IF NOT EXISTS `service_evaluations_source` Array(Int64) AFTER `service_evaluations_send_time`,
ADD COLUMN IF NOT EXISTS `service_evaluations_message_id` Array(String) AFTER `service_evaluations_source`,
ADD COLUMN IF NOT EXISTS `service_evaluations_desc` Array(String) AFTER `service_evaluations_message_id`,
ADD COLUMN IF NOT EXISTS `mark_time` DateTime64(3) AFTER `mark`,
ADD COLUMN IF NOT EXISTS `message_marks_id` Array(String) AFTER `mark_time`,
ADD COLUMN IF NOT EXISTS `message_marks_mark` Array(String) AFTER `message_marks_id`,
ADD COLUMN IF NOT EXISTS `order_info_history_status` Array(Array(String)) AFTER `order_info_time`,
ADD COLUMN IF NOT EXISTS `order_info_history_time` Array(Array(UInt64)) AFTER `order_info_history_status`,
ADD COLUMN IF NOT EXISTS `plat_goods_ids` Array(String) AFTER `order_info_history_time`,
ADD COLUMN IF NOT EXISTS `remark` String AFTER `wx_rule_add_stats_count`,
ADD COLUMN IF NOT EXISTS `desc` String AFTER `remark`;


-- ALTER TABLE dwd.xdqc_dialog_all ON CLUSTER cluster_3s_2r
-- DROP COLUMN IF EXISTS `msg_scenes_source`;
ALTER TABLE dwd.xdqc_dialog_all ON CLUSTER cluster_3s_2r
ADD COLUMN IF NOT EXISTS `service_evaluations_eval_code` Array(Int64) AFTER `qid`,
ADD COLUMN IF NOT EXISTS `service_evaluations_open_uid` Array(String) AFTER `service_evaluations_eval_code`,
ADD COLUMN IF NOT EXISTS `service_evaluations_eval_time` Array(String) AFTER `service_evaluations_open_uid`,
ADD COLUMN IF NOT EXISTS `service_evaluations_send_time` Array(String) AFTER `service_evaluations_eval_time`,
ADD COLUMN IF NOT EXISTS `service_evaluations_source` Array(Int64) AFTER `service_evaluations_send_time`,
ADD COLUMN IF NOT EXISTS `service_evaluations_message_id` Array(String) AFTER `service_evaluations_source`,
ADD COLUMN IF NOT EXISTS `service_evaluations_desc` Array(String) AFTER `service_evaluations_message_id`,
ADD COLUMN IF NOT EXISTS `mark_time` DateTime64(3) AFTER `mark`,
ADD COLUMN IF NOT EXISTS `message_marks_id` Array(String) AFTER `mark_time`,
ADD COLUMN IF NOT EXISTS `message_marks_mark` Array(String) AFTER `message_marks_id`,
ADD COLUMN IF NOT EXISTS `order_info_history_status` Array(Array(String)) AFTER `order_info_time`,
ADD COLUMN IF NOT EXISTS `order_info_history_time` Array(Array(UInt64)) AFTER `order_info_history_status`,
ADD COLUMN IF NOT EXISTS `plat_goods_ids` Array(String) AFTER `order_info_history_time`,
ADD COLUMN IF NOT EXISTS `remark` String AFTER `wx_rule_add_stats_count`,
ADD COLUMN IF NOT EXISTS `desc` String AFTER `remark`;