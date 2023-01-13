-- pulsar://pulsar-cluster01-slb:6650
-- persistent://statistics/kefu_eval/transfer
-- xdvector
ALTER TABLE xqc_ods.dialog_eval_local ON CLUSTER cluster_3s_2r
ADD COLUMN `open_uid` String AFTER `cnick`

ALTER TABLE xqc_ods.dialog_eval_all ON CLUSTER cluster_3s_2r
ADD COLUMN `open_uid` String AFTER `cnick`

DROP TABLE buffer.xqc_ods_dialog_eval_buffer ON CLUSTER cluster_3s_2r
CREATE TABLE buffer.xqc_ods_dialog_eval_buffer ON CLUSTER cluster_3s_2r
AS xqc_ods.dialog_eval_all
ENGINE = Buffer('xqc_ods', 'dialog_eval_all', 16, 15, 35, 81920, 409600, 16777216, 67108864)