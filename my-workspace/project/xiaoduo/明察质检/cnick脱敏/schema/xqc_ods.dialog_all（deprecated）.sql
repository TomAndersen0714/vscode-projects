-- pulsar://pulsar-cluster01-slb:6650
-- persistent://qc/event/dialog_stat
-- bigdata_xqc_tb

ALTER TABLE xqc_ods.dialog_local ON CLUSTER cluster_3s_2r
ADD COLUMN `buyer_one_id` String AFTER `cnick`,
ADD COLUMN `open_uid` String AFTER `buyer_one_id`

ALTER TABLE xqc_ods.dialog_all ON CLUSTER cluster_3s_2r
ADD COLUMN `buyer_one_id` String AFTER `cnick`,
ADD COLUMN `open_uid` String AFTER `buyer_one_id`

DROP TABLE buffer.xqc_ods_dialog_buffer ON CLUSTER cluster_3s_2r
CREATE TABLE buffer.xqc_ods_dialog_buffer ON CLUSTER cluster_3s_2r
AS xqc_ods.dialog_all
ENGINE = Buffer('xqc_ods', 'dialog_all', 16, 5, 10, 81920, 409600, 16777216, 67108864)