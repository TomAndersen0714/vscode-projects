-- "url": "pulsar://pulsar-cluster01-slb:6650",
-- "topic": "persistent://statistics/kefu_eval/detail",
-- "subscription_name": "xdvector"
PS: 调整表结构之前, 需要先停流, 避免数据写入失败

ALTER TABLE ods.kefu_eval_detail_local ON CLUSTER cluster_3s_2r
ADD COLUMN `real_buyer_nick` String AFTER `cnick`,
ADD COLUMN `open_uid` String AFTER `real_buyer_nick`

ALTER TABLE ods.kefu_eval_detail_all ON CLUSTER cluster_3s_2r
ADD COLUMN `real_buyer_nick` String AFTER `cnick`,
ADD COLUMN `open_uid` String AFTER `real_buyer_nick`


-- pulsar://pulsar-cluster01-slb:6650
-- persistent://statistics/kefu_eval/transfer
-- xdvector
ALTER TABLE xqc_ods.dialog_eval_local ON CLUSTER cluster_3s_2r
ADD COLUMN `real_buyer_nick` String AFTER `cnick`,
ADD COLUMN `open_uid` String AFTER `real_buyer_nick`

ALTER TABLE xqc_ods.dialog_eval_all ON CLUSTER cluster_3s_2r
ADD COLUMN `real_buyer_nick` String AFTER `cnick`,
ADD COLUMN `open_uid` String AFTER `real_buyer_nick`

DROP TABLE buffer.xqc_ods_dialog_eval_buffer ON CLUSTER cluster_3s_2r
CREATE TABLE buffer.xqc_ods_dialog_eval_buffer ON CLUSTER cluster_3s_2r
AS xqc_ods.dialog_eval_all
ENGINE = Buffer('xqc_ods', 'dialog_eval_all', 16, 15, 35, 81920, 409600, 16777216, 67108864)