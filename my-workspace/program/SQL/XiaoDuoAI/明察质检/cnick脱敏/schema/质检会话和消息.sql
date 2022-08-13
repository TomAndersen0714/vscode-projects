-- xdqc.dialog
ALTER TABLE ods.xdqc_dialog_local ON CLUSTER cluster_3s_2r
ADD COLUMN `real_buyer_nick` String AFTER `cnick`,
ADD COLUMN `open_uid` String AFTER `real_buyer_nick`

ALTER TABLE ods.xdqc_dialog_all ON CLUSTER cluster_3s_2r
ADD COLUMN `real_buyer_nick` String AFTER `cnick`,
ADD COLUMN `open_uid` String AFTER `real_buyer_nick`

DROP TABLE buffer.xdqc_dialog_buffer ON CLUSTER cluster_3s_2r
CREATE TABLE buffer.xdqc_dialog_buffer ON CLUSTER cluster_3s_2r
AS ods.xdqc_dialog_all
ENGINE = Buffer('ods', 'xdqc_dialog_all', 16, 10, 15, 81920, 409600, 67108864, 134217728)


ALTER TABLE ods.xdqc_dialog_update_local ON CLUSTER cluster_3s_2r
ADD COLUMN `real_buyer_nick` String AFTER `cnick`,
ADD COLUMN `open_uid` String AFTER `real_buyer_nick`

ALTER TABLE ods.xdqc_dialog_update_all ON CLUSTER cluster_3s_2r
ADD COLUMN `real_buyer_nick` String AFTER `cnick`,
ADD COLUMN `open_uid` String AFTER `real_buyer_nick`

DROP TABLE buffer.xdqc_dialog_update_buffer ON CLUSTER cluster_3s_2r
CREATE TABLE buffer.xdqc_dialog_update_buffer ON CLUSTER cluster_3s_2r
AS ods.xdqc_dialog_update_all
ENGINE = Buffer('ods', 'xdqc_dialog_update_all', 16, 10, 15, 81920, 409600, 67108864, 134217728)


ALTER TABLE dwd.xdqc_dialog_local ON CLUSTER cluster_3s_2r
ADD COLUMN `real_buyer_nick` String AFTER `cnick`,
ADD COLUMN `open_uid` String AFTER `real_buyer_nick`

ALTER TABLE dwd.xdqc_dialog_all ON CLUSTER cluster_3s_2r
ADD COLUMN `real_buyer_nick` String AFTER `cnick`,
ADD COLUMN `open_uid` String AFTER `real_buyer_nick`

-- xdqc.message
ALTER TABLE xqc_ods.message_local ON CLUSTER cluster_3s_2r
ADD COLUMN `real_buyer_nick` String AFTER `cnick`,
ADD COLUMN `open_uid` String AFTER `real_buyer_nick`

ALTER TABLE xqc_ods.message_all ON CLUSTER cluster_3s_2r
ADD COLUMN `real_buyer_nick` String AFTER `cnick`,
ADD COLUMN `open_uid` String AFTER `real_buyer_nick`

DROP TABLE buffer.xqc_message_buffer ON CLUSTER cluster_3s_2r
CREATE TABLE buffer.xqc_message_buffer ON CLUSTER cluster_3s_2r
AS xqc_ods.message_all
ENGINE = Buffer('xqc_ods', 'message_all', 16, 10, 15, 81920, 409600, 67108864, 134217728)

