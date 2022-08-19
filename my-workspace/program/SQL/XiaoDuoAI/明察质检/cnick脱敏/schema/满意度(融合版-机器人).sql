-- "url": "pulsar://pulsar-cluster01-slb:6650",
-- "topic": "persistent://statistics/kefu_eval/detail",
-- "subscription_name": "xdvector"
PS: 调整表结构之前, 需要先停流, 避免数据写入失败

ALTER TABLE ods.kefu_eval_detail_local ON CLUSTER cluster_3s_2r
ADD COLUMN `real_buyer_nick` String AFTER `eval_recer`,
ADD COLUMN `open_uid` String AFTER `real_buyer_nick`

ALTER TABLE ods.kefu_eval_detail_all ON CLUSTER cluster_3s_2r
ADD COLUMN `real_buyer_nick` String AFTER `eval_recer`,
ADD COLUMN `open_uid` String AFTER `real_buyer_nick`