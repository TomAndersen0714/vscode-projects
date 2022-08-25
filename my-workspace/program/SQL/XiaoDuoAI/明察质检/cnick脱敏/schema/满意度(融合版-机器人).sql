CREATE TABLE ods.kefu_eval_detail_local ON CLUSTER cluster_3s_2r
(
    `user_nick` String,
    `eval_code` Int32,
    `eval_recer` String,
    `eval_sender` String,
    `eval_time` String,
    `send_time` String,
    `source` Int32,
    `day` Int32
) ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
) PARTITION BY day
ORDER BY user_nick SETTINGS index_granularity = 8192,
    storage_policy = 'rr'

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