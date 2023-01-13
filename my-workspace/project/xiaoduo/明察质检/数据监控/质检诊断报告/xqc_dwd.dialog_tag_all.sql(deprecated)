CREATE DATABASE IF NOT EXISTS xqc_dwd ON CLUSTER cluster_3s_2r
ENGINE=Ordinary

-- DROP TABLE xqc_dwd.dialog_tag_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE xqc_dwd.dialog_tag_local ON CLUSTER cluster_3s_2r
(
    `day` Int32,
    `platform` String,
    `seller_nick` String,
    `snick` String,
    `dialog_id` String,
    `tag_id` String,
    `qc_norm_id` String,
    `tag_group_id` String,
    `tag_group_level` Int32,
    `tag_score` Int32,
    `tag_level_1_group_id` String,
    `tag_level_2_group_id` String
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
PARTITION BY day
ORDER BY (platform, seller_nick, snick)
SETTINGS index_granularity = 8192, storage_policy = 'rr'


-- DROP TABLE xqc_dwd.dialog_tag_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE xqc_dwd.dialog_tag_all ON CLUSTER cluster_3s_2r
AS xqc_dwd.dialog_tag_local
ENGINE = Distributed('cluster_3s_2r', 'xqc_dwd', 'dialog_tag_local', rand())