CREATE DATABASE IF NOT EXISTS xqc_dws ON CLUSTER cluster_3s_2r
ENGINE=Ordinary

-- DROP TABLE xqc_dws.tag_group_stat_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE xqc_dws.tag_group_stat_local ON CLUSTER cluster_3s_2r
(
    `day` Int32,
    `platform` String,
    `seller_nick` String,
    `snick` String,
    `qc_norm_id` String,
    `tag_group_id` String,
    `tag_group_level` Int64,
    `add_score_dialog_cnt` Int64,
    `subtract_score_dialog_cnt` Int64
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
PARTITION BY day
ORDER BY (platform, seller_nick, snick)
SETTINGS index_granularity = 8192, storage_policy = 'rr'


-- DROP TABLE xqc_dws.tag_group_stat_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE xqc_dws.tag_group_stat_all ON CLUSTER cluster_3s_2r
AS xqc_dws.tag_group_stat_local
ENGINE = Distributed('cluster_3s_2r', 'xqc_dws', 'tag_group_stat_local', rand())