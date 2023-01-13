CREATE DATABASE IF NOT EXISTS xqc_dws ON CLUSTER cluster_3s_2r
ENGINE=Ordinary

-- DROP TABLE xqc_dws.dialog_eval_stat_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE xqc_dws.dialog_eval_stat_local ON CLUSTER cluster_3s_2r
(
    `day` Int32,
    `platform` String,
    `seller_nick` String,
    `snick` String,
    `eval_code` Int32 ,
    `eval_cnt` Int64,
    `dialog_cnt` Int64
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
PARTITION BY (day, platform)
ORDER BY (seller_nick, snick)
SETTINGS index_granularity = 8192, storage_policy = 'rr'


-- DROP TABLE xqc_dws.dialog_eval_stat_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE xqc_dws.dialog_eval_stat_all ON CLUSTER cluster_3s_2r
AS xqc_dws.dialog_eval_stat_local
ENGINE = Distributed('cluster_3s_2r', 'xqc_dws', 'dialog_eval_stat_local', rand())


-- DROP TABLE buffer.xqc_dws_dialog_eval_stat_buffer ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE buffer.xqc_dws_dialog_eval_stat_buffer ON CLUSTER cluster_3s_2r
AS xqc_dws.dialog_eval_stat_all
ENGINE = Buffer('xqc_dws', 'dialog_eval_stat_all', 16, 15, 35, 81920, 409600, 16777216, 67108864)