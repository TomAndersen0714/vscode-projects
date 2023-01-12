-- DROP TABLE xqc_ods.dialog_eval_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE xqc_ods.dialog_eval_local ON CLUSTER cluster_3s_2r
(
    `platform` String,
    `seller_nick` String,
    `snick` String,
    `cnick` String,
    `dialog_id` String,
    `user_nick` String,
    `eval_code` Int32,
    `eval_recer` String,
    `eval_sender` String,
    `eval_time` String,
    `send_time` String,
    `source` Int32,
    `desc` String,
    `day` Int32
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
PARTITION BY (day, platform)
ORDER BY (seller_nick, snick, cnick)
SETTINGS index_granularity = 8192, storage_policy = 'rr'

-- DROP TABLE xqc_ods.dialog_eval_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE xqc_ods.dialog_eval_all ON CLUSTER cluster_3s_2r
AS xqc_ods.dialog_eval_local
ENGINE = Distributed('cluster_3s_2r', 'xqc_ods', 'dialog_eval_local', rand() )

-- DROP TABLE buffer.xqc_ods_dialog_eval_buffer ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE buffer.xqc_ods_dialog_eval_buffer ON CLUSTER cluster_3s_2r
AS xqc_ods.dialog_eval_all
ENGINE = Buffer('xqc_ods', 'dialog_eval_all', 16, 15, 35, 81920, 409600, 16777216, 67108864)
