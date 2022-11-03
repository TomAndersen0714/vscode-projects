CREATE DATABASE IF NOT EXISTS tmp ON CLUSTER cluster_3s_2r
ENGINE=Ordinary

-- DROP TABLE tmp.xqc_dws_xplat_snick_stat_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE tmp.xqc_dws_xplat_snick_stat_local ON CLUSTER cluster_3s_2r
(
    `day` Int32,
    `platform` String,
    `seller_nick` String,
    `snick` String,
    `employee_id` String,
    `employee_name` String,
    `department_id` String,
    `department_name` String,
    `subtract_score_sum` Int64,
    `add_score_sum` Int64,
    `ai_subtract_score_sum` Int64,
    `ai_add_score_sum` Int64,
    `custom_subtract_score_sum` Int64,
    `custom_add_score_sum` Int64,
    `manual_subtract_score_sum` Int64,
    `manual_add_score_sum` Int64,
    `dialog_cnt` Int64,
    `tagged_dialog_cnt` Int64,
    `ai_tagged_dialog_cnt` Int64,
    `custom_tagged_dialog_cnt` Int64,
    `manual_tagged_dialog_cnt` Int64,
    `subtract_score_dialog_cnt` Int64,
    `add_score_dialog_cnt` Int64,
    `manual_marked_dialog_cnt` Int64,
    `ai_subtract_score_dialog_cnt` Int64,
    `ai_add_score_dialog_cnt` Int64,
    `custom_subtract_score_dialog_cnt` Int64,
    `custom_add_score_dialog_cnt` Int64,
    `manual_subtract_score_dialog_cnt` Int64,
    `manual_add_score_dialog_cnt` Int64
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
ORDER BY (day, platform, seller_nick)
SETTINGS index_granularity = 8192, storage_policy = 'rr'


-- DROP TABLE tmp.xqc_dws_xplat_snick_stat_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE tmp.xqc_dws_xplat_snick_stat_all ON CLUSTER cluster_3s_2r
AS tmp.xqc_dws_xplat_snick_stat_local
ENGINE = Distributed('cluster_3s_2r', 'tmp', 'xqc_dws_xplat_snick_stat_local', rand())

-- DROP TABLE buffer.tmp_xqc_dws_xplat_snick_stat_buffer ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE buffer.tmp_xqc_dws_xplat_snick_stat_buffer ON CLUSTER cluster_3s_2r
AS tmp.xqc_dws_xplat_snick_stat_all
ENGINE = Buffer('tmp', 'xqc_dws_xplat_snick_stat_all', 16, 15, 35, 81920, 409600, 16777216, 67108864)