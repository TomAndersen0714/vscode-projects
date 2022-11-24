DROP TABLE xqc_dim.snick_full_info_local ON CLUSTER cluster_3s_2r
CREATE TABLE xqc_dim.snick_full_info_local ON CLUSTER cluster_3s_2r
(
    `company_id` String,
    `platform` String,
    `shop_id` String,
    `department_id` String,
    `department_name` String,
    `snick` String,
    `employee_id` String,
    `employee_name` String,
    `superior_id` String,
    `superior_name` String,
    `day` Int32
) ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
) PARTITION BY day
ORDER BY (company_id, platform) SETTINGS index_granularity = 8192,
    storage_policy = 'rr'

DROP TABLE xqc_dim.snick_full_info_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE xqc_dim.snick_full_info_all ON CLUSTER cluster_3s_2r
AS xqc_dim.snick_full_info_local
ENGINE = Distributed(
    'cluster_3s_2r',
    'xqc_dim',
    'snick_full_info_local',
    rand()
)

DROP TABLE xqc_dws.snick_stat_local ON CLUSTER cluster_3s_2r NO DELAY;
CREATE TABLE xqc_dws.snick_stat_local ON CLUSTER cluster_3s_2r
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
    `subtract_score_dialog_cnt` Int64,
    `add_score_dialog_cnt` Int64,
    `manual_marked_dialog_cnt` Int64,
    `ai_subtract_score_dialog_cnt` Int64,
    `ai_add_score_dialog_cnt` Int64,
    `custom_subtract_score_dialog_cnt` Int64,
    `custom_add_score_dialog_cnt` Int64,
    `manual_subtract_score_dialog_cnt` Int64,
    `manual_add_score_dialog_cnt` Int64
) ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
) PARTITION BY day
ORDER BY (platform, seller_nick, snick)
SETTINGS storage_policy = 'rr', index_granularity = 8192;


DROP TABLE xqc_dws.snick_stat_all ON CLUSTER cluster_3s_2r NO DELAY;
CREATE TABLE xqc_dws.snick_stat_all ON CLUSTER cluster_3s_2r
AS xqc_dws.snick_stat_local
ENGINE = Distributed('cluster_3s_2r', 'xqc_dws', 'snick_stat_local', rand());