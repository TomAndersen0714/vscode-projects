CREATE TABLE xqc_dws.snick_stat_local (
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
ORDER BY (platform, seller_nick, snick) SETTINGS storage_policy = 'rr',
    index_granularity = 8192