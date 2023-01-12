CREATE DATABASE IF NOT EXISTS xqc_ads ON CLUSTER cluster_3s_2r
ENGINE=Ordinary

-- DROP TABLE xqc_ads.platform_company_stat_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE xqc_ads.platform_company_stat_local ON CLUSTER cluster_3s_2r
(
    `day` Int32,
    `platform` String,
    `company_id` String,
    `company_name` String,
    `company_short_name` String,
    `is_active` String,
    `pv` Int64,
    `uv` Int64,
    `shop_cnt` Int64,
    `snick_cnt` Int64,
    `snick_uv` Int64,
    `cnick_uv` Int64,
    `account_uv` Int64,
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
    `manual_add_score_dialog_cnt` Int64,
    `alert_cnt` Int64,
    `level_1_alert_cnt` Int64,
    `level_2_alert_cnt` Int64,
    `level_3_alert_cnt` Int64,
    `level_1_alert_finished_cnt` Int64,
    `level_2_alert_finished_cnt` Int64,
    `level_3_alert_finished_cnt` Int64,
    `alert_finished_mins` Int64,
    `manual_qc_task_cnt` Int64,
    `manual_qc_target_dialog_sum` Int64,
    `manual_qc_finished_dialog_sum` Int64,
    `manual_qc_ontime_dialog_sum` Int64,
    `manual_qc_overdue_dialog_sum` Int64,
    `manual_qc_basic_dialog_sum` Int64,
    `manual_qc_advanced_dialog_sum` Int64,
    `wiped_tag_cnt` Int64,
    `wiped_ai_tag_cnt` Int64,
    `wiped_manual_tag_cnt` Int64,
    `wiped_custom_tag_cnt` Int64,
    `eval_cnt` Int64,
    `eval_levels` Array(String),
    `eval_level_cnts` Array(Int64),
    `qc_norm_cnt` Int64,
    `qc_norm_opened_cnt` Int64,
    `qc_norm_edit_cnt` Int64,
    `tag_cnt` Int64,
    `open_alert_tag_cnt` Int64,
    `ai_tag_cnt` Int64,
    `ai_tag_opened_cnt` Int64,
    `custom_tag_cnt` Int64,
    `custom_tag_opened_cnt` Int64,
    `manual_tag_cnt` Int64,
    `manual_tag_opened_cnt` Int64,
    `qc_word_cnt` Int64,
    `qc_word_opened_cnt` Int64,
    `appeal_task_cnt` Int64,
    `qt_task_cnt` Int64,
    `dialog_case_cnt` Int64
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
PARTITION BY day
ORDER BY (platform, company_id)
SETTINGS index_granularity = 8192, storage_policy = 'rr'


-- DROP TABLE xqc_ads.platform_company_stat_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE xqc_ads.platform_company_stat_all ON CLUSTER cluster_3s_2r
AS xqc_ads.platform_company_stat_local
ENGINE = Distributed('cluster_3s_2r', 'xqc_ads', 'platform_company_stat_local', rand())