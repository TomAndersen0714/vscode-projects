CREATE DATABASE IF NOT EXISTS xqc_ads ON CLUSTER cluster_3s_2r
ENGINE=Ordinary

-- DROP TABLE xqc_ads.shop_stat_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE xqc_ads.shop_stat_local ON CLUSTER cluster_3s_2r
(
    `day` Int32,
    `company_id` String,
    `company_name` String,
    `platform` String,
    `shop_id` String,
    `shop_name` String,
    `seller_nick` String,
    `snick_uv` Int64,
    `cnick_uv` Int64,
    `subtract_score_sum` Int64,
    `add_score_sum` Int64,
    `ai_subtract_score_sum` Int64,
    `ai_add_score_sum` Int64,
    `custom_subtract_score_sum` Int64,
    `custom_add_score_sum` Int64,
    `manual_subtract_score_sum` Int64,
    `manual_add_score_sum` Int64,
    `dialog_cnt` Int64,
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
    `wiped_tag_cnt` Int64,
    `wiped_ai_tag_cnt` Int64,
    `wiped_manual_tag_cnt` Int64,
    `wiped_custom_tag_cnt` Int64,
    `eval_cnt` Int64,
    `eval_levels` Array(String),
    `eval_level_cnts` Array(Int64)
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
PARTITION BY day
ORDER BY (platform, company_id)
SETTINGS index_granularity = 8192, storage_policy = 'rr'


-- DROP TABLE xqc_ads.shop_stat_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE xqc_ads.shop_stat_all ON CLUSTER cluster_3s_2r
AS xqc_ads.shop_stat_local
ENGINE = Distributed('cluster_3s_2r', 'xqc_ads', 'shop_stat_local', rand())