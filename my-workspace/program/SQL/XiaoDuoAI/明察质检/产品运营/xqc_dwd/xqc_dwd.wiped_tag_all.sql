CREATE DATABASE IF NOT EXISTS xqc_dwd ON CLUSTER cluster_3s_2r 
ENGINE=Ordinary


-- DROP TABLE xqc_dwd.wiped_tag_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE xqc_dwd.wiped_tag_local ON CLUSTER cluster_3s_2r
(
    `day` Int32,
    `company_id` String,
    `company_name` String,
    `platform` String,
    `shop_id` String,
    `shop_name` String,
    `seller_nick` String,
    `snick` String,
    `cnick` String,
    `wipe_id` String,
    `wipe_time` Int64,
    `dialog_id` String,
    `dialog_time` String,
    `messages_id` String,
    `abnormal_types` Array(String),
    `abnormal_scores` Array(Int32),
    `excellents_types` Array(String),
    `excellents_scores` Array(Int32),
    `rule_scores_ids` Array(String),
    `rule_scores_counts` Array(Int32),
    `rule_scores_scores` Array(Int32),
    `emotion_type` String,
    `emotion_score` Int32
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
ORDER BY (platform, company_id, day)
SETTINGS index_granularity = 8192, storage_policy = 'rr'


-- DROP TABLE xqc_dwd.wiped_tag_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE xqc_dwd.wiped_tag_all ON CLUSTER cluster_3s_2r
AS xqc_dwd.wiped_tag_local
ENGINE = Distributed('cluster_3s_2r', 'xqc_dwd', 'wiped_tag_local', rand())

-- DROP TABLE buffer.xqc_dwd_wiped_tag_buffer ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE buffer.xqc_dwd_wiped_tag_buffer ON CLUSTER cluster_3s_2r
AS xqc_dwd.wiped_tag_all
ENGINE = Buffer('xqc_dwd', 'wiped_tag_all', 16, 15, 35, 81920, 409600, 16777216, 67108864)