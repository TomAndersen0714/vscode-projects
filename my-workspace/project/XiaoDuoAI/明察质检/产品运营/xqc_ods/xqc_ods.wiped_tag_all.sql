CREATE DATABASE IF NOT EXISTS tmp ON CLUSTER cluster_3s_2r

-- DROP TABLE tmp.wiped_tag_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE tmp.wiped_tag_local ON CLUSTER cluster_3s_2r
(
    `_id` String,
    `platform` String,
    `company_id` String,
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
ORDER BY (platform, company_id)
SETTINGS index_granularity = 8192, storage_policy = 'rr'


-- DROP TABLE tmp.wiped_tag_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE tmp.wiped_tag_all ON CLUSTER cluster_3s_2r
AS tmp.wiped_tag_local
ENGINE = Distributed('cluster_3s_2r', 'tmp', 'wiped_tag_local', rand())

-- DROP TABLE buffer.tmp_wiped_tag_buffer ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE buffer.tmp_wiped_tag_buffer ON CLUSTER cluster_3s_2r
AS tmp.wiped_tag_all
ENGINE = Buffer('tmp', 'wiped_tag_all', 16, 15, 35, 81920, 409600, 16777216, 67108864)


CREATE DATABASE IF NOT EXISTS xqc_ods ON CLUSTER cluster_3s_2r

-- DROP TABLE xqc_ods.wiped_tag_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE xqc_ods.wiped_tag_local ON CLUSTER cluster_3s_2r
(
    `_id` String,
    `platform` String,
    `company_id` String,
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
ORDER BY (platform, company_id)
SETTINGS index_granularity = 8192, storage_policy = 'rr'


-- DROP TABLE xqc_ods.wiped_tag_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE xqc_ods.wiped_tag_all ON CLUSTER cluster_3s_2r
AS xqc_ods.wiped_tag_local
ENGINE = Distributed('cluster_3s_2r', 'xqc_ods', 'wiped_tag_local', rand())

-- DROP TABLE buffer.xqc_ods_wiped_tag_buffer ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE buffer.xqc_ods_wiped_tag_buffer ON CLUSTER cluster_3s_2r
AS xqc_ods.wiped_tag_all
ENGINE = Buffer('xqc_ods', 'wiped_tag_all', 16, 15, 35, 81920, 409600, 16777216, 67108864)