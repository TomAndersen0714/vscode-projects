CREATE DATABASE sxx_ods ON CLUSTER cluster_3s_2r

-- DROP TABLE sxx_ods.voc_qc_compensate_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE sxx_ods.voc_qc_compensate_local ON CLUSTER cluster_3s_2r
(
    `day` Int32,
    `platform` String,
    `seller_nick` String,
    `snick` String,
    `cnick` String,
    `dialog_id` String,
    `order_id` String,
    `focus_goods_id` String,
    `s_emotion_rule_id` Array(String),
    `s_emotion_count` Array(UInt32),
    `c_emotion_rule_id` Array(String),
    `c_emotion_count` Array(UInt32),
    `abnormals_rule_id` Array(String),
    `abnormals_count` Array(UInt32),
    `excellents_rule_id` Array(String),
    `excellents_count` Array(UInt32),
    `xrule_stats_id` Array(String),
    `xrule_stats_count` Array(UInt32),
    `top_xrules_id` Array(String),
    `top_xrules_count` Array(UInt32),
    `tag_score_stats_id` Array(String),
    `tag_score_stats_count` Array(UInt32),
    `tag_score_stats_md` Array(UInt8),
    `tag_score_add_stats_id` Array(String),
    `tag_score_add_stats_count` Array(UInt32),
    `tag_score_add_stats_md` Array(UInt8)
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
PARTITION BY (day, platform)
ORDER BY (order_id, focus_goods_id)
SETTINGS index_granularity = 8192, storage_policy = 'rr'


-- DROP TABLE sxx_ods.voc_qc_compensate_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE sxx_ods.voc_qc_compensate_all ON CLUSTER cluster_3s_2r
AS sxx_ods.voc_qc_compensate_local
ENGINE = Distributed('cluster_3s_2r', 'sxx_ods', 'voc_qc_compensate_local', rand())

-- DROP TABLE buffer.sxx_ods_voc_qc_compensate_buffer ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE buffer.sxx_ods_voc_qc_compensate_buffer ON CLUSTER cluster_3s_2r
AS sxx_ods.voc_qc_compensate_all
ENGINE = Buffer('sxx_ods', 'voc_qc_compensate_all', 16, 15, 35, 81920, 409600, 16777216, 67108864)