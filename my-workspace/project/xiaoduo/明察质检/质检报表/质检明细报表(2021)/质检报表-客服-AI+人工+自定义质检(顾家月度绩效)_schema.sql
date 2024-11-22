-- 创建临时本地表
CREATE TABLE tmp.xqc_qc_report_snick_local ON CLUSTER cluster_3s_2r
(
    `day` Int64,
    `platform` String,
    `seller_nick` String,
    `snick` String,
    `dialog_cnt` UInt64,
    `score` Int64,
    `score_add` Int64,
    `mark_score` Int64,
    `mark_score_add` Int64,
    `rule_score` Int64,
    `rule_score_add` Int64,
    `ai_score` Int64,
    `ai_score_add` Int64,
    `abnormal_dialog_cnt` UInt64,
    `excellents_dialog_cnt` UInt64,
    `mark_dialog_cnt` UInt64,
    `tag_score_dialog_cnt` UInt64,
    `tag_score_add_dialog_cnt` UInt64,
    `rule_dialog_cnt` UInt64,
    `rule_add_dialog_cnt` UInt64,
    `abnormal_type_1_cnt` UInt64,
    `abnormal_type_2_cnt` UInt64,
    `abnormal_type_3_cnt` UInt64,
    `abnormal_type_4_cnt` UInt64,
    `abnormal_type_5_cnt` UInt64,
    `abnormal_type_6_cnt` UInt64,
    `abnormal_type_7_cnt` UInt64,
    `abnormal_type_8_cnt` UInt64,
    `abnormal_type_9_cnt` UInt64,
    `abnormal_type_10_cnt` UInt64,
    `abnormal_type_11_cnt` UInt64,
    `abnormal_type_12_cnt` UInt64,
    `abnormal_type_13_cnt` UInt64,
    `abnormal_type_14_cnt` UInt64,
    `abnormal_type_15_cnt` UInt64,
    `abnormal_type_16_cnt` UInt64,
    `abnormal_type_17_cnt` UInt64,
    `abnormal_type_18_cnt` UInt64,
    `abnormal_type_19_cnt` UInt64,
    `abnormal_type_20_cnt` UInt64,
    `abnormal_type_21_cnt` UInt64,
    `abnormal_type_22_cnt` UInt64,
    `abnormal_type_23_cnt` UInt64,
    `abnormal_type_24_cnt` UInt64,
    `abnormal_type_25_cnt` UInt64,
    `abnormal_type_26_cnt` UInt64,
    `abnormal_type_27_cnt` UInt64,
    `abnormal_type_28_cnt` UInt64,
    `abnormal_type_29_cnt` UInt64,
    `excellent_type_1_cnt` UInt64,
    `excellent_type_2_cnt` UInt64,
    `excellent_type_3_cnt` UInt64,
    `excellent_type_4_cnt` UInt64,
    `excellent_type_5_cnt` UInt64,
    `excellent_type_6_cnt` UInt64,
    `excellent_type_7_cnt` UInt64,
    `excellent_type_8_cnt` UInt64,
    `excellent_type_9_cnt` UInt64,
    `excellent_type_10_cnt` UInt64,
    `excellent_type_11_cnt` UInt64,
    `excellent_type_12_cnt` UInt64,
    `excellent_type_13_cnt` UInt64,
    `c_emotion_type_1_cnt` UInt64,
    `c_emotion_type_2_cnt` UInt64,
    `c_emotion_type_3_cnt` UInt64,
    `c_emotion_type_4_cnt` UInt64,
    `c_emotion_type_5_cnt` UInt64,
    `c_emotion_type_6_cnt` UInt64,
    `c_emotion_type_7_cnt` UInt64,
    `c_emotion_type_8_cnt` UInt64,
    `c_emotion_type_9_cnt` UInt64,
    `s_emotion_type_8_cnt` UInt64,
    `human_check_tag_name_arr` Array(String),
    `human_check_tag_cnt_arr` Array(UInt64),
    `customize_check_tag_name_arr` Array(String),
    `customize_check_tag_cnt_arr` Array(UInt64)
)
ENGINE = ReplicatedMergeTree('/clickhouse/{database}/tables/{layer}_{shard}/{table}', '{replica}')
PARTITION BY day
ORDER BY (platform, seller_nick)
SETTINGS index_granularity = 8192, storage_policy = 'rr'

-- 创建临时分布式表
CREATE TABLE tmp.xqc_qc_report_snick_all ON CLUSTER cluster_3s_2r
AS tmp.xqc_qc_report_snick_local
ENGINE = Distributed('cluster_3s_2r', 'tmp', 'xqc_qc_report_snick_local', rand())