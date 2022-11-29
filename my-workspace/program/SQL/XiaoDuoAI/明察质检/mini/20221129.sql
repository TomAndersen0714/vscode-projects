CREATE DATABASE IF NOT EXISTS dwd ON CLUSTER cluster_3s_2r
ENGINE=Ordinary;


CREATE TABLE IF NOT EXISTS dwd.xdqc_backup_dialog_local ON CLUSTER cluster_3s_2r
(
    `_id` String,
    `platform` String,
    `channel` String,
    `group` String,
    `date` Int32,
    `seller_nick` String,
    `cnick` String,
    `real_buyer_nick` String,
    `open_uid` String,
    `snick` String,
    `begin_time` DateTime64(3),
    `end_time` DateTime64(3),
    `is_after_sale` UInt8,
    `is_inside` UInt8,
    `employee_name` String,
    `s_emotion_type` Array(UInt16),
    `s_emotion_rule_id` Array(String),
    `s_emotion_score` Array(Int32),
    `s_emotion_count` Array(UInt32),
    `c_emotion_type` Array(UInt16),
    `c_emotion_rule_id` Array(String),
    `c_emotion_score` Array(Int32),
    `c_emotion_count` Array(UInt32),
    `emotions` Array(String),
    `abnormals_type` Array(UInt16),
    `abnormals_rule_id` Array(String),
    `abnormals_score` Array(Int32),
    `abnormals_count` Array(UInt32),
    `excellents_type` Array(UInt16),
    `excellents_rule_id` Array(String),
    `excellents_score` Array(Int32),
    `excellents_count` Array(UInt32),
    `qc_word_source` Array(UInt8),
    `qc_word_word` Array(String),
    `qc_word_count` Array(UInt32),
    `qid` Array(Int64),
    `mark` String,
    `mark_judge` Int32,
    `mark_score` Int32,
    `mark_score_add` Int32,
    `mark_ids` Array(String),
    `last_mark_id` String,
    `human_check` UInt8,
    `tag_score_stats_id` Array(String),
    `tag_score_stats_score` Array(Int32),
    `tag_score_stats_count` Array(UInt32),
    `tag_score_stats_md` Array(UInt8),
    `tag_score_stats_mm` Array(UInt8),
    `tag_score_add_stats_id` Array(String),
    `tag_score_add_stats_score` Array(Int32),
    `tag_score_add_stats_count` Array(UInt32),
    `tag_score_add_stats_md` Array(UInt8),
    `tag_score_add_stats_mm` Array(UInt8),
    `rule_stats_id` Array(String),
    `rule_stats_score` Array(Int32),
    `rule_stats_count` Array(UInt32),
    `rule_add_stats_id` Array(String),
    `rule_add_stats_score` Array(Int32),
    `rule_add_stats_count` Array(UInt32),
    `xrule_stats_id` Array(String),
    `xrule_stats_score` Array(Int32),
    `xrule_stats_count` Array(UInt32),
    `top_xrules_id` Array(String),
    `top_xrules_score` Array(Int32),
    `top_xrules_count` Array(UInt32),
    `score` Int32,
    `score_add` Int32,
    `question_count` UInt32,
    `answer_count` UInt32,
    `first_answer_time` DateTime64(3),
    `qa_time_sum` UInt32,
    `qa_round_sum` UInt32,
    `focus_goods_id` String,
    `is_remind` UInt8,
    `task_list_id` String,
    `read_mark` Array(String),
    `last_msg_id` String,
    `consulte_transfor_v2` Int32,
    `order_info_id` Array(String),
    `order_info_status` Array(String),
    `order_info_payment` Array(Float32),
    `order_info_time` Array(UInt64),
    `intel_score` Int32,
    `remind_ntype` String,
    `first_follow_up_time` DateTime64(3),
    `is_follow_up_remind` UInt8,
    `emotion_detect_mode` Int32,
    `has_withdraw_robot_msg` UInt8,
    `is_order_matched` UInt8,
    `suspected_positive_emotion` UInt8,
    `suspected_problem` UInt8,
    `suspected_excellent` UInt8,
    `has_after` UInt8,
    `cnick_customize_rule` Array(String),
    `update_time` DateTime('Asia/Shanghai'),
    `wx_rule_stats_id` Array(String),
    `wx_rule_stats_score` Array(Int32),
    `wx_rule_stats_count` Array(UInt32),
    `wx_rule_add_stats_id` Array(String),
    `wx_rule_add_stats_score` Array(Int32),
    `wx_rule_add_stats_count` Array(UInt32),
    `sign` Int8
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
PARTITION BY toYYYYMMDD(begin_time)
PRIMARY KEY (platform, channel, seller_nick)
ORDER BY (platform, channel, seller_nick, _id)
SETTINGS index_granularity = 8192, storage_policy = 'rr';


CREATE TABLE IF NOT EXISTS dwd.xdqc_backup_dialog_all ON CLUSTER cluster_3s_2r
AS dwd.xdqc_backup_dialog_local
ENGINE = Distributed('cluster_3s_2r', 'dwd', 'xdqc_backup_dialog_local', rand());




CREATE DATABASE IF NOT EXISTS ods ON CLUSTER cluster_3s_2r
ENGINE=Ordinary;


DROP TABLE IF EXISTS ods.xdqc_dialog_update_local ON CLUSTER cluster_3s_2r NO DELAY;

CREATE TABLE IF NOT EXISTS ods.xdqc_dialog_update_local ON CLUSTER cluster_3s_2r
(
    `_id` String,
    `platform` String,
    `channel` String,
    `group` String,
    `date` Int32,
    `seller_nick` String,
    `cnick` String,
    `real_buyer_nick` String,
    `open_uid` String,
    `snick` String,
    `begin_time` DateTime64(3),
    `end_time` DateTime64(3),
    `is_after_sale` UInt8,
    `is_inside` UInt8,
    `employee_name` String,
    `s_emotion_type` Array(UInt16),
    `s_emotion_rule_id` Array(String),
    `s_emotion_score` Array(Int32),
    `s_emotion_count` Array(UInt32),
    `c_emotion_type` Array(UInt16),
    `c_emotion_rule_id` Array(String),
    `c_emotion_score` Array(Int32),
    `c_emotion_count` Array(UInt32),
    `emotions` Array(String),
    `abnormals_type` Array(UInt16),
    `abnormals_rule_id` Array(String),
    `abnormals_score` Array(Int32),
    `abnormals_count` Array(UInt32),
    `excellents_type` Array(UInt16),
    `excellents_rule_id` Array(String),
    `excellents_score` Array(Int32),
    `excellents_count` Array(UInt32),
    `qc_word_source` Array(UInt8),
    `qc_word_word` Array(String),
    `qc_word_count` Array(UInt32),
    `qid` Array(Int64),
    `mark` String,
    `mark_judge` Int32,
    `mark_score` Int32,
    `mark_score_add` Int32,
    `mark_ids` Array(String),
    `last_mark_id` String,
    `human_check` UInt8,
    `tag_score_stats_id` Array(String),
    `tag_score_stats_score` Array(Int32),
    `tag_score_stats_count` Array(UInt32),
    `tag_score_stats_md` Array(UInt8),
    `tag_score_stats_mm` Array(UInt8),
    `tag_score_add_stats_id` Array(String),
    `tag_score_add_stats_score` Array(Int32),
    `tag_score_add_stats_count` Array(UInt32),
    `tag_score_add_stats_md` Array(UInt8),
    `tag_score_add_stats_mm` Array(UInt8),
    `rule_stats_id` Array(String),
    `rule_stats_score` Array(Int32),
    `rule_stats_count` Array(UInt32),
    `rule_add_stats_id` Array(String),
    `rule_add_stats_score` Array(Int32),
    `rule_add_stats_count` Array(UInt32),
    `xrule_stats_id` Array(String),
    `xrule_stats_score` Array(Int32),
    `xrule_stats_count` Array(UInt32),
    `top_xrules_id` Array(String),
    `top_xrules_score` Array(Int32),
    `top_xrules_count` Array(UInt32),
    `score` Int32,
    `score_add` Int32,
    `question_count` UInt32,
    `answer_count` UInt32,
    `first_answer_time` DateTime64(3),
    `qa_time_sum` UInt32,
    `qa_round_sum` UInt32,
    `focus_goods_id` String,
    `is_remind` UInt8,
    `task_list_id` String,
    `read_mark` Array(String),
    `last_msg_id` String,
    `consulte_transfor_v2` Int32,
    `order_info_id` Array(String),
    `order_info_status` Array(String),
    `order_info_payment` Array(Float32),
    `order_info_time` Array(UInt64),
    `intel_score` Int32,
    `remind_ntype` String,
    `first_follow_up_time` DateTime64(3),
    `is_follow_up_remind` UInt8,
    `emotion_detect_mode` Int32,
    `has_withdraw_robot_msg` UInt8,
    `is_order_matched` UInt8,
    `suspected_positive_emotion` UInt8,
    `suspected_problem` UInt8,
    `suspected_excellent` UInt8,
    `has_after` UInt8,
    `cnick_customize_rule` Array(String),
    `update_time` DateTime('Asia/Shanghai'),
    `wx_rule_stats_id` Array(String),
    `wx_rule_stats_score` Array(Int32),
    `wx_rule_stats_count` Array(UInt32),
    `wx_rule_add_stats_id` Array(String),
    `wx_rule_add_stats_score` Array(Int32),
    `wx_rule_add_stats_count` Array(UInt32)
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
PARTITION BY toYYYYMMDD(update_time)
ORDER BY (toYYYYMMDD(begin_time), platform, seller_nick, _id)
SETTINGS storage_policy = 'rr', index_granularity = 8192;


DROP TABLE IF EXISTS ods.xdqc_dialog_update_all ON CLUSTER cluster_3s_2r NO DELAY;

CREATE TABLE IF NOT EXISTS ods.xdqc_dialog_update_all ON CLUSTER cluster_3s_2r
AS ods.xdqc_dialog_update_local
ENGINE = Distributed('cluster_3s_2r', 'ods', 'xdqc_dialog_update_local', rand());




ALTER TABLE xqc_dim.snick_full_info_local ON CLUSTER cluster_3s_2r
ADD COLUMN IF NOT EXISTS `company_name` String AFTER company_id,
ADD COLUMN IF NOT EXISTS `company_short_name` String AFTER company_name,
ADD COLUMN IF NOT EXISTS `shop_name` String AFTER shop_id,
ADD COLUMN IF NOT EXISTS `seller_nick` String AFTER shop_name;

ALTER TABLE xqc_dim.snick_full_info_all ON CLUSTER cluster_3s_2r
ADD COLUMN IF NOT EXISTS `company_name` String AFTER company_id,
ADD COLUMN IF NOT EXISTS `company_short_name` String AFTER company_name,
ADD COLUMN IF NOT EXISTS `shop_name` String AFTER shop_id,
ADD COLUMN IF NOT EXISTS `seller_nick` String AFTER shop_name;




ALTER TABLE xqc_dws.snick_stat_local ON CLUSTER cluster_3s_2r
ADD COLUMN IF NOT EXISTS `tagged_dialog_cnt` Int64 AFTER `dialog_cnt`,
ADD COLUMN IF NOT EXISTS `ai_tagged_dialog_cnt` Int64 AFTER `tagged_dialog_cnt`,
ADD COLUMN IF NOT EXISTS `custom_tagged_dialog_cnt` Int64 AFTER `ai_tagged_dialog_cnt`,
ADD COLUMN IF NOT EXISTS `manual_tagged_dialog_cnt` Int64 AFTER `custom_tagged_dialog_cnt`;

ALTER TABLE xqc_dws.snick_stat_all ON CLUSTER cluster_3s_2r
ADD COLUMN IF NOT EXISTS `tagged_dialog_cnt` Int64 AFTER `dialog_cnt`,
ADD COLUMN IF NOT EXISTS `ai_tagged_dialog_cnt` Int64 AFTER `tagged_dialog_cnt`,
ADD COLUMN IF NOT EXISTS `custom_tagged_dialog_cnt` Int64 AFTER `ai_tagged_dialog_cnt`,
ADD COLUMN IF NOT EXISTS `manual_tagged_dialog_cnt` Int64 AFTER `custom_tagged_dialog_cnt`;





CREATE DATABASE IF NOT EXISTS tmp ON CLUSTER cluster_3s_2r
ENGINE=Ordinary;



CREATE TABLE IF NOT EXISTS tmp.xqc_qc_report_snick_local ON CLUSTER cluster_3s_2r
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
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
PARTITION BY day
ORDER BY (platform, seller_nick)
SETTINGS index_granularity = 8192, storage_policy = 'rr';



CREATE TABLE IF NOT EXISTS tmp.xqc_qc_report_snick_all ON CLUSTER cluster_3s_2r
AS tmp.xqc_qc_report_snick_local
ENGINE = Distributed('cluster_3s_2r', 'tmp', 'xqc_qc_report_snick_local', rand());
