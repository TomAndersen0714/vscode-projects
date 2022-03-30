-- 创建本地表
CREATE TABLE xqc_ods.archived_dialog_local ON CLUSTER cluster_3s_2r (
    `_id` String,
    `platform` String,
    `channel` String,
    `cnick` String,
    `snick` String,
    `seller_nick` String,
    `room_id` String,
    `begin_time` DateTime64(3),
    `end_time` DateTime64(3),
    `is_after_sale` UInt8,
    `is_inside` UInt8,
    `employee_name` String,
    `s_emotion_type` Array(Int32),
    `s_emotion_count` Array(Int32),
    `c_emotion_type` Array(Int32),
    `c_emotion_count` Array(Int32),
    `emotions` Array(String),
    `abnormals_type` Array(Int32),
    `abnormals_count` Array(Int32),
    `excellents_type` Array(Int32),
    `excellents_count` Array(Int32),
    `qc_word_source` Array(Int32),
    `qc_word_word` Array(String),
    `qc_word_count` Array(Int32),
    `qc_word_is_robot` Array(Int32),
    `qid` Array(Int64),
    `mark` String,
    `mark_judge` Int32,
    `mark_score` Int32,
    `mark_score_add` Int32,
    `mark_ids` Array(String),
    `human_check` UInt8,
    `score` Int32,
    `score_add` Int32,
    `read_mark` Array(String),
    `suspected_problem` UInt8,
    `tag_score_stats_id` Array(String),
    `tag_score_stats_score` Array(Int32),
    `tag_score_stats_md` Array(UInt8),
    `tag_score_stats_mm` Array(UInt8),
    `tag_score_stats_count` Array(Int32),
    `tag_score_stats_name` Array(String),
    `tag_score_add_stats_id` Array(String),
    `tag_score_add_stats_score` Array(Int32),
    `tag_score_add_stats_md` Array(String),
    `tag_score_add_stats_mm` Array(String),
    `tag_score_add_stats_count` Array(Int32),
    `tag_score_add_stats_name` Array(Int32),
    `rule_stats_id` Array(String),
    `rule_stats_score` Array(Int32),
    `rule_stats_count` Array(Int32),
    `rule_add_stats_id` Array(String),
    `rule_add_stats_score` Array(Int32),
    `rule_add_stats_count` Array(Int32),
    `wx_rule_stats_id` Array(String),
    `wx_rule_stats_score` Array(Int32),
    `wx_rule_stats_count` Array(Int32),
    `wx_rule_add_stats_id` Array(String),
    `wx_rule_add_stats_score` Array(Int32),
    `wx_rule_add_stats_count` Array(Int32),
    `has_after` UInt8,
    `not_send_rules_ac` Array(Tuple(String,UInt8)),
    `not_send_rules_cb` Array(Tuple(String,UInt8)),
    `last_mark_id` String,
    `answer_count` Int64,
    `question_count` Int64,
    `first_answer_time` DateTime64(3),
    `qa_time_sum` Int32,
    `qa_round_sum` Int32,
    `focus_goods_id` String,
    `group` String,
    `has_withdraw_robot_msg` UInt8,
    `is_remind` UInt8,
    `order_info_id` Array(String),
    `order_info_status` Array(String),
    `order_info_payment` Array(Float64),
    `order_info_time` Array(Int64),
    `is_order_matched` UInt8,
    `emotion_detect_mode` Int32,
    `task_list_id` String,
    `consulte_transfor_v2` Int32,
    `intel_score` Int32,
    `suspected_positive_emotion` UInt8,
    `is_follow_up_remind` UInt8,
    `remind_ntype` Int32,
    `first_follow_up_time` DateTime64(3),
    `date` Int32,
    `last_msg_id` String,
    `cnick_customize_rule` Array(String),
    `suspected_excellent` UInt8,
    `procedure_rules` Array(String),
    `unorder_id` String,
    `sid_count` Array(Tuple(Int64,Int64)),
    `qc_task_flag` Int32,
    `create_time` DateTime64(3),
    `update_time` DateTime64(3),
    INDEX id_idx _id TYPE bloom_filter GRANULARITY 1
) 
ENGINE = ReplicatedMergeTree(
    '/clickhouse/xqc_ods/tables/{layer}_{shard}/archived_dialog_local', 
    '{replica}'
)
PARTITION BY `date`
ORDER BY (platform, seller_nick, snick, cnick)
SETTINGS storage_policy = 'rr', index_granularity = 8192

-- 创建分布式表
CREATE TABLE xqc_ods.archived_dialog_all
AS xqc_ods.archived_dialog_local
ENGINE = Distributed('cluster_3s_2r', 'xqc_ods', 'archived_dialog_local', rand())

-- 创建Buffer表
CREATE TABLE buffer.xqc_ods_archived_dialog_buffer
AS xqc_ods.archived_dialog_all
ENGINE = Buffer('xqc_ods', 'archived_dialog_all', 16, 10, 15, 81920, 409600, 67108864, 134217728)

-- 数据迁移
SELECT
    COUNT(1)
FROM dwd.xdqc_dialog_all 
WHERE toYYYYMMDD(begin_time) 
    BETWEEN 20210627 AND 20210911
AND seller_nick IN (
    SELECT distinct tenant_label FROM xqc_dim.company_tenant
) -- 14275183