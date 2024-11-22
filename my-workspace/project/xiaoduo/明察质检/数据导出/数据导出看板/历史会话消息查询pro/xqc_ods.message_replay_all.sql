-- DROP TABLE xqc_ods.message_replay_local ON CLUSTER cluster_3s_2r
CREATE TABLE xqc_ods.message_replay_local ON CLUSTER cluster_3s_2r
(
    `_id` String,
    `raw_id` String,
    `dialog_id` String,
    `iscardmsg` String,
    `create_time` String,
    `update_time` String,
    `platform` String,
    `plat_goods_id` String,
    `channel` String,
    `cnick` String,
    `real_buyer_nick` String,
    `open_uid` String,
    `snick` String,
    `seller_nick` String,
    `room_nick` String,
    `source` Int32,
    `content` String,
    `content_type` String,
    `time` DateTime64(3),
    `is_after_sale` String,
    `is_reminder` String,
    `is_inside` String,
    `employee_name` String,
    `intent` Array(Array(Float64)),
    `qid` Int64,
    `answer_explain` String,
    `emotion` Int32,
    `algo_emotion` Int32,
    `emotion_score` Int32,
    `suspected_emotion` String,
    `abnormal_model` Int32,
    `abnormal` Array(Int32),
    `abnormal_scroe.type` Array(Int32),
    `abnormal_scroe.score` Array(Int32),
    `excellent_model` Int32,
    `excellent` Array(Int32),
    `excellent_score.type` Array(Int32),
    `excellent_score.score` Array(Int32),
    `suspected_abnormals` Array(Int32),
    `qc_word_stats.source` Array(Int32),
    `qc_word_stats.word` Array(String),
    `qc_word_stats.count` Array(Int32),
    `auto_send` String,
    `send_from` String,
    `mark` String,
    `is_transfer` String,
    `ms_msg_time` DateTime64(3),
    `withdraw_ms_time` DateTime64(3),
    `rule_stats.id` Array(String),
    `rule_stats.count` Array(Int32),
    `rule_stats.score` Array(Int32),
    `rule_add_stats.id` Array(String),
    `rule_add_stats.count` Array(Int32),
    `rule_add_stats.score` Array(Int32),
    `xrule_stats.id` Array(String),
    `xrule_stats.count` Array(UInt32),
    `xrule_stats.score` Array(Int32),
    `wx_rule_stats.id` Array(String),
    `wx_rule_stats.count` Array(Int32),
    `wx_rule_stats.score` Array(Int32),
    `wx_rule_add_stats.id` Array(String),
    `wx_rule_add_stats.count` Array(Int32),
    `wx_rule_add_stats.score` Array(Int32),
    `tags.tag_id` Array(String),
    `tags.cal_op` Array(Int32),
    `tags.score` Array(Int32),
    `tags.name` Array(String),
    `day` UInt32
) ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
PARTITION BY day
ORDER BY (platform, seller_nick, snick)
SETTINGS storage_policy = 'rr', index_granularity = 8192

-- DROP TABLE xqc_ods.message_replay_all ON CLUSTER cluster_3s_2r
CREATE TABLE xqc_ods.message_replay_all ON CLUSTER cluster_3s_2r
AS xqc_ods.message_replay_local
ENGINE = Distributed('cluster_3s_2r', 'xqc_ods', 'message_replay_local', rand())

-- DROP TABLE buffer.xqc_ods_message_replay_buffer ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE buffer.xqc_ods_message_replay_buffer ON CLUSTER cluster_3s_2r
AS xqc_ods.message_replay_all
ENGINE = Buffer('xqc_ods', 'message_replay_all', 16, 15, 35, 81920, 409600, 16777216, 67108864)