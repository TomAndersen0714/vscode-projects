CREATE TABLE ods.message_all (
    `_id` String,
    `dialog_id` String,
    `raw_id` String,
    `plat_goods_id` String,
    `platform` String,
    `channel` String,
    `cnick` String,
    `snick` String,
    `seller_nick` String,
    `room_nick` String,
    `content` String,
    `content_type` String,
    `employee_name` String,
    `time` DateTime64(3),
    `qid` Int64,
    `source` Int32,
    `emotion` Int32,
    `emotion_score` Int32,
    `abnormal_model` Int32,
    `ms_msg_time` DateTime64(3),
    `withdraw_ms_time` DateTime64(3),
    `abnormal` Array(Int32),
    `excellent` Array(Int32),
    `abnormal_scroe.type` Array(Int32),
    `abnormal_scroe.score` Array(Int32),
    `excellent_score.type` Array(Int32),
    `excellent_score.score` Array(Int32),
    `qc_word_stats.source` Array(Int32),
    `qc_word_stats.word` Array(String),
    `qc_word_stats.count` Array(Int32),
    `wx_rule_stats_ids` Array(String),
    `wx_rule_add_stats_ids` Array(String),
    `day` UInt32
)
ENGINE = MergeTree() 
PARTITION BY day
ORDER BY (platform, channel) 
SETTINGS storage_policy = 'rr',index_granularity = 8192


CREATE TABLE buffer.message_buffer
AS ods.message_all
ENGINE = Buffer('ods', 'message_all', 16, 5, 10, 81920, 409600, 16777216, 67108864)