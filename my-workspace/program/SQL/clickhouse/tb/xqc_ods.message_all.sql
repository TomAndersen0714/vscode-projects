CREATE TABLE xqc_ods.message_local ON CLUSTER cluster_3s_2r(
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
    `is_transfer` String,
    `ms_msg_time` DateTime64(3),
    `withdraw_ms_time` DateTime64(3),
    `rule_stats.id` Array(String),
    `rule_stats.count` Array(Int32),
    `rule_stats.score` Array(Int32),
    `rule_add_stats.id` Array(String),
    `rule_add_stats.count` Array(Int32),
    `rule_add_stats.score` Array(Int32),
    `wx_rule_stats.id` Array(String),
    `wx_rule_stats.count` Array(Int32),
    `wx_rule_stats.score` Array(Int32),
    `wx_rule_add_stats.id` Array(String),
    `wx_rule_add_stats.count` Array(Int32),
    `wx_rule_add_stats.score` Array(Int32),
    `day` UInt32
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/xqc_ods/tables/{layer}_{shard}/message_local',
    '{replica}'
)
PARTITION BY `day`
ORDER BY (`platform`, `seller_nick`,`snick`) 
SETTINGS storage_policy = 'rr',index_granularity = 8192


CREATE TABLE xqc_ods.message_all ON CLUSTER cluster_3s_2r
AS xqc_ods.message_local
ENGINE = Distributed(
    'cluster_3s_2r','xqc_ods','message_local',rand()
)


CREATE TABLE buffer.xqc_message_buffer ON CLUSTER cluster_3s_2r
AS xqc_ods.message_all
ENGINE = Buffer('xqc_ods', 'message_all', 16, 5, 10, 81920, 409600, 16777216, 67108864)

ALTER TABLE xqc_ods.message_local ON CLUSTER cluster_3s_2r 
ALTER TABLE tmp.truncate_test_local_1 ON CLUSTER cluster_3s_2r MODIFY ORDER BY (`a`,`b`)


ALTER TABLE tmp.truncate_test_local_1 ON CLUSTER cluster_3s_2r 
ADD INDEX minmax_index (`a`,'b') TYPE minmax GRANULARITY 8192

ALTER TABLE tmp.truncate_test_local_1 ON CLUSTER cluster_3s_2r DROP INDEX new_index
ALTER TABLE tmp.truncate_test_local_1 ON CLUSTER cluster_3s_2r DROP INDEX minmax_index


ALTER TABLE xqc_ods.message_all ADD INDEX dialog_id_index (`dialog_id`) TYPE minmax GRANULARITY 8192