-- ClickHouse分词表
-- cut_word_qid 表,自动分词
DROP TABLE IF EXISTS ods.cut_word_qid_local ON CLUSTER cluster_3s_2r
CREATE TABLE IF NOT EXISTS ods.cut_word_qid_local ON CLUSTER cluster_3s_2r (
    `_id` String,
    `update_time` String,
    `ignore` String,
    `qid` String,
    `sid` String,
    `question` String,
    `subcategory_id` String,
    `include_speech` Array(String)
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/ods/tables/{layer}_{shard}/cut_word_qid_local',
    '{replica}'
)
ORDER BY (
    subcategory_id,
    qid,
    sid,
    _id
) 
SETTINGS index_granularity = 8192
-- 
DROP TABLE IF EXISTS ods.cut_word_qid_all ON CLUSTER cluster_3s_2r
CREATE TABLE ods.cut_word_qid_all ON CLUSTER cluster_3s_2r AS ods.cut_word_qid_local
ENGINE = Distributed(
    'cluster_3s_2r',
    'ods',
    'cut_word_qid_local',
    rand()
)

-- cut_word_personal 表,自定义分词
DROP TABLE IF EXISTS ods.cut_word_personal_local ON CLUSTER cluster_3s_2r
CREATE TABLE IF NOT EXISTS ods.cut_word_personal_local ON CLUSTER cluster_3s_2r (
    `_id` String,
    `seller_nick` String,
    `include_words` Array(String),
    `update_time` String
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/ods/tables/{layer}_{shard}/cut_word_personal_local',
    '{replica}'
)
ORDER BY (`_id`) 
SETTINGS index_granularity = 8192
-- 
DROP TABLE IF EXISTS ods.cut_word_personal_all ON CLUSTER cluster_3s_2r
CREATE TABLE IF NOT EXISTS ods.cut_word_personal_all ON CLUSTER cluster_3s_2r AS ods.cut_word_personal_local
ENGINE = Distributed(
    'cluster_3s_2r',
    'ods',
    'cut_word_personal_local',
    rand()
)

-- cut_word_dict 表,获取分词词典
DROP TABLE IF EXISTS ods.cut_word_dict_local ON CLUSTER cluster_3s_2r
CREATE TABLE IF NOT EXISTS ods.cut_word_dict_local ON CLUSTER cluster_3s_2r (
    `_id` String,
    `word` String,
    `code` String,
    `name` String,
    `update_time` String
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/ods/tables/{layer}_{shard}/cut_word_dict_local',
    '{replica}'
)
ORDER BY (
    `_id`
) 
SETTINGS index_granularity = 8192
-- 
DROP TABLE IF EXISTS ods.cut_word_dict_all ON CLUSTER cluster_3s_2r
CREATE TABLE IF NOT EXISTS ods.cut_word_dict_all ON CLUSTER cluster_3s_2r AS ods.cut_word_dict_local
ENGINE = Distributed(
    'cluster_3s_2r',
    'ods',
    'cut_word_dict_local',
    rand()
)