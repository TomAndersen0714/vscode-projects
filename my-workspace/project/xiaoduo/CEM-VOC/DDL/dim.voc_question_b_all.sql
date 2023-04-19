CREATE DATABASE IF NOT EXISTS dim ON CLUSTER cluster_3s_2r
ENGINE=Ordinary;

-- DROP TABLE dim.voc_question_b_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE IF NOT EXISTS dim.voc_question_b_local ON CLUSTER cluster_3s_2r
(
    `_id` String,
    `create_time` Int64,
    `update_time` Int64,
    `company_id` String,
    `name` String,
    `level` Int32,
    `group_id` String
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
ORDER BY company_id
SETTINGS index_granularity = 8192, storage_policy = 'rr';


-- DROP TABLE dim.voc_question_b_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE IF NOT EXISTS dim.voc_question_b_all ON CLUSTER cluster_3s_2r
AS dim.voc_question_b_local
ENGINE = Distributed('cluster_3s_2r', 'dim', 'voc_question_b_local', rand());