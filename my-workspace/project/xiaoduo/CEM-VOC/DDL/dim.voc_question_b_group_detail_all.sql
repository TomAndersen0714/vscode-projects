CREATE DATABASE IF NOT EXISTS dim ON CLUSTER cluster_3s_2r
ENGINE=Ordinary;

-- DROP TABLE dim.voc_question_b_group_detail_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE IF NOT EXISTS dim.voc_question_b_group_detail_local ON CLUSTER cluster_3s_2r
(
    `company_id` String,
    `group_id` String,
    `group_name` String,
    `group_level` Int32,
    `parent_group_id` String,
    `parent_group_name` String,
    `first_group_id` String,
    `first_group_name` String,
    `second_group_id` String,
    `second_group_name` String,
    `third_group_id` String,
    `third_group_name` String,
    `fourth_group_id` String,
    `fourth_group_name` String,
    `create_time` Int64,
    `update_time` Int64
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
ORDER BY (company_id, group_id)
SETTINGS index_granularity = 8192, storage_policy = 'rr';


-- DROP TABLE dim.voc_question_b_group_detail_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE IF NOT EXISTS dim.voc_question_b_group_detail_all ON CLUSTER cluster_3s_2r
AS dim.voc_question_b_group_detail_local
ENGINE = Distributed('cluster_3s_2r', 'dim', 'voc_question_b_group_detail_local', rand());