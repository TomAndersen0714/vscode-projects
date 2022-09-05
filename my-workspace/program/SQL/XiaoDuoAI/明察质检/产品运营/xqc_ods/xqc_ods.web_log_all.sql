CREATE DATABASE IF NOT EXISTS xqc_ods ON CLUSTER cluster_3s_2r

-- DROP TABLE xqc_ods.web_log_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE xqc_ods.web_log_local ON CLUSTER cluster_3s_2r
(
    `app_id` String,
    `distinct_id` String,
    `track_id` String,
    `platform` String,
    `channel` String,
    `type` String,
    `event` String,
    `url` String,
    `url_path` String,
    `title` String,
    `properties` String,
    `create_time` String,
    `day` Int32
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
PARTITION BY day
ORDER BY (platform)
SETTINGS index_granularity = 8192, storage_policy = 'rr'


-- DROP TABLE xqc_ods.web_log_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE xqc_ods.web_log_all ON CLUSTER cluster_3s_2r
AS xqc_ods.web_log_local
ENGINE = Distributed('cluster_3s_2r', 'xqc_ods', 'web_log_local', rand())

-- DROP TABLE buffer.xqc_ods_web_log_buffer ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE buffer.xqc_ods_web_log_buffer ON CLUSTER cluster_3s_2r
AS xqc_ods.web_log_all
ENGINE = Buffer('xqc_ods', 'web_log_all', 16, 15, 35, 81920, 409600, 16777216, 67108864)