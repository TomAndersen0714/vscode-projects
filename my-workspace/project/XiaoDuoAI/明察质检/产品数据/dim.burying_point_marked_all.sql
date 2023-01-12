-- 数据源
mongodb://10.20.131.195:27017 growth.marked_data

-- 本地表
CREATE TABLE dim.burying_point_marked_local ON CLUSTER cluster_3s_2r
(
    `_id` String,
    `app_id` String,
    `event` String,
    `url` String,
    `match_type` String,
    `marked_content` String,
    `element_selector` String,
    `element_content` String
)
ENGINE = ReplicatedMergeTree('/clickhouse/{database}/tables/{layer}_{shard}/{table}', '{replica}')
ORDER BY (`app_id`, `event`)
SETTINGS storage_policy = 'rr', index_granularity = 8192

-- 分布式表
CREATE TABLE dim.burying_point_marked_all ON CLUSTER cluster_3s_2r
AS dim.burying_point_marked_local
ENGINE = Distributed('cluster_3s_2r', 'dim', 'burying_point_marked_local', rand())