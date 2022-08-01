CREATE DATABASE IF NOT EXISTS xqc_dws ON CLUSTER cluster_3s_2r
ENGINE=Ordinary

-- DROP TABLE xqc_dws.alert_tag_stat_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE xqc_dws.alert_tag_stat_local ON CLUSTER cluster_3s_2r
(
    `day` Int32,
    `company_id` String,
    `company_name` String,
    `platform` String,
    `shop_id` String,
    `shop_name` String,
    `seller_nick` String,
    `level` Int64,
    `is_finished` String,
    `warning_tag_id` String,
    `warning_tag_name` String,
    `warning_cnt` Int64,
    `alert_elapsed_time` Int64,
    `finish_elapsed_time` Int64
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
ORDER BY (platform, company_id, day)
SETTINGS index_granularity = 8192, storage_policy = 'rr'


-- DROP TABLE xqc_dws.alert_tag_stat_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE xqc_dws.alert_tag_stat_all ON CLUSTER cluster_3s_2r
AS xqc_dws.alert_tag_stat_local
ENGINE = Distributed('cluster_3s_2r', 'xqc_dws', 'alert_tag_stat_local', rand())