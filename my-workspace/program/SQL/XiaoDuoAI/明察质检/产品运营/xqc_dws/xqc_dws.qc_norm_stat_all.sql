CREATE DATABASE IF NOT EXISTS xqc_dws ON CLUSTER cluster_3s_2r
ENGINE=Ordinary

-- DROP TABLE xqc_dws.qc_norm_stat_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE xqc_dws.qc_norm_stat_local ON CLUSTER cluster_3s_2r
(
    `day` Int32,
    `company_id` String,
    `company_name` String,
    `platform` String,
    `shop_id` String,
    `shop_name` String,
    `seller_nick` String,
    `qc_norm_group_id` String,
    `qc_norm_group_name` String,
    `qc_norm_group_full_name` String,
    `qc_norm_id` String,
    `qc_norm_name` String,
    `qc_rule_id` String,
    `qc_rule_name` String,
    `rule_category` Int32,
    `rule_type` Int32,
    `is_check` String,
    `status` String,
    `alert_level` Int32,
    `notify_way` Int32,
    `notify_target` Int32,
    `trigger_cnt` Int64
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
ORDER BY (platform, company_id, day)
SETTINGS index_granularity = 8192, storage_policy = 'rr'


-- DROP TABLE xqc_dws.qc_norm_stat_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE xqc_dws.qc_norm_stat_all ON CLUSTER cluster_3s_2r
AS xqc_dws.qc_norm_stat_local
ENGINE = Distributed('cluster_3s_2r', 'xqc_dws', 'qc_norm_stat_local', rand())