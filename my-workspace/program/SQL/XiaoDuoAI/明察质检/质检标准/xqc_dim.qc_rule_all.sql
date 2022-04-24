CREATE TABLE tmp.qc_rule_local ON CLUSTER cluster_3s_2r
(
    `_id` String,
    `create_time` String,
    `update_time` String,
    `company_id` String,
    `platform` String,
    `create_account` String,
    `update_account` String,
    `qc_norm_id` String,
    `qc_norm_group_id` String,
    `template_id` String,
    `name` String,
    `seller_nick` String,
    `rule_category` Int32,
    `rule_type` Int32,
    `settings` String,
    `check` String,
    `check_target` Int32,
    `alert_level` Int32,
    `notify_way` Int32,
    `notify_target` Int32,
    `score` Int32,
    `threshold` Float64,
    `special_settings` String,
    `status` Int32
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
ORDER BY company_id
SETTINGS storage_policy = 'rr', index_granularity = 8192


-- tmp.qc_rule_all
CREATE TABLE tmp.qc_rule_all ON CLUSTER cluster_3s_2r
AS tmp.qc_rule_local
ENGINE = Distributed('cluster_3s_2r', 'tmp', 'qc_rule_local', rand())


-- xqc_dim.qc_rule_local
CREATE TABLE xqc_dim.qc_rule_local ON CLUSTER cluster_3s_2r
(
    `_id` String,
    `create_time` String,
    `update_time` String,
    `company_id` String,
    `platform` String,
    `create_account` String,
    `update_account` String,
    `qc_norm_id` String,
    `qc_norm_group_id` String,
    `template_id` String,
    `name` String,
    `seller_nick` String,
    `rule_category` Int32,
    `rule_type` Int32,
    `settings` String,
    `check` String,
    `check_target` Int32,
    `alert_level` Int32,
    `notify_way` Int32,
    `notify_target` Int32,
    `score` Int32,
    `threshold` Float64,
    `special_settings` String,
    `status` Int32,
    `day` Int32
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
PARTITION BY day
ORDER BY company_id
SETTINGS storage_policy = 'rr', index_granularity = 8192


-- xqc_dim.qc_rule_all
CREATE TABLE xqc_dim.qc_rule_all ON CLUSTER cluster_3s_2r
AS xqc_dim.qc_rule_local
ENGINE = Distributed('cluster_3s_2r', 'xqc_dim', 'qc_rule_local', rand())