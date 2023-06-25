-- DROP TABLE tmp.xqc_dim_xqc_company_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE IF NOT EXISTS tmp.xqc_dim_xqc_company_local ON CLUSTER cluster_3s_2r
(
    `_id` String,
    `create_time` String,
    `update_time` String,
    `name` String,
    `shot_name` String,
    `logo` String,
    `url` String,
    `default_platform` String,
    `platforms` String,
    `pri_center_id` String,
    `expired_time` String,
    `downgrade_strategy` Int64,
    `need_init` String,
    `white_list` String
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
PARTITION BY day
ORDER BY (shop_id, snick)
SETTINGS index_granularity = 8192, storage_policy = 'rr';


-- DROP TABLE tmp.xqc_dim_xqc_company_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE IF NOT EXISTS tmp.xqc_dim_xqc_company_all ON CLUSTER cluster_3s_2r
AS tmp.xqc_dim_xqc_company_local
ENGINE = Distributed('cluster_3s_2r', 'tmp', 'xqc_dim_xqc_company_local', rand());