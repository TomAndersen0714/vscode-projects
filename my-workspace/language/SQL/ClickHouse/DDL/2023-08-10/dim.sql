CREATE DATABASE IF NOT EXISTS app_en ON CLUSTER cluster_3s_2r
ENGINE=Ordinary;

-- DROP TABLE app_en.customize_version_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE IF NOT EXISTS app_en.customize_version_local ON CLUSTER cluster_3s_2r
(
    `tenant_type` String,
    `tenant_id` String,
    `product_key` String,
    `cycle_start_time` String,
    `cycle_end_time` String,
    `created_time` String,
    `deleted_time` String,
    `updated_time` String,
    `version_code` String
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
ORDER BY (tenant_type, product_key)
SETTINGS index_granularity = 8192, storage_policy = 'rr';


-- DROP TABLE app_en.customize_version_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE IF NOT EXISTS app_en.customize_version_all ON CLUSTER cluster_3s_2r
AS app_en.customize_version_local
ENGINE = Distributed('cluster_3s_2r', 'app_en', 'customize_version_local', rand());
