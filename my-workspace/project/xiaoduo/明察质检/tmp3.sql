CREATE TABLE IF NOT EXISTS xqc_dim.company_version_local ON CLUSTER cluster_3s_2r (
    `company_id` String,
    `company_short_name` String,
    `tenant_id` String,
    `version_code` String,
    `version_name` String,
    `day` Int32
) ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
) PARTITION BY day
ORDER BY
    company_id SETTINGS index_granularity = 8192;