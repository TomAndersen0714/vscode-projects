CREATE TABLE xqc_dim.snick_full_info_local (
    `company_id` String,
    `company_name` String,
    `company_short_name` String,
    `platform` String,
    `shop_id` String,
    `shop_name` String,
    `seller_nick` String,
    `department_id` String,
    `department_name` String,
    `snick` String,
    `employee_id` String,
    `employee_name` String,
    `superior_id` String,
    `superior_name` String,
    `day` Int32
) ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
PARTITION BY day
ORDER BY (company_id, platform) SETTINGS index_granularity = 8192,
    storage_policy = 'rr'