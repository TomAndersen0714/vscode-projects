CREATE TABLE xqc_dws.xplat_shop_stat_local (
    `day` Int32,
    `platform` String,
    `company_id` String,
    `company_name` String,
    `company_short_name` String,
    `shop_id` String,
    `shop_name` String,
    `seller_nick` String,
    `dialog_cnt` Int64,
    `cnick_uv` Int64
) ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
) PARTITION BY (day, platform)
ORDER BY seller_nick SETTINGS index_granularity = 8192,
    storage_policy = 'rr'