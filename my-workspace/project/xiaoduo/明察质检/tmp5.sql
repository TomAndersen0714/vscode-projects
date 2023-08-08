CREATE TABLE dws.di_health_pred_target_customer_local (
    `customer_name` String,
    `customer_id` String,
    `tonnage_level` String,
    `order_id` String,
    `order_no` String,
    `contract_start_date` String,
    `real_contract_end_date` String,
    `service_end_date` String,
    `sq_delivery_time` Int64,
    `shop_id` String,
    `plat_user_id` String,
    `platform` String,
    `has_shouqian` String
) ENGINE = ReplicatedMergeTree(
    '/clickhouse/dws/tables/{layer}_{shard}/di_health_pred_target_customer_local',
    '{replica}'
)
ORDER BY order_id SETTINGS index_granularity = 8192,
    storage_policy = 'rr'