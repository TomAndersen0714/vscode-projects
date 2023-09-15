CREATE TABLE buffer.customer_pool_summary_platform_buffer (
    `shop_id` String,
    `keep` Int32,
    `outflow` Int32,
    `inflow` Int32,
    `update_time` DateTime,
    `day` Int32,
    `platform` String
) ENGINE = Buffer(
    'app_fishpond',
    'customer_pool_summary_platform_all',
    16,
    10,
    15,
    81920,
    409600,
    67108864,
    134217728
)