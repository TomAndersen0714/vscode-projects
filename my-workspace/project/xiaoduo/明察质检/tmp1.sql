CREATE TABLE ods.order_event_all (
    `order_id` String,
    `shop_id` String,
    `buyer_nick` String,
    `real_buyer_nick` String,
    `payment` Float64,
    `status` String,
    `time` DateTime('Asia/Shanghai'),
    `plat_goods_ids` Array(String),
    `step_trade_status` String,
    `step_paid_fee` Float64,
    `day` Int32
) ENGINE = Distributed(
    'cluster_3s_2r',
    'ods',
    'order_event_bak_local',
    xxHash64(shop_id, status, buyer_nick, order_id)
)

CREATE TABLE ods.order_event_bak_local (
    `order_id` String,
    `shop_id` String,
    `buyer_nick` String,
    `real_buyer_nick` String,
    `payment` Float64,
    `status` String,
    `time` DateTime('Asia/Shanghai'),
    `plat_goods_ids` Array(String),
    `step_trade_status` String,
    `step_paid_fee` Float64,
    `day` Int32
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/ods/tables/{layer}_{shard}/order_event_bak_local',
    '{replica}'
)
PARTITION BY day
PRIMARY KEY (shop_id, status, buyer_nick, order_id)
ORDER BY (shop_id, status, buyer_nick, order_id) SETTINGS index_granularity = 8192,
    storage_policy = 'rr'