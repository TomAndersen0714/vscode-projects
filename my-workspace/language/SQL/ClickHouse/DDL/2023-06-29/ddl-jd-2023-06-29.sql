CREATE DATABASE IF NOT EXISTS ods ON CLUSTER cluster_3s_2r
ENGINE=Ordinary;

-- DROP TABLE ods.pdd_order_event_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE IF NOT EXISTS ods.pdd_order_event_local ON CLUSTER cluster_3s_2r
(
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
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
PARTITION BY day
PRIMARY KEY (shop_id, status, buyer_nick, order_id)
ORDER BY (shop_id, status, buyer_nick, order_id)
SETTINGS index_granularity = 8192, storage_policy = 'rr';


-- DROP TABLE ods.pdd_order_event_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE IF NOT EXISTS ods.pdd_order_event_all ON CLUSTER cluster_3s_2r
AS ods.pdd_order_event_local
ENGINE = Distributed('cluster_3s_2r', 'ods', 'pdd_order_event_local', rand());


CREATE DATABASE IF NOT EXISTS buffer ON CLUSTER cluster_3s_2r
ENGINE=Ordinary;

-- DROP TABLE buffer.ods_pdd_order_event_buffer ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE IF NOT EXISTS buffer.ods_pdd_order_event_buffer ON CLUSTER cluster_3s_2r
AS ods.pdd_order_event_all
ENGINE = Buffer('ods', 'pdd_order_event_all', 16, 15, 35, 81920, 409600, 16777216, 67108864);