CREATE DATABASE IF NOT EXISTS dwd ON CLUSTER cluster_3s_2r
ENGINE=Ordinary;

-- DROP TABLE dwd.voc_buyer_latest_order_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE IF NOT EXISTS dwd.voc_buyer_latest_order_local ON CLUSTER cluster_3s_2r
(
    `day` UInt32,
    `platform` String,
    `shop_id` String,
    `buyer_nick` String,
    `real_buyer_nick` String,
    `order_id` String,
    `order_timestamps` Array(UInt64),
    `order_statuses` Array(String)
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
PARTITION BY day
ORDER BY (platform, shop_id, buyer_nick)
SETTINGS index_granularity = 8192, storage_policy = 'rr';


-- DROP TABLE dwd.voc_buyer_latest_order_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE IF NOT EXISTS dwd.voc_buyer_latest_order_all ON CLUSTER cluster_3s_2r
AS dwd.voc_buyer_latest_order_local
ENGINE = Distributed('cluster_3s_2r', 'dwd', 'voc_buyer_latest_order_local', rand());


CREATE DATABASE IF NOT EXISTS buffer ON CLUSTER cluster_3s_2r
ENGINE=Ordinary;


-- DROP TABLE buffer.dwd_voc_buyer_latest_order_buffer ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE IF NOT EXISTS buffer.dwd_voc_buyer_latest_order_buffer ON CLUSTER cluster_3s_2r
AS dwd.voc_buyer_latest_order_all
ENGINE = Buffer('dwd', 'voc_buyer_latest_order_all', 16, 15, 35, 81920, 409600, 16777216, 67108864);