DROP TABLE app_mp.focus_item_stat_v2_local NO DELAY
CREATE TABLE app_mp.focus_item_stat_v2_local
(
    `shop_name` String,
    `shop_id` String,
    `focus_item` String,
    `focus_item_name` String,
    `recommend_times` Int64,
    `order_count` Int64,
    `paid_count` Int64,
    `conversion_rate` Float,
    `day` Int32,
    `node_type` String,
    `focus_item_url` String
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/app_mp/tables/{layer}_{shard}/focus_item_stat_v2_local',
    '{replica}'
) PARTITION BY day
ORDER BY (day, shop_id)
SETTINGS index_granularity = 8192, storage_policy = 'rr';


DROP TABLE app_mp.recommend_goods_stat_v2_local NO DELAY
CREATE TABLE app_mp.recommend_goods_stat_v2_local 
(
    `shop_name` String,
    `shop_id` String,
    `recommend_goods_id` String,
    `recommend_goods_name` String,
    `recommend_times` Int64,
    `order_count` Int64,
    `paid_count` Int64,
    `conversion_rate` Float,
    `day` Int32,
    `node_type` String,
    `recommend_goods_url` String
) ENGINE = ReplicatedMergeTree(
    '/clickhouse/app_mp/tables/{layer}_{shard}/recommend_goods_stat_v2_local',
    '{replica}'
) PARTITION BY day
ORDER BY (day, shop_id)
SETTINGS index_granularity = 8192, storage_policy = 'rr';


DROP TABLE ods.recommend_order NO DELAY
CREATE TABLE ods.recommend_order (
    `shop_id` String,
    `buyer_nick` String,
    `tid` String,
    `iid` String,
    `modified` String,
    `payment` Int64,
    `day` Int32
) ENGINE = ReplicatedMergeTree(
    '/clickhouse/ods/tables/{layer}_{shard}/recommend_order',
    '{replica}'
) PARTITION BY day PRIMARY KEY (shop_id, buyer_nick)
ORDER BY (shop_id, buyer_nick)
SETTINGS index_granularity = 8192;