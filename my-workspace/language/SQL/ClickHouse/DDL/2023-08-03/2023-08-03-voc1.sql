-- DROP TABLE IF EXISTS ods.voc_customer_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE IF NOT EXISTS ods.voc_customer_local ON CLUSTER cluster_3s_2r
(
    `_id` String,
    `create_time` Int64,
    `update_time` Int64,
    `company_id` String,
    `shop_id` String,
    `platform` String,
    `seller_nick` String,
    `cnick` String,
    `real_buyer_nick` String,
    `reception_time` Int64,
    `tags` Array(Int64),
    `order_status` Int64,
    `dialog_info_dialog_ids` Array(String),
    `dialog_info_begin_times` Array(Int64),
    `dialog_info_goods_ids` Array(String),
    `dialog_info_snicks` Array(String),
    `day` Int64
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
PARTITION BY day
ORDER BY (company_id, shop_id)
SETTINGS index_granularity = 8192, storage_policy = 'rr';


-- DROP TABLE IF EXISTS ods.voc_customer_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE IF NOT EXISTS ods.voc_customer_all ON CLUSTER cluster_3s_2r
AS ods.voc_customer_local
ENGINE = Distributed('cluster_3s_2r', 'ods', 'voc_customer_local', rand());



-- DROP TABLE IF EXISTS dws.voc_customer_stat_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE IF NOT EXISTS dws.voc_customer_stat_local ON CLUSTER cluster_3s_2r
(
    `day` Int64,
    `company_id` String,
    `shop_id` String,
    `platform` String,
    `tag` Int64,
    `order_status` String,
    `goods_id` String,
    `cnick_id_bitmap` AggregateFunction(groupBitmap, UInt64)
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
PARTITION BY day
ORDER BY (company_id, shop_id)
SETTINGS index_granularity = 8192, storage_policy = 'rr';


-- DROP TABLE IF EXISTS dws.voc_customer_stat_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE IF NOT EXISTS dws.voc_customer_stat_all ON CLUSTER cluster_3s_2r
AS dws.voc_customer_stat_local
ENGINE = Distributed('cluster_3s_2r', 'dws', 'voc_customer_stat_local', rand());


-- DROP TABLE xqc_dim.goods_local ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE IF NOT EXISTS xqc_dim.goods_local ON CLUSTER cluster_3s_2r
(
    `id` String,
    `create_time` Int64,
    `update_time` Int64,
    `company_id` String,
    `shop_id` String,
    `platform` String,
    `name` String,
    `goods_id` String,
    `goods_url` String,
    `goods_img` String,
    `price` Float64,
    `status` Int64,
    `tags` Array(String),
    `added_time` Int64
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/{database}/tables/{layer}_{shard}/{table}',
    '{replica}'
)
ORDER BY (company_id, shop_id)
SETTINGS index_granularity = 8192, storage_policy = 'rr';


-- DROP TABLE xqc_dim.goods_all ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE IF NOT EXISTS xqc_dim.goods_all ON CLUSTER cluster_3s_2r
AS xqc_dim.goods_local
ENGINE = Distributed('cluster_3s_2r', 'xqc_dim', 'goods_local', rand());